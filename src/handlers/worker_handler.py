import logging

import tornado.ioloop
from bson import ObjectId
from tornado_json import schema

import src.constants.api_keys as api_key
import src.constants.db_keys as db_key
from src.auth import authenticate, authenticate_worker
from src.config import HEARTBEAT_INTERVAL, QUEUE_EMPTY_CODE
from src.utilities import get_time
from src.handlers.base_handler import BaseAPIHandler

logger = logging.getLogger()

# constants
grading_job_def = {
    "type": "object",
    "properties": {
        api_key.IMAGE: {"type": "string"},
        api_key.ENV: {"type": "array", "items": {"type": "string"}},
        api_key.ENTRY_POINT: {"type": "array", "items": {"type": "string"}},
        api_key.NETWORKING: {"type": "boolean"},
        api_key.HOST_NAME: {"type": "string"},
        api_key.TIMEOUT: {"type": "number"}
    },
    "required": [api_key.IMAGE],
    "additionalProperties": False
}


class WorkerRegisterHandler(BaseAPIHandler):
    @authenticate
    @schema.validate(
        output_schema={
            "type": "object",
            "properties": {
                api_key.WORKER_ID: {"type": "string"},
                api_key.HEARTBEAT: {"type": "number"}
            },
            "required": [api_key.WORKER_ID],
            "additionalProperties": False
        }
    )
    def get(self, hostname):
        db_resolver = self.get_db()
        worker_nodes_collection = db_resolver.get_worker_node_collection()

        worker_node = {db_key.WORKER_HOSTNAME: hostname, db_key.LAST_SEEN: get_time(), db_key.RUNNING_JOB: None}
        worker_id = str(worker_nodes_collection.insert_one(worker_node).inserted_id)
        logger.info("Worker with hostname {} joined as id {}".format(hostname, worker_id))

        return {api_key.WORKER_ID: worker_id, api_key.HEARTBEAT: HEARTBEAT_INTERVAL}


class GetGradingJobHandler(BaseAPIHandler):
    @authenticate
    @authenticate_worker
    @schema.validate(
        output_schema={
            "type": "object",
            "properties": {
                api_key.JOB_ID: {"type": "string"},
                api_key.STAGES: {
                    "type": "array",
                    "items": grading_job_def,
                },
                api_key.STUDENTS: {
                    "type": "array",
                    "items": {"type": "object"},
                }
            },
            "required": [api_key.JOB_ID, api_key.STAGES],
            "additionalProperties": False
        }
    )
    def get(self):
        db_resolver = self.get_db()
        job_queue = self.get_queue()
        worker_id = self.request.headers.get(api_key.WORKER_ID)

        try:
            job = job_queue.get_nowait()
            job_id = job.get(api_key.JOB_ID)

            db_resolver.get_grading_job_collection().update_one({db_key.ID: ObjectId(job_id)},
                                                                {"$set": {db_key.STARTED: get_time()}})

            db_resolver.get_worker_node_collection().update_one({db_key.ID: ObjectId(worker_id)},
                                                                {"$set": {db_key.RUNNING_JOB: job_id}})

            return job
        except Exception as e:
            self.set_status(QUEUE_EMPTY_CODE)
            return {api_key.JOB_ID: 'no id', api_key.STAGES: []}


class UpdateGradingJobHandler(BaseAPIHandler):
    def job_update_callback(self, job_id, grading_run_id):
        db_handler = self.get_db()
        grading_run_collection = db_handler.get_grading_run_collection()

        # update grading run: if last job finished then update finished_at. Update student_jobs_left if student job.
        # enqueue post processing if all student jobs finished

        grading_run = self.get_grading_run(grading_run_id)
        job_succeeded = self.body.get(api_key.SUCCESS)
        assert grading_run is not None
        assert db_key.CREATED in grading_run
        assert db_key.STARTED in grading_run
        assert db_key.FINISHED not in grading_run

        if grading_run.get(db_key.PRE_PROCESSING, "") == job_id:
            # pre processing job finished
            if job_succeeded:
                self.enqueue_student_jobs(grading_run)
            else:
                grading_run_collection.update_one({db_key.ID: ObjectId(grading_run_id)},
                                                  {"$set": {db_key.SUCCESS: False, db_key.FINISHED: get_time()}})

        elif grading_run.get(db_key.POST_PROCESSING, "") == job_id:
            # post processing job finished so the grading run is over
            assert grading_run.get(db_key.STUDENT_JOBS_LEFT) == 0
            grading_run_collection.update_one({db_key.ID: ObjectId(grading_run_id)},
                                              {"$set": {db_key.SUCCESS: job_succeeded, db_key.FINISHED: get_time()}})

        else:
            # a student's job finished
            assert grading_run.get(db_key.STUDENT_JOBS_LEFT) > 0
            grading_run_collection.update_one({db_key.ID: ObjectId(grading_run_id)},
                                              {"$inc": {db_key.STUDENT_JOBS_LEFT: -1}})

            if grading_run[db_key.STUDENT_JOBS_LEFT] == 1:
                # this was the last student job which finished so if post processing exists then schedule it
                if db_key.POST_PROCESSING in grading_run:
                    self.enqueue_job(grading_run.get(db_key.POST_PROCESSING), grading_run.get(db_key.STUDENTS))
                else:
                    grading_run_collection.update_one({db_key.ID: ObjectId(grading_run_id)},
                                                      {"$set": {db_key.SUCCESS: True, db_key.FINISHED: get_time()}})

    @authenticate
    @authenticate_worker
    @schema.validate(
        input_schema={
            "type": "object",
            "properties": {
                api_key.SUCCESS: {"type": "boolean"},
                api_key.RESULTS: {
                    "type": "array",
                    "items": {"type": "object"},
                },
                api_key.LOGS: {"type": "object"}
            },
            "required": [api_key.SUCCESS, api_key.RESULTS, api_key.LOGS],
            "additionalProperties": False
        }
    )
    def post(self, job_id):
        db_handler = self.get_db()
        worker_nodes_collection = db_handler.get_worker_node_collection()
        jobs_collection = db_handler.get_grading_job_collection()
        job_logs_collection = db_handler.get_job_logs_collection()

        # update worker node: remove this job from its currently running jobs
        worker_id = self.request.headers.get(api_key.WORKER_ID)
        worker_nodes_collection.update_one({db_key.ID: ObjectId(worker_id)},
                                           {"$set": {db_key.RUNNING_JOB: None}})

        # check if the job exists
        job = self.get_grading_job(job_id)
        if job is None:
            return

        # update job: finished_at and result
        assert db_key.CREATED in job
        assert db_key.QUEUED in job
        assert db_key.STARTED in job
        assert db_key.FINISHED not in job
        job_succeeded = self.body.get(api_key.SUCCESS)
        jobs_collection.update_one({db_key.ID: ObjectId(job_id)}, {
            "$set": {db_key.FINISHED: get_time(), db_key.RESULTS: self.body.get(api_key.RESULTS),
                     db_key.SUCCESS: job_succeeded}})

        # save logs in the logs DB along with the job id to identify it
        job_logs_collection.insert_one({db_key.JOB_ID: job_id, **self.body.get(api_key.LOGS)})

        # thread safe callback
        tornado.ioloop.IOLoop.current().add_callback(self.job_update_callback, job_id, job.get(db_key.GRADING_RUN))


class HeartBeatHandler(BaseAPIHandler):
    @authenticate
    @authenticate_worker
    def post(self):
        db_handler = self.get_db()
        worker_nodes_collection = db_handler.get_worker_node_collection()
        worker_id = self.request.headers.get(api_key.WORKER_ID)

        worker_nodes_collection.update_one({db_key.ID: ObjectId(worker_id)}, {"$set": {db_key.LAST_SEEN: get_time()}})
