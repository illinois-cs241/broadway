import logging
from queue import Queue

import tornado.ioloop
from bson import ObjectId
from tornado_json import schema
from tornado_json.requesthandlers import APIHandler

import src.constants.api_keys as api_key
import src.constants.constants as consts
import src.constants.db_keys as db_key
from src.auth import authenticate, authenticate_worker, validate_id
from src.config import BAD_REQUEST_CODE, HEARTBEAT_INTERVAL, QUEUE_EMPTY_CODE
from src.database import DatabaseResolver
from src.utilities import get_time, resolve_env_vars

logger = logging.getLogger()

# constants
grading_stage_def = {
    "type": "object",
    "properties": {
        api_key.IMAGE: {"type": "string"},
        api_key.ENV: {"type": "object"},
        api_key.ENTRY_POINT: {"type": "array", "items": {"type": "string"}},
        api_key.NETWORKING: {"type": "boolean"},
        api_key.HOST_NAME: {"type": "string"},
        api_key.TIMEOUT: {"type": "number"}
    },
    "required": [api_key.IMAGE],
    "additionalProperties": False
}

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


class BaseAPIHandler(APIHandler):
    def abort(self, data, status):
        self.set_status(status)
        self.fail(data)

    def get_db(self):
        # type: () -> DatabaseResolver
        return self.settings.get(consts.APP_DB)

    def get_queue(self):
        # type: () -> Queue
        return self.settings.get(consts.APP_QUEUE)

    def get_cluster_token(self):
        # type: () -> str
        return self.settings.get(consts.APP_TOKEN)

    @validate_id
    def get_worker_node(self, id_):
        db_resolver = self.get_db()
        worker_node = db_resolver.get_worker_node_collection().find_one({db_key.ID: ObjectId(id_)})
        if worker_node is None:
            self.abort({"message": "Worker node with id {} does not exist".format(id_)}, BAD_REQUEST_CODE)
        else:
            return worker_node

    @validate_id
    def get_grading_run(self, id_):
        db_resolver = self.get_db()
        grading_run = db_resolver.get_grading_run_collection().find_one({db_key.ID: ObjectId(id_)})
        if grading_run is None:
            self.abort({"message": "Grading run with id {} does not exist".format(id_)}, BAD_REQUEST_CODE)
        else:
            return grading_run

    @validate_id
    def get_grading_job(self, id_):
        db_resolver = self.get_db()
        grading_job = db_resolver.get_grading_job_collection().find_one({db_key.ID: ObjectId(id_)})
        if grading_job is None:
            self.abort({"message": "Grading job with id {} does not exist".format(id_)}, BAD_REQUEST_CODE)
        else:
            return grading_job

    def create_job(self, pipeline_name, student_env_vars=None):
        cur_job = []
        for stage in self.body.get(pipeline_name):
            cur_stage = stage.copy()
            cur_stage[api_key.ENV] = resolve_env_vars(cur_stage.get(api_key.ENV, {}),
                                                      self.body.get(api_key.ENV, {}), student_env_vars)
            cur_job.append(cur_stage)

        return cur_job

    def enqueue_job(self, job_id, students=None):
        db_resolver = self.get_db()
        job_queue = self.get_queue()

        jobs_collection = db_resolver.get_grading_job_collection()
        job = self.get_grading_job(job_id)

        cur_job = {api_key.STAGES: job[db_key.STAGES], api_key.JOB_ID: job_id}
        if students is not None:
            cur_job[api_key.STUDENTS] = students

        job_queue.put(cur_job)
        jobs_collection.update_one({db_key.ID: ObjectId(job_id)}, {"$set": {db_key.QUEUED: get_time()}})

    def enqueue_student_jobs(self, grading_run):
        for student_job_id in grading_run.get(db_key.STUDENT_JOBS):
            self.enqueue_job(student_job_id)


class AddGradingRunHandler(BaseAPIHandler):
    @authenticate
    @schema.validate(
        input_schema={
            "type": "object",
            "properties": {
                api_key.PRE_PROCESSING_PIPELINE: {
                    "type": "array",
                    "items": grading_stage_def,
                },
                api_key.STUDENT_PIPELINE: {
                    "type": "array",
                    "items": grading_stage_def,
                },
                api_key.POST_PROCESSING_PIPELINE: {
                    "type": "array",
                    "items": grading_stage_def,
                },
                api_key.STUDENTS: {
                    "type": "array",
                    "items": {"type": "object"},
                },
                api_key.ENV: {"type": "object"},
            },
            "required": [api_key.STUDENT_PIPELINE, api_key.STUDENTS],
            "additionalProperties": False
        },
        output_schema={
            "type": "object",
            "properties": {
                api_key.RUN_ID: {"type": "string"}
            },
            "required": [api_key.RUN_ID],
            "additionalProperties": False
        }
    )
    def post(self):
        # create grading run in DB
        db_handler = self.get_db()
        grading_runs_collection = db_handler.get_grading_run_collection()

        grading_run = {db_key.CREATED: get_time(), db_key.STUDENTS: self.body.get(api_key.STUDENTS)}
        grading_run_id = str(grading_runs_collection.insert_one(grading_run).inserted_id)

        jobs_collection = db_handler.get_grading_job_collection()

        # create all jobs in DB
        student_job_ids = []
        for student in self.body.get(api_key.STUDENTS):
            job = {db_key.CREATED: get_time(), db_key.GRADING_RUN: grading_run_id,
                   db_key.STAGES: self.create_job(api_key.STUDENT_PIPELINE, student)}

            student_job_ids.append(str(jobs_collection.insert_one(job).inserted_id))

        grading_runs_collection.update_one({db_key.ID: ObjectId(grading_run_id)}, {
            "$set": {db_key.STUDENT_JOBS: student_job_ids, db_key.STUDENT_JOBS_LEFT: len(student_job_ids)}})

        # create pre processing stage if it exists
        if api_key.PRE_PROCESSING_PIPELINE in self.body:
            pre_processing_job = {db_key.CREATED: get_time(), db_key.GRADING_RUN: grading_run_id,
                                  db_key.STAGES: self.create_job(api_key.PRE_PROCESSING_PIPELINE)}
            pre_processing_job_id = str(jobs_collection.insert_one(pre_processing_job).inserted_id)
            grading_runs_collection.update_one({db_key.ID: ObjectId(grading_run_id)},
                                               {"$set": {db_key.PRE_PROCESSING: pre_processing_job_id}})

        # create post processing stage if it exists
        if api_key.POST_PROCESSING_PIPELINE in self.body:
            post_processing_job = {db_key.CREATED: get_time(), db_key.GRADING_RUN: grading_run_id,
                                   db_key.STAGES: self.create_job(api_key.POST_PROCESSING_PIPELINE)}
            post_processing_job_id = str(jobs_collection.insert_one(post_processing_job).inserted_id)
            grading_runs_collection.update_one({db_key.ID: ObjectId(grading_run_id)},
                                               {"$set": {db_key.POST_PROCESSING: post_processing_job_id}})

        # return the run id to user
        return {api_key.RUN_ID: grading_run_id}


class GradingRunHandler(BaseAPIHandler):
    @authenticate
    def post(self, id_):
        db_handler = self.get_db()
        grading_runs_collection = db_handler.get_grading_run_collection()
        grading_run = self.get_grading_run(id_)

        # check to see if grading run has already started
        if db_key.STARTED in grading_run:
            self.abort({"message": "Grading Run with id {} has already been queued in the past".format(id_)},
                       BAD_REQUEST_CODE)
            return

        # update grading run that it has started
        grading_runs_collection.update_one({db_key.ID: ObjectId(id_)}, {"$set": {db_key.STARTED: get_time()}})

        # enqueue jobs
        if db_key.PRE_PROCESSING in grading_run:
            self.enqueue_job(grading_run.get(db_key.PRE_PROCESSING), grading_run.get(db_key.STUDENTS))
        else:
            self.enqueue_student_jobs(grading_run)

    # TODO when building a web app around this
    @authenticate
    def get(self, id_):
        pass


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
    def get(self):
        db_resolver = self.get_db()
        worker_nodes_collection = db_resolver.get_worker_node_collection()

        worker_node = {db_key.LAST_SEEN: get_time(), db_key.RUNNING_JOB: None}
        worker_id = str(worker_nodes_collection.insert_one(worker_node).inserted_id)
        logging.info("Worker {} joined".format(worker_id))

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
    def post(self, id_):
        db_handler = self.get_db()
        worker_nodes_collection = db_handler.get_worker_node_collection()
        jobs_collection = db_handler.get_grading_job_collection()
        job_logs_collection = db_handler.get_job_logs_collection()

        # update worker node: remove this job from its currently running jobs
        worker_id = self.request.headers.get(api_key.WORKER_ID)
        worker_nodes_collection.update_one({db_key.ID: ObjectId(worker_id)},
                                           {"$set": {db_key.RUNNING_JOB: None}})

        # check if the job exists
        job = self.get_grading_job(id_)
        if job is None:
            return

        # update job: finished_at and result
        assert db_key.CREATED in job
        assert db_key.QUEUED in job
        assert db_key.STARTED in job
        assert db_key.FINISHED not in job
        job_succeeded = self.body.get(api_key.SUCCESS)
        jobs_collection.update_one({db_key.ID: ObjectId(id_)}, {
            "$set": {db_key.FINISHED: get_time(), db_key.RESULTS: self.body.get(api_key.RESULTS),
                     db_key.SUCCESS: job_succeeded}})

        # save logs in the logs DB along with the job id to identify it
        job_logs_collection.insert_one({db_key.JOB_ID: id_, **self.body.get(api_key.LOGS)})

        # thread safe callback
        tornado.ioloop.IOLoop.current().add_callback(self.job_update_callback, id_, job.get(db_key.GRADING_RUN))


class HeartBeatHandler(BaseAPIHandler):
    @authenticate
    @authenticate_worker
    def post(self):
        db_handler = self.get_db()
        worker_nodes_collection = db_handler.get_worker_node_collection()
        worker_id = self.request.headers.get(api_key.WORKER_ID)

        worker_nodes_collection.update_one({db_key.ID: ObjectId(worker_id)}, {"$set": {db_key.LAST_SEEN: get_time()}})
        logging.info("Heartbeat from {}".format(worker_id))
