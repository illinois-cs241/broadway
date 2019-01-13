import logging
from queue import Empty

import tornado.ioloop
from bson import ObjectId
from tornado_json import schema

import src.constants.keys as key
import src.constants.constants as consts
from src.auth import authenticate_cluster_token, authenticate_worker
from src.config import HEARTBEAT_INTERVAL, QUEUE_EMPTY_CODE, BAD_REQUEST_CODE
from src.handlers.base_handler import BaseAPIHandler
from src.utilities import get_time, job_update_callback

logger = logging.getLogger()


class WorkerRegisterHandler(BaseAPIHandler):
    @authenticate_cluster_token
    @schema.validate(
        output_schema={
            "type": "object",
            "properties": {
                key.WORKER_ID: {"type": "string"},
                key.HEARTBEAT: {"type": "number"}
            },
            "required": [key.WORKER_ID],
            "additionalProperties": False
        }
    )
    def get(self, *args, **kwargs):
        hostname = kwargs.get(key.HOSTNAME_PARAM) if len(args) == 0 else args[0]
        db_resolver = self.get_db()
        worker_nodes_collection = db_resolver.get_worker_node_collection()

        worker_node = {key.WORKER_HOSTNAME: hostname, key.LAST_SEEN: get_time(), key.RUNNING_JOB: None}
        worker_id = str(worker_nodes_collection.insert_one(worker_node).inserted_id)
        logger.info("Worker with hostname {} joined as id {}".format(hostname, worker_id))

        return {key.WORKER_ID: worker_id, key.HEARTBEAT: HEARTBEAT_INTERVAL}


class GradingJobHandler(BaseAPIHandler):
    # GET used to poll a grading job
    @authenticate_cluster_token
    @authenticate_worker
    @schema.validate(
        on_empty_404=True,
        output_schema={
            "type": "object",
            "properties": {
                key.GRADING_JOB_ID: {"type": "string"},
                key.STAGES: {
                    "type": "array",
                    "items": consts.GRADING_STAGE_DEF,
                },
                key.STUDENTS: {
                    "type": "array",
                    "items": {"type": "object"},
                }
            },
            "required": [key.GRADING_JOB_ID, key.STAGES],
            "additionalProperties": False
        }
    )
    def get(self, *args, **kwargs):
        worker_id = kwargs.get(key.WORKER_ID_PARAM) if len(args) == 0 else args[0]

        db_resolver = self.get_db()
        job_queue = self.get_queue()

        try:
            job = job_queue.get_nowait()
            job_id = job.get(key.GRADING_JOB_ID)

            db_resolver.get_grading_job_collection().update_one({key.ID: ObjectId(job_id)},
                                                                {"$set": {key.STARTED: get_time()}})

            db_resolver.get_worker_node_collection().update_one({key.ID: ObjectId(worker_id)},
                                                                {"$set": {key.RUNNING_JOB: job_id}})

            return job
        except Empty:
            self.abort({"message": "The grading job queue is empty. No jobs available."}, QUEUE_EMPTY_CODE)

    # POST used to update grading job status upon completion
    @authenticate_cluster_token
    @authenticate_worker
    @schema.validate(
        input_schema={
            "type": "object",
            "properties": {
                key.GRADING_JOB_ID: {"type": "string"},
                key.SUCCESS: {"type": "boolean"},
                key.RESULTS: {
                    "type": "array",
                    "items": {"type": "object"},
                },
                key.LOGS: {"type": "object"}
            },
            "required": [key.GRADING_JOB_ID, key.SUCCESS, key.RESULTS, key.LOGS],
            "additionalProperties": False
        }
    )
    def post(self, *args, **kwargs):
        worker_id = kwargs.get(key.WORKER_ID_PARAM) if len(args) == 0 else args[0]

        db_handler = self.get_db()
        worker_nodes_collection = db_handler.get_worker_node_collection()
        jobs_collection = db_handler.get_grading_job_collection()
        job_logs_collection = db_handler.get_job_log_collection()

        # Error checks

        # check if the job exists
        job_id = self.body.get(key.GRADING_JOB_ID)
        job = self.get_grading_job(job_id)
        if job is None:
            return

        # make sure the job has the correct state
        if key.CREATED not in job:
            # does not necessarily mean anything wrong with the grader so we process the rest of the request
            logger.critical("Received an update for job {} which does not have the created field set in DB. "
                            "Something is wrong with application logic for job creation.".format(job_id))

        if key.QUEUED not in job:
            logger.critical("Received an update for job {} which had not been even queued yet.".format(job_id))
            self.abort({"message": "Grading job with id {} has not been queued yet".format(job_id)}, BAD_REQUEST_CODE)
            return

        if key.STARTED not in job:
            logger.critical("Received an update for job {} which had not been polled from the queue.".format(job_id))
            self.abort({"message": "Grading job with id {} has not polled yet".format(job_id)}, BAD_REQUEST_CODE)
            return

        if key.FINISHED in job:
            logger.critical("Received an update for job {} which had already been finished before.".format(job_id))
            self.abort({"message": "Grading job with id {} has already been finished and updated.".format(job_id)},
                       BAD_REQUEST_CODE)
            return

        # update worker node: remove this job from its currently running jobs
        worker_nodes_collection.update_one({key.ID: ObjectId(worker_id)},
                                           {"$set": {key.RUNNING_JOB: None}})

        # update job: finished_at and result
        job_succeeded = self.body.get(key.SUCCESS)
        jobs_collection.update_one({key.ID: ObjectId(job_id)}, {
            "$set": {key.FINISHED: get_time(), key.RESULTS: self.body.get(key.RESULTS),
                     key.SUCCESS: job_succeeded}})

        # save logs in the logs DB along with the job id to identify it
        job_logs_collection.insert_one({key.GRADING_JOB_ID: job_id, **self.body.get(key.LOGS)})

        # thread safe callback
        tornado.ioloop.IOLoop.current().add_callback(job_update_callback, db_handler, self.get_queue(), job_id,
                                                     job.get(key.GRADING_RUN_ID), self.body.get(key.SUCCESS))


class HeartBeatHandler(BaseAPIHandler):
    @authenticate_cluster_token
    @authenticate_worker
    def post(self, *args, **kwargs):
        worker_id = kwargs.get(key.WORKER_ID_PARAM) if len(args) == 0 else args[0]

        db_handler = self.get_db()
        worker_nodes_collection = db_handler.get_worker_node_collection()
        worker_nodes_collection.update_one({key.ID: ObjectId(worker_id)}, {"$set": {key.LAST_SEEN: get_time()}})
