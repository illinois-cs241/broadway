import logging
from queue import Queue

import tornado.ioloop
from bson import ObjectId
from tornado import gen
from tornado_json import schema
from tornado_json.requesthandlers import APIHandler

import src.constants.api_keys as api_key
import src.constants.constants as consts
import src.constants.db_keys as db_key
from src.auth import authenticate, authenticate_worker, validate_id
from src.config import BAD_REQUEST_CODE, HEARTBEAT_INTERVAL, QUEUE_EMPTY_CODE, JOB_POLL_TIMEOUT
from src.database import DatabaseResolver
from src.utilities import get_string_from_time, get_time, resolve_env_vars

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


class BaseAPIHandler(APIHandler):
    def abort(self, data, status):
        self.set_status(status)
        self.fail(data)

    @validate_id
    def get_worker_node(self, id_):
        db_resolver = self.settings.get(consts.APP_DB)  # type: DatabaseResolver
        worker_node = db_resolver.get_worker_node_collection().find_one({db_key.ID: ObjectId(id_)})
        if worker_node is None:
            self.abort({"message": "Worker node with id {} does not exist".format(id_)}, BAD_REQUEST_CODE)
        else:
            return worker_node

    @validate_id
    def get_grading_run(self, id_):
        db_resolver = self.settings.get(consts.APP_DB)  # type: DatabaseResolver
        grading_run = db_resolver.get_grading_run_collection().find_one({db_key.ID: ObjectId(id_)})
        if grading_run is None:
            self.abort({"message": "Grading run with id {} does not exist".format(id_)}, BAD_REQUEST_CODE)
        else:
            return grading_run

    @validate_id
    def get_grading_job(self, id_):
        db_resolver = self.settings.get(consts.APP_DB)  # type: DatabaseResolver
        grading_job = db_resolver.get_grading_job_collection().find_one({db_key.ID: ObjectId(id_)})
        if grading_job is None:
            self.abort({"message": "Grading job with id {} does not exist".format(id_)}, BAD_REQUEST_CODE)
        else:
            return grading_job


class AddGradingRunHandler(BaseAPIHandler):
    def create_job(self, pipeline_name, student_env_vars=None):
        cur_job = []
        for stage in self.body.get(pipeline_name):
            cur_stage = stage.copy()
            cur_stage[api_key.ENV] = resolve_env_vars(cur_stage.get(api_key.ENV, {}),
                                                      self.body.get(api_key.ENV, {}), student_env_vars)
            cur_job.append(cur_stage)

        return cur_job

    @authenticate
    @schema.validate(
        input_schema={
            "definitions": {
                "stage": grading_stage_def
            },

            "type": "object",
            "properties": {
                api_key.PRE_PROCESSING_PIPELINE: {
                    "type": "array",
                    "items": {"$ref": "#/definitions/stage"},
                },
                api_key.STUDENT_PIPELINE: {
                    "type": "array",
                    "items": {"$ref": "#/definitions/stage"},
                },
                api_key.POST_PROCESSING_PIPELINE: {
                    "type": "array",
                    "items": {"$ref": "#/definitions/stage"},
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
        db_handler = self.settings.get(consts.APP_DB)  # type: DatabaseResolver
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
                                               {"$set": {db_key.PRE_PROCESSING: post_processing_job_id}})

        # return the run id to user
        return {api_key.RUN_ID: grading_run_id}


class GradingRunHandler(BaseAPIHandler):
    @authenticate
    def post(self, id_):
        pass

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
        db_resolver = self.settings.get(consts.APP_DB)  # type: DatabaseResolver
        worker_nodes_collection = db_resolver.get_worker_node_collection()

        worker_node = {db_key.LAST_SEEN: get_string_from_time(), db_key.RUNNING_JOBS: {}}
        worker_id = str(worker_nodes_collection.insert_one(worker_node).inserted_id)
        logging.info("Worker {} joined".format(worker_id))

        return {api_key.WORKER_ID: worker_id, api_key.HEARTBEAT: HEARTBEAT_INTERVAL}


class GetGradingJobHandler(BaseAPIHandler):
    @gen.coroutine
    @authenticate
    @authenticate_worker
    @schema.validate(
        output_schema={
            "definitions": {
                "stage": grading_stage_def
            },

            "type": "object",
            "properties": {
                api_key.JOB_ID: {"type": "string"},
                api_key.STAGE: {
                    "type": "array",
                    "items": {"$ref": "#/definitions/stage"},
                },
                api_key.STUDENTS: {
                    "type": "array",
                    "items": {"type": "object"},
                }
            },
            "required": [api_key.JOB_ID, api_key.STAGE],
            "additionalProperties": False
        }
    )
    def get(self):
        db_resolver = self.settings.get(consts.APP_DB)  # type: DatabaseResolver
        job_queue = self.settings.get(consts.APP_QUEUE)  # type: Queue
        worker_id = self.request.headers.get(api_key.WORKER_ID)

        try:
            job = yield tornado.ioloop.IOLoop.current().run_in_executor(None,
                                                                        lambda: job_queue.get(block=True,
                                                                                              timeout=JOB_POLL_TIMEOUT))
            job_id = job.get(api_key.JOB_ID)

            # TODO see if connection is lost
            db_resolver.get_grading_job_collection().update_one({db_key.ID: ObjectId(job_id)},
                                                                {"$set": {db_key.STARTED: get_time()}})

            db_resolver.get_worker_node_collection().update_one({db_key.ID: ObjectId(worker_id)}, {
                "$set": {"{}:{}".format(db_key.RUNNING_JOBS, job_id): True}})

            return job
        except Exception as e:
            self.abort({'message': 'The queue is empty'}, QUEUE_EMPTY_CODE)


class UpdateGradingJobHandler(BaseAPIHandler):
    @authenticate
    @authenticate_worker
    @schema.validate(
        input_schema={
            "type": "object",
            "properties": {
                "worker_id": {"type": "string"},
                "success": {"type": "boolean"},
                "info": {"type": "string"}
            },
            "required": ["worker_id", "success", "info"],
            "additionalProperties": False
        }
    )
    def post(self, id_):
        pass


class HeartBeatHandler(BaseAPIHandler):
    @authenticate
    @authenticate_worker
    @schema.validate(
        input_schema={
            "type": "object",
            "properties": {
                "worker_id": {"type": "string"}
            },
            "required": ["worker_id"],
            "additionalProperties": False
        }
    )
    def post(self, id_):
        pass
