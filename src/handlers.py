import logging
import tornado.ioloop

from bson import ObjectId
from queue import Queue
from tornado import gen
from tornado_json import schema
from tornado_json.requesthandlers import APIHandler

import src.constants.api_keys as api_key
import src.constants.db_keys as db_key
import src.constants.constants as consts
from src.auth import authenticate, authenticate_worker, validate_id
from src.config import BAD_REQUEST_CODE, HEARTBEAT_INTERVAL, QUEUE_EMPTY_CODE, JOB_POLL_TIMEOUT
from src.database import DatabaseResolver
from src.utilities import get_string_from_time, get_time, resolve_env_vars

logger = logging.getLogger()

# constants
grading_stage_def = {
    "$id": "#stage",
    "type": "object",
    "properties": {
        api_key.IMAGE: {"type": "string"},
        api_key.ENV: {"type": "object"},
        api_key.ENTRY_POINT: {"type": "string"},
        api_key.NETWORKING: {"type": "boolean"},
        api_key.HOST_NAME: {"type": "string"}
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
    def create_job(self, pipeline_name, json_payload, job_specific_env=None):
        cur_job = []
        for stage in json_payload[pipeline_name]:
            cur_stage = stage.copy()

            try:
                cur_stage["env"] = resolve_env_vars(stage.get("env", {}), json_payload.get("env", {}), job_specific_env)

            except Exception as error:
                self.bad_request("{}: {}".format(pipeline_name, str(error)))
                return
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
                "pre_processing_pipeline": {
                    "type": "array",
                    "items": {"$ref": "#stage"},
                },
                "student_pipeline": {
                    "type": "array",
                    "items": {"$ref": "#stage"},
                    "default": []
                },
                "post_processing_pipeline": {
                    "type": "array",
                    "items": {"$ref": "#stage"},
                },
                "students": {
                    "type": "array",
                    "items": {"type": "object"},
                },
                "env": {"type": "object"},
            },
            "required": ["student_pipeline", "students"],
            "additionalProperties": False
        },
        output_schema={"type": "string"}
    )
    def post(self):
        pass


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
                    "items": {"$ref": "#stage"},
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
