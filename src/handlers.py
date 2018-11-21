import logging

from bson import ObjectId
from tornado import gen
from tornado_json import schema
from tornado_json.requesthandlers import APIHandler

from utils.utilities import get_string_from_time
from src.auth import authenticate, validate_id
from src.config import BAD_REQUEST_CODE, HEARTBEAT_INTERVAL
from src.database import DatabaseResolver
import src.constants as consts

logger = logging.getLogger()

# constants
grading_stage_def = {
    "$id": "#stage",
    "type": "object",
    "properties": {
        "image": {"type": "string"},
        "env": {"type": "object"},
        "entry_point": {"type": "string"},
        "enable_networking": {"type": "boolean"},
        "host_name": {"type": "string"}
    },
    "required": ["image"],
    "additionalProperties": False
}


class BaseAPIHandler(APIHandler):
    def abort(self, data, status):
        self.set_status(status)
        self.fail(data)

    @validate_id
    def get_worker_node(self, id_):
        db_resolver = self.settings.get("db_object")  # type: DatabaseResolver
        worker_node = db_resolver.get_worker_node_collection().find_one({'_id': ObjectId(id_)})
        if worker_node is None:
            self.abort({"message": "Worker node with id {} does not exist".format(id_)}, BAD_REQUEST_CODE)
        else:
            return worker_node

    @validate_id
    def get_grading_run(self, id_):
        db_resolver = self.settings.get("db_object")  # type: DatabaseResolver
        grading_run = db_resolver.get_grading_run_collection().find_one({'_id': ObjectId(id_)})
        if grading_run is None:
            self.abort({"message": "Grading run with id {} does not exist".format(id_)}, BAD_REQUEST_CODE)
        else:
            return grading_run

    @validate_id
    def get_grading_job(self, id_):
        db_resolver = self.settings.get("db_object")  # type: DatabaseResolver
        grading_job = db_resolver.get_grading_job_collection().find_one({'_id': ObjectId(id_)})
        if grading_job is None:
            self.abort({"message": "Grading job with id {} does not exist".format(id_)}, BAD_REQUEST_CODE)
        else:
            return grading_job


class AddGradingRunHandler(BaseAPIHandler):
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
                consts.WORKER_ID_KEY: {"type": "string"},
                consts.HEARTBEAT_KEY: {"type": "number"}
            },
            "required": [consts.WORKER_ID_KEY],
            "additionalProperties": False
        }
    )
    def get(self):
        db_handler = self.settings.get('db_object')  # type: DatabaseResolver
        worker_nodes_collection = db_handler.get_worker_node_collection()

        worker_node = {consts.LAST_SEEN_KEY: get_string_from_time(), consts.RUNNING_JOBS_KEY: {}}
        worker_id = str(worker_nodes_collection.insert_one(worker_node).inserted_id)
        logging.info("Worker {} joined".format(worker_id))

        return {consts.WORKER_ID_KEY: worker_id, consts.HEARTBEAT_KEY: HEARTBEAT_INTERVAL}


class GetGradingJobHandler(BaseAPIHandler):
    @authenticate
    @schema.validate(
        input_schema={
            "type": "object",
            "properties": {
                "worker_id": {"type": "string"},
            },
            "required": ["worker_id"],
            "additionalProperties": False
        },
        output_schema={
            "definitions": {
                "stage": grading_stage_def
            },

            "type": "object",
            "properties": {
                "job_id": {"type": "string"},
                "stages": {
                    "type": "array",
                    "items": {"$ref": "#stage"},
                },
                "students": {
                    "type": "array",
                    "items": {"type": "object"},
                }
            },
            "required": ["job_id", "stages"],
            "additionalProperties": False
        }
    )
    def get(self):
        pass


class UpdateGradingJobHandler(BaseAPIHandler):
    @authenticate
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
