import logging
from tornado_json import schema
from tornado_json.exceptions import APIError
from tornado_json.requesthandlers import APIHandler
from src.auth import authenticate

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


class AddGradingRunHandler(BaseAPIHandler):
    @authenticate
    @schema.validate(
        input_schema={
            "definitions": {
                "stage": grading_stage_def
            },

            "type": "object",
            "properties": {
                "preprocessing_pipeline": {
                    "type": "array",
                    "items": {"$ref": "#stage"},
                },
                "student_pipeline": {
                    "type": "array",
                    "items": {"$ref": "#stage"},
                    "default": []
                },
                "postprocessing_pipeline": {
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
                "worker_id": {"type": "string"},
                "heartbeat": {"type": "number"}
            },
            "required": ["worker_id"],
            "additionalProperties": False
        }
    )
    def get(self):
        pass


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
