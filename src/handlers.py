import logging
from tornado_json import schema
from tornado_json.exceptions import APIError
from tornado_json.requesthandlers import APIHandler
from src.auth import authenticate

logger = logging.getLogger()


class BaseAPIHandler(APIHandler):
    def abort(self, data, status):
        self.set_status(status)
        self.fail(data)


class AddGradingRunHandler(BaseAPIHandler):
    @authenticate
    @schema.validate(
        input_schema={
            "definitions": {
                "stage": {
                    "$id": "#stage",
                    "type": "object",
                    "properties": {
                        "image": {"type": "string"},
                        "env": {"type": "object"},
                        "entry_point": {"type": "string"},
                        "enable_networking": {"type": "boolean"},
                        "host_name": {"type": "string"}
                    },
                    "required": ["image"]
                }
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
                    "items": {
                        "type": "object",
                        "properties": {
                            "STUDENT_ID": {"type": "string"},
                        },
                        "additionalProperties": True,
                        "required": ["STUDENT_ID"]
                    },
                },
                "env": {"type": "object"},
            },
            "required": ["student_pipeline", "students"]
        }
    )
    def post(self):
        pass
