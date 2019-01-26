from enum import Enum

import src.constants.keys as api_key

HEX_REGEX = r"(?P<{}>[a-f0-9]+)"
ID_REGEX = r"(?P<{}>[-\w]+)"
STRING_REGEX = r"(?P<{}>[^()]+)"
TIMESTAMP_FORMAT = "%Y-%m-%d %H:%M:%S"


# enums
class GradingJobType(Enum):
    PRE_PROCESSING = "pre processing job"
    STUDENT = "student grading job"
    POST_PROCESSING = "post processing job"


class GradingJobState(Enum):
    QUEUED = "grading job has been scheduled"
    STARTED = "grading job is running"
    FAILED = "grading job failed"
    SUCCEEDED = "grading job was successful"


class GradingRunState(Enum):
    READY = "ready to be started"
    PRE_PROCESSING_STAGE = "pre processing job has been scheduled"
    STUDENTS_STAGE = "students grading jobs have been scheduled"
    POST_PROCESSING_STAGE = "post processing job has been scheduled"
    FINISHED = "grading run is complete"
    FAILED = "grading run failed"


# application specific constants
CLUSTER_TOKEN = "cluster_token"
APP_DB = "db_object"
APP_QUEUE = "job_queue"


# json validation formats
COURSE_CONFIG_DEF = {
    "type": "object",
    "patternProperties": {
        "": {"type": "array", "items": {"type": "string"}}
    }
}

GRADING_STAGE_DEF = {
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

GRADING_PIPELINE_DEF = {
    "type": "array",
    "items": GRADING_STAGE_DEF,
}

GRADING_CONFIG_DEF = {
    "type": "object",
    "properties": {
        api_key.PRE_PROCESSING_PIPELINE: GRADING_PIPELINE_DEF,
        api_key.STUDENT_PIPELINE: GRADING_PIPELINE_DEF,
        api_key.POST_PROCESSING_PIPELINE: GRADING_PIPELINE_DEF,
        api_key.ENV: {"type": "object"},
    },
    "required": [api_key.STUDENT_PIPELINE],
    "additionalProperties": False
}
