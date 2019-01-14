from enum import Enum

import src.constants.keys as api_key

HEX_REGEX = r"(?P<{}>[a-f0-9]+)"
ID_REGEX = r"(?P<{}>[-\w]+)"
STRING_REGEX = r"(?P<{}>[^()]+)"
TIMESTAMP_FORMAT = "%Y-%m-%d %H:%M:%S"


# enums
class GradingJobType(Enum):
    PRE_PROCESSING = 1
    STUDENT = 2
    POST_PROCESSING = 3


class GradingJobState(Enum):
    QUEUED = 1  # the grading job has been pushed into the job for a worker to poll it
    STARTED = 2  # a worker has polled this grading job and is being processed
    FINISHED = 3  # the worker finished working on this job


class GradingRunState(Enum):
    READY = 0  # the grading run is ready to be scheduled
    PRE_PROCESSING_STAGE = 1  # the pre processing job has been scheduled
    STUDENTS_STAGE = 2  # the students grading jobs have been scheduled to be distributed among the workers
    POST_PROCESSING_STAGE = 3  # the post processing job has been scheduled
    FINISHED = 4  # the grading run is complete


# application specific constants
CLUSTER_TOKEN = "cluster_token"
APP_DB = "db_object"
APP_QUEUE = "job_queue"

# course config constants
CONFIG_TOKENS = "tokens"
CONFIG_COURSES = "courses"

# json validation formats
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
