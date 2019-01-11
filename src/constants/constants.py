import src.constants.api_keys as api_key

HEX_REGEX = r"(?P<{}>[a-f0-9]+)"
ID_REGEX = r"(?P<{}>[-\w]+)"
STRING_REGEX = r"(?P<{}>[^()]+)"
TIMESTAMP_FORMAT = "%Y-%m-%d %H:%M:%S"

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
        api_key.ENV: {"type": "array", "items": {"type": "string"}},
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
        api_key.ENV: {"type": "array", "items": {"type": "string"}},
    },
    "required": [api_key.STUDENT_PIPELINE],
    "additionalProperties": False
}
