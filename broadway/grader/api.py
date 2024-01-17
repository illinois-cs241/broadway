# API keys
AUTH = "Authorization"
HOSTNAME = "hostname"
HEARTBEAT = "heartbeat"
GRADING_JOB_ID = "grading_job_id"
RESULTS = "results"
SUCCESS = "success"
LOGS = "logs"
STAGES = "stages"
ENV = "env"

# API endpoints
HEARTBEAT_ENDPOINT = "/api/v1/heartbeat"
GRADING_JOB_ENDPOINT = "/api/v1/grading_job"
GRADER_REGISTER_ENDPOINT = "/api/v1/worker"
WORKER_WS_ENDPOINT = "/api/v1/worker_ws"

SUCCESS_CODE = 200
QUEUE_EMPTY_CODE = 498
JOB_POLL_INTERVAL = 5
HEARTBEAT_INTERVAL = 10

GRADING_STAGE_DEF = {
    "type": ["object", "null"],
    "properties": {
        "image": {"type": "string"},
        "env": {"type": "object"},
        "entrypoint": {"type": "array", "items": {"type": "string"}},
        "networking": {"type": "boolean"},
        "privileged": {"type": "boolean"},
        "hostname": {"type": "string"},
        "timeout": {"type": "number"},
        "memory": {"type": "string"},
        "cpuset_cpus": {"type": "string"},
    },
    "required": ["image"],
    "additionalProperties": False,
}

GRADING_JOB_DEF = {
    "type": "object",
    "properties": {
        GRADING_JOB_ID: {"type": "string"},
        STAGES: {"type": "array", "items": GRADING_STAGE_DEF},
    },
    "required": [GRADING_JOB_ID, STAGES],
    "additionalProperties": False,
}
