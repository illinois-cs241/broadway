import broadway.grader.api_keys as api_keys

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
    },
    "required": ["image"],
    "additionalProperties": False,
}

GRADING_JOB_DEF = {
    "type": "object",
    "properties": {
        api_keys.GRADING_JOB_ID: {"type": "string"},
        api_keys.STAGES: {"type": "array", "items": GRADING_STAGE_DEF},
    },
    "required": [api_keys.GRADING_JOB_ID, api_keys.STAGES],
    "additionalProperties": False,
}
