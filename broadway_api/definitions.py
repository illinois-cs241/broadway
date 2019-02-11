course_config = {
    "type": "object",
    "patternProperties": {"": {"type": "array", "items": {"type": "string"}}},
}

grading_stage = {
    "type": ["object", "null"],
    "properties": {
        "image": {"type": "string"},
        "env": {"type": "object"},
        "entrypoint": {"type": "array", "items": {"type": "string"}},
        "networking": {"type": "boolean"},
        "hostname": {"type": "string"},
        "timeout": {"type": "number"},
        "capabilities": {
            "type": "array",
            "items": {"enum": ["SYS_ADMIN", "NET_ADMIN"]},
        },
    },
    "required": ["image"],
    "additionalProperties": False,
}

grading_pipeline = {"type": ["array", "null"], "items": grading_stage}

grading_config = {
    "type": "object",
    "properties": {
        "pre_processing_pipeline": grading_pipeline,
        "student_pipeline": grading_pipeline,
        "post_processing_pipeline": grading_pipeline,
        "env": {"type": ["object", "null"]},
    },
    "required": ["student_pipeline"],
    "additionalProperties": False,
}
