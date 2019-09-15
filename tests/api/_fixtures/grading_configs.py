valid_configs = [
    {
        "student_pipeline": [
            {"image": "alpine:3.5", "timeout": 20},
            {"image": "alpine:3.5", "hostname": "123456"},
            {"image": "alpine:3.5", "networking": True},
            {"image": "alpine:3.5", "env": {"var1": "val1", "var2": "val2"}},
            {"image": "alpine:3.5", "entrypoint": ["echo", "student-job"]},
        ],
        "pre_processing_pipeline": [
            {
                "image": "alpine:3.5",
                "env": {"STAGE": "pre"},
                "entrypoint": ["echo", "pre-processing-job"],
                "timeout": 20,
                "hostname": "123456",
                "networking": False,
                "privileged": True,
                "memory": "2g",
                "logs": False,
            }
        ],
        "post_processing_pipeline": [
            {
                "image": "alpine:3.5",
                "env": {"STAGE": "post"},
                "entrypoint": ["echo", "post-processing-job"],
                "timeout": 20,
                "hostname": "123456",
                "networking": False,
            }
        ],
        "env": {"TEST": "testing", "temp": "val3"},
    },
    {"student_pipeline": [{"image": "alpine:3.5"}]},
    {
        "student_pipeline": [
            {"image": "alpine:3.5", "timeout": 20},
            {"image": "alpine:3.5", "hostname": "123456"},
            {"image": "alpine:3.5", "entrypoint": ["echo", "student-job"]},
        ],
        "env": {"TEST": "testing", "temp": "val3"},
    },
    {
        "student_pipeline": [
            {"image": "alpine:3.5", "entrypoint": ["echo", "student-job"]}
        ],
        "pre_processing_pipeline": [
            {
                "image": "alpine:3.5",
                "env": {"STAGE": "pre"},
                "entrypoint": ["echo", "pre-processing-job"],
                "timeout": 20,
                "hostname": "123456",
                "networking": False,
            }
        ],
    },
    {
        "student_pipeline": [{"image": "alpine:3.5", "hostname": "123456"}],
        "post_processing_pipeline": [
            {
                "image": "alpine:3.5",
                "env": {"STAGE": "post"},
                "entrypoint": ["echo", "post-processing-job"],
                "timeout": 20,
                "hostname": "123456",
                "networking": False,
            }
        ],
        "env": {"TEST": "testing", "temp": "val3"},
    },
    {
        "student_pipeline": [
            {"image": "alpine:3.5", "hostname": "654321", "privileged": True}
        ],
        "post_processing_pipeline": [
            {
                "image": "alpine:3.5",
                "env": {"STAGE": "post"},
                "entrypoint": ["echo", "post-processing-job"],
                "timeout": 20,
                "hostname": "654321",
                "networking": False,
            }
        ],
        "env": {"TEST": "testing", "temp": "val3"},
    },
    {
        "student_pipeline": [
            {"image": "alpine:3.5", "hostname": "654321", "privileged": True}
        ],
        "post_processing_pipeline": [
            {
                "image": "alpine:3.5",
                "env": {"STAGE": "post"},
                "entrypoint": ["echo", "post-processing-job"],
                "timeout": 20,
                "hostname": "654321",
                "networking": False,
            }
        ],
        "env": {"TEST": "testing", "temp": "val3"},
    },
]

invalid_configs = [
    {
        "student_pipeline": [
            {
                "image": "alpine:3.5",
                "hostname": "654321",
                "privileged": "neither true or false",
            }
        ],
        "post_processing_pipeline": [
            {
                "image": "alpine:3.5",
                "env": {"STAGE": "post"},
                "entrypoint": ["echo", "post-processing-job"],
                "timeout": 20,
                "hostname": "654321",
                "networking": False,
            }
        ],
        "env": {"TEST": "testing", "temp": "val3"},
    },
    {
        "student_pipeline": [
            {"image": "alpine:3.5", "hostname": "654321", "privileged": "very high"}
        ],
        "post_processing_pipeline": [
            {
                "image": "alpine:3.5",
                "env": {"STAGE": "post"},
                "entrypoint": ["echo", "post-processing-job"],
                "timeout": 20,
                "hostname": "654321",
                "networking": False,
            }
        ],
        "env": {"TEST": "testing", "temp": "val3"},
    },
    {
        "student_pipeline": [
            {"hostname": "123456"},
            {"image": "alpine:3.5", "networking": True},
            {"image": "alpine:3.5", "env": {"var1": "val1", "var2": "val2"}},
            {"image": "alpine:3.5", "entrypoint": ["echo", "student-job"]},
        ],
        "pre_processing_pipeline": [
            {
                "image": "alpine:3.5",
                "env": {"STAGE": "pre"},
                "entrypoint": ["echo", "pre-processing-job"],
                "timeout": 20,
                "hostname": "123456",
                "networking": False,
            }
        ],
        "post_processing_pipeline": [
            {
                "image": "alpine:3.5",
                "env": {"STAGE": "post"},
                "entrypoint": ["echo", "post-processing-job"],
                "timeout": 20,
                "hostname": "123456",
                "networking": False,
            }
        ],
        "env": {"TEST": "testing", "temp": "val3"},
    },
    {
        "pre_processing_pipeline": [
            {
                "image": "alpine:3.5",
                "env": {"STAGE": "pre"},
                "entrypoint": ["echo", "pre-processing-job"],
                "timeout": 20,
                "hostname": "123456",
                "networking": False,
            }
        ],
        "post_processing_pipeline": [
            {
                "image": "alpine:3.5",
                "env": {"STAGE": "post"},
                "entrypoint": ["echo", "post-processing-job"],
                "timeout": 20,
                "hostname": "123456",
                "networking": False,
            }
        ],
        "env": {"TEST": "testing", "temp": "val3"},
    },
    {
        "student_pipeline": [{"image": "alpine:3.5", "timeout": 20}],
        "pre_processing_pipeline": [
            {
                "env": {"STAGE": "pre"},
                "entrypoint": ["echo", "pre-processing-job"],
                "timeout": 20,
                "hostname": "123456",
                "networking": False,
            }
        ],
    },
    {"student_pipeline": [{"image": "alpine:3.5", "hello": "world"}]},
    {"student_pipeline": [{"image": "alpine:3.5", "timeout": 20}], "hello": "world"},
    {"student_pipeline": "hello"},
    {},
]

only_student_config = {
    "student_pipeline": [{"image": "alpine:3.5"}],
    "env": {"env1": "global1", "env2": "global2"},
}

pre_processing_config = {
    "student_pipeline": [{"image": "alpine:3.5"}],
    "pre_processing_pipeline": [{"image": "alpine:3.5"}],
}

post_processing_config = {
    "student_pipeline": [{"image": "alpine:3.5"}],
    "post_processing_pipeline": [{"image": "alpine:3.5"}],
}

both_config = {
    "pre_processing_pipeline": [{"image": "alpine:3.5"}],
    "student_pipeline": [{"image": "alpine:3.5"}],
    "post_processing_pipeline": [{"image": "alpine:3.5"}],
}

complete_config = {
    "pre_processing_pipeline": [
        {
            "image": "alpine:3.1",
            "env": {"STAGE": "pre"},
            "entrypoint": ["echo", "pre-processing-job"],
            "timeout": 20,
            "hostname": "12",
            "networking": True,
        }
    ],
    "student_pipeline": [
        {
            "image": "alpine:3.2",
            "env": {"var1": "val1", "var2": "val2"},
            "entrypoint": ["echo", "student-job-1"],
            "timeout": 30,
            "hostname": "34",
            "networking": False,
        },
        {
            "image": "alpine:3.3",
            "env": {"var3": "val3", "var4": "val4"},
            "entrypoint": ["echo", "student-job-2"],
            "timeout": 40,
            "hostname": "56",
            "networking": True,
        },
    ],
    "post_processing_pipeline": [
        {
            "image": "alpine:3.4",
            "env": {"STAGE": "post"},
            "entrypoint": ["echo", "post-processing-job"],
            "timeout": 50,
            "hostname": "78",
            "networking": False,
        }
    ],
    "env": {"global_var1": "global_val1", "global_var2": "global_val2"},
}
