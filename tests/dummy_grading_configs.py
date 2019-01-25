import src.constants.keys as key

valid_configs = [
    {
        key.STUDENT_PIPELINE: [
            {
                key.IMAGE: "alpine:3.5",
                key.TIMEOUT: 20
            },
            {
                key.IMAGE: "alpine:3.5",
                key.HOST_NAME: "123456"
            },
            {
                key.IMAGE: "alpine:3.5",
                key.NETWORKING: True
            },
            {
                key.IMAGE: "alpine:3.5",
                key.ENV: {"var1": "val1", "var2": "val2"}
            },
            {
                key.IMAGE: "alpine:3.5",
                key.ENTRY_POINT: ["echo", "student-job"]
            }
        ],
        key.PRE_PROCESSING_PIPELINE: [
            {
                key.IMAGE: "alpine:3.5",
                key.ENV: {"STAGE": "pre"},
                key.ENTRY_POINT: [
                    "echo", "pre-processing-job"
                ],
                key.TIMEOUT: 20,
                key.HOST_NAME: "123456",
                key.NETWORKING: False
            }
        ],
        key.POST_PROCESSING_PIPELINE: [
            {
                key.IMAGE: "alpine:3.5",
                key.ENV: {"STAGE": "post"},
                key.ENTRY_POINT: [
                    "echo", "post-processing-job"
                ],
                key.TIMEOUT: 20,
                key.HOST_NAME: "123456",
                key.NETWORKING: False
            }
        ],
        key.ENV: {"TEST": "testing", "temp": "val3"}
    },
    {
        key.STUDENT_PIPELINE: [
            {
                key.IMAGE: "alpine:3.5",
            }
        ]
    },
    {
        key.STUDENT_PIPELINE: [
            {
                key.IMAGE: "alpine:3.5",
                key.TIMEOUT: 20
            },
            {
                key.IMAGE: "alpine:3.5",
                key.HOST_NAME: "123456"
            },
            {
                key.IMAGE: "alpine:3.5",
                key.ENTRY_POINT: ["echo", "student-job"]
            }
        ],
        key.ENV: {"TEST": "testing", "temp": "val3"}
    },
    {
        key.STUDENT_PIPELINE: [
            {
                key.IMAGE: "alpine:3.5",
                key.ENTRY_POINT: ["echo", "student-job"]
            }
        ],
        key.PRE_PROCESSING_PIPELINE: [
            {
                key.IMAGE: "alpine:3.5",
                key.ENV: {"STAGE": "pre"},
                key.ENTRY_POINT: [
                    "echo", "pre-processing-job"
                ],
                key.TIMEOUT: 20,
                key.HOST_NAME: "123456",
                key.NETWORKING: False
            }
        ]
    },
    {
        key.STUDENT_PIPELINE: [
            {
                key.IMAGE: "alpine:3.5",
                key.HOST_NAME: "123456"
            }
        ],
        key.POST_PROCESSING_PIPELINE: [
            {
                key.IMAGE: "alpine:3.5",
                key.ENV: {"STAGE": "post"},
                key.ENTRY_POINT: [
                    "echo", "post-processing-job"
                ],
                key.TIMEOUT: 20,
                key.HOST_NAME: "123456",
                key.NETWORKING: False
            }
        ],
        key.ENV: {"TEST": "testing", "temp": "val3"}
    },
    {
        key.STUDENT_PIPELINE: [
            {
                key.IMAGE: "alpine:3.5",
                key.HOST_NAME: "654321",
                key.CAPABILITIES: ["SYS_ADMIN", "NET_ADMIN"]
            }
        ],
        key.POST_PROCESSING_PIPELINE: [
            {
                key.IMAGE: "alpine:3.5",
                key.ENV: {"STAGE": "post"},
                key.ENTRY_POINT: [
                    "echo", "post-processing-job"
                ],
                key.TIMEOUT: 20,
                key.HOST_NAME: "654321",
                key.NETWORKING: False
            }
        ],
        key.ENV: {"TEST": "testing", "temp": "val3"}
    },
    {
        key.STUDENT_PIPELINE: [
            {
                key.IMAGE: "alpine:3.5",
                key.HOST_NAME: "654321",
                key.CAPABILITIES: ["SYS_ADMIN", "SYS_ADMIN"]
            }
        ],
        key.POST_PROCESSING_PIPELINE: [
            {
                key.IMAGE: "alpine:3.5",
                key.ENV: {"STAGE": "post"},
                key.ENTRY_POINT: [
                    "echo", "post-processing-job"
                ],
                key.TIMEOUT: 20,
                key.HOST_NAME: "654321",
                key.NETWORKING: False
            }
        ],
        key.ENV: {"TEST": "testing", "temp": "val3"}
    }
]

invalid_configs = [
    {
        key.STUDENT_PIPELINE: [
            {
                key.IMAGE: "alpine:3.5",
                key.HOST_NAME: "654321",
                key.CAPABILITIES: ["SYS_HACKER"]
            }
        ],
        key.POST_PROCESSING_PIPELINE: [
            {
                key.IMAGE: "alpine:3.5",
                key.ENV: {"STAGE": "post"},
                key.ENTRY_POINT: [
                    "echo", "post-processing-job"
                ],
                key.TIMEOUT: 20,
                key.HOST_NAME: "654321",
                key.NETWORKING: False
            }
        ],
        key.ENV: {"TEST": "testing", "temp": "val3"}
    },
    {
        key.STUDENT_PIPELINE: [
            {
                key.IMAGE: "alpine:3.5",
                key.HOST_NAME: "654321",
                key.CAPABILITIES: ["NET_CRACKER"]
            }
        ],
        key.POST_PROCESSING_PIPELINE: [
            {
                key.IMAGE: "alpine:3.5",
                key.ENV: {"STAGE": "post"},
                key.ENTRY_POINT: [
                    "echo", "post-processing-job"
                ],
                key.TIMEOUT: 20,
                key.HOST_NAME: "654321",
                key.NETWORKING: False
            }
        ],
        key.ENV: {"TEST": "testing", "temp": "val3"}
    },
    {
        key.STUDENT_PIPELINE: [
            {
                key.HOST_NAME: "123456"
            },
            {
                key.IMAGE: "alpine:3.5",
                key.NETWORKING: True
            },
            {
                key.IMAGE: "alpine:3.5",
                key.ENV: {"var1": "val1", "var2": "val2"}
            },
            {
                key.IMAGE: "alpine:3.5",
                key.ENTRY_POINT: ["echo", "student-job"]
            }
        ],
        key.PRE_PROCESSING_PIPELINE: [
            {
                key.IMAGE: "alpine:3.5",
                key.ENV: {"STAGE": "pre"},
                key.ENTRY_POINT: [
                    "echo", "pre-processing-job"
                ],
                key.TIMEOUT: 20,
                key.HOST_NAME: "123456",
                key.NETWORKING: False
            }
        ],
        key.POST_PROCESSING_PIPELINE: [
            {
                key.IMAGE: "alpine:3.5",
                key.ENV: {"STAGE": "post"},
                key.ENTRY_POINT: [
                    "echo", "post-processing-job"
                ],
                key.TIMEOUT: 20,
                key.HOST_NAME: "123456",
                key.NETWORKING: False
            }
        ],
        key.ENV: {"TEST": "testing", "temp": "val3"}
    },
    {
        key.PRE_PROCESSING_PIPELINE: [
            {
                key.IMAGE: "alpine:3.5",
                key.ENV: {"STAGE": "pre"},
                key.ENTRY_POINT: [
                    "echo", "pre-processing-job"
                ],
                key.TIMEOUT: 20,
                key.HOST_NAME: "123456",
                key.NETWORKING: False
            }
        ],
        key.POST_PROCESSING_PIPELINE: [
            {
                key.IMAGE: "alpine:3.5",
                key.ENV: {"STAGE": "post"},
                key.ENTRY_POINT: [
                    "echo", "post-processing-job"
                ],
                key.TIMEOUT: 20,
                key.HOST_NAME: "123456",
                key.NETWORKING: False
            }
        ],
        key.ENV: {"TEST": "testing", "temp": "val3"}
    },
    {
        key.STUDENT_PIPELINE: [
            {
                key.IMAGE: "alpine:3.5",
                key.TIMEOUT: 20
            }
        ],
        key.PRE_PROCESSING_PIPELINE: [
            {
                key.ENV: {"STAGE": "pre"},
                key.ENTRY_POINT: [
                    "echo", "pre-processing-job"
                ],
                key.TIMEOUT: 20,
                key.HOST_NAME: "123456",
                key.NETWORKING: False
            }
        ]
    },
    {
        key.STUDENT_PIPELINE: [
            {
                key.IMAGE: "alpine:3.5",
                "hello": "world"
            }
        ]
    },
    {
        key.STUDENT_PIPELINE: [
            {
                key.IMAGE: "alpine:3.5",
                key.TIMEOUT: 20
            }
        ],
        "hello": "world"
    },
    {
        key.STUDENT_PIPELINE: "hello"
    },
    {}
]

only_student_config = {
    key.STUDENT_PIPELINE: [
        {
            key.IMAGE: "alpine:3.5",
        }
    ],
    key.ENV: {
        "env1": "global1",
        "env2": "global2"
    }
}

pre_processing_config = {
    key.STUDENT_PIPELINE: [
        {
            key.IMAGE: "alpine:3.5",
        }
    ],
    key.PRE_PROCESSING_PIPELINE: [
        {
            key.IMAGE: "alpine:3.5"
        }
    ]
}

post_processing_config = {
    key.STUDENT_PIPELINE: [
        {
            key.IMAGE: "alpine:3.5",
        }
    ],
    key.POST_PROCESSING_PIPELINE: [
        {
            key.IMAGE: "alpine:3.5"
        }
    ]
}

both_config = {
    key.PRE_PROCESSING_PIPELINE: [
        {
            key.IMAGE: "alpine:3.5"
        }
    ],
    key.STUDENT_PIPELINE: [
        {
            key.IMAGE: "alpine:3.5",
        }
    ],
    key.POST_PROCESSING_PIPELINE: [
        {
            key.IMAGE: "alpine:3.5"
        }
    ]
}

complete_config = {
    key.PRE_PROCESSING_PIPELINE: [
        {
            key.IMAGE: "alpine:3.1",
            key.ENV: {"STAGE": "pre"},
            key.ENTRY_POINT: [
                "echo", "pre-processing-job"
            ],
            key.TIMEOUT: 20,
            key.HOST_NAME: "12",
            key.NETWORKING: True
        }
    ],
    key.STUDENT_PIPELINE: [
        {
            key.IMAGE: "alpine:3.2",
            key.ENV: {"var1": "val1", "var2": "val2"},
            key.ENTRY_POINT: [
                "echo", "student-job-1"
            ],
            key.TIMEOUT: 30,
            key.HOST_NAME: "34",
            key.NETWORKING: False
        },
        {
            key.IMAGE: "alpine:3.3",
            key.ENV: {"var3": "val3", "var4": "val4"},
            key.ENTRY_POINT: [
                "echo", "student-job-2"
            ],
            key.TIMEOUT: 40,
            key.HOST_NAME: "56",
            key.NETWORKING: True
        }
    ],
    key.POST_PROCESSING_PIPELINE: [
        {
            key.IMAGE: "alpine:3.4",
            key.ENV: {"STAGE": "post"},
            key.ENTRY_POINT: [
                "echo", "post-processing-job"
            ],
            key.TIMEOUT: 50,
            key.HOST_NAME: "78",
            key.NETWORKING: False
        }
    ],
    key.ENV: {"global_var1": "global_val1", "global_var2": "global_val2"}
}
