import src.constants.api_keys as api_key

valid_config = {
    api_key.STUDENT_PIPELINE: [
        {
            api_key.IMAGE: "alpine:3.5",
            api_key.TIMEOUT: 20
        },
        {
            api_key.IMAGE: "alpine:3.5",
            api_key.HOST_NAME: "step 2"
        },
        {
            api_key.IMAGE: "alpine:3.5",
            api_key.NETWORKING: True,
            api_key.ENV: {
                "var": "val"
            }
        }
    ],
    api_key.PRE_PROCESSING_PIPELINE: [
        {
            api_key.IMAGE: "alpine:3.5",
            api_key.ENV: {
                "SEMESTER": "sp18"
            },
            api_key.ENTRY_POINT: [
                "echo", "pre-processing-job"
            ]
        }
    ],
    api_key.POST_PROCESSING_PIPELINE: [
        {
            api_key.IMAGE: "alpine:3.5",
            api_key.ENV: {
                "SEMESTER": "fa17"
            },
            api_key.ENTRY_POINT: [
                "echo", "post-processing-job"
            ]
        },
        {
            api_key.IMAGE: "abc:abc"
        }
    ],
    api_key.ENV: {
        "TEST": "testing"
    },
    api_key.STUDENTS: [
        {
            "NETID": "ayushr2",
        },
        {
            "NETID": "waf",
            "extra": "stuff"
        }
    ]
}

valid_jobs = [
    [
        {
            api_key.IMAGE: "alpine:3.5",
            api_key.ENV: [
                "SEMESTER=sp18", "TEST=testing"
            ],
            api_key.ENTRY_POINT: [
                "echo", "pre-processing-job"
            ]
        }
    ],
    [
        {
            api_key.IMAGE: "alpine:3.5",
            api_key.TIMEOUT: 20,
            api_key.ENV: [
                "NETID=ayushr2", "TEST=testing"
            ]
        },
        {
            api_key.IMAGE: "alpine:3.5",
            api_key.HOST_NAME: "step 2",
            api_key.ENV: [
                "NETID=ayushr2", "TEST=testing"
            ]
        },
        {
            api_key.IMAGE: "alpine:3.5",
            api_key.NETWORKING: True,
            api_key.ENV: [
                "NETID=ayushr2", "TEST=testing", "var=val"
            ]
        }
    ],
    [
        {
            api_key.IMAGE: "alpine:3.5",
            api_key.TIMEOUT: 20,
            api_key.ENV: [
                "NETID=waf", "extra=stuff", "TEST=testing"
            ]
        },
        {
            api_key.IMAGE: "alpine:3.5",
            api_key.HOST_NAME: "step 2",
            api_key.ENV: [
                "NETID=waf", "extra=stuff", "TEST=testing"
            ]
        },
        {
            api_key.IMAGE: "alpine:3.5",
            api_key.NETWORKING: True,
            api_key.ENV: [
                "NETID=waf", "extra=stuff", "TEST=testing", "var=val"
            ]
        }
    ],
    [
        {
            api_key.IMAGE: "alpine:3.5",
            api_key.ENV: {
                "SEMESTER=fa17", "TEST=testing"
            },
            api_key.ENTRY_POINT: [
                "echo", "post-processing-job"
            ]
        },
        {
            api_key.IMAGE: "abc:abc",
            api_key.ENV: {
                "TEST=testing"
            }
        }
    ]
]

invalid_configs = [
    {
        api_key.STUDENT_PIPELINE: [
            {
                api_key.IMAGE: "alpine:3.5",
                api_key.TIMEOUT: 20
            }
        ],
        api_key.STUDENTS: [
            {
                "NETID": "ayushr2",
            }
        ],
        "extra": "stuff"
    },
    {
        api_key.STUDENT_PIPELINE:
            {
                api_key.IMAGE: "alpine:3.5",
                api_key.TIMEOUT: 20
            },
        api_key.STUDENTS: [
            {
                "NETID": "ayushr2",
            }
        ]
    },
    {
        api_key.STUDENT_PIPELINE: [
            {
                api_key.IMAGE: "alpine:3.5",
                api_key.TIMEOUT: 20
            }
        ],
    },
    {
        api_key.STUDENTS: [
            {
                "NETID": "ayushr2",
            }
        ]
    },
    {
        api_key.STUDENT_PIPELINE: [
            {
                api_key.IMAGE: "alpine:3.5",
                api_key.TIMEOUT: 20
            }
        ],
        api_key.STUDENTS: [
            {
                "NETID": "ayushr2",
            }
        ],
        api_key.PRE_PROCESSING_PIPELINE: [
            {
                api_key.ENV: {
                    "SEMESTER": "sp18"
                },
                api_key.ENTRY_POINT: [
                    "echo", "pre-processing-job"
                ]
            }
        ]
    },
    {
        api_key.STUDENT_PIPELINE: [
            {
                api_key.IMAGE: "alpine:3.5",
                api_key.TIMEOUT: 20
            }
        ],
        api_key.STUDENTS: [
            {
                "NETID": "ayushr2",
            }
        ],
        api_key.ENV: [{"a": "a"}]
    },
    {}
]
