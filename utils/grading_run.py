import src.constants.api_keys as api_key

valid_json = {
    api_key.STUDENT_PIPELINE: [
        {
            api_key.IMAGE: "alpine:3.5",
            api_key.ENTRY_POINT: [
                "env"
            ],
            api_key.ENV: {
                "ASSIGNMENT": "",
                "NETID": "$NETID"
            }
        },
        {
            api_key.IMAGE: "alpine:3.5",
            api_key.ENTRY_POINT: [
                "sleep", "5"
            ]
        }
    ],
    api_key.PRE_PROCESSING_PIPELINE: [
        {
            api_key.IMAGE: "alpine:3.5",
            api_key.ENTRY_POINT: [
                "echo", "pre-processing"
            ]
        }
    ],
    api_key.POST_PROCESSING_PIPELINE: [
        {
            api_key.IMAGE: "alpine:3.5",
            api_key.ENTRY_POINT: [
                "echo", "post-processing"
            ]
        }
    ],
    api_key.ENV: {
        "TEST": "testing",
        "SEMESTER": "sp18",
        "ASSIGNMENT": "mp1"
    },
    api_key.STUDENTS: [
        {
            "NETID": "waf"
        },
        {
            "NETID": "nwalter2"
        },
        {
            "NETID": "ayushr2"
        },
        {
            "NETID": "lmao"
        },
        {
            "NETID": "lol"
        },
        {
            "NETID": "cs241"
        },
        {
            "NETID": "trump"
        },
        {
            "NETID": "no-idea-what-to-put-here"
        }
    ]
}
