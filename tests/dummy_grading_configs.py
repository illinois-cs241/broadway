import src.constants.api_keys as api_key

valid_configs = [{
    api_key.STUDENT_PIPELINE: [
        {
            api_key.IMAGE: "alpine:3.5",
            api_key.TIMEOUT: 20
        },
        {
            api_key.IMAGE: "alpine:3.5",
            api_key.HOST_NAME: "123456"
        },
        {
            api_key.IMAGE: "alpine:3.5",
            api_key.NETWORKING: True
        },
        {
            api_key.IMAGE: "alpine:3.5",
            api_key.ENV: {
                "var1": "val1",
                "var2": "val2"
            }
        },
        {
            api_key.IMAGE: "alpine:3.5",
            api_key.ENTRY_POINT: ["echo", "student-job"]
        }
    ],
    api_key.PRE_PROCESSING_PIPELINE: [
        {
            api_key.IMAGE: "alpine:3.5",
            api_key.ENV: {
                "STAGE": "pre"
            },
            api_key.ENTRY_POINT: [
                "echo", "pre-processing-job"
            ],
            api_key.TIMEOUT: 20,
            api_key.HOST_NAME: "123456",
            api_key.NETWORKING: False
        }
    ],
    api_key.POST_PROCESSING_PIPELINE: [
        {
            api_key.IMAGE: "alpine:3.5",
            api_key.ENV: {
                "STAGE": "post"
            },
            api_key.ENTRY_POINT: [
                "echo", "post-processing-job"
            ],
            api_key.TIMEOUT: 20,
            api_key.HOST_NAME: "123456",
            api_key.NETWORKING: False
        }
    ],
    api_key.ENV: {
        "TEST": "testing",
        "temp": "val3"
    }
}]
