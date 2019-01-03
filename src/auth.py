import logging
import os
import uuid

import src.constants.api_keys as api_key
from src.config import UNAUTHORIZED_REQUEST_CODE, BAD_REQUEST_CODE, TOKEN_ENV_VAR

logger = logging.getLogger()


def initialize_token():
    if os.environ.get(TOKEN_ENV_VAR):
        logger.info("using authentication token from environment")
        token = os.environ.get(TOKEN_ENV_VAR)
    else:
        logger.info("generating authentication token")
        token = str(uuid.uuid4())
        logger.info("authentication token is {}".format(token))
    return token


def authenticate(func):
    def wrapper(*args, **kwargs):
        base_handler_instance = args[0]
        token = base_handler_instance.get_cluster_token()
        request_token = base_handler_instance.request.headers.get(api_key.AUTH)

        if (request_token is None) or not request_token.startswith("Bearer ") or len(request_token.split(" ")) != 2:
            base_handler_instance.abort({"message": "Cluster token in wrong format. Expect format \'Bearer <token>\'"},
                                        BAD_REQUEST_CODE)
        elif token != request_token.split(" ")[1]:
            base_handler_instance.abort({"message": "Not authorized. Wrong token."}, UNAUTHORIZED_REQUEST_CODE)
        else:
            return func(*args, **kwargs)

    return wrapper
