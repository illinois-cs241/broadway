import logging
import os
import uuid

from bson import ObjectId

import src.constants.api_keys as api_key
from src.config import UNAUTHORIZED_REQUEST_CODE, BAD_REQUEST_CODE

logger = logging.getLogger()


def initialize_token():
    if os.environ.get("API_TOKEN"):
        logger.info("using authentication token from environment")
        token = os.environ.get("API_TOKEN")
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

        if (request_token is None) or (token != request_token):
            base_handler_instance.abort({"message": "Not authorized"}, UNAUTHORIZED_REQUEST_CODE)
        else:
            return func(*args, **kwargs)

    return wrapper


def authenticate_worker(func):
    def wrapper(*args, **kwargs):
        base_handler_instance = args[0]
        worker_id = kwargs.get("worker_id") if len(args) < 2 else args[1]

        if base_handler_instance.get_worker_node(worker_id) is None:  # this call aborts if it returns None
            return
        else:
            return func(*args, **kwargs)

    return wrapper


def validate_id(func):
    def wrapper(*args, **kwargs):
        base_handler_instance = args[0]
        id_ = kwargs.get("id_") if len(args) < 2 else args[1]

        if ObjectId.is_valid(id_):
            return func(*args, **kwargs)
        else:
            base_handler_instance.abort({"message": "ID {} is not a valid bson ObjectId".format(id_)}, BAD_REQUEST_CODE)

    return wrapper
