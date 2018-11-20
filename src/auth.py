import logging
import os
import uuid

from bson import ObjectId

from src.config import UNAUTHORIZED_REQUEST_CODE, BAD_REQUEST_CODE
import src.handlers

logger = logging.getLogger()


def initialize_token():
    if os.environ.get("WARDEN_TOKEN"):
        logger.info("using authentication token from environment")
        token = os.environ.get("WARDEN_TOKEN")
    else:
        logger.info("generating authentication token")
        token = str(uuid.uuid4())
        logger.info("authentication token is {}".format(token))
    return token


def authenticate(func):
    def wrapper(*args, **kwargs):
        self = args[0]
        token = self.settings.get("token")
        request_token = self.request.headers.get("Authorization")

        if (request_token is None) or (token != request_token):
            self.set_status(UNAUTHORIZED_REQUEST_CODE)
            self.fail({"message": "Not authorized"})
        else:
            return func(*args, **kwargs)

    return wrapper


def validate_id(func):
    def wrapper(*args, **kwargs):
        self = args[0]  # type: src.handlers.BaseAPIHandler
        id_ = args[1]  # type: str

        if ObjectId.is_valid(id_):
            return func(*args, **kwargs)
        else:
            self.abort({"message": "ID {} is not a valid bson ObjectId".format(id_)}, BAD_REQUEST_CODE)

    return wrapper
