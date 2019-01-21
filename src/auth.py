import logging
import os
import uuid

import jsonschema

import src.constants.constants as consts
import src.constants.keys as key
from src.config import UNAUTHORIZED_REQUEST_CODE, BAD_REQUEST_CODE, CLUSTER_TOKEN_ENV_VAR
from src.database import DatabaseResolver

logger = logging.getLogger()


def initialize_cluster_token():
    if os.environ.get(CLUSTER_TOKEN_ENV_VAR):
        logger.info("using cluster token from environment")
        token = os.environ.get(CLUSTER_TOKEN_ENV_VAR)
    else:
        logger.info("generating cluster token")
        token = str(uuid.uuid4())
        logger.info("cluster token is {}".format(token))
    return token


def configure_course_tokens(db_resolver, course_tokens):
    """
    Each course can have any number of tokens. This functions aims in resolving that.
    Example config:
    {
       "course1": ["token1", "token2"],
       "course2": ["token3"]
    }

    :param db_resolver: DatabaseResolver object
    :type db_resolver: DatabaseResolver
    :param course_tokens: object containing the configuration of courses and their tokens
    :type course_tokens: dict

    :raises ValidationError: if the course config is invalid according to the schema defined
    """
    jsonschema.validate(course_tokens, consts.COURSE_CONFIG_DEF)

    courses_collection = db_resolver.get_course_collection()
    courses_collection.drop()

    for course_id, tokens in course_tokens.items():
        course = {key.ID: course_id, key.TOKENS: tokens}
        courses_collection.insert_one(course)


def authenticate_worker(func):
    def wrapper(*args, **kwargs):
        base_handler_instance = args[0]
        worker_id = kwargs.get(key.WORKER_ID_PARAM)
        if base_handler_instance.get_worker_node_or_abort(worker_id) is None:
            return
        else:
            return func(*args, **kwargs)

    return wrapper


def validate_assignment(func):
    def wrapper(*args, **kwargs):
        base_handler_instance = args[0]
        course_id = kwargs.get(key.COURSE_ID_PARAM)
        assignment_name = kwargs.get(key.ASSIGNMENT_NAME_PARAM)
        if base_handler_instance.get_assignment_or_abort(
                base_handler_instance.get_assignment_id(course_id, assignment_name)) is not None:
            return func(*args, **kwargs)

    return wrapper


def authenticate_cluster_token(func):
    def wrapper(*args, **kwargs):
        base_handler_instance = args[0]
        expected_token = base_handler_instance.get_cluster_token()
        request_token = base_handler_instance.request.headers.get(key.AUTH)

        if (request_token is None) or not request_token.startswith("Bearer ") or len(request_token.split(" ")) != 2:
            base_handler_instance.abort({"message": "Cluster token in wrong format. Expect format \'Bearer <token>\'"},
                                        UNAUTHORIZED_REQUEST_CODE)
        elif expected_token != request_token.split(" ")[1]:
            base_handler_instance.abort({"message": "Not authorized. Wrong token."}, UNAUTHORIZED_REQUEST_CODE)
        else:
            return func(*args, **kwargs)

    return wrapper


def authenticate_course(func):
    def wrapper(*args, **kwargs):
        base_handler_instance = args[0]

        request_token = base_handler_instance.request.headers.get(key.AUTH)
        if (request_token is None) or not request_token.startswith("Bearer ") or len(request_token.split(" ")) != 2:
            base_handler_instance.abort({"message": "Cluster token in wrong format. Expect format \'Bearer <token>\'"},
                                        UNAUTHORIZED_REQUEST_CODE)
            return

        request_token = request_token.split(" ")[1]

        course_id = kwargs.get(key.COURSE_ID_PARAM)
        course = base_handler_instance.get_course_or_abort(course_id)
        if course is None:
            return

        if request_token in course.get(key.TOKENS):
            return func(*args, **kwargs)
        else:
            base_handler_instance.abort({"message": "Not authorized. Wrong token."}, UNAUTHORIZED_REQUEST_CODE)

    return wrapper
