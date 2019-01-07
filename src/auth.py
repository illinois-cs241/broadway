import logging
import os
import uuid

import jsonschema

import src.constants.api_keys as api_key
import src.constants.db_keys as db_key
import src.constants.constants as consts
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
    Creates course and token documents in the DB correctly and links them together.

    :param db_resolver: DatabaseResolver object
    :type db_resolver: DatabaseResolver
    :param course_tokens: object containing the configuration of courses and their tokens
    :type course_tokens: dict
    """
    # Expected format specified below
    jsonschema.validate(course_tokens, {
        "type": "object",
        "properties": {
            consts.CONFIG_TOKENS: {"type": "object",
                                   "patternProperties": {"": {"type": "string"}}},
            consts.CONFIG_COURSES: {"type": "object",
                                    "patternProperties": {
                                        "": {"type": "array", "items": {"type": "string"}}}}
        },
        "additionalProperties": False
    })

    courses_collection = db_resolver.get_course_collection()
    courses_collection.drop()

    tokens_collection = db_resolver.get_token_collection()
    tokens_collection.drop()

    token_name_to_id = {}
    for token_name in course_tokens.get(consts.CONFIG_TOKENS, {}):
        token_name_to_id[token_name] = str(
            tokens_collection.insert_one({db_key.TOKEN: course_tokens[consts.CONFIG_TOKENS][token_name]}).inserted_id)

    for course_id in course_tokens.get(consts.CONFIG_COURSES, {}):
        course = {db_key.ID: course_id, db_key.TOKEN_IDS: []}
        for token_name in course_tokens[consts.CONFIG_COURSES][course_id]:
            if token_name not in token_name_to_id:
                logger.critical("Token name {} does not exist in course tokens config".format(token_name))
                raise KeyError

            course[db_key.TOKEN_IDS].append(token_name_to_id[token_name])

        courses_collection.insert_one(course)


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
