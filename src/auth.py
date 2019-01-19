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
    The courses and their tokens have a many-to-many relationship. A course can own multiple tokens (for instance the
    course has multiple services pinging the API and wants to keep their tokens independent) and a token can be
    owned by multiple courses (there can be many versions of a course which register as separate courses but use the
    same token).

    This function aims to understand that relationship and feed it into the DB in the desired format.
    Example config:
    {
       "tokens":{
          "token1":"abcd",
          "token2":"efgh"
       },
       "courses":{
          "cs241-honors":[
             "token1"
          ],
          "cs241":[
             "token1",
             "token2"
          ]
       }
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

    tokens_collection = db_resolver.get_token_collection()
    tokens_collection.drop()

    token_name_to_id = {}
    for token_name in course_tokens.get(consts.CONFIG_TOKENS, {}):
        token_name_to_id[token_name] = str(
            tokens_collection.insert_one({key.TOKEN: course_tokens[consts.CONFIG_TOKENS][token_name]}).inserted_id)

    for course_id in course_tokens.get(consts.CONFIG_COURSES, {}):
        course = {key.ID: course_id, key.TOKEN_IDS: []}
        for token_name in course_tokens[consts.CONFIG_COURSES][course_id]:
            if token_name not in token_name_to_id:
                logger.critical("Token name {} does not exist in course tokens config".format(token_name))
                raise KeyError

            course[key.TOKEN_IDS].append(token_name_to_id[token_name])

        courses_collection.insert_one(course)


def authenticate_worker(func):
    def wrapper(*args, **kwargs):
        base_handler_instance = args[0]
        worker_id = kwargs.get(key.WORKER_ID_PARAM)
        if base_handler_instance.get_worker_node(worker_id) is None:
            # get_worker_node() internally aborts the request so return
            return
        else:
            return func(*args, **kwargs)

    return wrapper


def validate_assignment(func):
    def wrapper(*args, **kwargs):
        base_handler_instance = args[0]
        course_id = kwargs.get(key.COURSE_ID_PARAM)
        assignment_name = kwargs.get(key.ASSIGNMENT_NAME_PARAM)
        assignment_id = "{}/{}".format(course_id, assignment_name)
        assignment = base_handler_instance.get_db().get_assignment_collection().find_one({key.ID: assignment_id})
        if assignment is None:
            base_handler_instance.abort(
                {"message": "Course {} has not uploaded a config for assignment {}".format(course_id, assignment_name)},
                BAD_REQUEST_CODE)
            return
        else:
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
        course = base_handler_instance.get_course(course_id)
        if course is None:
            # get_course() internally aborts the request so return
            return

        for token_id in course.get(key.TOKEN_IDS):
            cur_token = base_handler_instance.get_token(token_id).get(key.TOKEN)
            if cur_token == request_token:
                return func(*args, **kwargs)

        base_handler_instance.abort({"message": "Not authorized. Wrong token."}, UNAUTHORIZED_REQUEST_CODE)

    return wrapper
