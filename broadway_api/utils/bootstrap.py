import json
import jsonschema
import logging
import os
import uuid

from broadway_api.definitions import course_config
from broadway_api.daos import CourseDao
from broadway_api.models import Course

logger = logging.getLogger("bootstrap")


def initialize_cluster_token():
    if os.environ.get("BROADWAY_TOKEN"):
        logger.info("using cluster token from environment")
        token = os.environ.get("BROADWAY_TOKEN")
    else:
        logger.info("generating cluster token")
        token = str(uuid.uuid4())
        logger.info("cluster token is {}".format(token))
    return token


def initialize_course_tokens(settings, course_config_path):
    if course_config_path is None:
        logger.warning(
            "no course configuration specified, using existing configuration"
        )
        return

    with open(course_config_path) as course_config_file:
        courses = json.load(course_config_file)
    jsonschema.validate(courses, course_config)

    logger.info("course config found for {} courses".format(len(courses)))
    logger.info("dropping existing courses and loading new configuration")
    course_dao = CourseDao(settings)
    course_dao.drop_all()

    for course_id, tokens in courses.items():
        course = Course(id_=course_id, tokens=tokens)
        course_dao.insert_or_update(course)
