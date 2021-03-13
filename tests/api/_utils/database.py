from broadway.api.models.course import Course
from broadway.api.daos.course import CourseDao


def initialize_db(settings, course_config):
    course_dao = CourseDao(settings)
    for course_id, course in course_config.items():
        course = Course(
            id_=course_id,
            tokens=course["tokens"],
            query_tokens=course.get("query_tokens", []),
        )
        course_dao.insert_or_update(course)


def clear_db(settings):
    db = settings["DB"]
    config = settings["FLAGS"]
    db.drop_database(config["mongodb_primary"])
    db.drop_database(config["mongodb_logs"])
