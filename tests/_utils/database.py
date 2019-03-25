from broadway_api.models.course import Course
from broadway_api.daos.course import CourseDao


def initialize_db(settings, course_config):
    course_dao = CourseDao(settings)
    for course_id, tokens in course_config.items():
        course = Course(id_=course_id, tokens=tokens)
        course_dao.insert_or_update(course)


def clear_db(settings):
    config = settings["CONFIG"]
    db = settings["DB"]
    db.drop_database(config["DB_PRIMARY"])
    db.drop_database(config["DB_LOGS"])
