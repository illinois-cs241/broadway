import logging

from broadway_api.daos import AssignmentConfigDao, CourseDao, WorkerNodeDao

logger = logging.getLogger("auth")


def _is_token_valid(token):
    return (
        (token is not None)
        and token.startswith("Bearer ")
        and len(token.split(" ")) == 2
    )


def authenticate_worker(func):
    def wrapper(*args, **kwargs):
        handler = args[0]
        worker_id = kwargs.get("worker_id")

        dao = WorkerNodeDao(handler.settings)

        worker = dao.find_by_id(worker_id)

        if worker is None:
            handler.abort({"message": "worker not found"}, status=401)
            return

        return func(*args, **kwargs)

    return wrapper


def validate_assignment(func):
    def wrapper(*args, **kwargs):
        handler = args[0]
        course_id = kwargs.get("course_id")
        assignment_name = kwargs.get("assignment_name")
        assignment_id = AssignmentConfigDao.id_from(course_id, assignment_name)

        dao = AssignmentConfigDao(handler.settings)
        if dao.find_by_id(assignment_id) is None:
            handler.abort({"message": "assignment not found"}, status=401)
            return
        return func(*args, **kwargs)

    return wrapper


def authenticate_cluster_token(func):
    def wrapper(*args, **kwargs):
        handler = args[0]
        expected_token = handler.get_token()
        request_token = handler.request.headers.get("Authorization")

        if not _is_token_valid(request_token):
            handler.abort({"message": "invalid token format"}, status=401)
            return
        elif expected_token != request_token.split(" ")[1]:
            handler.abort({"message": "invalid token"}, status=401)
            return
        return func(*args, **kwargs)

    return wrapper


def authenticate_course(func):
    def wrapper(*args, **kwargs):
        handler = args[0]

        request_token = handler.request.headers.get("Authorization")
        if not _is_token_valid(request_token):
            handler.abort({"message": "invalid token format"}, status=401)
            return

        request_token = request_token.split(" ")[1]
        course_id = kwargs.get("course_id")

        dao = CourseDao(handler.settings)
        course = dao.find_by_id(course_id)
        if course is None:
            handler.abort({"message": "course not found"}, status=401)
            return

        if request_token not in course.tokens:
            handler.abort({"message": "invalid token"}, status=401)
            return
        return func(*args, **kwargs)

    return wrapper


__all__ = [
    "authenticate_cluster_token",
    "authenticate_course",
    "authenticate_worker",
    "validate_assignment",
]
