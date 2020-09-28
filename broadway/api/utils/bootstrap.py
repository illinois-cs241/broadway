import json
import jsonschema
import logging
import os
import sys
import uuid
import signal

from typing import Dict, Any

from pymongo import MongoClient
from pymongo.errors import ConnectionFailure

import tornado
import tornado.web
import tornado.ioloop
import tornado.httpserver

from logging.handlers import TimedRotatingFileHandler

from broadway.api.definitions import course_config
from broadway.api.daos import CourseDao, WorkerNodeDao
from broadway.api.models import Course
from broadway.api.utils.multiqueue import MultiQueue

import broadway.api.callbacks as callbacks
import broadway.api.handlers.client as client_handlers
import broadway.api.handlers.worker as worker_handlers
import broadway.api.handlers.worker_ws as worker_ws_handlers

logger = logging.getLogger(__name__)


def initialize_logger(flags: Dict[str, Any]):
    log_dir = flags["log_dir"]
    log_level = flags["log_level"]
    log_rotate = flags["log_rotate"]
    log_backup = flags["log_backup"]
    log_timestamps = flags["log_timestamps"]

    os.makedirs(log_dir, exist_ok=True)

    rotating_handler = TimedRotatingFileHandler(
        "{}/log".format(log_dir), when=log_rotate, backupCount=log_backup
    )

    if log_timestamps:
        format = "%(asctime)s %(levelname)s %(module)s.%(funcName)s: %(message)s"
    else:
        format = "%(levelname)s %(module)s.%(funcName)s: %(message)s"

    logging.basicConfig(
        handlers=[rotating_handler, logging.StreamHandler()],
        format=format,
        level=log_level,
    )

    # redirecting tornado logs to the file handler
    logging.getLogger("tornado").addHandler(rotating_handler)
    logging.getLogger("tornado").propagate = False


def initialize_global_settings(flags: Dict[str, Any]) -> Dict[str, Any]:
    if flags["token"] is None:
        flags["token"] = str(uuid.uuid4())
        logger.info("token not given, using {}".format(flags["token"]))

    return {"FLAGS": flags, "DB": None, "QUEUE": MultiQueue(), "WS_CONN_MAP": {}}


def initialize_course_tokens(settings: Dict[str, Any], flags: Dict[str, Any]):
    logger.info("initializing course config")

    if flags["course_config"] is None:
        logger.warning(
            "no course configuration specified, using existing configuration"
        )
        return

    with open(flags["course_config"]) as f:
        courses = json.load(f)

    jsonschema.validate(courses, course_config)

    logger.info("course config found for {} courses".format(len(courses)))
    logger.info("dropping existing courses and loading new configuration")

    course_dao = CourseDao(settings)
    course_dao.drop_all()

    for course_id, tokens in courses.items():
        course = Course(id_=course_id, tokens=tokens)
        course_dao.insert_or_update(course)


def initialize_database(settings: Dict[str, Any], flags: Dict[str, Any]):
    logger.info("initializing database")

    try:
        db_client = MongoClient(
            flags["mongodb_dsn"],
            serverSelectionTimeoutMS=flags["mongodb_timeout"] * 1000,
        )
        db_client.server_info()

        settings["DB"] = db_client

        dao = WorkerNodeDao(settings)
        logger.info("resetting ws worker nodes")
        dao.reset_worker_nodes()

    except ConnectionFailure as e:
        logger.critical("failed to connect to mongo server: {}".format(repr(e)))
        sys.exit(1)


def initialize_signal_handler(settings: Dict[str, Any], flags: Dict[str, Any]):
    logger.info("initializing signal handler")

    def shutdown():
        logger.info("shutting down")

        ioloop = tornado.ioloop.IOLoop.current()
        ioloop.add_callback(ioloop.stop)

    def handler(sig, frame):
        tornado.ioloop.IOLoop.current().add_callback_from_signal(shutdown)

    signal.signal(signal.SIGINT, handler)


def initialize_app(
    settings: Dict[str, Any], flags: Dict[str, Any]
) -> tornado.web.Application:
    id_regex = r"(?P<{}>[-\w0-9]+)"
    string_regex = r"(?P<{}>[^()]+)"

    app = tornado.web.Application(
        [
            # -------- Client Endpoints --------
            (
                r"/api/v1/grading_config/{}/{}".format(
                    id_regex.format("course_id"), id_regex.format("assignment_name")
                ),
                client_handlers.GradingConfigHandler,
            ),
            (
                r"/api/v1/grading_run/{}/{}".format(
                    id_regex.format("course_id"), id_regex.format("assignment_name")
                ),
                client_handlers.GradingRunHandler,
            ),
            (
                r"/api/v1/grading_run_status/{}/{}".format(
                    id_regex.format("course_id"), id_regex.format("run_id")
                ),
                client_handlers.GradingRunStatusHandler,
            ),
            (
                r"/api/v1/grading_job_log/{}/{}".format(
                    id_regex.format("course_id"), id_regex.format("job_id")
                ),
                client_handlers.GradingJobLogHandler,
            ),
            (
                r"/api/v1/worker/{}/{}".format(
                    id_regex.format("course_id"), string_regex.format("scope")
                ),
                client_handlers.CourseWorkerNodeHandler,
            ),
            (
                r"/api/v1/queue/{}/length".format(id_regex.format("course_id")),
                client_handlers.CourseQueueLengthHandler,
            ),
            (
                r"/api/v1/queue/{}/{}/position".format(
                    id_regex.format("course_id"), id_regex.format("run_id")
                ),
                client_handlers.GradingRunQueuePositionHandler,
            ),
            # ----------------------------------
            # ------- Worker Endpoints ---------
            (
                r"/api/v1/worker/{}".format(id_regex.format("worker_id")),
                worker_handlers.WorkerRegisterHandler,
            ),
            (
                r"/api/v1/grading_job/{}".format(id_regex.format("worker_id")),
                worker_handlers.GradingJobHandler,
            ),
            (
                r"/api/v1/heartbeat/{}".format(id_regex.format("worker_id")),
                worker_handlers.HeartBeatHandler,
            ),
            (
                r"/api/v1/worker_ws/{}".format(id_regex.format("worker_id")),
                worker_ws_handlers.WorkerConnectionHandler,
            )
            # ----------------------------------
        ],
        **settings
    )

    app.listen(flags["bind_port"], flags["bind_addr"])

    logger.info("app binded on {}:{}".format(flags["bind_addr"], flags["bind_port"]))

    # registering heartbeat callback
    tornado.ioloop.PeriodicCallback(
        lambda: callbacks.worker_heartbeat_callback(app.settings),
        flags["heartbeat_interval"] * 1000,
    ).start()

    return app
