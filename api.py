#!/usr/bin/env python3
import argparse
import os
import logging
import tornado
import tornado.web
import tornado.ioloop
import tornado.httpserver
import signal
import sys

import broadway_api.callbacks as callbacks
import broadway_api.handlers.client as client_handlers
import broadway_api.handlers.worker as worker_handlers
from broadway_api.utils.bootstrap import (
    initialize_cluster_token,
    initialize_course_tokens,
)

from logging.handlers import TimedRotatingFileHandler
from pymongo import MongoClient
from queue import Queue

import config

os.makedirs(config.LOGS_DIR, exist_ok=True)
logging.basicConfig(
    handlers=[
        TimedRotatingFileHandler(
            "{}/log".format(config.LOGS_DIR),
            when=config.LOGS_ROTATE_WHEN,
            backupCount=config.LOGS_BACKUP_COUNT,
        ),
        logging.StreamHandler(),
    ],
    level=logging.INFO if not config.DEBUG else logging.DEBUG,
)


logger = logging.getLogger("api")


def parse_args():
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--course-config",
        help="A JSON file that specifies the courses and their tokens",
    )
    parser.add_argument(
        "--https", action="store_true", help="Use HTTPS to serve requests"
    )
    parser.add_argument("--ssl-certificate", help="Path to the SSL certificate")
    parser.add_argument("--ssl-key", help="Path to the SSL private key file")
    return parser.parse_args()


def initialize_app():
    config_keys = filter(lambda x: not x.startswith("_"), config.__dict__)
    settings = {
        "CLUSTER_TOKEN": initialize_cluster_token(),
        "CONFIG": dict((k, config.__dict__[k]) for k in config_keys),
        "DB": MongoClient(),
        "QUEUE": Queue(),
    }

    id_regex = r"(?P<{}>[-\w0-9]+)"
    string_regex = r"(?P<{}>[^()]+)"

    return tornado.web.Application(
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
            # ----------------------------------
        ],
        **settings
    )


def signal_handler(sig, frame):
    tornado.ioloop.IOLoop.instance().add_callback_from_signal(shutdown)


def shutdown():
    tornado.ioloop.IOLoop.current().stop()


if __name__ == "__main__":
    args = parse_args()
    if args.https and not (args.ssl_certificate and args.ssl_key):
        logger.critical("SSL certificate and key must be specified with HTTPS")
        sys.exit(1)
    if args.course_config and not os.path.isfile(args.course_config):
        logger.critical("course config at '{}' not found".format(args.course_config))
        sys.exit(1)

    logger.info("bootstrapping application")
    app = initialize_app()
    initialize_course_tokens(app.settings, args.course_config)

    logger.info("getting ready to serve")
    if args.https:
        http_server = tornado.httpserver.HTTPServer(
            app, ssl_options={"certfile": args.ssl_certificate, "keyfile": args.ssl_key}
        )
        http_server.listen(config.PORT)
        logger.info("listening on port {} (HTTPS)".format(config.PORT))
    else:
        app.listen(config.PORT)
        logger.info("listening on port {} (HTTP)".format(config.PORT))

    logger.info("registering application callbacks")
    tornado.ioloop.PeriodicCallback(
        lambda: callbacks.worker_heartbeat_callback(app.settings),
        config.HEARTBEAT_INTERVAL * 1000,
    ).start()

    logger.info("ready to serve")
    signal.signal(signal.SIGINT, signal_handler)
    tornado.ioloop.IOLoop.instance().start()
