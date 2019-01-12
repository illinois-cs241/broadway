import json
import logging
import os
import signal
from logging.handlers import TimedRotatingFileHandler
from queue import Queue

import tornado
import tornado.ioloop
import tornado.web
from bson import ObjectId

import src.constants.constants as consts
import src.constants.db_keys as db_key
import src.constants.api_keys as api_key
from src.auth import initialize_cluster_token, configure_course_tokens
from src.config import PORT, HEARTBEAT_INTERVAL, LOGS_DIR, LOGS_ROTATE_WHEN, LOGS_BACKUP_COUNT, COURSES_CONFIG_FILE
from src.config import WORKER_REGISTER_ENDPOINT, GRADING_JOB_ENDPOINT, GRADING_CONFIG_ENDPOINT, GRADING_RUN_ENDPOINT, \
    HEARTBEAT_ENDPOINT
from src.database import DatabaseResolver
from src.handlers.client_handlers import AddGradingRunHandler, GradingRunHandler, GradingConfigHandler, StartGradingRunHandler
from src.handlers.worker_handlers import WorkerRegisterHandler, GradingJobHandler, HeartBeatHandler
from src.utilities import get_time, job_update_callback

# setting up logger
os.makedirs(LOGS_DIR, exist_ok=True)
logging.basicConfig(
    handlers=[
        TimedRotatingFileHandler('{}/log'.format(LOGS_DIR), when=LOGS_ROTATE_WHEN, backupCount=LOGS_BACKUP_COUNT),
        logging.StreamHandler()
    ],
    level=logging.INFO
)
logger = logging.getLogger()


def shutdown():
    tornado.ioloop.IOLoop.current().stop()


def signal_handler(sig, frame):
    tornado.ioloop.IOLoop.instance().add_callback_from_signal(shutdown)


def handle_lost_worker_node(db_resolver, worker_node):
    running_job_id = worker_node.get(db_key.RUNNING_JOB)
    worker_id = str(worker_node.get(db_key.ID))
    logger.critical("Worker with hostname {} and id {} executing {} went offline unexpectedly".format(
        worker_node.get(db_key.WORKER_HOSTNAME), worker_id, running_job_id))

    if running_job_id is None:
        return

    jobs_collection = db_resolver.get_grading_job_collection()
    job = jobs_collection.find_one({db_key.ID: ObjectId(running_job_id)})
    if job is None:
        logger.critical(
            "Job {} which could not be completed because worker went offline does not exist".format(running_job_id))
        return

    # update job: finished_at and result
    jobs_collection.update_one({db_key.ID: ObjectId(running_job_id)}, {
        "$set": {db_key.FINISHED: get_time(), db_key.SUCCESS: False,
                 db_key.RESULTS: [{"result": "Worker died while executing this job"}]}})

    tornado.ioloop.IOLoop.current().add_callback(job_update_callback, db_resolver, app.settings.get(consts.APP_QUEUE),
                                                 running_job_id, job.get(db_key.GRADING_RUN), False)


def heartbeat_validator(db_resolver):
    """
    Checks if any of the worker went offline. It decides so if the worker has not sent any
    heartbeat in the past 2 X HEARTBEAT_INTERVAL seconds.
    :type db_resolver: DatabaseResolver
    """
    worker_nodes_collection = db_resolver.get_worker_node_collection()

    cur_time = get_time()
    for worker_node in worker_nodes_collection.find():
        last_seen_time = worker_node.get(db_key.LAST_SEEN)

        # the worker node dead if it does not send a heartbeat for 2 intervals
        if (cur_time - last_seen_time).total_seconds() >= 2 * HEARTBEAT_INTERVAL:
            handle_lost_worker_node(db_resolver, worker_node)
            worker_nodes_collection.delete_one({db_key.ID: worker_node.get(db_key.ID)})


def make_app(cluster_token, db_resolver, course_tokens):
    if course_tokens:
        configure_course_tokens(db_resolver, course_tokens)

    settings = {
        consts.CLUSTER_TOKEN: cluster_token,
        consts.APP_DB: db_resolver,
        consts.APP_QUEUE: Queue()
    }

    return tornado.web.Application([
        # -------- Client Endpoints --------
        # POST to add config for assignment
        (r"{}/{}/{}".format(GRADING_CONFIG_ENDPOINT, consts.ID_REGEX.format(api_key.COURSE_ID_PARAM),
                            consts.ID_REGEX.format(api_key.ASSIGNMENT_NAME_PARAM)), GradingConfigHandler),

        (r"{}/{}/{}".format(GRADING_RUN_ENDPOINT, consts.ID_REGEX.format(api_key.COURSE_ID_PARAM),
                            consts.ID_REGEX.format(api_key.ASSIGNMENT_NAME_PARAM)), StartGradingRunHandler),

        # POST to add grading run
        (GRADING_RUN_ENDPOINT, AddGradingRunHandler),

        # POST to start grading run.
        # GET to get statuses of all jobs
        (r"{}/{}".format(GRADING_RUN_ENDPOINT, consts.HEX_REGEX.format("grading_run_id")), GradingRunHandler),
        # ----------------------------------

        # ------- Worker Endpoints ---------
        # GET to register node and get worked ID
        (r"{}/{}".format(WORKER_REGISTER_ENDPOINT, consts.STRING_REGEX.format(api_key.HOSTNAME_PARAM)),
         WorkerRegisterHandler),

        # GET to get a grading job
        # POST to update status of job
        (r"{}/{}".format(GRADING_JOB_ENDPOINT, consts.HEX_REGEX.format(api_key.WORKER_ID_PARAM)), GradingJobHandler),

        # POST to register heartbeat
        (r"{}/{}".format(HEARTBEAT_ENDPOINT, consts.HEX_REGEX.format(api_key.WORKER_ID_PARAM)), HeartBeatHandler),
        # ----------------------------------
    ], **settings)


if __name__ == "__main__":
    logger.info("initializing application")
    if os.path.isfile(COURSES_CONFIG_FILE):
        logger.info("Initializing courses and tokens. Overwriting previous DB contents.")
        with open(COURSES_CONFIG_FILE) as courses_config_file:
            courses = json.load(courses_config_file)
    else:
        logger.info("No courses config file found. Retaining previous DB contents.")
        courses = None

    db_object = DatabaseResolver()
    app = make_app(cluster_token=initialize_cluster_token(), db_resolver=db_object, course_tokens=courses)

    logger.info("listening on port {}".format(PORT))
    app.listen(PORT)

    signal.signal(signal.SIGINT, signal_handler)

    # Checks if any worker node disconnected every HEARTBEAT_INTERVAL seconds.
    tornado.ioloop.PeriodicCallback(lambda: heartbeat_validator(db_object), HEARTBEAT_INTERVAL * 1000).start()
    tornado.ioloop.IOLoop.instance().start()

    # now that we have stopped serving requests, shutdown DB
    db_object.shutdown()
