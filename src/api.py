import logging
import os
import signal
from queue import Queue

import tornado
import tornado.ioloop
import tornado.web
from bson import ObjectId

import src.constants.constants as consts
import src.constants.db_keys as db_key
import src.handlers as handlers
from src.auth import initialize_token
from src.config import PORT, HEARTBEAT_INTERVAL
from src.config import GRADER_REGISTER_ENDPOINT, GRADING_JOB_ENDPOINT, GRADING_RUN_ENDPOINT, HEARTBEAT_ENDPOINT
from src.database import DatabaseResolver
from src.utilities import get_time, get_string_from_time

# setting up logger
os.makedirs(consts.LOGS_DIR, exist_ok=True)
logging.basicConfig(
    handlers=[
        logging.FileHandler('{}/{}.log'.format(consts.LOGS_DIR, get_string_from_time())),
        logging.StreamHandler()
    ],
    level=logging.INFO
)
logger = logging.getLogger()


def shutdown():
    # TODO notify all blocking calls that API is shutting down
    tornado.ioloop.IOLoop.current().stop()
    db_resolver = app.settings.get(consts.APP_DB)  # type: DatabaseResolver
    db_resolver.shutdown()


def signal_handler(sig, frame):
    tornado.ioloop.IOLoop.instance().add_callback_from_signal(shutdown)


def handle_lost_worker_node(worker_node, db_resolver):
    # type: (dict, DatabaseResolver) -> None
    logging.critical("Grader {} executing {} went offline unexpectedly".format(str(worker_node.get(db_key.ID)),
                                                                               worker_node.get(db_key.RUNNING_JOBS)))
    for job_id in worker_node.get(db_key.RUNNING_JOBS):
        job = db_resolver.get_grading_job_collection().find_one({db_key.ID: ObjectId(job_id)})
        db_resolver.get_grading_run_collection().update_one({db_key.ID: ObjectId(job.get(db_key.GRADING_RUN))},
                                                            {"$inc": {db_key.STUDENT_JOBS_LEFT: -1}})


def heartbeat_validator():
    """
    Checks if any of the grader machines went offline. It decides so if the grader machine has not sent any
    heartbeat in the past 2 X HEARTBEAT_INTERVAL seconds.
    """
    global app

    logging.info("Checking for any disconnected worker nodes")

    db_resolver = app.settings.get(consts.APP_DB)  # type: DatabaseResolver
    worker_nodes_collection = db_resolver.get_worker_node_collection()

    cur_time = get_time()
    for worker_node in worker_nodes_collection.find():  # type: dict
        last_seen_time = worker_node.get(db_key.LAST_SEEN)

        # the worker node dead if it does not send a heartbeat for 2 intervals
        if (cur_time - last_seen_time).total_seconds() >= 2 * HEARTBEAT_INTERVAL:
            handle_lost_worker_node(worker_node, db_resolver)
            worker_nodes_collection.delete_one({db_key.ID: worker_node.get(db_key.ID)})


def make_app(token, db_object):
    settings = {
        consts.APP_TOKEN: token,
        consts.APP_DB: db_object,
        consts.APP_QUEUE: Queue()
    }

    return tornado.web.Application([
        # ---------Client Endpoints---------
        # POST to add grading run
        (GRADING_RUN_ENDPOINT, handlers.AddGradingRunHandler),

        # POST to start grading run.
        # GET to get statuses of all jobs
        (r"{}/{}".format(GRADING_RUN_ENDPOINT, consts.ID_REGEX), handlers.GradingRunHandler),
        # ----------------------------------

        # --------Grader Endpoints-----------
        # GET to register node and get worked ID
        (GRADER_REGISTER_ENDPOINT, handlers.WorkerRegisterHandler),

        # GET to get a grading job
        (GRADING_JOB_ENDPOINT, handlers.GetGradingJobHandler),

        # POST to update status of job
        (r"{}/{}".format(GRADING_JOB_ENDPOINT, consts.ID_REGEX), handlers.UpdateGradingJobHandler),

        # POST to register heartbeat
        (HEARTBEAT_ENDPOINT, handlers.HeartBeatHandler),
        # ----------------------------------
    ], **settings)


if __name__ == "__main__":
    logger.info("initializing application")
    app = make_app(token=initialize_token(), db_object=DatabaseResolver())

    logger.info("listening on port {}".format(PORT))
    app.listen(PORT)

    signal.signal(signal.SIGINT, signal_handler)

    # Checks if any worker node disconnected every HEARTBEAT_INTERVAL seconds.
    tornado.ioloop.PeriodicCallback(heartbeat_validator, HEARTBEAT_INTERVAL * 1000).start()
    tornado.ioloop.IOLoop.instance().start()
