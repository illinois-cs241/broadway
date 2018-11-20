import logging
import os
import signal

import tornado
import tornado.ioloop
import tornado.web

import src.constants as consts
import src.handlers as handlers
from src.auth import initialize_token
from src.config import PORT, HEARTBEAT_INTERVAL
from src.constants import LOGS_DIR, ID_REGEX
from src.database import DatabaseResolver
from utils.utilities import get_time, get_string_from_time, get_time_from_string

# setting up logger
os.makedirs(LOGS_DIR, exist_ok=True)
logging.basicConfig(
    handlers=[
        logging.FileHandler('{}/{}.log'.format(LOGS_DIR, get_string_from_time())),
        logging.StreamHandler()
    ],
    level=logging.INFO
)
logger = logging.getLogger()


def shutdown():
    # TODO notify all blocking calls that API is shutting down
    tornado.ioloop.IOLoop.current().stop()
    db_resolver = app.settings.get("db_object")  # type: DatabaseResolver
    db_resolver.shutdown()


def signal_handler(sig, frame):
    tornado.ioloop.IOLoop.instance().add_callback_from_signal(shutdown)


# TODO
def handle_lost_worker_node(worker_node, db_resolver):
    # type: (dict, DatabaseResolver) -> None
    pass


def heartbeat_validator():
    """
    Checks if any of the grader machines went offline. It decides so if the grader machine has not sent any
    heartbeat in the past 2 X HEARTBEAT_INTERVAL seconds.
    """
    global app

    logging.info("Checking for any disconnected worker nodes")

    db_resolver = app.settings.get("db_object")  # type: DatabaseResolver
    worker_nodes_collection = db_resolver.get_worker_node_collection()

    cur_time = get_time()
    for worker_node in worker_nodes_collection.find():  # type: dict
        last_seen_time = get_time_from_string(worker_node.get(consts.LAST_SEEN_KEY))

        # the worker node dead if it does not send a heartbeat for 2 intervals
        if (cur_time - last_seen_time).total_seconds() >= 2 * HEARTBEAT_INTERVAL:
            handle_lost_worker_node(worker_node, db_resolver)
            worker_nodes_collection.delete_one({consts.ID_KEY: worker_node.get(consts.ID_KEY)})


def make_app(token, db_object):
    return tornado.web.Application([
        # ---------Client Endpoints---------
        # POST to add grading run
        (r"/api/v1/grading_run", handlers.AddGradingRunHandler),

        # POST to start grading run.
        # GET to get statuses of all jobs
        (r"/api/v1/grading_run/{}".format(ID_REGEX), handlers.GradingRunHandler),
        # ----------------------------------

        # -----Grader Endpoints--------
        # GET to register node and get worked ID
        (r"/api/v1/worker_register", handlers.WorkerRegisterHandler),

        # GET to get a grading job
        (r"/api/v1/grading_job", handlers.GetGradingJobHandler),

        # POST to update status of job
        (r"/api/v1/grading_job/{}".format(ID_REGEX), handlers.UpdateGradingJobHandler),

        # POST to register heartbeat
        (r"/api/v1/heartbeat", handlers.HeartBeatHandler),
        # ----------------------------------
    ], token=token, db_object=db_object)


if __name__ == "__main__":
    logger.info("initializing application")
    app = make_app(token=initialize_token(), db_object=DatabaseResolver())

    logger.info("listening on port {}".format(PORT))
    app.listen(PORT)

    signal.signal(signal.SIGINT, signal_handler)

    # Checks if any worker node disconnected every HEARTBEAT_INTERVAL seconds.
    tornado.ioloop.PeriodicCallback(heartbeat_validator, HEARTBEAT_INTERVAL * 1000).start()
    tornado.ioloop.IOLoop.instance().start()
