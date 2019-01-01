import json
import logging
import os
import signal
from logging.handlers import TimedRotatingFileHandler
from queue import Queue
from threading import Thread, Condition

import tornado
import tornado.ioloop
import tornado.web
from tornado import httpclient

import src.constants.api_keys as api_key
import src.constants.constants as consts
import src.constants.db_keys as db_key
from src.auth import initialize_token
from src.config import WORKER_REGISTER_ENDPOINT, GRADING_JOB_ENDPOINT, GRADING_RUN_ENDPOINT, HEARTBEAT_ENDPOINT
from src.config import PORT, HEARTBEAT_INTERVAL, LOGS_DIR, LOGS_ROTATE_WHEN, LOGS_BACKUP_COUNT
from src.database import DatabaseResolver
from src.handlers.client_handler import AddGradingRunHandler, GradingRunHandler
from src.handlers.worker_handler import WorkerRegisterHandler, GradingJobHandler, HeartBeatHandler
from src.utilities import get_time

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
heartbeat_running = True
heartbeat_cv = Condition()


def shutdown():
    global heartbeat_running
    heartbeat_running = False

    tornado.ioloop.IOLoop.current().stop()
    db_resolver = app.settings.get(consts.APP_DB)  # type: DatabaseResolver
    db_resolver.shutdown()


def signal_handler(sig, frame):
    tornado.ioloop.IOLoop.instance().add_callback_from_signal(shutdown)


def handle_lost_worker_node(worker_node):
    running_job_id = worker_node.get(db_key.RUNNING_JOB)
    worker_id = str(worker_node.get(db_key.ID))
    logger.critical("Worker with hostname {} and id {} executing {} went offline unexpectedly".format(
        worker_node.get(db_key.WORKER_HOSTNAME), worker_id, running_job_id))

    if running_job_id is None:
        return

    # Make a fake update request on behalf of the dead worker so that the job is marked as failed and the rest of the
    # jobs for the grading run can be handled as expected and scheduled in the right order
    http_client = httpclient.HTTPClient()
    res = {api_key.JOB_ID: running_job_id, api_key.SUCCESS: False,
           api_key.RESULTS: [{"result": "Worker died while executing this job"}],
           api_key.LOGS: {"logs": "No logs available for this job since the worker died while executing this job"}}
    update_request = httpclient.HTTPRequest(
        "http://localhost:{}{}/{}".format(PORT, GRADING_JOB_ENDPOINT, worker_id),
        headers={api_key.AUTH: cluster_token},
        method="POST", body=json.dumps(res))
    http_client.fetch(update_request)
    http_client.close()


def heartbeat_validator():
    """
    Checks if any of the worker went offline. It decides so if the worker has not sent any
    heartbeat in the past 2 X HEARTBEAT_INTERVAL seconds.
    """
    global heartbeat_running

    while heartbeat_running:
        db_resolver = app.settings.get(consts.APP_DB)  # type: DatabaseResolver
        worker_nodes_collection = db_resolver.get_worker_node_collection()

        cur_time = get_time()
        for worker_node in worker_nodes_collection.find():  # type: dict
            last_seen_time = worker_node.get(db_key.LAST_SEEN)

            # the worker node dead if it does not send a heartbeat for 2 intervals
            if (cur_time - last_seen_time).total_seconds() >= 2 * HEARTBEAT_INTERVAL:
                handle_lost_worker_node(worker_node)
                worker_nodes_collection.delete_one({db_key.ID: worker_node.get(db_key.ID)})

        heartbeat_cv.acquire()
        heartbeat_cv.wait(HEARTBEAT_INTERVAL)
        heartbeat_cv.release()


def make_app(token, db_object):
    settings = {
        consts.APP_TOKEN: token,
        consts.APP_DB: db_object,
        consts.APP_QUEUE: Queue()
    }

    return tornado.web.Application([
        # -------- Client Endpoints --------
        # POST to add grading run
        (GRADING_RUN_ENDPOINT, AddGradingRunHandler),

        # POST to start grading run.
        # GET to get statuses of all jobs
        (r"{}/{}".format(GRADING_RUN_ENDPOINT, consts.HEX_REGEX.format("grading_run_id")), GradingRunHandler),
        # ----------------------------------

        # ------- Worker Endpoints ---------
        # GET to register node and get worked ID
        (r"{}/{}".format(WORKER_REGISTER_ENDPOINT, consts.STRING_REGEX.format("hostname")), WorkerRegisterHandler),

        # GET to get a grading job
        # POST to update status of job
        (r"{}/{}".format(GRADING_JOB_ENDPOINT, consts.HEX_REGEX.format("worker_id")), GradingJobHandler),

        # POST to register heartbeat
        (r"{}/{}".format(HEARTBEAT_ENDPOINT, consts.HEX_REGEX.format("worker_id")), HeartBeatHandler),
        # ----------------------------------
    ], **settings)


if __name__ == "__main__":
    logger.info("initializing application")
    cluster_token = initialize_token()
    app = make_app(token=cluster_token, db_object=DatabaseResolver())

    logger.info("listening on port {}".format(PORT))
    app.listen(PORT)

    signal.signal(signal.SIGINT, signal_handler)

    # Checks if any worker node disconnected every HEARTBEAT_INTERVAL seconds.
    heartbeat_thread = Thread(target=heartbeat_validator)
    heartbeat_thread.start()

    # start event loop
    tornado.ioloop.IOLoop.instance().start()

    # shutdown heartbeat thread cleanly
    heartbeat_cv.acquire()
    heartbeat_cv.notify_all()
    heartbeat_cv.release()
    heartbeat_thread.join()
