import logging
import os
import uuid

import tornado
import tornado.ioloop
import tornado.web

from src.settings import LOGS_DIR, ID_REGEX, PORT
from utils.utilities import get_formatted_time
from src.database import DatabaseResolver
import src.handlers as handlers

# setting up logger
os.makedirs(LOGS_DIR, exist_ok=True)
logging.basicConfig(
    handlers=[
        logging.FileHandler('{}/{}.log'.format(LOGS_DIR, get_formatted_time())),
        logging.StreamHandler()
    ],
    level=logging.INFO
)
logger = logging.getLogger()


def make_app(token, db_resolver):
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
    ], cluster_token=token, db_object=db_resolver)


if __name__ == "__main__":
    if os.environ.get("WARDEN_TOKEN"):
        logger.debug("using authentication token from environment")
        token = os.environ.get("WARDEN_TOKEN")
    else:
        logger.debug("generating authentication token")
        token = str(uuid.uuid4())
        logger.info("authentication token is {}".format(token))

    logger.debug("initializing application")
    app = make_app(token=token, db_resolver=DatabaseResolver())

    logger.info("listening on port {}".format(PORT))
    app.listen(PORT)

    try:
        tornado.ioloop.IOLoop.instance().start()
    except KeyboardInterrupt:
        tornado.ioloop.IOLoop.current().stop()
