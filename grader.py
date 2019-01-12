import json
import logging
import os
import signal
import sys
import time
import socket
from concurrent.futures import ThreadPoolExecutor, wait, FIRST_COMPLETED
from logging.handlers import TimedRotatingFileHandler
from subprocess import Popen, PIPE

from tornado import httpclient, escape

import api_keys as api_key
from config import GRADER_REGISTER_ENDPOINT, HEARTBEAT_ENDPOINT, GRADING_JOB_ENDPOINT, HEARTBEAT_INTERVAL, \
    JOB_POLL_INTERVAL, VERBOSE
from config import LOGS_DIR, GRADING_RUN_RES_FILE, QUEUE_EMPTY_CODE
from utils import get_time, get_url, print_usage

# globals
worker_id = None
worker_thread = None
heartbeat_running = True
worker_running = True

# setting up logger
os.makedirs(LOGS_DIR, exist_ok=True)
logging.basicConfig(
    handlers=[
        TimedRotatingFileHandler('{}/log'.format(LOGS_DIR), when='midnight', backupCount=7),
        logging.StreamHandler()
    ],
    level=logging.INFO
)
logger = logging.getLogger()


def signal_handler(sig, frame):
    global worker_running
    worker_running = False


def heartbeat_routine():
    http_client = httpclient.HTTPClient()
    heartbeat_request = httpclient.HTTPRequest(get_url("{}/{}".format(HEARTBEAT_ENDPOINT, worker_id)), headers=header,
                                               method="POST", body="")

    while heartbeat_running:
        try:
            http_client.fetch(heartbeat_request)
            time.sleep(HEARTBEAT_INTERVAL)
        except httpclient.HTTPClientError as e:
            logger.critical("Heartbeat failed!\nError: {}".format(e.response.body.decode('utf-8')))
            return
    http_client.close()


def worker_routine():
    http_client = httpclient.HTTPClient()
    job_request = httpclient.HTTPRequest(get_url("{}/{}".format(GRADING_JOB_ENDPOINT, worker_id)), method="GET",
                                         headers=header)

    while worker_running:
        # poll from queue
        try:
            response = http_client.fetch(job_request)
        except httpclient.HTTPClientError as e:
            if e.code == QUEUE_EMPTY_CODE:
                time.sleep(JOB_POLL_INTERVAL)
                continue
            else:
                logger.critical("Bad server response while trying to poll job.")
                if e.response.body is not None:
                    logger.critical("Error: {}".format(e.response.body.decode('utf-8')))
                return

        # we successfully polled a job
        job = json.loads(response.body.decode('utf-8')).get('data')
        job_id = job.get(api_key.GRADING_JOB_ID)
        logger.info("Starting job {}".format(job_id))

        # execute the job runner with job as json string
        docker_runner = Popen(['node', 'src/jobRunner.js', json.dumps(job), GRADING_RUN_RES_FILE], stderr=PIPE,
                              stdout=PIPE, universal_newlines=True)
        containers_stdout, containers_stderr = docker_runner.communicate()
        logger.info("Finished job {}".format(job_id))
        if VERBOSE:
            print("Job stdout:\n{}".format(containers_stdout))
            print("Job stderr:\n{}".format(containers_stderr))

        # send back the results to the server
        if not os.path.isfile(GRADING_RUN_RES_FILE):
            logger.critical("Grading run result file did not get generated.")
            return

        with open(GRADING_RUN_RES_FILE) as res_file:
            grading_job_result = json.load(res_file)

        assert api_key.RESULTS in grading_job_result
        assert api_key.SUCCESS in grading_job_result
        grading_job_result[api_key.LOGS] = {'stdout': containers_stdout,
                                            'stderr': containers_stderr}
        grading_job_result[api_key.GRADING_JOB_ID] = job_id
        update_request = httpclient.HTTPRequest(get_url("{}/{}".format(GRADING_JOB_ENDPOINT, worker_id)),
                                                headers=header, method="POST", body=json.dumps(grading_job_result))

        try:
            logger.info("Sending job results")
            http_client.fetch(update_request)
        except httpclient.HTTPClientError as e:
            logger.critical("Bad server response while updating about job status.\nError: {}".format(
                e.response.body.decode('utf-8')))
            return

    http_client.close()


def register_node():
    global worker_id
    global worker_running
    global heartbeat_running

    http_client = httpclient.HTTPClient()
    req = httpclient.HTTPRequest(get_url("{}/{}".format(GRADER_REGISTER_ENDPOINT, socket.gethostname())),
                                 headers=header, method="GET")

    try:
        response = http_client.fetch(req)
        logger.info("Registered to server at {}".format(get_time()))
        server_response = json.loads(response.body.decode('utf-8')).get('data')

        # read worker id
        if api_key.WORKER_ID in server_response:
            worker_id = server_response.get(api_key.WORKER_ID)
        else:
            logger.critical("Bad server response on registration. Missing argument \'{}\'.".format(api_key.WORKER_ID))
            raise Exception("Invalid response")
    except Exception as e:
        logger.critical("Registration failed!\nError: {}".format(str(e)))
        worker_running = False
        heartbeat_running = False
        exit(-1)

    http_client.close()


if __name__ == "__main__":
    # check valid usage
    if len(sys.argv) != 2:
        print_usage()
        exit(-1)

    signal.signal(signal.SIGINT, signal_handler)

    # register node to server
    header = {api_key.AUTH: "Bearer {}".format(sys.argv[1])}
    register_node()

    # run the grader on two separate threads. If any of the routines fail, the grader shuts down
    executor = ThreadPoolExecutor(max_workers=2)
    futures = [executor.submit(heartbeat_routine), executor.submit(worker_routine)]
    wait(futures, return_when=FIRST_COMPLETED)
    worker_running = False
    heartbeat_running = False
    executor.shutdown()
