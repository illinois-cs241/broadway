import json
import logging
import os
import signal
import sys
import time
from concurrent.futures import ThreadPoolExecutor, wait, FIRST_COMPLETED
from logging.handlers import TimedRotatingFileHandler
from subprocess import Popen, PIPE

from tornado import httpclient, escape

import api_keys as api_key
from config import GRADER_REGISTER_ENDPOINT, HEARTBEAT_ENDPOINT, GRADING_JOB_ENDPOINT, HEARTBEAT_INTERVAL, \
    JOB_POLL_INTERVAL
from config import LOGS_DIR, GRADING_RUN_RES_FILE, QUEUE_EMPTY_CODE
from utils import get_time, get_url, get_header, print_usage

# globals
worker_id = None
worker_thread = None
running = True

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
    global running
    running = False


def heartbeat_routine():
    http_client = httpclient.HTTPClient()
    heartbeat_request = httpclient.HTTPRequest(get_url(HEARTBEAT_ENDPOINT),
                                               headers=get_header(sys.argv[1], worker_id), method="POST", body="")

    while running:
        try:
            http_client.fetch(heartbeat_request)
            time.sleep(HEARTBEAT_INTERVAL)
        except httpclient.HTTPClientError as e:
            logger.critical("Heartbeat failed!\nError: {}".format(e.response.body.decode('utf-8')))
            return
    http_client.close()


def worker_routine():
    http_client = httpclient.HTTPClient()
    job_request = httpclient.HTTPRequest(get_url(GRADING_JOB_ENDPOINT), headers=get_header(sys.argv[1], worker_id),
                                         method="GET")

    while running:
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
        job_id = job.get(api_key.JOB_ID)
        logger.info("Starting job {}".format(job_id))

        # execute the job runner with job as json string
        docker_runner = Popen(['node', 'src/jobRunner.js', json.dumps(job), GRADING_RUN_RES_FILE], stderr=PIPE,
                              stdout=PIPE)
        container_stdout, container_stderr = docker_runner.communicate()
        logger.info("Finished job {}".format(job_id))

        # send back the results to the server
        with open(GRADING_RUN_RES_FILE) as res_file:
            grading_job_result = json.load(res_file)

        assert api_key.RESULTS in grading_job_result
        assert api_key.SUCCESS in grading_job_result
        grading_job_result[api_key.LOGS] = {'stdout': escape.to_basestring(container_stdout),
                                            'stderr': escape.to_basestring(container_stderr)}
        update_request = httpclient.HTTPRequest("{}/{}".format(get_url(GRADING_JOB_ENDPOINT), job_id),
                                                headers=get_header(sys.argv[1], worker_id), method="POST",
                                                body=json.dumps(grading_job_result))

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
    global running

    http_client = httpclient.HTTPClient()
    req = httpclient.HTTPRequest(get_url(GRADER_REGISTER_ENDPOINT), headers=get_header(sys.argv[1]), method="GET")

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
        running = False
        exit(-1)

    http_client.close()


if __name__ == "__main__":
    # check valid usage
    if len(sys.argv) != 2:
        print_usage()
        exit(-1)

    signal.signal(signal.SIGINT, signal_handler)

    # register node to server
    register_node()

    # run the grader on two separate threads. If any of the routines fail, the grader shuts down
    executor = ThreadPoolExecutor(max_workers=2)
    futures = [executor.submit(heartbeat_routine), executor.submit(worker_routine)]
    wait(futures, return_when=FIRST_COMPLETED)
    running = False
    executor.shutdown()
