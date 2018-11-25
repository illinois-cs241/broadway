import asyncio
import json
import logging
import os
import sys
from subprocess import PIPE, Popen

from tornado import httpclient, gen, escape

import api_keys as api_key
from config import GRADER_REGISTER_ENDPOINT, HEARTBEAT_ENDPOINT, GRADING_JOB_ENDPOINT
from config import LOGS_DIR, QUEUE_EMPTY_CODE
from utils import get_time, get_url, get_header, print_usage

# globals
worker_id = None
worker_thread = None
heartbeat = True

# setting up logger
os.makedirs(LOGS_DIR, exist_ok=True)
logging.basicConfig(
    handlers=[
        logging.FileHandler(
            '{}/{}.log'.format(LOGS_DIR, get_time())),
        logging.StreamHandler()
    ],
    level=logging.INFO
)
logger = logging.getLogger()


@gen.coroutine
def heartbeat_routine():
    while heartbeat:
        http_client = httpclient.AsyncHTTPClient()
        heartbeat_request = httpclient.HTTPRequest(get_url(HEARTBEAT_ENDPOINT),
                                                   headers=get_header(sys.argv[1], worker_id), method="POST", body="")
        try:
            yield http_client.fetch(heartbeat_request)
            logger.info("Send heartbeat")
            yield asyncio.sleep(HEARTBEAT_INTERVAL)
        except httpclient.HTTPClientError as e:
            logger.critical("Heartbeat failed!\nError: {}".format(e.response.body.decode('utf-8')))
            exit(-1)

        http_client.close()


@gen.coroutine
def worker_routine():
    while True:
        http_client = httpclient.AsyncHTTPClient()
        job_request = httpclient.HTTPRequest(get_url(GRADING_JOB_ENDPOINT), headers=get_header(sys.argv[1], worker_id),
                                             method="GET")

        # poll from queue
        try:
            response = yield http_client.fetch(job_request)
        except httpclient.HTTPClientError as e:
            if e.code == QUEUE_EMPTY_CODE:
                logger.info("Queue is empty")
            else:
                logger.critical(
                    "Bad server response while trying to poll job.\nError: {}".format(e.response.body.decode('utf-8')))
                exit(-1)
            http_client.close()
            continue

        http_client.close()

        # we successfully polled a job. execute the job
        job = json.loads(response.body.decode('utf-8')).get('data')
        job_id = job.get(api_key.JOB_ID)
        logger.info("Starting job {}".format(job_id))

        # execute the job runner with job as json string
        runner_process = Popen(['node', 'src/jobRunner.js', json.dumps(job)])
        yield asyncio.get_event_loop().run_in_executor(None, lambda: runner_process.wait())
        logger.info("Finished job {}".format(job.get(api_key.JOB_ID)))
        with open("temp_result.json") as res_file:
            res = json.load(res_file)

        # send back the results to the server
        http_client = httpclient.AsyncHTTPClient()
        assert api_key.INFO in res
        assert api_key.SUCCESS in res
        update_request = httpclient.HTTPRequest("{}/{}".format(get_url(GRADING_JOB_ENDPOINT), job_id),
                                                headers=get_header(sys.argv[1], worker_id), method="POST",
                                                body=json.dumps(res))

        try:
            logger.info("Sending job results")
            yield http_client.fetch(update_request)
        except httpclient.HTTPClientError as e:
            logger.critical("Bad server response while updating about job status.\nError: {}".format(
                e.response.body.decode('utf-8')))

        http_client.close()


def register_node():
    global worker_id
    global HEARTBEAT_INTERVAL

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

        # read heartbeat
        if api_key.HEARTBEAT in server_response:
            HEARTBEAT_INTERVAL = server_response.get(api_key.HEARTBEAT)
            if type(HEARTBEAT_INTERVAL) is not int:
                logger.critical("Bad server response on registration. {}".format("Heartbeat interval is not int."))
                raise Exception("Invalid response")
    except Exception as e:
        logger.critical("Registration failed!\nError: {}".format(str(e)))
        exit(-1)

    http_client.close()


if __name__ == "__main__":
    # check valid usage
    if len(sys.argv) != 2:
        print_usage()
        exit(-1)

    # register node to server
    register_node()

    # run the grader forever
    asyncio.ensure_future(heartbeat_routine())
    asyncio.ensure_future(worker_routine())
    asyncio.get_event_loop().run_forever()
