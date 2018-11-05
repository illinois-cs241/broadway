import asyncio
import datetime as dt
import json
import logging
import os
import sys
import time
from subprocess import PIPE, Popen
from tornado import httpclient, gen

# constants
SERVER_HOSTNAME = "127.0.0.1:8888"
LOGS_DIR_NAME = "logs"
TIMESTAMP_FORMAT = "%Y-%m-%d %H:%M:%S"
headers = {'Content-Type': 'application/json; charset=UTF-8'}
SUCCESS_CODE = 200
HEARTBEAT_INTERVAL = 2

# globals
worker_id = None
heartbeat_thread = None
worker_thread = None
heartbeat = True


def get_time():
    return dt.datetime.fromtimestamp(time.time()).strftime(TIMESTAMP_FORMAT)


def print_usage():
    print("Wrong number of arguments provided. Usage:\n\tpython grader.py <cluster token>")


@gen.coroutine
def heartbeat_routine():
    while heartbeat:
        http_client = httpclient.AsyncHTTPClient()
        heartbeat_request = httpclient.HTTPRequest(
            "http://{}/api/v1/heartbeat?worker_id={}".format(SERVER_HOSTNAME, worker_id), headers=headers,
            method="POST", body="")
        try:
            yield http_client.fetch(heartbeat_request)
            yield asyncio.sleep(HEARTBEAT_INTERVAL)
        except httpclient.HTTPError as e:
            logging.critical("Heartbeat failed!\nError: {}".format(str(e)))

        http_client.close()


@gen.coroutine
def worker_routine():
    while True:
        http_client = httpclient.AsyncHTTPClient(defaults=dict(request_timeout=180))
        job_request = httpclient.HTTPRequest(
            "http://{}/api/v1/grading_job?worker_id={}".format(SERVER_HOSTNAME, worker_id),
            headers=headers, method="GET")

        # poll from queue
        try:
            response = yield http_client.fetch(job_request)
        except httpclient.HTTPError as e:
            logging.critical("Bad server response while trying to poll job.\nError: {}".format(str(e)))
            http_client.close()
            continue

        http_client.close()

        # we successfully polled a job. execute the job
        json_payload = response.body.decode('utf-8')
        if json_payload == "":
            continue

        job = json.loads(json_payload)
        assert "job_id" in job

        logging.info("Starting job {}".format(job["job_id"]))

        # execute the job runner with job as json string
        runner_process = Popen(['node', 'src/jobRunner.js', json_payload], stderr=PIPE)
        res = runner_process.communicate()[1]  # capture its stderr which holds the results. This blocks.
        logging.info("Finished job {}".format(job["job_id"]))

        # send back the results to the server
        http_client = httpclient.AsyncHTTPClient()
        req_body = json.dumps({'worker_id': worker_id, 'result': res})
        update_request = httpclient.HTTPRequest(
            "http://{}/api/v1/grading_job/{}".format(SERVER_HOSTNAME, job["job_id"]), headers=headers, method="POST",
            body=req_body)

        try:
            yield http_client.fetch(update_request)
        except httpclient.HTTPError as e:
            logging.critical("Bad server response while updating about job status.\nError: {}".format(str(e)))

        http_client.close()


@gen.coroutine
def register_node(cluster_token):
    global worker_id
    global heartbeat_thread
    global HEARTBEAT_INTERVAL
    http_client = httpclient.AsyncHTTPClient()
    req = httpclient.HTTPRequest("http://{}/api/v1/worker_register?token={}".format(SERVER_HOSTNAME, cluster_token),
                                 headers=headers, method="GET")

    try:
        response = yield http_client.fetch(req)
        logging.info("Registered to server at {}".format(get_time()))
        server_response = json.loads(response.body.decode('utf-8'))

        # read worker id
        if 'worker_id' in server_response:
            worker_id = server_response['worker_id']
        else:
            logging.critical("Bad server response on registration. Missing argument \'worker_id\'.")
            raise Exception("Invalid response")

        # read heartbeat
        if 'heartbeat' in server_response and type(server_response['heartbeat']) is int:
            HEARTBEAT_INTERVAL = server_response['heartbeat']
        else:
            logging.critical("Bad server response on registration. {}".format(
                "Missing argument \'heartbeat\'." if 'heartbeat' not in server_response else "Argument \'heartbeat\' "
                                                                                             "is of wrong type."))
            raise Exception("Invalid response")
    except Exception as e:
        logging.critical("Registration failed!\nError: {}".format(str(e)))
        http_client.close()
        exit(-1)

    http_client.close()


if __name__ == "__main__":
    # check valid usage
    if len(sys.argv) != 2:
        print_usage()
        exit(-1)

    # set up logger
    if not os.path.exists(LOGS_DIR_NAME):
        os.makedirs(LOGS_DIR_NAME)
    logging.basicConfig(filename='{}/{}.log'.format(LOGS_DIR_NAME, get_time()), level=logging.DEBUG)

    # register node to server
    loop = asyncio.get_event_loop()
    loop.run_until_complete(register_node(sys.argv[1]))

    asyncio.ensure_future(heartbeat_routine())
    asyncio.ensure_future(worker_routine())
    loop.run_forever()
