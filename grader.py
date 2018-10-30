from subprocess import PIPE, Popen
from threading import Condition
from threading import Thread
import logging
import os
import sys
import signal
import datetime as dt
import time
import requests
import json

# constants
SERVER_HOSTNAME = "fa18-cs241-437:8888"
LOGS_DIR_NAME = "logs"
TIMESTAMP_FORMAT = "%Y-%m-%d %H:%M:%S"
SUCCESS_CODE = 200
EMPTY_QUEUE_CODE = 412
NUM_WORKER_THREADS = 1
HEARTBEAT_INTERVAL = 10
WORKER_INTERVAL = 2

# globals
worker_id = None
heartbeat_thread = None
worker_threads = []
running = True
heartbeat = True
worker_cv = Condition()
heartbeat_cv = Condition()


def signal_handler(sig, frame):
    global running
    running = False


def get_time():
    return dt.datetime.fromtimestamp(time.time()).strftime(TIMESTAMP_FORMAT)


def print_usage():
    print("Wrong number of arguments provided. Usage:\n\tpython grader.py <cluster token>")


def heartbeat_routine():
    while heartbeat:
        r = requests.post("http://{}/api/v1/heartbeat".format(SERVER_HOSTNAME), data={'worker_id': worker_id})
        if r.status_code == SUCCESS_CODE:
            heartbeat_cv.acquire()
            heartbeat_cv.wait(timeout=HEARTBEAT_INTERVAL)
            heartbeat_cv.release()
        else:
            logging.critical("Registration failed!\nStatus Code: {}\nReason: {}".format(r.status_code, r.text))
            exit(-1)


def worker_routine():
    global running

    while running:
        # poll from queue
        while True:
            r = requests.get("http://{}/api/v1/grading_job".format(SERVER_HOSTNAME), data={'worker_id': worker_id})
            # if error occurs or if the queue is empty then continue
            if r.status_code == SUCCESS_CODE:
                worker_cv.acquire()
                worker_cv.notify_all()
                worker_cv.release()
                break
            elif r.status_code == EMPTY_QUEUE_CODE:
                worker_cv.acquire()
                worker_cv.wait(timeout=WORKER_INTERVAL)
                worker_cv.release()
            elif r.status_code != SUCCESS_CODE:
                logging.critical("Bad server response while trying to poll job. Error: {}".format(r.text))
                exit(-1)

            if not running:
                return

        # we successfully polled a job. execute the job
        job = json.loads(r.text)
        assert "job_id" in job

        logging.info("Starting job {}".format(job["job_id"]))

        # execute the job runner with job as json string
        runner_process = Popen(['node', 'src/jobRunner.js', r.text], stderr=PIPE)
        res = runner_process.communicate()[1]  # capture its stderr which holds the results. This blocks.

        logging.info("Finished job {}".format(job["job_id"]))

        r = requests.post("http://{}/api/v1/grading_job/{}".format(SERVER_HOSTNAME, job["job_id"]),
                          data={'worker_id': worker_id, 'result': res})
        if r.status_code != SUCCESS_CODE:
            logging.critical("Bad server response while updating about job status. Error: {}".format(r.text))
            exit(-1)


def register_node(cluster_token):
    global worker_id
    global heartbeat_thread
    global HEARTBEAT_INTERVAL

    r = requests.get("http://{}/api/v1/worker_register".format(SERVER_HOSTNAME), data={'token': cluster_token})
    if r.status_code == SUCCESS_CODE:
        logging.info("Registered to server at {}".format(get_time()))
        server_response = json.loads(r.text)

        # read worker id
        if 'worker_id' in server_response:
            worker_id = server_response['worker_id']
        else:
            logging.critical("Bad server response on registration. Missing argument \'worker_id\'.")
            exit(-1)

        # read heartbeat
        if 'heartbeat' in server_response and type(server_response['heartbeat']) is int:
            HEARTBEAT_INTERVAL = server_response['heartbeat']
        else:
            logging.critical("Bad server response on registration. {}".format(
                "Missing argument \'heartbeat\'." if 'heartbeat' not in server_response else "Argument \'heartbeat\' "
                                                                                             "is of wrong type."))
            exit(-1)
    else:
        logging.critical("Registration failed!\nStatus Code: {}\nReason: {}".format(r.status_code, r.text))
        exit(-1)


if __name__ == "__main__":
    # check valid usage
    if len(sys.argv) != 2:
        print_usage()
        exit(-1)

    # register SIGINT handler
    signal.signal(signal.SIGINT, signal_handler)

    # set up logger
    if not os.path.exists(LOGS_DIR_NAME):
        os.makedirs(LOGS_DIR_NAME)
    logging.basicConfig(filename='{}/{}.log'.format(LOGS_DIR_NAME, get_time()), level=logging.DEBUG)

    # register node to server
    register_node(sys.argv[1])

    # start heartbeats
    heartbeat_thread = Thread(target=heartbeat_routine)
    heartbeat_thread.start()

    # start worker threads
    for i in range(NUM_WORKER_THREADS):
        cur_worker_thread = Thread(target=worker_routine)
        cur_worker_thread.start()
        worker_threads.append(cur_worker_thread)

    # this will join when SIGINT is received. Until then the main thread blocks
    for worker_thread in worker_threads:
        worker_thread.join()

    # if code reached here we know that we got SIGINT so stop heartbeat now and end program
    heartbeat = False
    heartbeat_cv.acquire()
    heartbeat_cv.notify()
    heartbeat_cv.release()
    heartbeat_thread.join()
