import os
import sys
import signal
import socket
import asyncio
import logging
import argparse

from threading import Event
from logging.handlers import TimedRotatingFileHandler
from concurrent.futures import ThreadPoolExecutor, wait, FIRST_COMPLETED

import requests
from chainlink import Chainlink

from config import *
from grader.utils import get_url
import grader.api_keys as api_keys

# globals
worker_id = None
hostname = None
worker_thread = None
heartbeat_interval = HEARTBEAT_INTERVAL

event_loop = asyncio.new_event_loop()

exit_event = Event()

# setting up logger
os.makedirs(LOGS_DIR, exist_ok=True)
logging.basicConfig(
    handlers=[
        TimedRotatingFileHandler(
            "{}/log".format(LOGS_DIR), when="midnight", backupCount=7
        ),
        logging.StreamHandler(),
    ],
    level=logging.INFO,
)
logger = logging.getLogger()


def halt_all():
    exit_event.set()


def signal_handler(sig, frame):
    halt_all()


def heartbeat_routine():
    while not exit_event.is_set():
        response = requests.post(
            get_url("{}/{}".format(HEARTBEAT_ENDPOINT, worker_id)),
            headers=header,
            data="",
        )
        if response.status_code != SUCCESS_CODE:
            logger.critical("Heartbeat failed!\nError: {}".format(response.text))
            return

        exit_event.wait(heartbeat_interval)


def worker_routine():
    asyncio.set_event_loop(event_loop)

    while not exit_event.is_set():
        # poll from queue
        response = requests.get(
            get_url("{}/{}".format(GRADING_JOB_ENDPOINT, worker_id)), headers=header
        )

        # if the queue is empty then sleep for a while
        if response.status_code == QUEUE_EMPTY_CODE:
            exit_event.wait(JOB_POLL_INTERVAL)
            continue

        if response.status_code != SUCCESS_CODE:
            logger.critical(
                "Bad server response while trying to poll job.\nError: {}".format(
                    response.text
                )
            )
            return

        # we successfully polled a job
        job = response.json()["data"]
        job_id = job.get(api_keys.GRADING_JOB_ID)
        logger.info("Starting job {}".format(job_id))

        # execute job
        try:
            chain = Chainlink(job[api_keys.STAGES], workdir=os.getcwd())
            job_results = chain.run({})
        except Exception as ex:
            logger.critical("Grading job failed with exception:\n{}", ex)
            job_results = [
                {
                    "logs": {
                        "stdout": b"The container crashed",
                        "stderr": bytes(str(ex), "utf-8"),
                    },
                    "success": False,
                }
            ]

        job_stdout = "\n".join(
            [r["logs"]["stdout"].decode("utf-8") for r in job_results]
        )
        job_stderr = "\n".join(
            [r["logs"]["stderr"].decode("utf-8") for r in job_results]
        )

        # remove logs from result array because logs can be bulky we will store then separately
        for r in job_results:
            del r["logs"]

        logger.info("Finished job {}".format(job_id))
        if VERBOSE:
            logger.info("Job stdout:\n" + job_stdout)
            logger.info("Job stderr:\n" + job_stderr)

        grading_job_result = {
            api_keys.RESULTS: job_results,
            api_keys.SUCCESS: job_results[-1]["success"],
            api_keys.LOGS: {"stdout": job_stdout, "stderr": job_stderr},
            api_keys.GRADING_JOB_ID: job_id,
        }

        logger.info("Sending job results")
        response = requests.post(
            get_url("{}/{}".format(GRADING_JOB_ENDPOINT, worker_id)),
            json=grading_job_result,
            headers=header,
        )
        if response.status_code != SUCCESS_CODE:
            logger.critical(
                "Bad server response while updating about job status.\nError: {}".format(
                    response.text
                )
            )
            return


def register_node():
    global worker_id

    response = requests.post(
        get_url("{}/{}".format(GRADER_REGISTER_ENDPOINT, worker_id)),
        headers=header,
        json={api_keys.HOSTNAME: hostname}
    )
    if response.status_code != SUCCESS_CODE:
        logger.critical("Registration failed!\nError: {}".format(response.text))
        exit(-1)

    logger.info("Registered to server")
    server_response = response.json()["data"]
    
    # set heartbeat interval
    if api_keys.HEARTBEAT in server_response:
        heartbeat_interval = server_response[api_keys.HEARTBEAT]
    else:
        logger.info("Server response did not include heartbeat, using default {}".format(heartbeat_interval))


def parse_args():
    parser = argparse.ArgumentParser()
    parser.add_argument("token", help="Broadway cluster token")
    parser.add_argument("worker_id", metavar="worker-id", help="Unique worker id for registration")
    return parser.parse_args()


if __name__ == "__main__":
    # check valid usage
    args = parse_args()

    signal.signal(signal.SIGINT, signal_handler)

    worker_id = args.worker_id
    hostname = socket.gethostname()

    # register node to server
    header = {api_keys.AUTH: "Bearer {}".format(args.token)}
    register_node()

    # run the grader on two separate threads. If any of the routines fail, the grader shuts down
    executor = ThreadPoolExecutor(max_workers=2)
    futures = [executor.submit(heartbeat_routine), executor.submit(worker_routine)]
    wait(futures, return_when=FIRST_COMPLETED)
    executor.shutdown()
