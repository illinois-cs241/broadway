import os
import signal
import socket
import asyncio
import logging

from threading import Event
from concurrent.futures import ThreadPoolExecutor, wait, FIRST_COMPLETED

import requests
from chainlink import Chainlink

import broadway.grader.api as api
from broadway.grader.api import (
    GRADER_REGISTER_ENDPOINT,
    GRADING_JOB_ENDPOINT,
    HEARTBEAT_ENDPOINT,
    SUCCESS_CODE,
    QUEUE_EMPTY_CODE,
    JOB_POLL_INTERVAL,
    HEARTBEAT_INTERVAL,
)

# globals
_grader_id = None
_hostname = None
_worker_thread = None
_heartbeat_interval = HEARTBEAT_INTERVAL
_api_host = None
_header = None
_verbose = None

_event_loop = asyncio.new_event_loop()
_exit_event = Event()

logger = logging.getLogger(__name__)


def _halt_all():
    _exit_event.set()


def _signal_handler(sig, frame):
    _halt_all()


def _get_url(endpoint):
    return "{}{}".format(_api_host, endpoint)


def _heartbeat_routine():
    while not _exit_event.is_set():
        response = requests.post(
            _get_url("{}/{}".format(HEARTBEAT_ENDPOINT, _grader_id)),
            headers=_header,
            data="",
        )
        if response.status_code != SUCCESS_CODE:
            logger.critical("Heartbeat failed!\nError: {}".format(response.text))
            return

        _exit_event.wait(_heartbeat_interval)


def _worker_routine():
    asyncio.set_event_loop(_event_loop)

    while not _exit_event.is_set():
        # poll from queue
        response = requests.get(
            _get_url("{}/{}".format(GRADING_JOB_ENDPOINT, _grader_id)), headers=_header
        )

        # if the queue is empty then sleep for a while
        if response.status_code == QUEUE_EMPTY_CODE:
            _exit_event.wait(JOB_POLL_INTERVAL)
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
        job_id = job.get(api.GRADING_JOB_ID)
        logger.info("Starting job {}".format(job_id))

        # execute job
        try:
            chain = Chainlink(job[api.STAGES], workdir=os.getcwd())
            job_results = chain.run({})
        except Exception as ex:
            logger.critical("Grading job failed with exception:\n{}".format(ex))
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

        # remove logs from result array because logs can be bulky
        # we will store then separately
        for r in job_results:
            del r["logs"]

        logger.info("Finished job {}".format(job_id))
        if _verbose:
            logger.info("Job stdout:\n" + job_stdout)
            logger.info("Job stderr:\n" + job_stderr)

        grading_job_result = {
            api.RESULTS: job_results,
            api.SUCCESS: job_results[-1]["success"],
            api.LOGS: {"stdout": job_stdout, "stderr": job_stderr},
            api.GRADING_JOB_ID: job_id,
        }

        logger.info("Sending job results")
        response = requests.post(
            _get_url("{}/{}".format(GRADING_JOB_ENDPOINT, _grader_id)),
            json=grading_job_result,
            headers=_header,
        )
        if response.status_code != SUCCESS_CODE:
            logger.critical(
                (
                    "Bad server response while updating"
                    + "about job status.\nError: {}"
                ).format(response.text)
            )
            return


def _register_node():
    global _grader_id
    global _heartbeat_interval

    response = requests.post(
        _get_url("{}/{}".format(GRADER_REGISTER_ENDPOINT, _grader_id)),
        headers=_header,
        json={api.HOSTNAME: _hostname},
    )
    if response.status_code != SUCCESS_CODE:
        raise RuntimeError("failed to register: {}".format(response.text))

    logger.info("Registered to server")
    server_response = response.json()["data"]

    # set heartbeat interval
    if api.HEARTBEAT in server_response:
        _heartbeat_interval = server_response[api.HEARTBEAT]
    else:
        logger.info(
            "Server response did not include heartbeat, using default {}".format(
                _heartbeat_interval
            )
        )


def run_http_grader(flags):
    global _grader_id
    global _hostname
    global _header
    global _api_host
    global _verbose

    signal.signal(signal.SIGINT, _signal_handler)

    _grader_id = flags["grader_id"]
    _hostname = socket.gethostname()
    _api_host = flags["api_host"]
    _verbose = flags["verbose"]

    # register node to server
    _header = {api.AUTH: "Bearer {}".format(flags["token"])}
    _register_node()

    # run the grader on two separate threads.
    # If any of the routines fail, the grader shuts down
    executor = ThreadPoolExecutor(max_workers=2)
    futures = [executor.submit(_heartbeat_routine), executor.submit(_worker_routine)]
    wait(futures, return_when=FIRST_COMPLETED)
    executor.shutdown()
