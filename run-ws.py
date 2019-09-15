import os
import json
import socket
import signal
import asyncio
import logging
import argparse
import websockets

from jsonschema import validate, ValidationError, SchemaError
from chainlink import Chainlink

import grader.api_keys as api_keys
from grader.definitions import GRADING_JOB_DEF

from config import *

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger()


async def exec_job(job):
    job_id = job[api_keys.GRADING_JOB_ID]
    stages = job[api_keys.STAGES]

    logger.info("starting job {}".format(job_id))

    # execute job
    try:
        chain = Chainlink(stages, workdir=os.getcwd())
        job_results = await chain.run_async({})
    except Exception as ex:
        logger.critical("grading job failed with exception:\n{}".format(ex))
        job_results = [
            {
                "logs": {
                    "stdout": b"the container crashed",
                    "stderr": bytes(str(ex), "utf-8"),
                },
                "success": False,
            }
        ]

    job_stdout = "\n".join([r["logs"]["stdout"].decode("utf-8") for r in job_results])
    job_stderr = "\n".join([r["logs"]["stderr"].decode("utf-8") for r in job_results])

    for r in job_results:
        del r["logs"]

    logger.info("finished job {}".format(job_id))

    if VERBOSE:
        logger.info("job stdout:\n" + job_stdout)
        logger.info("job stderr:\n" + job_stderr)

    return {
        api_keys.RESULTS: job_results,
        api_keys.SUCCESS: job_results[-1]["success"],
        api_keys.LOGS: {"stdout": job_stdout, "stderr": job_stderr},
        api_keys.GRADING_JOB_ID: job_id,
    }


async def run(token, worker_id):
    url = "{}://{}:{}{}{}/{}".format(
        "wss" if USE_SSL else "ws",
        API_HOSTNAME,
        API_PORT,
        API_PROXY,
        WORKER_WS_ENDPOINT,
        worker_id,
    )

    headers = {api_keys.AUTH: "Bearer {}".format(token)}
    hostname = socket.gethostname()

    async with websockets.connect(
        url, ping_interval=HEARTBEAT_INTERVAL, extra_headers=headers
    ) as ws:
        # poll job
        try:
            await ws.send(
                json.dumps({"type": "register", "args": {"hostname": hostname}})
            )

            ack = json.loads(await ws.recv())

            if not ack["success"]:
                raise Exception("failed to register")

            logger.info("registered as {}".format(worker_id))

            while True:
                job = json.loads(await ws.recv())

                validate(instance=job, schema=GRADING_JOB_DEF)

                job_result = await exec_job(job)

                await ws.send(json.dumps({"type": "job_result", "args": job_result}))

        except websockets.ConnectionClosed as e:
            logger.critical("connection closed: {}".format(repr(e)))

        except ValidationError as e:
            logger.critical("validation error: {}".format(repr(e)))

        except SchemaError as e:
            logger.critical("schema error: {}".format(repr(e)))


def parse_args():
    parser = argparse.ArgumentParser()
    parser.add_argument("token", help="Broadway cluster token")
    parser.add_argument(
        "worker_id", metavar="worker-id", help="Unique worker id for registration"
    )
    return parser.parse_args()


def shutdown(sig, task):
    logger.info("signal received: {}, shutting down".format(signal.Signals(sig).name))
    task.cancel()


if __name__ == "__main__":
    args = parse_args()

    loop = asyncio.get_event_loop()
    task = loop.create_task(run(args.token, args.worker_id))

    loop.add_signal_handler(signal.SIGINT, lambda: shutdown(signal.SIGINT, task))

    try:
        loop.run_until_complete(task)
    except asyncio.CancelledError:
        logger.info("task cancelled")
    except Exception as e:
        logger.critical("unexpected error: {}".format(repr(e)))
