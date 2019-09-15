import os
import json
import socket
import signal
import asyncio
import logging
import websockets

from jsonschema import validate, ValidationError, SchemaError
from chainlink import Chainlink

import broadway.grader.api_keys as api_keys
from broadway.grader.api_keys import WORKER_WS_ENDPOINT, HEARTBEAT_INTERVAL
from broadway.grader.definitions import GRADING_JOB_DEF

logger = logging.getLogger(__name__)


async def _exec_job(flags, job):
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

    if flags["verbose"]:
        logger.info("job stdout:\n" + job_stdout)
        logger.info("job stderr:\n" + job_stderr)

    return {
        api_keys.RESULTS: job_results,
        api_keys.SUCCESS: job_results[-1]["success"],
        api_keys.LOGS: {"stdout": job_stdout, "stderr": job_stderr},
        api_keys.GRADING_JOB_ID: job_id,
    }


async def _run(flags):
    url = "{}{}/{}".format(flags["api_host"], WORKER_WS_ENDPOINT, flags["grader_id"])

    headers = {api_keys.AUTH: "Bearer {}".format(flags["token"])}
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

            logger.info("registered as {}".format(flags["grader_id"]))

            while True:
                job = json.loads(await ws.recv())

                validate(instance=job, schema=GRADING_JOB_DEF)

                job_result = await _exec_job(flags, job)

                await ws.send(json.dumps({"type": "job_result", "args": job_result}))

        except websockets.ConnectionClosed as e:
            logger.critical("connection closed: {}".format(repr(e)))

        except ValidationError as e:
            logger.critical("validation error: {}".format(repr(e)))

        except SchemaError as e:
            logger.critical("schema error: {}".format(repr(e)))


def _shutdown(sig, task):
    logger.info("signal received: {}, shutting down".format(signal.Signals(sig).name))
    task.cancel()


def run_ws_grader(flags):
    loop = asyncio.get_event_loop()
    task = loop.create_task(_run(flags))

    loop.add_signal_handler(signal.SIGINT, lambda: _shutdown(signal.SIGINT, task))

    try:
        loop.run_until_complete(task)
    except asyncio.CancelledError:
        logger.info("task cancelled")
    except Exception as e:
        logger.critical("unexpected error: {}".format(repr(e)))
