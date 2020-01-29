import logging

import tornado.ioloop

from broadway.api.handlers.base import BaseWSAPIHandler
from broadway.api.decorators.auth import authenticate_cluster_token_ws

import broadway.api.daos as daos
import broadway.api.models as models

from broadway.api.callbacks.job import job_update_callback
from broadway.api.callbacks.worker import worker_lost_callback, worker_schedule_job
from broadway.api.utils.time import get_time

logger = logging.getLogger(__name__)


class WorkerConnectionHandler(BaseWSAPIHandler):
    def __init__(self, *args, **kwargs):
        self.worker_id = None
        super().__init__(*args, **kwargs)

    @authenticate_cluster_token_ws
    def open(self, worker_id):
        self.worker_id = worker_id
        self.registered = False
        logger.info("worker '{}' opened a connection".format(self.worker_id))

    @BaseWSAPIHandler.msg_type(
        "register",
        {
            "type": "object",
            "properties": {"hostname": {"type": "string"}},
            "required": ["hostname"],
            "additionalProperties": False,
        },
    )
    def handler_register(self, hostname):
        if self.worker_id is None:
            return

        worker_node_dao = daos.WorkerNodeDao(self.settings)

        dup = worker_node_dao.find_by_id(self.worker_id)

        if dup is None:
            self.worker_node = models.WorkerNode(
                id_=self.worker_id,
                hostname=hostname,
                last_seen=get_time(),
                is_alive=True,
                use_ws=True,
            )
            logger.info(
                "new worker '{}' joined on '{}'".format(self.worker_id, hostname)
            )
            worker_node_dao.insert(self.worker_node)
        elif not dup.is_alive:
            self.worker_node = dup
            self.worker_node.hostname = hostname
            self.worker_node.last_seen = get_time()
            self.worker_node.is_alive = True
            logger.info(
                "worker '{}' alive again on '{}'".format(self.worker_id, hostname)
            )
            worker_node_dao.update(self.worker_node)
        else:
            msg = "worker id '{}' already exists".format(self.worker_id)
            logger.info(msg)
            self.send({"success": False})
            self.close(reason=msg, code=1002)
            return

        self.registered = True
        self.get_ws_conn_map()[self.worker_id] = self

        self.send({"success": True})

        # trigger schedule event
        tornado.ioloop.IOLoop.current().add_callback(worker_schedule_job, self.settings)

    @BaseWSAPIHandler.msg_type(
        "job_result",
        {
            "type": "object",
            "properties": {
                "grading_job_id": {"type": "string"},
                "success": {"type": "boolean"},
                "results": {"type": "array", "items": {"type": "object"}},
                "logs": {"type": "object"},
            },
            "required": ["grading_job_id", "success", "results", "logs"],
            "additionalProperties": False,
        },
    )
    def handler_job_result(self, grading_job_id, success, results, logs):
        if not self.registered:
            logger.info(
                "worker '{}' submitted before registering".format(self.worker_id)
            )
            self.close(reason="submitting before registering", code=1002)
            return

        grading_job_dao = daos.GradingJobDao(self.settings)
        job = grading_job_dao.find_by_id(grading_job_id)

        if not job:
            self.close(reason="job with the given ID not found", code=1002)
            return

        job_state = job.get_state()

        if job_state != models.GradingJobState.STARTED:
            logger.critical(
                "job with id '{}' updated when in state '{}'".format(
                    grading_job_id, job_state.value
                )
            )
            self.close(
                reason="cannot update job that is not in STARTED state", code=1002
            )
            return

        worker_node_dao = daos.WorkerNodeDao(self.settings)
        worker_node = worker_node_dao.find_by_id(self.worker_id)

        if not worker_node:
            msg = "unknown worker '{}' successfully updated job".format(self.worker_id)
            logger.critical(msg)
            self.close(reason=msg, code=1002)
            return

        logger.info(
            "worker '{}' submitted job result for job '{}'".format(
                self.worker_id, grading_job_id
            )
        )

        # clear the worker node's job
        worker_node.running_job_id = None
        worker_node_dao.update(worker_node)

        # finish the job
        job.finished_at = get_time()
        job.results = results
        job.success = success
        grading_job_dao.update(job)

        # store the logs
        job_log_dao = daos.GradingJobLogDao(self.settings)
        job_log = models.GradingJobLog(job_id=grading_job_id, **logs)
        job_log_dao.insert(job_log)

        # thread safe callback
        tornado.ioloop.IOLoop.current().add_callback(
            job_update_callback, self.settings, grading_job_id, job.run_id
        )

        # trigger schedule event
        tornado.ioloop.IOLoop.current().add_callback(worker_schedule_job, self.settings)

    def on_close(self):
        if self.worker_id is not None and self.registered:
            tornado.ioloop.IOLoop.current().add_callback(
                worker_lost_callback, self.settings, self.worker_id
            )

            del self.get_ws_conn_map()[self.worker_id]
        else:
            logger.info(
                "worker '{}' went down before registering".format(self.worker_id)
            )

    def on_ping(self, data):
        # ping messages have the same function as heartbeat requests
        # for normal http workers

        if self.worker_id is None:
            logger.critical("worker is not initialized")
            return

        worker_node_dao = daos.WorkerNodeDao(self.settings)
        worker_node = worker_node_dao.find_by_id(self.worker_id)

        if not worker_node:
            logger.critical(
                "unknown ws node with ID '{}' successfully sent heartbeat".format(
                    self.worker_id
                )
            )
            return

        worker_node.last_seen = get_time()
        worker_node_dao.update(worker_node)
