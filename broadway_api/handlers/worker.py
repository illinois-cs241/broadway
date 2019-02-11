import logging
import tornado.ioloop
from queue import Empty
from tornado_json import schema

import broadway_api.daos as daos
import broadway_api.definitions as definitions
import broadway_api.models as models
from broadway_api.decorators.auth import authenticate_cluster_token, authenticate_worker
from broadway_api.callbacks.job import job_update_callback
from broadway_api.handlers.base import BaseAPIHandler
from broadway_api.utils.time import get_time

logger = logging.getLogger("worker")


class WorkerRegisterHandler(BaseAPIHandler):
    @authenticate_cluster_token
    @schema.validate(
        output_schema={
            "type": "object",
            "properties": {
                "worker_id": {"type": "string"},
                "heartbeat": {"type": "number"},
            },
            "required": ["worker_id"],
            "additionalProperties": False,
        }
    )
    def get(self, *args, **kwargs):
        worker_node_dao = daos.WorkerNodeDao(self.settings)
        worker_node = models.WorkerNode(
            hostname=kwargs.get("hostname") if len(args) == 0 else args[0],
            last_seen=get_time(),
            is_alive=True,
        )
        worker_id = str(worker_node_dao.insert(worker_node).inserted_id)

        logger.info(
            "worker '{}' joined as id '{}'".format(worker_node.hostname, worker_id)
        )
        return {
            "worker_id": worker_id,
            "heartbeat": self.get_config()["HEARTBEAT_INTERVAL"],
        }


class GradingJobHandler(BaseAPIHandler):
    @authenticate_cluster_token
    @authenticate_worker
    @schema.validate(
        on_empty_404=True,
        output_schema={
            "type": "object",
            "properties": {
                "grading_job_id": {"type": "string"},
                "stages": {"type": "array", "items": definitions.grading_stage},
            },
            "required": ["grading_job_id", "stages"],
            "additionalProperties": False,
        },
    )
    def get(self, *args, **kwargs):
        """
        Allows workers to request their next grading job
        """
        worker_id = kwargs.get("worker_id") if len(args) == 0 else args[0]
        worker_node_dao = daos.WorkerNodeDao(self.settings)
        worker_node = worker_node_dao.find_by_id(worker_id)
        if not worker_node:
            logger.critical(
                "unknown node with ID '{}' successfully requested job".format(worker_id)
            )
            self.abort({"message": ""}, status=404)
            return

        try:
            grading_job_id = self.get_queue().get_nowait()
            grading_job_dao = daos.GradingJobDao(self.settings)
            grading_job = grading_job_dao.find_by_id(grading_job_id)
            if not grading_job:
                logger.critical(
                    "found job ID '{}' in queue, but job does not exist".format(
                        grading_job_id
                    )
                )
                self.abort(
                    {"message": "a failure occurred while getting next job"}, status=500
                )
                return

            grading_job.started_at = get_time()
            grading_job.worker_id = worker_id
            grading_job_dao.update(grading_job)

            worker_node.running_job_id = grading_job_id
            worker_node.jobs_processed += 1
            worker_node_dao.update(worker_node)

            return {"grading_job_id": grading_job_id, "stages": grading_job.stages}
        except Empty:
            self.abort({"message": "no jobs available"}, status=498)

    @authenticate_cluster_token
    @authenticate_worker
    @schema.validate(
        input_schema={
            "type": "object",
            "properties": {
                "grading_job_id": {"type": "string"},
                "success": {"type": "boolean"},
                "results": {"type": "array", "items": {"type": "object"}},
                "logs": {"type": "object"},
            },
            "required": ["grading_job_id", "success", "results", "logs"],
            "additionalProperties": False,
        }
    )
    def post(self, *args, **kwargs):
        """
        Allows workers to update grading job status on completion
        """
        worker_id = kwargs.get("worker_id") if len(args) == 0 else args[0]
        job_id = self.body.get("grading_job_id")

        grading_job_dao = daos.GradingJobDao(self.settings)
        job = grading_job_dao.find_by_id(job_id)
        if not job:
            self.abort({"message": "job with the given ID not found"})
            return

        job_state = job.get_state()
        if job_state != models.GradingJobState.STARTED:
            logger.critical(
                "job with id '{}' updated when in state '{}'".format(
                    job_id, job_state.value
                )
            )
            self.abort({"message": "cannot update job that is not in STARTED state"})
            return

        worker_node_dao = daos.WorkerNodeDao(self.settings)
        worker_node = worker_node_dao.find_by_id(worker_id)
        if not worker_node:
            logger.critical(
                "unknown node with ID '{}' successfully updated job".format(worker_id)
            )
            self.abort({"message": ""}, status=404)
            return

        # clear the worker node's job
        worker_node.running_job = None
        worker_node_dao.update(worker_node)

        # finish the job
        job.finished_at = get_time()
        job.results = self.body.get("results")
        job.success = self.body.get("success")
        grading_job_dao.update(job)

        # store the logs
        job_log_dao = daos.GradingJobLogDao(self.settings)
        job_log = models.GradingJobLog(job_id=job_id, **self.body.get("logs"))
        job_log_dao.insert(job_log)

        # thread safe callback
        tornado.ioloop.IOLoop.current().add_callback(
            job_update_callback, self.settings, job_id, job.run_id
        )


class HeartBeatHandler(BaseAPIHandler):
    @authenticate_cluster_token
    @authenticate_worker
    def post(self, *args, **kwargs):
        worker_id = kwargs.get("worker_id") if len(args) == 0 else args[0]

        worker_node_dao = daos.WorkerNodeDao(self.settings)
        worker_node = worker_node_dao.find_by_id(worker_id)
        if not worker_node:
            logger.critical(
                "unknown node with ID '{}' successfully sent heartbeat".format(
                    worker_id
                )
            )
            self.abort({"message": ""}, status=404)
            return

        worker_node.last_seen = get_time()
        worker_node_dao.update(worker_node)
