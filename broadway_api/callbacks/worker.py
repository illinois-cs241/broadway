import logging

from queue import Empty

import tornado
import tornado.ioloop

from broadway_api.callbacks import job_update_callback
from broadway_api.daos import GradingJobDao, WorkerNodeDao
from broadway_api.utils.time import get_time

logger = logging.getLogger("broadway.callbacks.worker")


def worker_heartbeat_callback(settings):
    """
    Checks if any workers went offline (after 2 * heartbeat_interval seconds)
    """
    heartbeat_timestamp = get_time()
    heartbeat_interval = settings["FLAGS"]["heartbeat_interval"]
    conn_map = settings["WS_CONN_MAP"]

    dao = WorkerNodeDao(settings)

    for node in dao.find_by_liveness(alive=True):
        if (
            heartbeat_timestamp - node.last_seen
        ).total_seconds() >= 2 * heartbeat_interval:
            if node.use_ws and node.id in conn_map:
                conn_map[node.id].close()

            _handle_lost_worker_node(settings, node)


def worker_lost_callback(settings, worker_id, reason="closed connection"):
    dao = WorkerNodeDao(settings)
    worker = dao.find_by_id(worker_id)

    if worker is None:
        logger.critical("dead worker {} not found".format(worker_id))
        return

    _handle_lost_worker_node(settings, worker, reason=reason)


# assign available jobs to whoever is currently not working
# triggered upon the following events
# 1. client submitting a new job
# 2. worker finishing a job
def worker_schedule_job(settings):
    conn_map = settings["WS_CONN_MAP"]
    job_queue = settings["QUEUE"]

    grading_job_dao = GradingJobDao(settings)
    worker_node_dao = WorkerNodeDao(settings)

    idle_workers = worker_node_dao.find_by_idleness()

    for idle_worker in idle_workers:
        if idle_worker.use_ws and idle_worker.id in conn_map:
            conn = conn_map[idle_worker.id]

            try:
                grading_job_id = job_queue.get_nowait()
                grading_job = grading_job_dao.find_by_id(grading_job_id)

                if not grading_job:
                    logger.critical(
                        "found job ID '{}' in queue, but job does not exist".format(
                            grading_job_id
                        )
                    )
                    return

                grading_job.started_at = get_time()
                grading_job.worker_id = idle_worker.id
                grading_job_dao.update(grading_job)

                idle_worker.running_job_id = grading_job_id
                idle_worker.jobs_processed += 1
                worker_node_dao.update(idle_worker)

                conn.send(
                    {"grading_job_id": grading_job_id, "stages": grading_job.stages}
                )

            except Empty:
                # no more jobs available
                return

            except Exception as e:
                logger.critical(
                    "failed to assign job to {}: {}".format(idle_worker.id, repr(e))
                )


def _handle_lost_worker_node(settings, worker, reason="timeout"):
    lost_run_id = worker.running_job_id

    worker.is_alive = False
    worker.running_job_id = None
    worker_dao = WorkerNodeDao(settings)
    worker_dao.update(worker)

    if not lost_run_id:
        logger.critical(
            "worker '{}' went offline unexpectedly on '{}' due to {}".format(
                worker.id, worker.hostname, reason
            )
        )
        return
    else:
        logger.critical(
            "worker '{}' went offline unexpectedly on '{}' while"
            " executing '{}' due to {}".format(
                worker.id, worker.hostname, lost_run_id, reason
            )
        )

        jobs_dao = GradingJobDao(settings)
        job = jobs_dao.find_by_id(lost_run_id)
        if job is None:
            logger.critical(
                (
                    "worker was reportedly executing job '{}' "
                    "but this job does not exist"
                ).format(lost_run_id)
            )
            return

        job.finished = get_time()
        job.success = True
        job.results = [{"result": "worker died while executing job"}]
        jobs_dao.update(job)

        tornado.ioloop.IOLoop.current().add_callback(
            job_update_callback, settings, lost_run_id, job.run_id
        )


__all__ = ["worker_heartbeat_callback", "worker_lost_callback", "worker_schedule_job"]
