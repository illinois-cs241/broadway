import logging
import tornado
import tornado.ioloop

from broadway_api.callbacks import job_update_callback
from broadway_api.daos import GradingJobDao, WorkerNodeDao
from broadway_api.utils.time import get_time

logger = logging.getLogger("worker-callbacks")


def worker_heartbeat_callback(settings):
    """
    Checks if any workers went offline (after 2 * HEARTBEAT_INTERVAL seconds)
    """
    heartbeat_timestamp = get_time()
    heartbeat_interval = settings["CONFIG"]["HEARTBEAT_INTERVAL"]

    dao = WorkerNodeDao(settings)
    for node in dao.find_by_liveness(alive=True):
        if (
            heartbeat_timestamp - node.last_seen
        ).total_seconds() >= 2 * heartbeat_interval:
            _handle_lost_worker_node(settings, node)


def _handle_lost_worker_node(settings, worker):
    lost_run_id = worker.running_job_id

    worker.is_alive = False
    worker.running_job_id = None
    worker_dao = WorkerNodeDao(settings)
    worker_dao.update(worker)

    if not lost_run_id:
        logger.critical(
            "worker {} went offline unexpectedly on {}".format(
                worker.id, worker.hostname
            )
        )
        return
    else:
        logger.critical(
            "worker {} went offline unexpectedly on {} while executing '{}'".format(
                worker.id, worker.hostname, lost_run_id
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


__all__ = ["worker_heartbeat_callback"]
