import logging

import broadway.api.daos as daos
from broadway.api.models.grading_job import GradingJobType
from broadway.api.utils.run import continue_grading_run, fail_grading_run

logger = logging.getLogger(__name__)


def job_update_callback(settings, grading_job_id, grading_run_id):
    job_dao = daos.GradingJobDao(settings)
    job = job_dao.find_by_id(grading_job_id)
    if job is None:
        logger.critical(
            "cannot update non-existent job with ID '{}'".format(grading_job_id)
        )
        return

    run_dao = daos.GradingRunDao(settings)
    run = run_dao.find_by_id(grading_run_id)
    if run is None:
        logger.critical(
            "cannot update non-existent run with ID '{}'".format(grading_run_id)
        )
        return
    if run.finished_at is not None:
        logger.critical(
            "cannot update run with ID '{}' (already finished)".format(grading_run_id)
        )
        return

    if job.type == GradingJobType.PRE_PROCESSING:
        if job.success:
            continue_grading_run(settings, run)
        else:
            fail_grading_run(settings, run)
    elif job.type == GradingJobType.POST_PROCESSING:
        if run.student_jobs_left != 0:
            logger.critical(
                "post-processing job finished when {} student jobs remain".format(
                    run.student_jobs_left
                )
            )
            return

        if job.success:
            continue_grading_run(settings, run)
        else:
            fail_grading_run(settings, run)
    elif job.type == GradingJobType.STUDENT:
        if run.student_jobs_left <= 0:
            logger.critical(
                "student job finished when {} student jobs remain".format(
                    run.student_jobs_left
                )
            )
            return

        run.student_jobs_left -= 1
        run_dao.update(run)

        if run.student_jobs_left == 0:
            # last job in this stage is complete
            continue_grading_run(settings, run)
    else:
        logger.critical("cannot update run with last job type '{}'".format(job.type))
