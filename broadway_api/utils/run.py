import logging

import broadway_api.daos as daos
import broadway_api.models as models
from broadway_api.models.grading_job import GradingJobType
from broadway_api.models.grading_run import GradingRunState
from broadway_api.utils.time import get_time

logger = logging.getLogger("run-utils")


def continue_grading_run(settings, grading_run):
    """
    Moves grading run into next state or finishes it
    """
    assignment_config_dao = daos.AssignmentConfigDao(settings)
    assignment = assignment_config_dao.find_by_id(grading_run.assignment_id)

    global_environ = assignment.env or {}
    global_environ["GRADING_RUN_ID"] = grading_run.id

    queue = settings["QUEUE"]
    if grading_run.state == GradingRunState.READY:
        if assignment.pre_processing_pipeline:
            _update_run_state(
                settings, grading_run, GradingRunState.PRE_PROCESSING_STAGE
            )
            next_job = _prepare_next_job(
                settings,
                grading_run,
                global_environ,
                grading_run.pre_processing_env or {},
                assignment.pre_processing_pipeline,
                GradingJobType.PRE_PROCESSING,
            )
            queue.put(next_job)
            return True
    if (
        grading_run.state == GradingRunState.READY
        or grading_run.state == GradingRunState.PRE_PROCESSING_STAGE
    ):
        _update_run_state(settings, grading_run, GradingRunState.STUDENTS_STAGE)
        for runtime_environ in grading_run.students_env:
            next_job = _prepare_next_job(
                settings,
                grading_run,
                global_environ,
                runtime_environ,
                assignment.student_pipeline,
                GradingJobType.STUDENT,
            )
            queue.put(next_job)
        return True
    if grading_run.state == GradingRunState.STUDENTS_STAGE:
        if assignment.post_processing_pipeline:
            _update_run_state(
                settings, grading_run, GradingRunState.POST_PROCESSING_STAGE
            )
            next_job = _prepare_next_job(
                settings,
                grading_run,
                global_environ,
                grading_run.post_processing_env or {},
                assignment.post_processing_pipeline,
                GradingJobType.POST_PROCESSING,
            )
            queue.put(next_job)
            return True
        else:
            _finish_grading_run(settings, grading_run)
            return True
    if grading_run.state == GradingRunState.POST_PROCESSING_STAGE:
        _finish_grading_run(settings, grading_run)
        return True
    logger.critical("invalid grading run state for run '{}'".format(grading_run.id))
    return False


def fail_grading_run(settings, run):
    run_dao = daos.GradingRunDao(settings)
    if run is None:
        logger.critical("cannot fail non-existent run with ID '{}'".format(run.id))
        return

    run.finished_at = get_time()
    run.state = GradingRunState.FAILED
    run.success = False
    run_dao.update(run)


def _update_run_state(settings, grading_run, state):
    """
    Updates the state for a grading run
    """
    grading_run_dao = daos.GradingRunDao(settings)
    grading_run.state = state
    grading_run_dao.update(grading_run)


def _prepare_next_job(
    settings, grading_run, global_job_environ, runtime_job_environ, job_stages, job_type
):
    """
    Prepares a job to be submitted to queue
    """
    grading_job_dao = daos.GradingJobDao(settings)
    grading_job = models.GradingJob(
        job_type=job_type, run_id=grading_run.id, queued_at=get_time()
    )
    grading_job.id = str(grading_job_dao.insert(grading_job).inserted_id)

    global_job_environ["GRADING_JOB_ID"] = grading_job.id
    grading_job.set_stages(job_stages, global_job_environ, runtime_job_environ)
    grading_job_dao.update(grading_job)

    return grading_job.id


def _finish_grading_run(settings, grading_run):
    grading_run_dao = daos.GradingRunDao(settings)
    grading_run.state = GradingRunState.FINISHED
    grading_run.finished_at = get_time()
    grading_run.success = True
    grading_run_dao.update(grading_run)
