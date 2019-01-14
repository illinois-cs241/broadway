import logging

from bson import ObjectId

import src.constants.keys as key
from src.database import DatabaseResolver
from src.constants.constants import GradingRunState, GradingJobType
from queue import Queue

from src.utilities import get_time

logger = logging.getLogger()


def build_pipeline(pipeline, global_env_var, run_env_var):
    """
    Builds the pipeline by populating it with environment variables appropriately. Injects global env vars and run
    specific env vars into every stage. If there is overlap, run time env var is given priority

    :param pipeline: Pipeline document as specified in the assignment config
    :type pipeline: list
    :param global_env_var: these are global env vars available to all pipelines
    :type global_env_var: dict
    :param run_env_var: these are run specific env vars. will be available to all pipelines for this run
    :type run_env_var: dict
    :return: fully configured pipeline
    :rtype: list
    """
    for grading_stage in pipeline:
        if key.ENV not in grading_stage:
            grading_stage[key.ENV] = {}

        # inject global env vars and run specific env vars into the stage. If there is overlap, run time env var is
        # given priority
        grading_stage[key.ENV].update(global_env_var)
        grading_stage[key.ENV].update(run_env_var)

    return pipeline


def progress_grading_run(db_resolver, job_queue, grading_run_id):
    """
    Progresses the grading run, i.e. moves on to the next stage for this grading run and schedules jobs appropriately.

    :param db_resolver: database object which provides access to the DB
    :type db_resolver: DatabaseResolver
    :param job_queue: the job queue which the workers poll
    :type job_queue: Queue
    :param grading_run_id: used to identify the grading run for which we need to schedule jobs
    :type grading_run_id: str
    :return:
    """
    grading_run_collection = db_resolver.get_grading_run_collection()
    grading_job_collection = db_resolver.get_grading_job_collection()

    grading_run = grading_run_collection.find_one({key.ID: ObjectId(grading_run_id)})
    assignment = db_resolver.get_assignment_collection().find_one({key.ID: grading_run.get(key.ASSIGNMENT_ID)})
    old_state = grading_run.get(key.STATE)

    # schedule appropriate jobs
    if old_state == GradingRunState.READY.value and key.PRE_PROCESSING_PIPELINE in assignment:
        grading_run_collection.update_one({key.ID: ObjectId(grading_run_id)},
                                          {'$set': {key.STATE: GradingRunState.PRE_PROCESSING_STAGE.value}})

        pre_processing_job = {key.TYPE: GradingJobType.PRE_PROCESSING.value,
                              key.GRADING_RUN_ID: grading_run_id,
                              key.STUDENTS: grading_run.get(key.STUDENTS_ENV),
                              key.QUEUED: get_time(),
                              key.STAGES: build_pipeline(assignment.get(key.PRE_PROCESSING_PIPELINE),
                                                         assignment.get(key.ENV, {}),
                                                         grading_run.get(key.PRE_PROCESSING_ENV, {}))}
        job_queue.put(str(grading_job_collection.insert_one(pre_processing_job).inserted_id))

    elif (old_state == GradingRunState.READY.value and key.PRE_PROCESSING_PIPELINE not in assignment) or (
            old_state == GradingRunState.PRE_PROCESSING_STAGE.value):
        grading_run_collection.update_one({key.ID: ObjectId(grading_run_id)},
                                          {'$set': {key.STATE: GradingRunState.STUDENTS_STAGE.value}})

        for cur_student_env in grading_run.get(key.STUDENTS_ENV):
            cur_student_job = {key.TYPE: GradingJobType.STUDENT.value,
                               key.GRADING_RUN_ID: grading_run_id,
                               key.QUEUED: get_time(),
                               key.STAGES: build_pipeline(assignment.get(key.STUDENT_PIPELINE),
                                                          assignment.get(key.ENV, {}),
                                                          cur_student_env)}
            job_queue.put(str(grading_job_collection.insert_one(cur_student_job).inserted_id))

    elif old_state == GradingRunState.STUDENTS_STAGE.value and key.POST_PROCESSING_PIPELINE in assignment:
        if grading_run.get(key.STUDENT_JOBS_LEFT) != 0:
            logger.critical("Invalid: Attempted to schedule post processing job when {} student jobs left.".format(
                grading_run.get(key.STUDENT_JOBS_LEFT)))
            return

        grading_run_collection.update_one({key.ID: ObjectId(grading_run_id)},
                                          {'$set': {key.STATE: GradingRunState.POST_PROCESSING_STAGE.value}})

        post_processing_job = {key.TYPE: GradingJobType.POST_PROCESSING.value,
                               key.GRADING_RUN_ID: grading_run_id,
                               key.STUDENTS: grading_run.get(key.STUDENTS_ENV),
                               key.QUEUED: get_time(),
                               key.STAGES: build_pipeline(assignment.get(key.POST_PROCESSING_PIPELINE),
                                                          assignment.get(key.ENV, {}),
                                                          grading_run.get(key.POST_PROCESSING_ENV, {}))}
        job_queue.put(str(grading_job_collection.insert_one(post_processing_job).inserted_id))
    elif (old_state == GradingRunState.STUDENTS_STAGE.value and key.POST_PROCESSING_PIPELINE not in assignment) or (
            old_state == GradingRunState.POST_PROCESSING_STAGE.value):
        if grading_run.get(key.STUDENT_JOBS_LEFT) != 0:
            logger.critical("Invalid: Attempted to finish grading run when {} student jobs left.".format(
                grading_run.get(key.STUDENT_JOBS_LEFT)))
            return

        grading_run_collection.update_one({key.ID: ObjectId(grading_run_id)},
                                          {'$set': {key.STATE: GradingRunState.FINISHED.value}})

        grading_run_collection.update_one({key.ID: ObjectId(grading_run_id)}, {"$set": {key.FINISHED: get_time()}})
    else:
        logger.critical("Invalid grading run state for grading run with id {}".format(grading_run_id))
