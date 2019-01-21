import logging

from bson import ObjectId

import src.constants.keys as key
from src.database import DatabaseResolver
from src.constants.constants import GradingRunState, GradingJobType
from queue import Queue

from src.utilities import get_time, build_pipeline

logger = logging.getLogger()


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
        grading_run_collection.update_one({key.ID: ObjectId(grading_run_id)},
                                          {'$set': {key.STATE: GradingRunState.FINISHED.value, key.FINISHED: get_time(),
                                                    key.SUCCESS: True}})

    else:
        logger.critical("Invalid grading run state for grading run with id {}".format(grading_run_id))


def fail_grading_run(db_resolver, grading_run_id):
    grading_run_collection = db_resolver.get_grading_run_collection()
    grading_run_collection.update_one({key.ID: ObjectId(grading_run_id)}, {
        "$set": {key.SUCCESS: False, key.FINISHED: get_time(), key.STATE: GradingRunState.FAILED.value}})


def on_job_update(db_resolver, job_queue, grading_job_id, grading_run_id):
    grading_run_collection = db_resolver.get_grading_run_collection()
    grading_run = grading_run_collection.find_one({key.ID: ObjectId(grading_run_id)})
    student_jobs_left = grading_run.get(key.STUDENT_JOBS_LEFT)

    grading_job = db_resolver.get_grading_job_collection().find_one({key.ID: ObjectId(grading_job_id)})
    job_succeeded = grading_job.get(key.SUCCESS)
    job_type = grading_job.get(key.TYPE)

    if grading_run is None:
        logger.critical("Grading run with id {} does not exist".format(grading_run_id))
        return

    if key.FINISHED in grading_run:
        logger.critical("Received a job update for a grading run {} which already finished".format(grading_run_id))
        return

    if job_type == GradingJobType.PRE_PROCESSING.value:
        if job_succeeded:
            progress_grading_run(db_resolver, job_queue, grading_run_id)
        else:
            fail_grading_run(db_resolver, grading_run_id)

    elif job_type == GradingJobType.POST_PROCESSING.value:
        if student_jobs_left != 0:
            logger.critical("Processed post processing job when {} student jobs are left.".format(student_jobs_left))
            return

        if job_succeeded:
            progress_grading_run(db_resolver, job_queue, grading_run_id)
        else:
            fail_grading_run(db_resolver, grading_run_id)

    elif job_type == GradingJobType.STUDENT.value:
        # a student's job finished
        if student_jobs_left <= 0:
            logger.critical("Processed another student job when {} student jobs are left.".format(student_jobs_left))
            return

        grading_run_collection.update_one({key.ID: ObjectId(grading_run_id)}, {"$inc": {key.STUDENT_JOBS_LEFT: -1}})

        if student_jobs_left == 1:
            progress_grading_run(db_resolver, job_queue, grading_run_id)
