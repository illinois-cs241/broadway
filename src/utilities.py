import datetime as dt
import logging
import time
from threading import Thread, Condition

from bson import ObjectId

import src.constants.api_keys as api_key
import src.constants.db_keys as db_key
from src.constants.constants import TIMESTAMP_FORMAT

logger = logging.getLogger()


class PeriodicCallbackThread:
    def _routine(self):
        while self._running:
            self._callback(*self._args)
            self._cv.acquire()
            self._cv.wait(timeout=self._interval)
            self._cv.release()

    def __init__(self, callback, interval, args=()):
        self._callback = callback
        self._interval = interval
        self._args = args
        self._running = False
        self._cv = Condition()
        self._thread = Thread(target=self._routine)

    def start(self):
        self._running = True
        self._thread.start()

    def stop(self):
        self._running = False
        self._cv.acquire()
        self._cv.notify_all()
        self._cv.release()
        self._thread.join()


def get_time():
    # type: () -> dt.datetime
    """
    :return: the datetime object representing the current time
    """
    return dt.datetime.fromtimestamp(time.time())


def get_time_from_string(str_time):
    # type: (str) -> dt.datetime
    """
    :param str_time: string representation of time in TIMESTAMP_FORMAT
    :return: the datetime object representing the time
    """
    return dt.datetime.strptime(str_time, TIMESTAMP_FORMAT)


def get_string_from_time():
    # type: () -> str
    """
    :return: string representation of current time with TIMESTAMP_FORMAT
    """
    return get_time().strftime(TIMESTAMP_FORMAT)


def get_status(student_job):
    # type: (dict) -> str
    """
    Given a student job, gives a string description of what state the job is in.
    :param student_job: dictionary representing student job
    :return: state description
    """
    if db_key.FINISHED in student_job:
        return "Finished"
    elif db_key.STARTED in student_job:
        return "Running"
    elif db_key.QUEUED in student_job:
        return "Queued"
    else:
        return "Created"


def get_header(token):
    return {api_key.AUTH: "Bearer {}".format(token)}


def resolve_env_vars(stage_env_vars, global_env_vars, student_env_vars=None):
    # type: (dict, dict, dict) -> list
    """
    Builds the environment variables for any given stage. First adds all global environment variables and student
    specific environment variables. Then adds all stage specific environment variables. If the variable can be expanded
    by the global or student env vars, then expands it. In other words, if env var value is of form "$<var_name>" and
    var_name is one of the global or student env vars, then it replaces the value of that stage env var with the value
    of var_name.

    :param stage_env_vars: stage specific env vars. Format: {<var_name>: <value>}
    :param global_env_vars: global env vars. Format: {<var_name>: <value>}
    :param student_env_vars: student specific env vars. Format: {<var_name>: <value>}
    :return: final list of all aggregated environment variables. Format: ["<var_name>=value"]
    """

    def get_result_format(var_name, var_value):
        return "{}={}".format(var_name, var_value)

    if student_env_vars is None:
        student_env_vars = {}

    res_vars = []

    if any(global_env_vars):
        for global_var, global_value in global_env_vars.items():
            res_vars.append(get_result_format(global_var, global_value))

    if any(student_env_vars):
        for student_var, student_value in student_env_vars.items():
            res_vars.append(get_result_format(student_var, student_value))

    for var_name in stage_env_vars:
        # value is not specified: if the env var is defined anywhere else, replace it
        if len(stage_env_vars[var_name]) == 0:
            if var_name in global_env_vars:
                res_vars.append(get_result_format(var_name, global_env_vars[var_name]))
            elif var_name in student_env_vars:
                res_vars.append(get_result_format(var_name, student_env_vars[var_name]))
            else:
                res_vars.append(get_result_format(var_name, stage_env_vars[var_name]))

        # if the env var is dependent on another, substitute appropriately
        elif stage_env_vars[var_name][0] == '$':
            var_to_sub = stage_env_vars[var_name][1:]
            if var_to_sub in global_env_vars:
                res_vars.append(get_result_format(var_name, global_env_vars[var_to_sub]))
            elif var_to_sub in student_env_vars:
                res_vars.append(get_result_format(var_name, student_env_vars[var_to_sub]))
            else:
                res_vars.append(get_result_format(var_name, stage_env_vars[var_name]))

        # if the value is independent, just copy it over
        else:
            res_vars.append(get_result_format(var_name, stage_env_vars[var_name]))

    return res_vars


def enqueue_job(db_resolver, job_queue, job_id, students=None):
    """
    Enqueues a job into the queue passed in the correct format

    :param db_resolver: DatabaseResolver object
    :param job_queue: thread-safe Queue into which the job is pushed
    :param job_id: ID of the Mongo document representing grading job to be queued
    :param students: list of dict representing roster
    """
    jobs_collection = db_resolver.get_grading_job_collection()
    job = jobs_collection.find_one({db_key.ID: ObjectId(job_id)})

    cur_job = {api_key.STAGES: job[db_key.STAGES], api_key.GRADING_JOB_ID: job_id}
    if students is not None:
        cur_job[api_key.STUDENTS] = students

    job_queue.put(cur_job)
    jobs_collection.update_one({db_key.ID: ObjectId(job_id)}, {"$set": {db_key.QUEUED: get_time()}})


def enqueue_student_jobs(db_resolver, job_queue, grading_run):
    """
    Enqueues all student jobs for the given grading run

    :param db_resolver: DatabaseResolver object
    :param job_queue:thread-safe Queue into which the jobs are pushed
    :param grading_run: the grading run document which contains the roster for this run
    """
    for student_job_id in grading_run.get(db_key.STUDENT_JOBS):
        enqueue_job(db_resolver, job_queue, student_job_id)


def job_update_callback(db_resolver, job_queue, job_id, grading_run_id, job_succeeded):
    grading_run_collection = db_resolver.get_grading_run_collection()

    # update grading run: if last job finished then update finished_at. Update student_jobs_left if student job.
    # enqueue post processing if all student jobs finished

    grading_run = grading_run_collection.find_one({db_key.ID: ObjectId(grading_run_id)})
    if grading_run is None:
        logger.critical("Received job update for job {}. Its document points to a non-existent grading run with "
                        "id {}.".format(job_id, grading_run_id))
        return

    # Since the job is in valid state, the following errors imply that there is some error with application logic
    if db_key.CREATED not in grading_run:
        logger.critical("CREATED field not set in grading run with id {}".format(grading_run_id))
        return

    if db_key.STARTED not in grading_run:
        logger.critical("Received a job update for a job with id {} belonging to a grading run with id {} that "
                        "had not even been started".format(job_id, grading_run_id))
        return

    if db_key.FINISHED in grading_run:
        logger.critical("Received a job update for a job with id {} belonging to a grading run with id {} that "
                        "had already finished".format(job_id, grading_run_id))
        return

    if grading_run.get(db_key.PRE_PROCESSING, "") == job_id:
        # pre processing job finished
        if job_succeeded:
            enqueue_student_jobs(db_resolver, job_queue, grading_run)
        else:
            grading_run_collection.update_one({db_key.ID: ObjectId(grading_run_id)},
                                              {"$set": {db_key.SUCCESS: False, db_key.FINISHED: get_time()}})

    elif grading_run.get(db_key.POST_PROCESSING, "") == job_id:
        # post processing job finished so the grading run is over
        if grading_run.get(db_key.STUDENT_JOBS_LEFT) != 0:
            logger.critical("Processed post processing job when {} student jobs are left. Something is wrong with "
                            "the scheduling logic".format(grading_run.get(db_key.STUDENT_JOBS_LEFT)))
            return

        grading_run_collection.update_one({db_key.ID: ObjectId(grading_run_id)},
                                          {"$set": {db_key.SUCCESS: job_succeeded, db_key.FINISHED: get_time()}})

    else:
        # a student's job finished
        if grading_run.get(db_key.STUDENT_JOBS_LEFT) <= 0:
            logger.critical("Processed another student job when the number of student jobs is not positive. "
                            "Something is wrong with the counting logic. Possible race condition.")
            return

        grading_run_collection.update_one({db_key.ID: ObjectId(grading_run_id)},
                                          {"$inc": {db_key.STUDENT_JOBS_LEFT: -1}})

        if grading_run[db_key.STUDENT_JOBS_LEFT] == 1:
            # this was the last student job which finished so if post processing exists then schedule it
            if db_key.POST_PROCESSING in grading_run:
                enqueue_job(db_resolver, job_queue, grading_run.get(db_key.POST_PROCESSING),
                            grading_run.get(db_key.STUDENTS))
            else:
                grading_run_collection.update_one({db_key.ID: ObjectId(grading_run_id)},
                                                  {"$set": {db_key.SUCCESS: True, db_key.FINISHED: get_time()}})
