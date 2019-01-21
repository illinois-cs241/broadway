import datetime as dt
import logging
import time
from threading import Thread, Condition

import src.constants.keys as key
from src.constants.constants import TIMESTAMP_FORMAT, GradingJobState

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
    """
    Get current time represented by a datetime object

    :rtype: dt.datetime
    :return: the datetime object representing the current time
    """
    return dt.datetime.fromtimestamp(time.time())


def get_time_from_string(str_time):
    """
    Convert string representation of time to datetime object using the timestamp format specified

    :type str_time: str
    :param str_time: string representation of time in TIMESTAMP_FORMAT
    :rtype: dt.datetime
    :return: the datetime object representing the time
    """
    return dt.datetime.strptime(str_time, TIMESTAMP_FORMAT)


def get_string_from_time():
    """
    Get the string representation of current time

    :rtype: str
    :return: string representation of current time with TIMESTAMP_FORMAT
    """
    return get_time().strftime(TIMESTAMP_FORMAT)


def get_job_status(student_job):
    """
    Given a student job, gives a string description of what state the job is in.

    :type student_job: dict
    :param student_job: dictionary representing student job
    :rtype: GradingJobState
    :return: state description
    """
    if key.FINISHED in student_job:
        return GradingJobState.FINISHED
    elif key.STARTED in student_job:
        return GradingJobState.STARTED
    elif key.QUEUED in student_job:
        return GradingJobState.QUEUED
    else:
        logger.critical("Grading job document passed is invalid")
        return None


def get_header(token):
    """
    Constructs the HTTP request header in correct format

    :type token: str
    :param token: the token used for authentication. Could be cluster token or a course related token
    :rtype: dict
    :return: the HTTP Request header as a dict
    """
    return {key.AUTH: "Bearer {}".format(token)}


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
