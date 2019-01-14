import datetime as dt
import logging
import time
from threading import Thread, Condition

from bson import ObjectId

import src.constants.keys as key
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


def get_status(student_job):
    """
    Given a student job, gives a string description of what state the job is in.

    :type student_job: dict
    :param student_job: dictionary representing student job
    :rtype: str
    :return: state description
    """
    if key.FINISHED in student_job:
        return "Finished"
    elif key.STARTED in student_job:
        return "Running"
    elif key.QUEUED in student_job:
        return "Queued"
    else:
        return "Created"


def get_header(token):
    """
    Constructs the HTTP request header in correct format

    :type token: str
    :param token: the token used for authentication. Could be cluster token or a course related token
    :rtype: dict
    :return: the HTTP Request header as a dict
    """
    return {key.AUTH: "Bearer {}".format(token)}


def enqueue_job(db_resolver, job_queue, job_id, students=None):
    """
    Enqueues a job into the queue passed in the correct format

    :param db_resolver: DatabaseResolver object
    :param job_queue: thread-safe Queue into which the job is pushed
    :param job_id: ID of the Mongo document representing grading job to be queued
    :param students: list of dict representing roster
    """
    jobs_collection = db_resolver.get_grading_job_collection()
    job = jobs_collection.find_one({key.ID: ObjectId(job_id)})

    cur_job = {key.STAGES: job[key.STAGES], key.GRADING_JOB_ID: job_id}
    if students is not None:
        cur_job[key.STUDENTS] = students

    job_queue.put(cur_job)
    jobs_collection.update_one({key.ID: ObjectId(job_id)}, {"$set": {key.QUEUED: get_time()}})


def enqueue_student_jobs(db_resolver, job_queue, grading_run):
    """
    Enqueues all student jobs for the given grading run

    :param db_resolver: DatabaseResolver object
    :param job_queue:thread-safe Queue into which the jobs are pushed
    :param grading_run: the grading run document which contains the roster for this run
    """
    for student_job_id in grading_run.get(key.STUDENT_JOBS):
        enqueue_job(db_resolver, job_queue, student_job_id)


def job_update_callback(db_resolver, job_queue, job_id, grading_run_id, job_succeeded):
    grading_run_collection = db_resolver.get_grading_run_collection()

    # update grading run: if last job finished then update finished_at. Update student_jobs_left if student job.
    # enqueue post processing if all student jobs finished

    grading_run = grading_run_collection.find_one({key.ID: ObjectId(grading_run_id)})
    if grading_run is None:
        logger.critical("Received job update for job {}. Its document points to a non-existent grading run with "
                        "id {}.".format(job_id, grading_run_id))
        return

    # Since the job is in valid state, the following errors imply that there is some error with application logic
    if key.CREATED not in grading_run:
        logger.critical("CREATED field not set in grading run with id {}".format(grading_run_id))
        return

    if key.STARTED not in grading_run:
        logger.critical("Received a job update for a job with id {} belonging to a grading run with id {} that "
                        "had not even been started".format(job_id, grading_run_id))
        return

    if key.FINISHED in grading_run:
        logger.critical("Received a job update for a job with id {} belonging to a grading run with id {} that "
                        "had already finished".format(job_id, grading_run_id))
        return

    if grading_run.get(key.PRE_PROCESSING, "") == job_id:
        # pre processing job finished
        if job_succeeded:
            enqueue_student_jobs(db_resolver, job_queue, grading_run)
        else:
            grading_run_collection.update_one({key.ID: ObjectId(grading_run_id)},
                                              {"$set": {key.SUCCESS: False, key.FINISHED: get_time()}})

    elif grading_run.get(key.POST_PROCESSING, "") == job_id:
        # post processing job finished so the grading run is over
        if grading_run.get(key.STUDENT_JOBS_LEFT) != 0:
            logger.critical("Processed post processing job when {} student jobs are left. Something is wrong with "
                            "the scheduling logic".format(grading_run.get(key.STUDENT_JOBS_LEFT)))
            return

        grading_run_collection.update_one({key.ID: ObjectId(grading_run_id)},
                                          {"$set": {key.SUCCESS: job_succeeded, key.FINISHED: get_time()}})

    else:
        # a student's job finished
        if grading_run.get(key.STUDENT_JOBS_LEFT) <= 0:
            logger.critical("Processed another student job when the number of student jobs is not positive. "
                            "Something is wrong with the counting logic. Possible race condition.")
            return

        grading_run_collection.update_one({key.ID: ObjectId(grading_run_id)},
                                          {"$inc": {key.STUDENT_JOBS_LEFT: -1}})

        if grading_run[key.STUDENT_JOBS_LEFT] == 1:
            # this was the last student job which finished so if post processing exists then schedule it
            if key.POST_PROCESSING in grading_run:
                enqueue_job(db_resolver, job_queue, grading_run.get(key.POST_PROCESSING),
                            grading_run.get(key.STUDENTS))
            else:
                grading_run_collection.update_one({key.ID: ObjectId(grading_run_id)},
                                                  {"$set": {key.SUCCESS: True, key.FINISHED: get_time()}})
