import datetime as dt
from src.constants.constants import TIMESTAMP_FORMAT
import src.constants.db_keys as db_key
import src.constants.api_keys as api_key
from threading import Thread, Condition
import time


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
