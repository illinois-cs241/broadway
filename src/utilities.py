import datetime as dt
import time
from src.constants.constants import TIMESTAMP_FORMAT


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
