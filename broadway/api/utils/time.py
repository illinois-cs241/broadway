import datetime as dt
import time


def get_time():
    """
    Get current time represented by a datetime object

    :rtype: dt.datetime
    :return: the datetime object representing the current time
    """
    return dt.datetime.fromtimestamp(time.time())


def get_time_from_string(str_time, stamp_format="%Y-%m-%d %H:%M:%S"):
    """
    Convert string representation of time to datetime using the specified format

    :param str_time: string representation of time in TIMESTAMP_FORMAT
    :rtype: dt.datetime
    :param stamp_format: the format to use to parse the string
    :type str_time: str
    :return: the datetime object representing the time
    """
    return dt.datetime.strptime(str_time, stamp_format)


def get_string_from_time(stamp_format="%Y-%m-%d %H:%M:%S"):
    """
    Get the string representation of current time

    :type stamp_format: format to return the time in
    :rtype: str
    :return: string representation of current time with TIMESTAMP_FORMAT
    """
    return get_time().strftime(stamp_format)
