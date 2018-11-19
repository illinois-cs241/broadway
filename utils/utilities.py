import datetime as dt
import time
from src.settings import TIMESTAMP_FORMAT


def get_formatted_time():
    return dt.datetime.fromtimestamp(time.time()).strftime(TIMESTAMP_FORMAT)
