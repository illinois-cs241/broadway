import datetime as dt
import time

import api_keys as api_key
from config import TIMESTAMP_FORMAT, API_HOSTNAME, API_PORT


def get_time():
    return dt.datetime.fromtimestamp(time.time()).strftime(TIMESTAMP_FORMAT)


def get_url(endpoint):
    return "http://{}:{}{}".format(API_HOSTNAME, API_PORT, endpoint)


def get_header(token, worker_id=None):
    header = {api_key.AUTH: token}
    if worker_id is not None:
        header[api_key.WORKER_ID] = worker_id
    return header


def print_usage():
    print("Wrong number of arguments provided. Usage:\n\tpython grader.py <cluster token>")
