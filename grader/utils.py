from config import API_HOSTNAME, API_PORT


def get_url(endpoint):
    return "http://{}:{}{}".format(API_HOSTNAME, API_PORT, endpoint)


def print_usage():
    print("Wrong number of arguments provided. Usage:\n\tpython grader.py <cluster token>")
