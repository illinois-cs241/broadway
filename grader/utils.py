from config import API_HOSTNAME, API_PORT, API_PROXY


def get_url(endpoint):
    return "http://{}:{}{}{}".format(API_HOSTNAME, API_PORT, API_PROXY, endpoint)


def print_usage():
    print(
        "Wrong number of arguments provided. Usage:\n\tpython grader.py <cluster token> <worker id>"
    )
