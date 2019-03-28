from config import API_HOSTNAME, API_PORT, API_PROXY, PROTOCOL


def get_url(endpoint):
    return "{}://{}:{}{}{}".format(PROTOCOL, API_HOSTNAME, API_PORT, API_PROXY, endpoint)


def print_usage():
    print(
        "Wrong number of arguments provided. Usage:\n\tpython grader.py <cluster token> <worker id>"
    )
