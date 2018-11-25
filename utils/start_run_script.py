import json
import sys

from tornado import httpclient

import src.constants.api_keys as api_key
from src.config import GRADING_RUN_ENDPOINT

if __name__ == "__main__":
    if len(sys.argv) != 5:
        print("Usage: python start_run_script.py <HOST> <PORT> <cluster token> <path to json config>")
        exit(-1)

    HOST = sys.argv[1]
    PORT = sys.argv[2]
    token = sys.argv[3]


    def get_url(endpoint):
        return "http://{}:{}{}".format(HOST, PORT, endpoint)


    with open(sys.argv[4]) as f:
        config = json.load(f)
        headers = {api_key.AUTH: token}
        http_client = httpclient.AsyncHTTPClient()
        add_request = httpclient.HTTPRequest(get_url(GRADING_RUN_ENDPOINT), method="POST",
                                             body=json.dumps(config))
        response = http_client.fetch(add_request)
        id_ = json.loads(response.body)["data"][api_key.RUN_ID]
        response = http_client.fetch("{}/{}".format(get_url(GRADING_RUN_ENDPOINT), id_), method='POST', headers=headers,
                                     body="")
        print("Grading run with id {} started!".format(id_))
