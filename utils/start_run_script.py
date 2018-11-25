import json
import sys

from tornado import httpclient
import utils.grading_run

import src.constants.api_keys as api_key
from src.config import GRADING_RUN_ENDPOINT

if __name__ == "__main__":
    if len(sys.argv) != 4:
        print("Usage: python start_run_script.py <HOST> <PORT> <cluster token>")
        exit(-1)

    HOST = sys.argv[1]
    PORT = sys.argv[2]
    token = sys.argv[3]

    def get_url(endpoint):
        return "http://{}:{}{}".format(HOST, PORT, endpoint)


    headers = {api_key.AUTH: token}
    http_client = httpclient.HTTPClient()
    add_request = httpclient.HTTPRequest(get_url(GRADING_RUN_ENDPOINT), method="POST",
                                         body=json.dumps(utils.grading_run.valid_json))
    response = http_client.fetch(add_request)
    id_ = json.loads(response.body)["data"][api_key.RUN_ID]
    response = http_client.fetch("{}/{}".format(get_url(GRADING_RUN_ENDPOINT), id_), method='POST', headers=headers,
                                 body="")
    print("Grading run with id {} started!".format(id_))
