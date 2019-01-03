import json
import requests
import sys

HOST = ""
PORT = ""

if __name__ == "__main__":
    if len(sys.argv) != 3:
        print("Usage: python start_run_script.py <path to config json> <token>")
        exit(-1)
    token = sys.argv[2]
    with open(sys.argv[1]) as f:
        headers = {"Authorization": "Bearer ".format(token)}
        config = json.load(f)
        r = requests.post("http://{}:{}/api/v1/grading_run".format(HOST, PORT), headers=headers,
                          data=json.dumps(config))
        if r.status_code != 200:
            print("Error in uploading config: {}".format(r.text))
            exit(-1)

        id_ = json.loads(r.text)["data"]["grading_run_id"]
        r = requests.post("http://{}:{}/api/v1/grading_run/{}".format(HOST, PORT, id_), headers=headers)
        if r.status_code != 200:
            print("Error in starting run: {}".format(r.text))
            exit(-1)

        print("Grading run with id {} started!".format(id_))
