import json
import requests
import sys

PROTOCOL = "https"
HOST = ""
COURSE = ""
ASSIGNMENT = ""

if __name__ == "__main__":
    if len(sys.argv) != 4:
        print(
            "Usage: python start_run_script.py "
            "<path to config json> <path to roster json> <token>"
        )
        exit(1)
    token = sys.argv[3]
    headers = {"Authorization": "Bearer {}".format(token)}

    with open(sys.argv[1]) as f:
        config = json.load(f)

    r = requests.post(
        "{}://{}/api/v1/grading_config/{}/{}".format(
            PROTOCOL, HOST, COURSE, ASSIGNMENT
        ),
        headers=headers,
        data=json.dumps(config),
    )
    if r.status_code != 200:
        print("Error in uploading config: {}".format(r.text))
        exit(1)

    with open(sys.argv[2]) as f:
        roster = json.load(f)

    r = requests.post(
        "{}://{}/api/v1/grading_run/{}/{}".format(PROTOCOL, HOST, COURSE, ASSIGNMENT),
        headers=headers,
        data=json.dumps(roster),
    )
    if r.status_code != 200:
        print("Error in starting run: {}".format(r.text))
        exit(1)

    id_ = json.loads(r.text)["data"]["grading_run_id"]
    print("Grading run with id {} started!".format(id_))
