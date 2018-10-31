import json
import requests
import sys

HOST = "fa18-cs241-437"
PORT = "8888"

if __name__ == "__main__":
    if len(sys.argv) != 2:
        print("Usage: python start_run_script.py <path to json config>")
        exit(-1)

    with open(sys.argv[1]) as f:
        config = json.load(f)
        r = requests.post("https://{}:{}/api/v1/grading_run".format(HOST, PORT),
                          data={'json_payload': json.dumps(config)})
        if r.status_code != 200:
            print("Error in uploading config: {}".format(r.text))
            exit(-1)

        id_ = json.loads(r.text)["id"]
        r = requests.post("https://{}:{}/api/v1/grading_run/{}".format(HOST, PORT, id_))
        if r.status_code != 200:
            print("Error in starting run: {}".format(r.text))
            exit(-1)

        print("Grading run with id {} started!".format(id_))
