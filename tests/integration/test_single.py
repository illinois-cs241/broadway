import json
import requests
import logging
import time

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

COURSE = "test-course"
ASSIGNMENT = "test-assignment"

ASSIGNMENT_CONFIG = "tests/_fixtures/assignment.json"
ROSTER = "tests/_fixtures/roster.json"

API_HOST = "http://api:1470"
TOKEN = "course-token"


def testSingleJobRun():
    headers = {"Authorization": "Bearer {}".format(TOKEN)}

    with open(ASSIGNMENT_CONFIG) as f:
        config = json.load(f)

    # register assignment config
    r = requests.post(
        "{}/api/v1/grading_config/{}/{}".format(API_HOST, COURSE, ASSIGNMENT),
        headers=headers,
        data=json.dumps(config),
    )

    if r.status_code != 200:
        raise RuntimeError(
            "failed to upload config: {} {}".format(r.status_code, r.text)
        )

    with open(ROSTER) as f:
        roster = json.load(f)

    # initiate grading run
    r = requests.post(
        "{}/api/v1/grading_run/{}/{}".format(API_HOST, COURSE, ASSIGNMENT),
        headers=headers,
        data=json.dumps(roster),
    )

    if r.status_code != 200:
        raise RuntimeError(
            "failed to start grading run: {} {}".format(r.status_code, r.text)
        )

    run_id = json.loads(r.text)["data"]["grading_run_id"]
    logger.info("grading run {} started".format(run_id))

    while True:  # wait for grading run to complete
        r = requests.get(
            "{}/api/v1/grading_run_status/{}/{}".format(API_HOST, COURSE, run_id),
            headers=headers,
        )

        if r.status_code != 200:
            raise RuntimeError(
                "failed to get grading run status: {} {}".format(r.status_code, r.text)
            )

        res = json.loads(r.text)["data"]
        job_state = res["student_jobs_state"]

        logger.info("run status: {}".format(res))

        if "complete" in res["state"]:
            break

        time.sleep(3)

    assert len(job_state) == 1, "expecting only one job"

    job_id, status = list(job_state.items())[0]

    assert "successful" in status, "job did not succeed"

    # check grading run output
    r = requests.get(
        "{}/api/v1/grading_job_log/{}/{}".format(API_HOST, COURSE, job_id),
        headers=headers,
    )

    if r.status_code != 200:
        raise RuntimeError(
            "failed to get grading run status: {} {}".format(r.status_code, r.text)
        )

    log = json.loads(r.text)["data"]
    logger.info("job log: {}".format(log))

    assert "student-id" in log["stdout"], "unable to find expected output"
