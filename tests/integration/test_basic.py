import json
import logging
import time

from tests.integration.base import BaseTest

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class TestBasic(BaseTest):
    def testSingleJobRun(self):
        COURSE = "test-course"
        ASSIGNMENT = "test-assignment"

        ASSIGNMENT_CONFIG = "tests/_fixtures/assignment.json"
        ROSTER = "tests/_fixtures/roster.json"

        with open(ASSIGNMENT_CONFIG) as f:
            config = json.load(f)

        # register assignment config
        r = self.api_post(
            "/api/v1/grading_config/{}/{}".format(COURSE, ASSIGNMENT), config
        )
        self.assertResponse(r, 200)

        with open(ROSTER) as f:
            roster = json.load(f)

        # initiate grading run
        r = self.api_post(
            "/api/v1/grading_run/{}/{}".format(COURSE, ASSIGNMENT), roster
        )
        self.assertResponse(r, 200)

        run_id = json.loads(r.text)["data"]["grading_run_id"]
        logger.info("grading run {} started".format(run_id))

        while True:  # wait for grading run to complete
            r = self.api_get("/api/v1/grading_run_status/{}/{}".format(COURSE, run_id))
            self.assertResponse(r, 200)

            res = json.loads(r.text)["data"]
            job_state = res["student_jobs_state"]

            logger.info("run status: {}".format(res))

            if "complete" in res["state"]:
                break

            time.sleep(3)

        self.assertEqual(len(job_state), 1, "expecting only one job")

        job_id, status = list(job_state.items())[0]

        self.assertTrue("successful" in status, "expecting job to succeed")

        # check grading run output
        r = self.api_get("/api/v1/grading_job_log/{}/{}".format(COURSE, job_id))
        self.assertResponse(r, 200)

        log = json.loads(r.text)["data"]
        logger.info("job log: {}".format(log))

        self.assertTrue("student-id" in log["stdout"], "unable to find expected output")
