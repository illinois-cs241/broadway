import json

import src.constants.api_keys as api_key
import tests.configs
from src.config import UNAUTHORIZED_REQUEST_CODE, BAD_REQUEST_CODE, OK_REQUEST_CODE
from src.config import GRADING_JOB_ENDPOINT, GRADING_RUN_ENDPOINT, WORKER_REGISTER_ENDPOINT, HEARTBEAT_ENDPOINT
from tests.base import BaseTest


class TestRegisterGrader(BaseTest):
    def test_register(self):
        self.assertIsNotNone(self.register_worker())

    def test_unauthorized(self):
        response = self.fetch(self.get_url("{}/{}".format(WORKER_REGISTER_ENDPOINT, "mockhostname")), method='GET',
                              headers=None, body=None)
        self.assertEqual(response.code, BAD_REQUEST_CODE)


class TestPollGradingJob(BaseTest):
    def test_unauthorized(self):
        response = self.fetch(
            self.get_url("{}/{}".format(GRADING_JOB_ENDPOINT, "123")), method='GET', headers=None, body=None
        )
        self.assertEqual(response.code, BAD_REQUEST_CODE)

    def test_invalid_id(self):
        response = self.fetch(
            self.get_url("{}/{}".format(GRADING_JOB_ENDPOINT, "-1")), method='GET', headers=self.grader_header, body=None
        )
        self.assertNotEqual(response.code, OK_REQUEST_CODE)

    def test_invalid_worker_id(self):
        response = self.fetch(
            self.get_url("{}/{}".format(GRADING_JOB_ENDPOINT, "1234")), method='GET', headers=self.grader_header, body=None
        )
        self.assertEqual(response.code, BAD_REQUEST_CODE)

    def test_empty_poll(self):
        worker_id = self.register_worker()
        self.poll_job(worker_id, True)


class TestUpdateGradingJob(BaseTest):
    def test_unauthorized(self):
        response = self.fetch(
            self.get_url("{}/{}".format(GRADING_JOB_ENDPOINT, "123")), method='POST', headers=None, body=''
        )
        self.assertEqual(response.code, BAD_REQUEST_CODE)

    def test_invalid_id(self):
        res = {api_key.JOB_ID: "123", api_key.SUCCESS: True,
               api_key.RESULTS: [{"result": "Worker died while executing this job"}],
               api_key.LOGS: {"logs": "No logs available for this job since the worker died while executing this job"}}

        response = self.fetch(self.get_url("{}/{}".format(GRADING_JOB_ENDPOINT, "-1")), method='POST',
                              headers=self.grader_header, body=json.dumps(res))
        self.assertNotEqual(response.code, OK_REQUEST_CODE)

    def test_invalid_worker_id(self):
        res = {api_key.JOB_ID: "123", api_key.SUCCESS: True,
               api_key.RESULTS: [{"result": "Worker died while executing this job"}],
               api_key.LOGS: {"logs": "No logs available for this job since the worker died while executing this job"}}

        response = self.fetch(self.get_url("{}/{}".format(GRADING_JOB_ENDPOINT, "123")), method='POST',
                              headers=self.grader_header, body=json.dumps(res))
        self.assertEqual(response.code, BAD_REQUEST_CODE)


class TestAddGradingRun(BaseTest):
    def test_valid_run(self):
        self.assertIsNotNone(self.add_grading_run())

    def test_invalid_runs(self):
        for invalid_config in tests.configs.invalid_configs:
            response = self.fetch(
                self.get_url(GRADING_RUN_ENDPOINT), method='POST', headers=self.grader_header, body=json.dumps(invalid_config)
            )
            self.assertEqual(response.code, BAD_REQUEST_CODE)


class TestJobPollOrder(BaseTest):
    def test_order_pre_and_post(self):
        worker_id = self.register_worker()
        self.assertIsNotNone(worker_id)

        run_id = self.add_grading_run()
        self.assertIsNotNone(run_id)
        self.start_run(run_id)

        # pre processing job
        pre_processing_job = self.poll_job(worker_id)
        self.assert_equal_job(pre_processing_job.get(api_key.STAGES), tests.configs.valid_jobs[0])
        self.assertIn(api_key.STUDENTS, pre_processing_job)
        self.assertEqual(pre_processing_job.get(api_key.STUDENTS), tests.configs.valid_config.get(api_key.STUDENTS))
        self.poll_job(worker_id, True)  # none of the student jobs should have been scheduled yet
        self.post_job_result(worker_id, pre_processing_job.get(api_key.JOB_ID))

        # student jobs
        for i in range(1, len(tests.configs.valid_jobs) - 1):
            if i == 1:
                student_job = self.safe_poll_job(worker_id)
            else:
                student_job = self.poll_job(worker_id)

            self.assert_equal_job(student_job.get(api_key.STAGES), tests.configs.valid_jobs[i])

            if i == len(tests.configs.valid_jobs) - 2:
                # the post processing job should not have been scheduled yet
                # since we have to notified the API that the last student job finished
                self.poll_job(worker_id, True)

            self.post_job_result(worker_id, student_job.get(api_key.JOB_ID))

        # post processing job
        post_processing_job = self.poll_job(worker_id)
        self.poll_job(worker_id, True)
        self.assert_equal_job(post_processing_job.get(api_key.STAGES), tests.configs.valid_jobs[-1])
        self.assertIn(api_key.STUDENTS, post_processing_job)
        self.assertEqual(post_processing_job.get(api_key.STUDENTS), tests.configs.valid_config.get(api_key.STUDENTS))
        self.post_job_result(worker_id, post_processing_job.get(api_key.JOB_ID))
        self.poll_job(worker_id, True)


class TestHeartBeat(BaseTest):
    def test_valid_heartbeat(self):
        worker_id = self.register_worker()
        response = self.fetch(self.get_url("{}/{}".format(HEARTBEAT_ENDPOINT, worker_id)), method='POST',
                              headers=self.grader_header, body='')
        self.assertEqual(response.code, OK_REQUEST_CODE)
