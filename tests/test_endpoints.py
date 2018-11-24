import json

import src.constants.api_keys as api_key
import tests.configs
from src.config import UNAUTHORIZED_REQUEST_CODE, BAD_REQUEST_CODE, OK_REQUEST_CODE
from src.config import GRADING_JOB_ENDPOINT, GRADING_RUN_ENDPOINT, GRADER_REGISTER_ENDPOINT, HEARTBEAT_ENDPOINT
from tests.base import BaseTest


class TestRegisterGrader(BaseTest):
    def test_register(self):
        self.assertIsNotNone(self.register_worker())

    def test_unauthorized(self):
        response = self.fetch(
            self.get_url(GRADER_REGISTER_ENDPOINT), method='GET', headers=None, body=None
        )
        self.assertEqual(response.code, UNAUTHORIZED_REQUEST_CODE)


class TestPollGradingJob(BaseTest):
    def test_unauthorized(self):
        response = self.fetch(
            self.get_url(GRADING_JOB_ENDPOINT), method='GET', headers=None, body=None
        )
        self.assertEqual(response.code, UNAUTHORIZED_REQUEST_CODE)

    def test_invalid_id(self):
        headers = {api_key.AUTH: self.token, api_key.WORKER_ID: '-1'}

        response = self.fetch(
            self.get_url(GRADING_JOB_ENDPOINT), method='GET', headers=headers, body=None
        )
        self.assertEqual(response.code, BAD_REQUEST_CODE)

    def test_invalid_worker_id(self):
        headers = {api_key.AUTH: self.token, api_key.WORKER_ID: '1234'}

        response = self.fetch(
            self.get_url(GRADING_JOB_ENDPOINT), method='GET', headers=headers, body=None
        )
        self.assertEqual(response.code, BAD_REQUEST_CODE)


class TestAddGradingRun(BaseTest):
    def test_valid_run(self):
        self.assertIsNotNone(self.add_grading_run())

    def test_invalid_runs(self):
        headers = {api_key.AUTH: self.token}

        for invalid_config in tests.configs.invalid_configs:
            response = self.fetch(
                self.get_url(GRADING_RUN_ENDPOINT), method='POST', headers=headers,
                body=json.dumps(invalid_config)
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
        self.post_job_result(worker_id, pre_processing_job.get(api_key.JOB_ID))

        # student jobs
        for i in range(1, len(tests.configs.valid_jobs) - 1):
            student_job = self.poll_job(worker_id)
            self.assert_equal_job(student_job.get(api_key.STAGES), tests.configs.valid_jobs[i])
            self.post_job_result(worker_id, student_job.get(api_key.JOB_ID))

        # post processing job
        post_processing_job = self.poll_job(worker_id)
        self.assert_equal_job(post_processing_job.get(api_key.STAGES), tests.configs.valid_jobs[-1])
        self.assertIn(api_key.STUDENTS, post_processing_job)
        self.assertEqual(post_processing_job.get(api_key.STUDENTS), tests.configs.valid_config.get(api_key.STUDENTS))
        self.post_job_result(worker_id, post_processing_job.get(api_key.JOB_ID))


class TestHeartBeat(BaseTest):
    def test_valid_heartbeat(self):
        worker_id = self.register_worker()
        headers = {api_key.AUTH: self.token, api_key.WORKER_ID: worker_id}
        response = self.fetch(self.get_url(HEARTBEAT_ENDPOINT), method='POST', headers=headers, body='')
        self.assertEqual(response.code, OK_REQUEST_CODE)
