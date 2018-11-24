import json

import src.constants.api_keys as api_key
import tests.configs
from src.config import UNAUTHORIZED_REQUEST_CODE, BAD_REQUEST_CODE
from tests.base import BaseTest


class TestRegisterGrader(BaseTest):
    def test_register(self):
        self.assertIsNotNone(self.register_worker())

    def test_unauthorized(self):
        response = self.fetch(
            self.get_url(self.register_grader), method='GET', headers=None, body=None
        )
        self.assertEqual(response.code, UNAUTHORIZED_REQUEST_CODE)


class TestPollGradingJob(BaseTest):
    def test_unauthorized(self):
        response = self.fetch(
            self.get_url(self.get_job_endpoint), method='GET', headers=None, body=None
        )
        self.assertEqual(response.code, UNAUTHORIZED_REQUEST_CODE)

    def test_invalid_id(self):
        headers = {api_key.AUTH: self.token, api_key.WORKER_ID: '-1'}

        response = self.fetch(
            self.get_url(self.get_job_endpoint), method='GET', headers=headers, body=None
        )
        self.assertEqual(response.code, BAD_REQUEST_CODE)

    def test_invalid_worker_id(self):
        headers = {api_key.AUTH: self.token, api_key.WORKER_ID: '1234'}

        response = self.fetch(
            self.get_url(self.get_job_endpoint), method='GET', headers=headers, body=None
        )
        self.assertEqual(response.code, BAD_REQUEST_CODE)


class TestAddGradingRun(BaseTest):
    def test_valid_run(self):
        self.assertIsNotNone(self.add_grading_run())

    def test_invalid_runs(self):
        headers = {api_key.AUTH: self.token}

        for invalid_config in tests.configs.invalid_configs:
            response = self.fetch(
                self.get_url(self.grading_run_endpoint), method='POST', headers=headers,
                body=json.dumps(invalid_config)
            )
            self.assertEqual(response.code, BAD_REQUEST_CODE)
