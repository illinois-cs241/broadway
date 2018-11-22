import json

from tornado import escape

import src.constants.api_keys as api_key
from src.config import UNAUTHORIZED_REQUEST_CODE, BAD_REQUEST_CODE
from tests.base import BaseTest
import tests.configs


class TestRegisterGrader(BaseTest):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.endpoint = "/api/v1/grader_register"

    def test_register(self):
        headers = {api_key.AUTH: self.token}

        response = self.fetch(
            self.get_url(self.endpoint), method='GET', headers=headers, body=None)
        self.assertEqual(response.code, 200)
        response_body = json.loads(escape.to_basestring(response.body))
        self.assertTrue(api_key.WORKER_ID in response_body["data"])

    def test_unauthorized(self):
        response = self.fetch(
            self.get_url(self.endpoint), method='GET', headers=None, body=None
        )
        self.assertEqual(response.code, UNAUTHORIZED_REQUEST_CODE)


class TestPollGradingJob(BaseTest):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.endpoint = "/api/v1/grading_job"

    def test_unauthorized(self):
        response = self.fetch(
            self.get_url(self.endpoint), method='GET', headers=None, body=None
        )
        self.assertEqual(response.code, UNAUTHORIZED_REQUEST_CODE)

    def test_invalid_id(self):
        headers = {api_key.AUTH: self.token, api_key.WORKER_ID: '-1'}

        response = self.fetch(
            self.get_url(self.endpoint), method='GET', headers=headers, body=None
        )
        self.assertEqual(response.code, BAD_REQUEST_CODE)

    def test_invalid_worker_id(self):
        headers = {api_key.AUTH: self.token, api_key.WORKER_ID: '1234'}

        response = self.fetch(
            self.get_url(self.endpoint), method='GET', headers=headers, body=None
        )
        self.assertEqual(response.code, BAD_REQUEST_CODE)


class TestAddGradingRun(BaseTest):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.register_grader = "/api/v1/grader_register"
        self.get_job_endpoint = "/api/v1/grading_job"
        self.grading_run_endpoint = "/api/v1/grading_run"

    def test_valid_run(self):
        headers = {api_key.AUTH: self.token}
        response = self.fetch(
            self.get_url(self.grading_run_endpoint), method='POST', headers=headers,
            body=json.dumps(tests.configs.valid_config)
        )
        self.assertEqual(response.code, 200)
        response = json.loads(response.body)
        self.assertTrue(api_key.RUN_ID in response.get('data'))

    def test_invalid_runs(self):
        headers = {api_key.AUTH: self.token}

        for invalid_config in tests.configs.invalid_configs:
            response = self.fetch(
                self.get_url(self.grading_run_endpoint), method='POST', headers=headers,
                body=json.dumps(invalid_config)
            )
            self.assertEqual(response.code, BAD_REQUEST_CODE)
