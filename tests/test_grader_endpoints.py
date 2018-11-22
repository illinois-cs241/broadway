import json

import src.constants.api_keys as api_key
from src.config import UNAUTHORIZED_REQUEST_CODE, QUEUE_EMPTY_CODE, BAD_REQUEST_CODE
from tests.base import BaseTest

from tornado import escape


class TestRegisterGrader(BaseTest):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.endpoint = "/api/v1/worker_register"

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

    # TODO add more tests
