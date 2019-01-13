import json

import src.constants.keys as api_key
from src.config import GRADING_JOB_ENDPOINT, WORKER_REGISTER_ENDPOINT, HEARTBEAT_ENDPOINT
from src.config import UNAUTHORIZED_REQUEST_CODE, BAD_REQUEST_CODE, OK_REQUEST_CODE
from src.utilities import get_header
from tests.base import BaseTest


class TestRegisterGrader(BaseTest):
    def test_register(self):
        self.assertIsNotNone(self.register_worker())

    def test_unauthorized(self):
        response = self.fetch(self.get_url("{}/{}".format(WORKER_REGISTER_ENDPOINT, "mockhostname")), method='GET',
                              headers=None, body=None)
        self.assertEqual(response.code, BAD_REQUEST_CODE)

    def test_wrong_token(self):
        response = self.fetch(self.get_url("{}/{}".format(WORKER_REGISTER_ENDPOINT, "mockhostname")), method='GET',
                              headers=get_header("lol"), body=None)
        self.assertEqual(response.code, UNAUTHORIZED_REQUEST_CODE)


class TestPollGradingJob(BaseTest):
    def test_unauthorized(self):
        worker_id = self.register_worker()
        response = self.fetch(
            self.get_url("{}/{}".format(GRADING_JOB_ENDPOINT, worker_id)), method='GET', headers=None, body=None
        )
        self.assertEqual(response.code, BAD_REQUEST_CODE)

    def test_wrong_token(self):
        worker_id = self.register_worker()
        response = self.fetch(self.get_url("{}/{}".format(GRADING_JOB_ENDPOINT, worker_id)), method='GET',
                              headers=get_header("lol"), body=None)
        self.assertEqual(response.code, UNAUTHORIZED_REQUEST_CODE)

    def test_invalid_worker_id(self):
        response = self.fetch(
            self.get_url("{}/{}".format(GRADING_JOB_ENDPOINT, "1234")), method='GET', headers=self.grader_header,
            body=None
        )
        self.assertEqual(response.code, BAD_REQUEST_CODE)

    def test_empty_poll(self):
        worker_id = self.register_worker()
        self.poll_job(worker_id, True)


class TestUpdateGradingJob(BaseTest):
    def test_unauthorized(self):
        worker_id = self.register_worker()
        response = self.fetch(self.get_url("{}/{}".format(GRADING_JOB_ENDPOINT, worker_id)), method='POST', body='',
                              headers=None)
        self.assertEqual(response.code, BAD_REQUEST_CODE)

    def test_wrong_token(self):
        worker_id = self.register_worker()
        response = self.fetch(self.get_url("{}/{}".format(GRADING_JOB_ENDPOINT, worker_id)), method='GET',
                              headers=get_header("lol"), body=None)
        self.assertEqual(response.code, UNAUTHORIZED_REQUEST_CODE)

    def test_invalid_worker_id(self):
        res = {api_key.GRADING_JOB_ID: "123", api_key.SUCCESS: True,
               api_key.RESULTS: [{"result": "Worker died while executing this job"}],
               api_key.LOGS: {"logs": "No logs available for this job since the worker died while executing this job"}}

        response = self.fetch(self.get_url("{}/{}".format(GRADING_JOB_ENDPOINT, "123")), method='POST',
                              headers=self.grader_header, body=json.dumps(res))
        self.assertEqual(response.code, BAD_REQUEST_CODE)


class TestHeartBeat(BaseTest):
    def test_unauthorized(self):
        worker_id = self.register_worker()
        response = self.fetch(
            self.get_url("{}/{}".format(HEARTBEAT_ENDPOINT, worker_id)), method='POST', headers=None, body=''
        )
        self.assertEqual(response.code, BAD_REQUEST_CODE)

    def test_wrong_token(self):
        worker_id = self.register_worker()
        response = self.fetch(self.get_url("{}/{}".format(HEARTBEAT_ENDPOINT, worker_id)), method='POST', body='',
                              headers=get_header("lol"))
        self.assertEqual(response.code, UNAUTHORIZED_REQUEST_CODE)

    def test_invalid_worker_id(self):
        response = self.fetch(
            self.get_url("{}/{}".format(HEARTBEAT_ENDPOINT, "1234")), method='POST', headers=self.grader_header,
            body='')
        self.assertEqual(response.code, BAD_REQUEST_CODE)

    def test_valid_heartbeat(self):
        worker_id = self.register_worker()
        response = self.fetch(self.get_url("{}/{}".format(HEARTBEAT_ENDPOINT, worker_id)), method='POST',
                              headers=self.grader_header, body='')
        self.assertEqual(response.code, OK_REQUEST_CODE)
