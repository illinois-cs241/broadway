import json

from tornado.testing import AsyncHTTPTestCase

import src.constants.api_keys as api_key
import tests.configs
from src.api import make_app
from src.config import OK_REQUEST_CODE, QUEUE_EMPTY_CODE, GRADING_JOB_ENDPOINT, GRADER_REGISTER_ENDPOINT, \
    GRADING_RUN_ENDPOINT
from src.database import DatabaseResolver

MOCK_TOKEN = "testing"


class BaseTest(AsyncHTTPTestCase):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.token = MOCK_TOKEN

    def get_app(self):
        self.db_resolver = DatabaseResolver(db_name='__test', logs_db_name='__test_logs')
        return make_app(token=MOCK_TOKEN, db_object=self.db_resolver)

    def tearDown(self):
        super().tearDown()
        self.db_resolver.clear_db()
        # self.db_resolver.shutdown()

    def register_worker(self):
        headers = {api_key.AUTH: self.token}

        response = self.fetch(
            self.get_url(GRADER_REGISTER_ENDPOINT), method='GET', headers=headers, body=None)
        self.assertEqual(response.code, OK_REQUEST_CODE)
        response_body = json.loads(response.body)
        self.assertIn(api_key.WORKER_ID, response_body["data"])
        return response_body["data"].get(api_key.WORKER_ID)

    def add_grading_run(self, config_obj=tests.configs.valid_config):
        headers = {api_key.AUTH: self.token}
        response = self.fetch(
            self.get_url(GRADING_RUN_ENDPOINT), method='POST', headers=headers,
            body=json.dumps(config_obj)
        )
        self.assertEqual(response.code, OK_REQUEST_CODE)
        response_body = json.loads(response.body)
        self.assertIn(api_key.RUN_ID, response_body["data"])
        return response_body["data"].get(api_key.RUN_ID)

    def start_run(self, run_id):
        headers = {api_key.AUTH: self.token}
        response = self.fetch(
            self.get_url("{}/{}".format(GRADING_RUN_ENDPOINT, run_id)), method='POST', headers=headers, body=""
        )
        self.assertEqual(response.code, OK_REQUEST_CODE)

    def poll_job(self, worker_id, empty_job=False):
        headers = {api_key.AUTH: self.token, api_key.WORKER_ID: worker_id}

        response = self.fetch(
            self.get_url(GRADING_JOB_ENDPOINT), method='GET', headers=headers, body=None
        )

        self.assertEqual(response.code, QUEUE_EMPTY_CODE if empty_job else OK_REQUEST_CODE)
        response_body = json.loads(response.body)
        self.assertIn(api_key.JOB_ID, response_body["data"])
        self.assertIn(api_key.STAGES, response_body["data"])
        return response_body["data"]

    def safe_poll_job(self, worker_id):
        headers = {api_key.AUTH: self.token, api_key.WORKER_ID: worker_id}

        while True:
            response = self.fetch(
                self.get_url(GRADING_JOB_ENDPOINT), method='GET', headers=headers, body=None
            )
            if response.code == OK_REQUEST_CODE:
                break
            else:
                self.assertEqual(response.code, QUEUE_EMPTY_CODE)

        response_body = json.loads(response.body)
        self.assertIn(api_key.JOB_ID, response_body["data"])
        self.assertIn(api_key.STAGES, response_body["data"])
        return response_body["data"]

    def post_job_result(self, worker_id, job_id):
        headers = {api_key.AUTH: self.token, api_key.WORKER_ID: worker_id}
        body = {api_key.SUCCESS: True,
                api_key.RESULTS: [{"res": "container 1 success"}, {"res": "container 2 success"}],
                api_key.LOGS: {"logs": "test logs"}}
        response = self.fetch(
            self.get_url("{}/{}".format(GRADING_JOB_ENDPOINT, job_id)), method='POST', headers=headers,
            body=json.dumps(body)
        )
        self.assertEqual(response.code, OK_REQUEST_CODE)

    def assert_equal_job(self, actual_job, expected_job):
        self.assertEqual(type(actual_job), list)
        self.assertEqual(type(expected_job), list)
        self.assertEqual(len(actual_job), len(expected_job))

        for i in range(len(actual_job)):
            self.assert_equal_stage(actual_job[i], expected_job[i])

    def assert_equal_stage(self, actual_stage, expected_stage):
        # type: (dict, dict) -> None
        self.assertEqual(type(actual_stage), dict)
        self.assertEqual(type(expected_stage), dict)
        self.assertEqual(set(actual_stage.keys()), set(expected_stage.keys()))
        for key in expected_stage:
            if key == api_key.ENV or key == api_key.ENTRY_POINT:
                self.assertEqual(sorted(actual_stage.get(key)), sorted(expected_stage.get(key)))
            else:
                self.assertEqual(actual_stage.get(key), expected_stage.get(key))
