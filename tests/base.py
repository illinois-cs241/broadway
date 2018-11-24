import json

from tornado.testing import AsyncHTTPTestCase

import src.constants.api_keys as api_key
import tests.configs
from src.api import make_app
from src.database import DatabaseResolver

MOCK_TOKEN = "testing"


class BaseTest(AsyncHTTPTestCase):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.register_grader = "/api/v1/grader_register"
        self.get_job_endpoint = "/api/v1/grading_job"
        self.grading_run_endpoint = "/api/v1/grading_run"
        self.token = MOCK_TOKEN

    def get_app(self):
        self.db_resolver = DatabaseResolver(db_name='__test')
        return make_app(token=MOCK_TOKEN, db_object=self.db_resolver)

    def tearDown(self):
        super().tearDown()
        self.db_resolver.clear_db()
        # self.db_resolver.shutdown()

    def register_worker(self):
        headers = {api_key.AUTH: self.token}

        response = self.fetch(
            self.get_url(self.register_grader), method='GET', headers=headers, body=None)
        self.assertEqual(response.code, 200)
        response_body = json.loads(response.body)
        self.assertIn(api_key.WORKER_ID, response_body["data"])
        return response_body["data"].get(api_key.WORKER_ID)

    def add_grading_run(self):
        headers = {api_key.AUTH: self.token}
        response = self.fetch(
            self.get_url(self.grading_run_endpoint), method='POST', headers=headers,
            body=json.dumps(tests.configs.valid_config)
        )
        self.assertEqual(response.code, 200)
        response_body = json.loads(response.body)
        self.assertIn(api_key.RUN_ID, response_body["data"])
        return response_body["data"].get(api_key.RUN_ID)

    def start_run(self, run_id):
        headers = {api_key.AUTH: self.token}
        response = self.fetch(
            self.get_url("{}/{}".format(self.grading_run_endpoint, run_id)), method='POST', headers=headers, body=""
        )
        self.assertEqual(response.code, 200)

    def poll_job(self, worker_id):
        headers = {api_key.AUTH: self.token, api_key.WORKER_ID: worker_id}

        response = self.fetch(
            self.get_url(self.get_job_endpoint), method='GET', headers=headers, body=None
        )
        self.assertEqual(response.code, 200)
        response_body = json.loads(response.body)
        self.assertIn(api_key.JOB_ID, response_body["data"])
        self.assertIn(api_key.STAGES, response_body["data"])
        return response_body["data"]

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
