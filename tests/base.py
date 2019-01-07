import json
import time

from tornado.testing import AsyncHTTPTestCase

import src.constants.api_keys as api_key
import src.constants.constants as consts
import tests.configs
from src.api import make_app
from src.config import OK_REQUEST_CODE, QUEUE_EMPTY_CODE, GRADING_JOB_ENDPOINT, WORKER_REGISTER_ENDPOINT, \
    GRADING_RUN_ENDPOINT
from src.database import DatabaseResolver
from src.utilities import get_header

MOCK_CLUSTER_TOKEN = "testing"

MOCK_COURSE1 = "mock_course1"
MOCK_COURSE2 = "mock_course2"

MOCK_CLIENT_TOKEN1 = "12345"
MOCK_CLIENT_TOKEN2 = "67890"


class BaseTest(AsyncHTTPTestCase):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.grader_header = get_header(MOCK_CLUSTER_TOKEN)
        self.client_header1 = get_header(MOCK_CLIENT_TOKEN1)
        self.client_header2 = get_header(MOCK_CLIENT_TOKEN2)
        self.course1 = MOCK_COURSE1
        self.course2 = MOCK_COURSE2

    def get_app(self):
        self.db_resolver = DatabaseResolver(db_name='__test', logs_db_name='__test_logs')
        return make_app(cluster_token=MOCK_CLUSTER_TOKEN, db_resolver=self.db_resolver,
                        course_tokens={
                            consts.CONFIG_TOKENS: {"token1": MOCK_CLIENT_TOKEN1, "token2": MOCK_CLIENT_TOKEN2},
                            consts.CONFIG_COURSES: {MOCK_COURSE1: ["token1"],
                                                    MOCK_COURSE2: ["token1", "token2"]}})

    def tearDown(self):
        super().tearDown()
        self.db_resolver.clear_db()
        # self.db_resolver.shutdown()

    def add_grading_run(self, config_obj=tests.configs.valid_config):
        response = self.fetch(
            self.get_url(GRADING_RUN_ENDPOINT), method='POST', headers=self.grader_header, body=json.dumps(config_obj)
        )
        self.assertEqual(response.code, OK_REQUEST_CODE)
        response_body = json.loads(response.body)
        self.assertIn(api_key.RUN_ID, response_body["data"])
        return response_body["data"].get(api_key.RUN_ID)

    def start_run(self, run_id):
        response = self.fetch(
            self.get_url("{}/{}".format(GRADING_RUN_ENDPOINT, run_id)), method='POST', headers=self.grader_header,
            body=""
        )
        self.assertEqual(response.code, OK_REQUEST_CODE)

    def register_worker(self):
        response = self.fetch(self.get_url("{}/{}".format(WORKER_REGISTER_ENDPOINT, "mock_hostname")), method='GET',
                              headers=self.grader_header, body=None)
        self.assertEqual(response.code, OK_REQUEST_CODE)
        response_body = json.loads(response.body)
        self.assertIn(api_key.WORKER_ID, response_body["data"])
        return response_body["data"].get(api_key.WORKER_ID)

    def poll_job(self, worker_id, empty_job=False):
        response = self.fetch(
            self.get_url("{}/{}".format(GRADING_JOB_ENDPOINT, worker_id)), method='GET', headers=self.grader_header,
            body=None
        )

        self.assertEqual(response.code, QUEUE_EMPTY_CODE if empty_job else OK_REQUEST_CODE)
        response_body = json.loads(response.body)
        self.assertIn(api_key.JOB_ID, response_body["data"])
        self.assertIn(api_key.STAGES, response_body["data"])
        return response_body["data"]

    def safe_poll_job(self, worker_id):
        while True:
            response = self.fetch(
                self.get_url("{}/{}".format(GRADING_JOB_ENDPOINT, worker_id)), method='GET', headers=self.grader_header,
                body=None
            )

            if response.code == OK_REQUEST_CODE:
                break

            self.assertEqual(response.code, QUEUE_EMPTY_CODE)
            time.sleep(1)

        response_body = json.loads(response.body)
        self.assertIn(api_key.JOB_ID, response_body["data"])
        self.assertIn(api_key.STAGES, response_body["data"])
        return response_body["data"]

    def post_job_result(self, worker_id, job_id):
        body = {api_key.JOB_ID: job_id,
                api_key.SUCCESS: True,
                api_key.RESULTS: [{"res": "container 1 success"}, {"res": "container 2 success"}],
                api_key.LOGS: {"logs": "test logs"}}
        response = self.fetch(
            self.get_url("{}/{}".format(GRADING_JOB_ENDPOINT, worker_id)), method='POST', headers=self.grader_header,
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
