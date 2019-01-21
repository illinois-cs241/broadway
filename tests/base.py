import json
import time
import unittest

import jsonschema
from tornado.testing import AsyncHTTPTestCase

import src.constants.constants as consts
import src.constants.keys as key
from src.api import make_app
from src.config import GRADING_JOB_ENDPOINT, WORKER_REGISTER_ENDPOINT, GRADING_CONFIG_ENDPOINT, GRADING_RUN_ENDPOINT, \
    HEARTBEAT_ENDPOINT
from src.config import OK_REQUEST_CODE, QUEUE_EMPTY_CODE
from src.database import DatabaseResolver
from src.utilities import get_header

MOCK_CLUSTER_TOKEN = "testing"

MOCK_COURSE1 = "mock_course1"
MOCK_COURSE2 = "mock_course2"

MOCK_CLIENT_TOKEN1 = "12345"
MOCK_CLIENT_TOKEN2 = "67890"


class BaseTest(unittest.TestCase):
    def assert_equal_grading_config(self, actual_config, expected_config):
        jsonschema.validate(actual_config, consts.GRADING_CONFIG_DEF)
        jsonschema.validate(expected_config, consts.GRADING_CONFIG_DEF)

        self.assertEqual(set(actual_config.keys()), set(expected_config.keys()))

        for config_key in expected_config:
            if config_key == key.ENV:
                self.assertEqual(sorted(actual_config.get(config_key)), sorted(expected_config.get(config_key)))
            else:
                self.assert_equal_grading_pipeline(actual_config.get(config_key), expected_config.get(config_key))

    def assert_equal_grading_pipeline(self, actual_pipeline, expected_pipeline):
        jsonschema.validate(actual_pipeline, consts.GRADING_PIPELINE_DEF)
        jsonschema.validate(expected_pipeline, consts.GRADING_PIPELINE_DEF)

        self.assertEqual(len(actual_pipeline), len(expected_pipeline))

        for i in range(len(expected_pipeline)):
            self.assert_equal_grading_stage(actual_pipeline[i], expected_pipeline[i])

    def assert_equal_grading_stage(self, actual_stage, expected_stage):
        jsonschema.validate(actual_stage, consts.GRADING_STAGE_DEF)
        jsonschema.validate(expected_stage, consts.GRADING_STAGE_DEF)

        self.assertEqual(set(actual_stage.keys()), set(expected_stage.keys()))

        for stage_key in expected_stage:
            if stage_key == key.ENV or stage_key == key.ENTRY_POINT:
                self.assertEqual(sorted(actual_stage.get(stage_key)), sorted(expected_stage.get(stage_key)))
            else:
                self.assertEqual(actual_stage.get(stage_key), expected_stage.get(stage_key))


class BaseEndpointTest(BaseTest, AsyncHTTPTestCase):
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
                        course_tokens={MOCK_COURSE1: [MOCK_CLIENT_TOKEN1],
                                       MOCK_COURSE2: [MOCK_CLIENT_TOKEN1, MOCK_CLIENT_TOKEN2]})

    def tearDown(self):
        super().tearDown()
        self.db_resolver.clear_db()
        # self.db_resolver.shutdown()

    # ------------ CLIENT HELPER METHODS ------------

    def upload_grading_config(self, course_id, assignment_name, header, grading_config, expected_code):
        response = self.fetch(self.get_url("{}/{}/{}".format(GRADING_CONFIG_ENDPOINT, course_id, assignment_name)),
                              method='POST', body=json.dumps(grading_config), headers=header)
        self.assertEqual(response.code, expected_code)

    def get_grading_config(self, course_id, assignment_name, header, expected_code):
        response = self.fetch(self.get_url("{}/{}/{}".format(GRADING_CONFIG_ENDPOINT, course_id, assignment_name)),
                              method='GET', headers=header)
        self.assertEqual(response.code, expected_code)

        if response.code == OK_REQUEST_CODE:
            response_body = json.loads(response.body)
            return response_body["data"]

    def start_grading_run(self, course_id, assignment_name, header, students, expected_code):
        response = self.fetch(self.get_url("{}/{}/{}".format(GRADING_RUN_ENDPOINT, course_id, assignment_name)),
                              method='POST', headers=header, body=json.dumps(students))
        self.assertEqual(response.code, expected_code)

        if response.code == OK_REQUEST_CODE:
            response_body = json.loads(response.body)
            return response_body["data"][key.GRADING_RUN_ID]

    def get_grading_run_state(self, course_id, assignment_name, grading_run_id, header):
        response = self.fetch(
            self.get_url("{}/{}/{}/{}".format(GRADING_RUN_ENDPOINT, course_id, assignment_name, grading_run_id)),
            method='GET', headers=header)
        self.assertEqual(response.code, OK_REQUEST_CODE)

        response_body = json.loads(response.body)
        return response_body["data"].get(key.STATE)

    def check_grading_run_status(self, course_id, assignment_name, grading_run_id, header, expected_code,
                                 expected_state=None):
        response = self.fetch(
            self.get_url("{}/{}/{}/{}".format(GRADING_RUN_ENDPOINT, course_id, assignment_name, grading_run_id)),
            method='GET', headers=header)
        self.assertEqual(response.code, expected_code)

        if response.code == OK_REQUEST_CODE:
            response_body = json.loads(response.body)
            self.assertEqual(response_body["data"].get(key.STATE), expected_state)

    # ------------ GRADER HELPER METHODS ------------

    def register_worker(self, header, expected_code=OK_REQUEST_CODE, hostname="mock_hostname"):
        response = self.fetch(self.get_url("{}/{}".format(WORKER_REGISTER_ENDPOINT, hostname)), method='GET',
                              headers=header)
        self.assertEqual(response.code, expected_code)

        if expected_code == OK_REQUEST_CODE:
            response_body = json.loads(response.body)
            self.assertIn(key.WORKER_ID, response_body["data"])
            return response_body["data"].get(key.WORKER_ID)

    def poll_job(self, worker_id, header):
        response = self.fetch(self.get_url("{}/{}".format(GRADING_JOB_ENDPOINT, worker_id)), method='GET',
                              headers=header)

        if response.code == OK_REQUEST_CODE:
            self.assertEqual(response.code, OK_REQUEST_CODE)
            response_body = json.loads(response.body)
            self.assertIn(key.GRADING_JOB_ID, response_body["data"])
            self.assertIn(key.STAGES, response_body["data"])
            return response_body["data"]

        return response.code

    def post_job_result(self, worker_id, header, job_id, job_success=True, expected_code=OK_REQUEST_CODE):
        body = {key.GRADING_JOB_ID: job_id,
                key.SUCCESS: job_success,
                key.RESULTS: [{"res": "container 1 res"}, {"res": "container 2 res"}],
                key.LOGS: {"logs": "test logs"}}
        response = self.fetch(
            self.get_url("{}/{}".format(GRADING_JOB_ENDPOINT, worker_id)), method='POST', headers=header,
            body=json.dumps(body)
        )
        self.assertEqual(response.code, expected_code)

    def send_heartbeat(self, worker_id, header, expected_code=OK_REQUEST_CODE):
        response = self.fetch(self.get_url("{}/{}".format(HEARTBEAT_ENDPOINT, worker_id)), method='POST', body='',
                              headers=header)
        self.assertEqual(response.code, expected_code)
