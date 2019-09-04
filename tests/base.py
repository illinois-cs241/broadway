import json
import jsonschema
import uuid
import unittest

import websockets

from tornado.testing import AsyncHTTPTestCase

import broadway_api.definitions as definitions

from broadway_api.bootstrap import initialize_database, initialize_app
from broadway_api import gen_global_settings, gen_flags

import tests._utils.database as database_utils

MOCK_COURSE1 = "mock_course1"
MOCK_COURSE2 = "mock_course2"

MOCK_CLIENT_TOKEN1 = "12345"
MOCK_CLIENT_TOKEN2 = "67890"


class AsyncHTTPMixin(AsyncHTTPTestCase):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

    def get_app(self):
        """
        Note: this is called by setUp in AsyncHTTPTestCase
        """

        fset = gen_flags()
        flags = fset.parse(["tests/_fixtures/config.json"], use_exc=True)

        self.app = initialize_app(gen_global_settings(flags), flags)

        initialize_database(self.app.settings, flags)

        database_utils.initialize_db(
            self.app.settings,
            {
                MOCK_COURSE1: [MOCK_CLIENT_TOKEN1],
                MOCK_COURSE2: [MOCK_CLIENT_TOKEN1, MOCK_CLIENT_TOKEN2],
            },
        )

        return self.app

    def get_token(self):
        return self.app.settings["CLUSTER_TOKEN"]

    def get_header(self, override=None):
        return {
            "Authorization": "Bearer "
            + (self.get_token() if not override else override)
        }

    def tearDown(self):
        super().tearDown()
        database_utils.clear_db(self.app.settings)


class ClientMixin(AsyncHTTPMixin):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.client_header1 = {"Authorization": "Bearer " + MOCK_CLIENT_TOKEN1}
        self.client_header2 = {"Authorization": "Bearer " + MOCK_CLIENT_TOKEN2}
        self.course1 = MOCK_COURSE1
        self.course2 = MOCK_COURSE2

    def upload_grading_config(
        self, course_id, assignment_name, header, grading_config, expected_code
    ):
        response = self.fetch(
            self.get_url(
                "/api/v1/grading_config/{}/{}".format(course_id, assignment_name)
            ),
            method="POST",
            body=json.dumps(grading_config),
            headers=header,
        )
        self.assertEqual(response.code, expected_code)

    def get_grading_config(self, course_id, assignment_name, header, expected_code):
        response = self.fetch(
            self.get_url(
                "/api/v1/grading_config/{}/{}".format(course_id, assignment_name)
            ),
            method="GET",
            headers=header,
        )
        self.assertEqual(response.code, expected_code)

        if response.code == 200:
            response_body = json.loads(response.body.decode("utf-8"))
            return response_body["data"]

    def start_grading_run(
        self, course_id, assignment_name, header, students, expected_code
    ):
        response = self.fetch(
            self.get_url(
                "/api/v1/grading_run/{}/{}".format(course_id, assignment_name)
            ),
            method="POST",
            headers=header,
            body=json.dumps(students),
        )
        self.assertEqual(response.code, expected_code)

        if response.code == 200:
            response_body = json.loads(response.body.decode("utf-8"))
            return response_body["data"]["grading_run_id"]

    def get_grading_run_state(self, course_id, grading_run_id, header):
        response = self.fetch(
            self.get_url(
                "/api/v1/grading_run_status/{}/{}".format(course_id, grading_run_id)
            ),
            method="GET",
            headers=header,
        )
        self.assertEqual(response.code, 200)

        response_body = json.loads(response.body.decode("utf-8"))
        return response_body["data"]

    def check_grading_run_status(
        self, course_id, grading_run_id, header, expected_code, expected_state=None
    ):
        response = self.fetch(
            self.get_url(
                "/api/v1/grading_run_status/{}/{}".format(course_id, grading_run_id)
            ),
            method="GET",
            headers=header,
        )
        self.assertEqual(response.code, expected_code)

        if response.code == 200:
            response_body = json.loads(response.body.decode("utf-8"))
            self.assertEqual(response_body["data"].get("state"), expected_state)

    def get_grading_job_log(self, course_id, job_id, header, expected_code):
        response = self.fetch(
            self.get_url("/api/v1/grading_job_log/{}/{}".format(course_id, job_id)),
            method="GET",
            headers=header,
        )
        self.assertEqual(response.code, expected_code)

        if response.code == 200:
            response_body = json.loads(response.body.decode("utf-8"))
            return response_body["data"]

    def get_course_worker_nodes(self, course_id, scope, header, expected_code):
        response = self.fetch(
            self.get_url("/api/v1/worker/{}/{}".format(course_id, scope)),
            method="GET",
            headers=header,
        )
        self.assertEqual(response.code, expected_code)

        if response.code == 200:
            response_body = json.loads(response.body.decode("utf-8"))
            return response_body["data"]


class GraderMixin(AsyncHTTPMixin):
    def register_worker(
        self, header, expected_code=200, worker_id=None, hostname="mock_hostname"
    ):
        worker_id = worker_id or str(uuid.uuid4())

        response = self.fetch(
            self.get_url("/api/v1/worker/{}".format(worker_id)),
            method="POST",
            headers=header,
            body=json.dumps({"hostname": hostname}),
        )

        self.assertEqual(response.code, expected_code)

        if expected_code == 200:
            return worker_id

    def poll_job(self, worker_id, header):
        response = self.fetch(
            self.get_url("/api/v1/grading_job/{}".format(worker_id)),
            method="GET",
            headers=header,
        )

        if response.code == 200:
            self.assertEqual(response.code, 200)
            response_body = json.loads(response.body.decode("utf-8"))
            self.assertIn("grading_job_id", response_body["data"])
            self.assertIn("stages", response_body["data"])
            return response_body["data"]

        return response.code

    def post_job_result(
        self, worker_id, header, job_id, job_success=True, expected_code=200
    ):
        body = {
            "grading_job_id": job_id,
            "success": job_success,
            "results": [{"res": "container 1 res"}, {"res": "container 2 res"}],
            "logs": {"stdout": "stdout", "stderr": "stderr"},
        }
        response = self.fetch(
            self.get_url("/api/v1/grading_job/{}".format(worker_id)),
            method="POST",
            headers=header,
            body=json.dumps(body),
        )
        self.assertEqual(response.code, expected_code)

    def send_heartbeat(self, worker_id, header, expected_code=200):
        response = self.fetch(
            self.get_url("/api/v1/heartbeat/{}".format(worker_id)),
            method="POST",
            body="",
            headers=header,
        )
        self.assertEqual(response.code, expected_code)


class EqualityMixin(unittest.TestCase):
    def assert_equal_grading_config(self, actual_config, expected_config):
        jsonschema.validate(actual_config, definitions.grading_config)
        jsonschema.validate(expected_config, definitions.grading_config)

        for config_key in expected_config:
            if config_key == "env":
                self.assertEqual(
                    sorted(actual_config.get(config_key)),
                    sorted(expected_config[config_key]),
                )
            else:
                self.assert_equal_grading_pipeline(
                    actual_config.get(config_key), expected_config[config_key]
                )

    def assert_equal_grading_pipeline(self, actual_pipeline, expected_pipeline):
        jsonschema.validate(actual_pipeline, definitions.grading_pipeline)
        jsonschema.validate(expected_pipeline, definitions.grading_pipeline)

        for i in range(len(expected_pipeline)):
            self.assert_equal_grading_stage(actual_pipeline[i], expected_pipeline[i])

    def assert_equal_grading_stage(self, actual_stage, expected_stage):
        jsonschema.validate(actual_stage, definitions.grading_stage)
        jsonschema.validate(expected_stage, definitions.grading_stage)

        for stage_key in expected_stage:
            if stage_key == "env":
                self.assertTrue(
                    set(expected_stage["env"].keys()).issubset(
                        set(actual_stage["env"].keys())
                    )
                )
                for env_key in expected_stage[stage_key]:
                    self.assertEqual(
                        actual_stage["env"].get(env_key), expected_stage["env"][env_key]
                    )
            else:
                self.assertEqual(actual_stage.get(stage_key), expected_stage[stage_key])


class WorkerWSMixin(AsyncHTTPMixin):
    # lower level conn
    def worker_ws_conn(self, worker_id, headers):
        url = self.get_url("/api/v1/worker_ws/{}".format(worker_id)).replace(
            "http://", "ws://"
        )
        return websockets.connect(url, extra_headers=headers)

    def worker_ws_conn_register(self, conn, hostname):
        return conn.send(
            json.dumps({"type": "register", "args": {"hostname": hostname}})
        )

    def worker_ws_conn_reulst(self, conn, job_id, job_success):
        args = {
            "grading_job_id": job_id,
            "success": job_success,
            "results": [{"res": "container 1 res"}, {"res": "container 2 res"}],
            "logs": {"stdout": "stdout", "stderr": "stderr"},
        }

        return conn.send(json.dumps({"type": "job_result", "args": args}))

    # need to be closed
    async def worker_ws(self, worker_id, headers, hostname="eniac"):
        conn = await self.worker_ws_conn(worker_id=worker_id, headers=headers)

        await self.worker_ws_conn_register(conn, hostname)

        ack = json.loads(await conn.recv())
        self.assertTrue(ack["success"])

        return conn


class BaseTest(WorkerWSMixin, EqualityMixin, ClientMixin, GraderMixin):
    pass
