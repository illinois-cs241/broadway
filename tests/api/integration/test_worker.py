import logging
import time
import json
import websockets

from tests.api.base import BaseTest

import tornado.testing

from broadway.api.callbacks import worker_heartbeat_callback

logging.disable(logging.WARNING)


class RegisterGraderEndpointsTest(BaseTest):
    def test_register(self):
        self.assertIsNotNone(self.register_worker(self.get_header()))

    def test_duplicate_id(self):
        worker_id = "duplicate"
        self.register_worker(self.get_header(), worker_id=worker_id, expected_code=200)
        self.register_worker(self.get_header(), worker_id=worker_id, expected_code=400)

    def test_reregister_id(self):
        worker_id = self.register_worker(self.get_header(), expected_code=200)
        time.sleep(self.app.settings["FLAGS"]["heartbeat_interval"] * 2 + 1)
        worker_heartbeat_callback(self.app.settings)
        self.register_worker(self.get_header(), worker_id=worker_id, expected_code=200)

    def test_unauthorized(self):
        self.register_worker(None, 401)

    def test_wrong_token(self):
        self.register_worker(self.get_header("invalid"), 401)


class PollGradingJobEndpointsTest(BaseTest):
    def test_unauthorized(self):
        worker_id = self.register_worker(self.get_header())
        self.assertEqual(self.poll_job(worker_id, None), 401)

    def test_wrong_token(self):
        worker_id = self.register_worker(self.get_header())
        self.assertEqual(self.poll_job(worker_id, self.get_header("invalid")), 401)

    def test_invalid_worker_id(self):
        self.assertEqual(self.poll_job("1234", self.get_header()), 401)

    def test_empty_poll(self):
        worker_id = self.register_worker(self.get_header())
        self.assertEqual(self.poll_job(worker_id, self.get_header()), 498)


class UpdateGradingJobEndpointsTest(BaseTest):
    def test_unauthorized(self):
        worker_id = self.register_worker(self.get_header())
        self.post_job_result(worker_id, None, "1234", True, 401)

    def test_wrong_token(self):
        worker_id = self.register_worker(self.get_header())
        self.post_job_result(worker_id, self.get_header("invalid"), "1234", True, 401)

    def test_invalid_worker_id(self):
        self.post_job_result("1234", self.get_header(), "1234", True, 401)


class HeartBeatEndpointsTest(BaseTest):
    def test_unauthorized(self):
        worker_id = self.register_worker(self.get_header())
        self.send_heartbeat(worker_id, None, 401)

    def test_wrong_token(self):
        worker_id = self.register_worker(self.get_header())
        self.send_heartbeat(worker_id, self.get_header("fake"), 401)

    def test_invalid_worker_id(self):
        self.send_heartbeat("1234", self.get_header(), 401)

    def test_valid_heartbeat(self):
        worker_id = self.register_worker(self.get_header())
        self.send_heartbeat(worker_id, self.get_header())


class WorkerWSEndpointTest(BaseTest):
    @tornado.testing.gen_test
    async def test_decode_error(self):
        async with self.worker_ws_conn(
            worker_id="test_worker", headers=self.get_header()
        ) as conn:
            try:
                await conn.send("i'm not json")
            except Exception as e:
                self.assertEqual(e.code, 1011)

    # submit job result before registering
    @tornado.testing.gen_test
    async def test_bad_job_result(self):
        async with self.worker_ws_conn(
            worker_id="test_worker", headers=self.get_header()
        ) as conn:
            try:
                await conn.send(
                    json.dumps(
                        {
                            "type": "job_result",
                            "args": {
                                "grading_job_id": "someid",
                                "success": True,
                                "results": [{"res": "spoof"}],
                                "logs": {"stdout": "stdout", "stderr": "stderr"},
                            },
                        }
                    )
                )

                await conn.recv()
            except Exception as e:
                self.assertEqual(e.code, 1002)

    @tornado.testing.gen_test
    async def test_register(self):
        async with self.worker_ws_conn(
            worker_id="test_worker", headers=self.get_header()
        ) as conn:
            await conn.send(
                json.dumps({"type": "register", "args": {"hostname": "eniac"}})
            )

            ack = json.loads(await conn.recv())
            self.assertTrue(ack["success"])

    @tornado.testing.gen_test
    async def test_pong(self):
        async with self.worker_ws_conn(
            worker_id="test_worker", headers=self.get_header()
        ) as conn:
            await conn.send(
                json.dumps({"type": "register", "args": {"hostname": "eniac"}})
            )

            ack = json.loads(await conn.recv())
            self.assertTrue(ack["success"])

            await conn.pong()

    @tornado.testing.gen_test
    async def test_no_token(self):
        async with self.worker_ws_conn(worker_id="test_worker", headers=None) as conn:
            try:
                await conn.send(
                    json.dumps({"type": "register", "args": {"hostname": "eniac"}})
                )

                ack = json.loads(await conn.recv())
                self.assertFalse(ack["success"])
            except websockets.exceptions.ConnectionClosed as e:
                self.assertEqual(e.code, 1008)

    @tornado.testing.gen_test
    async def test_wrong_token(self):
        async with self.worker_ws_conn(
            worker_id="test_worker", headers=self.get_header("invalid")
        ) as conn:
            try:
                await conn.send(
                    json.dumps({"type": "register", "args": {"hostname": "eniac"}})
                )

                ack = json.loads(await conn.recv())
                self.assertFalse(ack["success"])
            except websockets.exceptions.ConnectionClosed as e:
                self.assertEqual(e.code, 1008)

    @tornado.testing.gen_test
    async def test_duplicate_token(self):
        async with self.worker_ws_conn(
            worker_id="test_worker", headers=self.get_header()
        ) as conn1:
            await conn1.send(
                json.dumps({"type": "register", "args": {"hostname": "eniac"}})
            )

            # worker 1 should successfully register
            ack = json.loads(await conn1.recv())
            self.assertTrue(ack["success"])

            async with self.worker_ws_conn(
                worker_id="test_worker", headers=self.get_header()
            ) as conn2:

                try:
                    await conn2.send(
                        json.dumps({"type": "register", "args": {"hostname": "eniac"}})
                    )

                    # worker 2 should fail
                    ack = json.loads(await conn2.recv())
                    self.assertFalse(ack["success"])
                except websockets.exceptions.ConnectionClosed as e:
                    self.assertEqual(e.code, 1002)

    @tornado.testing.gen_test
    async def test_reregister(self):
        async with self.worker_ws_conn(
            worker_id="test_worker", headers=self.get_header()
        ) as conn1:
            await conn1.send(
                json.dumps({"type": "register", "args": {"hostname": "eniac"}})
            )

            # worker 1 should succeed
            ack = json.loads(await conn1.recv())
            self.assertTrue(ack["success"])

        async with self.worker_ws_conn(
            worker_id="test_worker", headers=self.get_header()
        ) as conn2:
            await conn2.send(
                json.dumps({"type": "register", "args": {"hostname": "eniac"}})
            )

            # worker 2 should also succeed
            ack = json.loads(await conn2.recv())
            self.assertTrue(ack["success"])

    @tornado.testing.gen_test
    async def test_wrong_job_id(self):
        async with self.worker_ws_conn(
            worker_id="test_worker", headers=self.get_header()
        ) as conn:
            await conn.send(
                json.dumps({"type": "register", "args": {"hostname": "eniac"}})
            )

            ack = json.loads(await conn.recv())
            self.assertTrue(ack["success"])

            try:
                await conn.send(
                    json.dumps(
                        {
                            "type": "job_result",
                            "args": {
                                "grading_job_id": "no_such_id",
                                "success": True,
                                "results": [{"res": "spoof"}],
                                "logs": {"stdout": "stdout", "stderr": "stderr"},
                            },
                        }
                    )
                )
            except websockets.exceptions.ConnectionClosed as e:
                self.assertEqual(e.code, 1002)
