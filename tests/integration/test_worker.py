import logging
import time

from tests.base import BaseTest
from tests._fixtures.config import HEARTBEAT_INTERVAL

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
        time.sleep(HEARTBEAT_INTERVAL * 2 + 1)
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
