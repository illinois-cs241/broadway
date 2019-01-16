from src.config import UNAUTHORIZED_REQUEST_CODE, BAD_REQUEST_CODE, QUEUE_EMPTY_CODE
from src.utilities import get_header
from tests.base import BaseEndpointTest


class EndpointTestRegisterGrader(BaseEndpointTest):
    def test_register(self):
        self.assertIsNotNone(self.register_worker(self.grader_header))

    def test_unauthorized(self):
        self.register_worker(None, UNAUTHORIZED_REQUEST_CODE)

    def test_wrong_token(self):
        self.register_worker(get_header("fake"), UNAUTHORIZED_REQUEST_CODE)


class EndpointTestPollGradingJob(BaseEndpointTest):
    def test_unauthorized(self):
        worker_id = self.register_worker(self.grader_header)
        self.poll_job(worker_id, None, UNAUTHORIZED_REQUEST_CODE)

    def test_wrong_token(self):
        worker_id = self.register_worker(self.grader_header)
        self.poll_job(worker_id, get_header("fake"), UNAUTHORIZED_REQUEST_CODE)

    def test_invalid_worker_id(self):
        self.poll_job("1234", self.grader_header, BAD_REQUEST_CODE)

    def test_empty_poll(self):
        worker_id = self.register_worker(self.grader_header)
        self.poll_job(worker_id, self.grader_header, QUEUE_EMPTY_CODE)


class EndpointTestUpdateGradingJob(BaseEndpointTest):
    def test_unauthorized(self):
        worker_id = self.register_worker(self.grader_header)
        self.post_job_result(worker_id, None, '1234', True, UNAUTHORIZED_REQUEST_CODE)

    def test_wrong_token(self):
        worker_id = self.register_worker(self.grader_header)
        self.post_job_result(worker_id, get_header("fake"), '1234', True, UNAUTHORIZED_REQUEST_CODE)

    def test_invalid_worker_id(self):
        self.post_job_result("1234", self.grader_header, "1234", True, BAD_REQUEST_CODE)


class EndpointTestHeartBeat(BaseEndpointTest):
    def test_unauthorized(self):
        worker_id = self.register_worker(self.grader_header)
        self.send_heartbeat(worker_id, None, UNAUTHORIZED_REQUEST_CODE)

    def test_wrong_token(self):
        worker_id = self.register_worker(self.grader_header)
        self.send_heartbeat(worker_id, get_header("fake"), UNAUTHORIZED_REQUEST_CODE)

    def test_invalid_worker_id(self):
        self.send_heartbeat("1234", self.grader_header, BAD_REQUEST_CODE)

    def test_valid_heartbeat(self):
        worker_id = self.register_worker(self.grader_header)
        self.send_heartbeat(worker_id, self.grader_header)
