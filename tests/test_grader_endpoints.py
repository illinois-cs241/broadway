import json

from src.config import UNAUTHORIZED_REQUEST_CODE
from src.constants import AUTH_KEY
from tests.base import BaseTest
import src.constants as consts

from tornado import escape


class TestRegisterGrader(BaseTest):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.endpoint = "/api/v1/worker_register"

    def test_register(self):
        headers = {AUTH_KEY: self.token}

        response = self.fetch(
            self.get_url(self.endpoint), method='GET', headers=headers, body=None)
        self.assertEqual(response.code, 200)
        response_body = json.loads(escape.to_basestring(response.body))
        self.assertTrue(consts.WORKER_ID_KEY in response_body["data"])

    def test_register_unauthorized(self):
        response = self.fetch(
            self.get_url(self.endpoint), method='GET', headers=None, body=None
        )
        self.assertEqual(response.code, UNAUTHORIZED_REQUEST_CODE)
