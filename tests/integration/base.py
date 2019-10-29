import json
import requests
import unittest


class BaseTest(unittest.TestCase):
    def get_api_host(self):
        return "http://api:1470"

    def get_token(self):
        return "course-token"

    def get_default_headers(self):
        return {"Authorization": "Bearer {}".format(self.get_token())}

    def api_get(self, endpoint, *args, **kwargs):
        if "headers" not in kwargs:
            # use a correct header if not otherwise overwritten
            kwargs["headers"] = self.get_default_headers()

        return requests.get(
            "{}{}".format(self.get_api_host(), endpoint), *args, **kwargs
        )

    def api_post(self, endpoint, data, *args, **kwargs):
        if "headers" not in kwargs:
            # use a correct header if not otherwise overwritten
            kwargs["headers"] = self.get_default_headers()

        return requests.post(
            "{}{}".format(self.get_api_host(), endpoint),
            *args,
            data=json.dumps(data),
            **kwargs
        )

    def assertResponse(self, resp, status):
        self.assertEqual(
            resp.status_code,
            status,
            "assert status code for request to {}: {}".format(resp.url, resp.text),
        )
