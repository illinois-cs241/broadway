import json

import tests.configs
from src.config import BAD_REQUEST_CODE, GRADING_RUN_ENDPOINT
from tests.base import BaseTest


class TestAddGradingRun(BaseTest):
    def test_valid_run(self):
        self.assertIsNotNone(self.add_grading_run())

    def test_invalid_runs(self):
        for invalid_config in tests.configs.invalid_configs:
            response = self.fetch(self.get_url(GRADING_RUN_ENDPOINT), method='POST', headers=self.grader_header,
                                  body=json.dumps(invalid_config))
            self.assertEqual(response.code, BAD_REQUEST_CODE)
