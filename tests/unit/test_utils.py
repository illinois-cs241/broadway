import json
import logging
import unittest.mock as mock

from broadway_api.bootstrap import initialize_course_tokens
from broadway_api.daos.course import CourseDao
from tests.base import BaseTest

logging.disable(logging.WARNING)


class TestCourseTokenUtils(BaseTest):
    def test_init_tokens(self):
        course_tokens = {"cs225": ["token1"], "cs241": ["token1", "token2"]}

        with mock.patch(
            "builtins.open", mock.mock_open(read_data=json.dumps(course_tokens))
        ):
            initialize_course_tokens(self.app.settings, self.app.settings["FLAGS"])

        dao = CourseDao(self.app.settings)
        cs225 = dao.find_by_id("cs225")
        cs241 = dao.find_by_id("cs241")

        self.assertIsNotNone(cs225)
        self.assertIsNotNone(cs241)

        self.assertIn("token1", cs225.tokens)
        self.assertNotIn("token2", cs225.tokens)
        self.assertIn("token1", cs241.tokens)
        self.assertIn("token2", cs241.tokens)
