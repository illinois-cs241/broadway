import json
import logging
import unittest.mock as mock

from broadway.api.utils.bootstrap import (
    initialize_course_tokens,
    initialize_global_settings,
)

from broadway.api.flags import app_flags
from broadway.api.daos.course import CourseDao

from tests.api.base import BaseTest

logging.disable(logging.WARNING)


class TestClusterTokenUtils(BaseTest):
    def test_cluster_token_generated(self):
        flags = app_flags.parse([], env={})
        settings = initialize_global_settings(flags)
        self.assertIsNotNone(flags.get("token"))
        self.assertIsNotNone(settings["FLAGS"]["token"])


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
