import json
import logging
import unittest.mock as mock

from jsonschema import ValidationError

from broadway_api.utils.bootstrap import (
    initialize_cluster_token,
    initialize_course_tokens,
)
from broadway_api.daos.course import CourseDao
from tests.base import BaseTest

logging.disable(logging.WARNING)


class TestClusterTokenUtils(BaseTest):
    def test_cluster_token_from_env(self):
        with mock.patch.dict("os.environ", {"BROADWAY_TOKEN": "val"}):
            self.assertEqual(initialize_cluster_token(), "val")

    def test_cluster_token_generated(self):
        with mock.patch.dict("os.environ", {}):
            self.assertIsNotNone(initialize_cluster_token())


class TestCourseTokenUtils(BaseTest):
    def test_init_tokens(self):
        course_tokens = {"cs225": ["token1"], "cs241": ["token1", "token2"]}

        with mock.patch(
            "builtins.open", mock.mock_open(read_data=json.dumps(course_tokens))
        ):
            initialize_course_tokens(self.app.settings, "")

        dao = CourseDao(self.app.settings)
        cs225 = dao.find_by_id("cs225")
        cs241 = dao.find_by_id("cs241")

        self.assertIsNotNone(cs225)
        self.assertIsNotNone(cs241)

        self.assertIn("token1", cs225.tokens)
        self.assertNotIn("token2", cs225.tokens)
        self.assertIn("token1", cs241.tokens)
        self.assertIn("token2", cs241.tokens)

    def test_init_no_tokens(self):
        initialize_course_tokens(self.app.settings, None)

    def test_init_tokens_invalid_format(self):
        course_tokens = {"cs125": [1]}

        with self.assertRaises(ValidationError):
            with mock.patch(
                "builtins.open", mock.mock_open(read_data=json.dumps(course_tokens))
            ):
                initialize_course_tokens(self.app.settings, course_tokens)
