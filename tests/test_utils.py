import time
import unittest

from jsonschema import ValidationError

import src.constants.keys as key
from src.auth import configure_course_tokens
from src.database import DatabaseResolver
from src.utilities import PeriodicCallbackThread, build_pipeline
from tests.base import BaseTest


class TestPeriodicCallbackThread(unittest.TestCase):
    def test_periodic_callback_thread(self):
        def routine(num):
            num[0] += 1

        counter = [0]
        prev = 0
        interval = 0.01
        thread = PeriodicCallbackThread(callback=routine, interval=interval, args=[counter])
        thread.start()
        for _ in range(10):
            self.assertLessEqual(prev, counter[0])
            prev += 1
            time.sleep(interval)
        thread.stop()


class TestCourseConfig(unittest.TestCase):
    def setUp(self):
        self.db_resolver = DatabaseResolver(db_name='__test', logs_db_name='__test_logs')

    def tearDown(self):
        self.db_resolver.clear_db()
        self.db_resolver.shutdown()

    def test_valid_course_course(self):
        course_tokens = {"cs125": ["token1", "token2"],
                         "cs225": ["token2", "token1"],
                         "cs233": ["token1"],
                         "cs241": ["token2"]}
        configure_course_tokens(self.db_resolver, course_tokens)

        cs125 = self.db_resolver.get_course_collection().find_one({key.ID: "cs125"})
        cs225 = self.db_resolver.get_course_collection().find_one({key.ID: "cs225"})
        cs233 = self.db_resolver.get_course_collection().find_one({key.ID: "cs233"})
        cs241 = self.db_resolver.get_course_collection().find_one({key.ID: "cs241"})

        self.assertIsNotNone(cs125)
        self.assertIsNotNone(cs225)
        self.assertIsNotNone(cs233)
        self.assertIsNotNone(cs241)

        assert "token1" in cs125.get(key.TOKENS)
        assert "token2" in cs125.get(key.TOKENS)
        assert len(cs125.get(key.TOKENS)) == 2

        assert "token1" in cs225.get(key.TOKENS)
        assert "token2" in cs225.get(key.TOKENS)
        assert len(cs225.get(key.TOKENS)) == 2

        assert "token1" in cs233.get(key.TOKENS)
        assert len(cs233.get(key.TOKENS)) == 1

        assert "token2" in cs241.get(key.TOKENS)
        assert len(cs241.get(key.TOKENS)) == 1

    def test_wrong_config_format_1(self):
        course_tokens = {"cs125": [1]}

        with self.assertRaises(ValidationError):
            configure_course_tokens(self.db_resolver, course_tokens)

    def test_wrong_config_format_2(self):
        course_tokens = {"cs125": 1}

        with self.assertRaises(ValidationError):
            configure_course_tokens(self.db_resolver, course_tokens)


class TestBuildPipeline(BaseTest):
    def test_empty_env(self):
        pipeline = [
            {
                key.IMAGE: "alpine:3.5",
                key.TIMEOUT: 20
            },
            {
                key.IMAGE: "alpine:3.5",
                key.HOST_NAME: "123456"
            },
            {
                key.IMAGE: "alpine:3.5",
                key.NETWORKING: True
            },
            {
                key.IMAGE: "alpine:3.5",
                key.ENV: {"var1": "val1", "var2": "val2"}
            },
            {
                key.IMAGE: "alpine:3.5",
                key.ENTRY_POINT: ["echo", "student-job"]
            }
        ]
        result_pipeline = build_pipeline(pipeline, {}, {})

        self.assert_equal_grading_pipeline(result_pipeline, pipeline)

    def test_insert_env(self):
        pipeline = [
            {
                key.IMAGE: "alpine:3.5",
                key.ENV: {"image": "1"}
            },
            {
                key.IMAGE: "alpine:3.5",
                key.ENV: {"image": "2"}
            }
        ]

        expected_pipeline = [
            {
                key.IMAGE: "alpine:3.5",
                key.ENV: {"image": "1", "global1": "var1", "global2": "var2", "net-id": "test-id"}
            },
            {
                key.IMAGE: "alpine:3.5",
                key.ENV: {"image": "2", "global1": "var1", "global2": "var2", "net-id": "test-id"}
            }
        ]

        result_pipeline = build_pipeline(pipeline, {"global1": "var1", "global2": "var2"}, {"net-id": "test-id"})
        self.assert_equal_grading_pipeline(result_pipeline, expected_pipeline)
