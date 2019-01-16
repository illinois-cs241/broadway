from jsonschema import ValidationError

from src.database import DatabaseResolver
from src.utilities import PeriodicCallbackThread, build_pipeline
from src.auth import configure_course_tokens
import src.constants.constants as consts
import src.constants.keys as key
import time
import unittest

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
        course_tokens = {
            consts.CONFIG_TOKENS: {"token1": "123", "token2": "456"},
            consts.CONFIG_COURSES: {"cs125": ["token1", "token2"],
                                    "cs225": ["token2", "token1"],
                                    "cs233": ["token1"],
                                    "cs241": ["token2"]}
        }
        configure_course_tokens(self.db_resolver, course_tokens)
        token1_id = str(self.db_resolver.get_token_collection().find_one({key.TOKEN: "123"}).get(key.ID))
        token2_id = str(self.db_resolver.get_token_collection().find_one({key.TOKEN: "456"}).get(key.ID))

        cs125 = self.db_resolver.get_course_collection().find_one({key.ID: "cs125"})
        cs225 = self.db_resolver.get_course_collection().find_one({key.ID: "cs225"})
        cs233 = self.db_resolver.get_course_collection().find_one({key.ID: "cs233"})
        cs241 = self.db_resolver.get_course_collection().find_one({key.ID: "cs241"})

        assert cs125 is not None
        assert cs225 is not None
        assert cs233 is not None
        assert cs241 is not None

        assert token1_id in cs125.get(key.TOKEN_IDS)
        assert token2_id in cs125.get(key.TOKEN_IDS)
        assert len(cs125.get(key.TOKEN_IDS)) == 2

        assert token1_id in cs225.get(key.TOKEN_IDS)
        assert token2_id in cs225.get(key.TOKEN_IDS)
        assert len(cs225.get(key.TOKEN_IDS)) == 2

        assert token1_id in cs233.get(key.TOKEN_IDS)
        assert len(cs233.get(key.TOKEN_IDS)) == 1

        assert token2_id in cs241.get(key.TOKEN_IDS)
        assert len(cs241.get(key.TOKEN_IDS)) == 1

    def test_missing_token(self):
        course_tokens = {
            consts.CONFIG_COURSES: {"cs125": ["token1", "token2"],
                                    "cs225": ["token2", "token1"],
                                    "cs233": ["token1"],
                                    "cs241": ["token2"]}
        }

        with self.assertRaises(KeyError):
            configure_course_tokens(self.db_resolver, course_tokens)

    def test_wrong_token(self):
        course_tokens = {
            consts.CONFIG_TOKENS: {"token": "123"},
            consts.CONFIG_COURSES: {"cs125": ["token1"]}
        }

        with self.assertRaises(KeyError):
            configure_course_tokens(self.db_resolver, course_tokens)

    def test_wrong_token_format(self):
        course_tokens = {
            consts.CONFIG_TOKENS: {"token1": 1},
            consts.CONFIG_COURSES: {"cs125": ["token1"]}
        }

        with self.assertRaises(ValidationError):
            configure_course_tokens(self.db_resolver, course_tokens)

    def test_wrong_config_format_1(self):
        course_tokens = {
            consts.CONFIG_COURSES: {"cs125": [1]}
        }

        with self.assertRaises(ValidationError):
            configure_course_tokens(self.db_resolver, course_tokens)

    def test_wrong_config_format_2(self):
        course_tokens = {
            consts.CONFIG_COURSES: {"cs125": 1}
        }

        with self.assertRaises(ValidationError):
            configure_course_tokens(self.db_resolver, course_tokens)

    def test_wrong_config_format_3(self):
        course_tokens = {
            consts.CONFIG_COURSES: {"cs125": "hello"}
        }

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
