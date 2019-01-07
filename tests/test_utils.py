from jsonschema import ValidationError

from src.database import DatabaseResolver
from src.utilities import PeriodicCallbackThread
from src.auth import configure_course_tokens
import src.constants.constants as consts
import src.constants.db_keys as db_key
import time
import unittest


def test_periodic_callback_thread():
    def routine(num):
        num[0] += 1

    counter = [0]
    prev = 0
    interval = 0.01
    thread = PeriodicCallbackThread(callback=routine, interval=interval, args=[counter])
    thread.start()
    for _ in range(10):
        assert prev <= counter[0]
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
        token1_id = str(self.db_resolver.get_token_collection().find_one({db_key.TOKEN: "123"}).get(db_key.ID))
        token2_id = str(self.db_resolver.get_token_collection().find_one({db_key.TOKEN: "456"}).get(db_key.ID))

        cs125 = self.db_resolver.get_course_collection().find_one({db_key.ID: "cs125"})
        cs225 = self.db_resolver.get_course_collection().find_one({db_key.ID: "cs225"})
        cs233 = self.db_resolver.get_course_collection().find_one({db_key.ID: "cs233"})
        cs241 = self.db_resolver.get_course_collection().find_one({db_key.ID: "cs241"})

        assert cs125 is not None
        assert cs225 is not None
        assert cs233 is not None
        assert cs241 is not None

        assert token1_id in cs125.get(db_key.TOKEN_IDS)
        assert token2_id in cs125.get(db_key.TOKEN_IDS)
        assert len(cs125.get(db_key.TOKEN_IDS)) == 2

        assert token1_id in cs225.get(db_key.TOKEN_IDS)
        assert token2_id in cs225.get(db_key.TOKEN_IDS)
        assert len(cs225.get(db_key.TOKEN_IDS)) == 2

        assert token1_id in cs233.get(db_key.TOKEN_IDS)
        assert len(cs233.get(db_key.TOKEN_IDS)) == 1

        assert token2_id in cs241.get(db_key.TOKEN_IDS)
        assert len(cs241.get(db_key.TOKEN_IDS)) == 1

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
