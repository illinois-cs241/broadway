import json
import logging
import unittest.mock as mock

from broadway.api.utils.bootstrap import (
    initialize_course_tokens,
    initialize_global_settings,
)
from broadway.api.utils.multiqueue import MultiQueue

from broadway.api.flags import app_flags
from broadway.api.daos.course import CourseDao

from tests.api.base import BaseTest

from queue import Empty

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


class TestMultiQueue(BaseTest):
    def setUp(self):
        super().setUp()
        self.multiqueue = MultiQueue()

    def add_queues(self, queues):
        for queue in queues:
            self.multiqueue.add_queue(queue)

    def test_add_queue(self):
        self.add_queues(["cs225", "cs241"])

        # try adding a queue that already exists
        with self.assertRaises(Exception):
            self.multiqueue.add_queue("cs241")

        self.assertEqual(2, len(self.multiqueue.keys))
        self.assertEqual(2, len(self.multiqueue.queues))

    def test_push(self):
        self.add_queues(["cs225", "cs233", "cs241"])

        # try pushing to a non-existent queue
        with self.assertRaises(Exception):
            self.multiqueue.push("invalid_id", 1)

        self.multiqueue.push("cs225", 225)
        self.multiqueue.push("cs225", 296 - 25)

        self.multiqueue.push("cs233", 233)

        self.multiqueue.push("cs241", 241)
        self.multiqueue.push("cs241", 295 - 41)

        self.assertEqual(2, self.multiqueue.queues["cs225"].qsize())
        self.assertEqual(1, self.multiqueue.queues["cs233"].qsize())
        self.assertEqual(2, self.multiqueue.queues["cs241"].qsize())

    def test_pull(self):

        # try pulling from a multiqueue with no queues in it yet
        with self.assertRaises(Empty):
            rv = self.multiqueue.pull()

        self.add_queues(["cs225", "cs233", "cs241", "ece411"])

        # try pulling from a multiqueue with multiple empty queues
        with self.assertRaises(Empty):
            rv = self.multiqueue.pull()

        for i in range(10):
            self.multiqueue.push("cs225", "cs225-" + str(i))

        for i in range(100):
            self.multiqueue.push("cs233", "cs233-" + str(i))

        for i in range(241):
            self.multiqueue.push("cs241", "cs241-" + str(i))

        for i in range(10):
            rv = self.multiqueue.pull()
            self.assertEqual("cs225-" + str(i), rv)
            rv = self.multiqueue.pull()
            self.assertEqual("cs233-" + str(i), rv)
            rv = self.multiqueue.pull()
            self.assertEqual("cs241-" + str(i), rv)

        for i in range(10, 100):
            rv = self.multiqueue.pull()
            self.assertEqual("cs233-" + str(i), rv)
            rv = self.multiqueue.pull()
            self.assertEqual("cs241-" + str(i), rv)

        for i in range(100, 241):
            rv = self.multiqueue.pull()
            self.assertEqual("cs241-" + str(i), rv)

        # the multiqueue should be empty now
        with self.assertRaises(Empty):
            self.multiqueue.pull()
