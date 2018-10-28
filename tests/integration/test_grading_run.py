from tornado.testing import AsyncHTTPTestCase, AsyncHTTPClient, gen_test
from tornado import gen
import json
from src.api import make_app
from src.database import DatabaseResolver
from src.settings import EMPTY_QUEUE_CODE
import urllib
import tornado


@gen.coroutine
def fetch(url, **kwargs):
    http_client = AsyncHTTPClient()
    response = yield http_client.fetch(url, **kwargs)
    raise gen.Return(response)


class HTTPTestBase(AsyncHTTPTestCase):

    def get_app(self):
        self.db_resolver = DatabaseResolver(db_name='__test')
        return make_app(self.db_resolver)

    def tearDown(self):
        super().tearDown()
        self.db_resolver.clear_db()


class TestGradingRun(HTTPTestBase):

    @gen.coroutine
    def send_initial_request(self, payload):
        body = urllib.parse.urlencode(dict(json_payload=json.dumps(payload)))

        res = yield fetch(self.get_url('/api/v1/grading_run'), method='POST', headers=None, body=body)
        json_response = json.loads(res.body.decode('utf-8'))
        raise gen.Return(json_response)

    @gen_test
    def test_sanity(self):
        payload = dict(
            student_pipeline=[],
            students=[],
        )

        json_res = yield self.send_initial_request(payload)
        job_id = json_res['id']
        stat_url = '/api/v1/grading_run/{}'.format(job_id)

        res = yield fetch(self.get_url(stat_url), method='GET')
        student_statuses = json.loads(res.body.decode('UTF-8'))["student_statuses"]
        self.assertEqual(student_statuses, [])

    @gen_test
    def test_set_get(self):
        test_student_dict = dict(
            student="241grader",
        )
        payload = dict(
            student_pipeline=[
                dict(
                    image='foo',
                    env=dict(bar='yeet', student=""),
                )
            ],
            students=[
                test_student_dict
            ],
        )

        json_res = yield self.send_initial_request(payload)
        job_id = json_res['id']
        stat_url = '/api/v1/grading_run/{}'.format(job_id)

        res = yield fetch(self.get_url(stat_url), method='GET')
        student_statuses = json.loads(res.body.decode('UTF-8'))
        received_responses = student_statuses['student_statuses']
        self.assertEqual(len(received_responses), 1)

        received_student = received_responses[0]
        self.assertEqual(received_student['status'], 'Created')

        stages = received_student.get('stages', [])
        self.assertEqual(len(stages), 1)

        expected_stage = {'image': 'foo', 'env': {'student': '241grader', 'bar': 'yeet'}}
        self.assertEqual(stages[0], expected_stage)

    @gen_test
    def test_set_start_get(self):
        test_student_dict = dict(
            student="241grader",
        )
        payload = dict(
            student_pipeline=[
                dict(
                    image='foo',
                    env=dict(bar='yeet', student=""),
                )
            ],
            students=[
                test_student_dict
            ],
        )

        json_res = yield self.send_initial_request(payload)
        job_id = json_res['id']
        stat_url = '/api/v1/grading_run/{}'.format(job_id)

        yield fetch(self.get_url(stat_url), method='POST', body='')
        res = yield fetch(self.get_url(stat_url), method='GET')
        student_statuses = json.loads(res.body.decode('UTF-8'))
        received_responses = student_statuses['student_statuses']
        self.assertEqual(len(received_responses), 1)
        received_student = received_responses[0]
        self.assertEqual(received_student['status'], 'Queued')

    @gen_test
    def test_no_pipeline(self):
        payload = dict(
            students=[]
        )

        with self.assertRaises(tornado.httpclient.HTTPError) as context:
            yield self.send_initial_request(payload)

        self.assertTrue(400 <= context.exception.code <= 499)

    @gen_test
    def test_no_student(self):
        payload = dict(
            student_pipeline=[]
        )

        with self.assertRaises(tornado.httpclient.HTTPError) as context:
            yield self.send_initial_request(payload)

        self.assertTrue(400 <= context.exception.code <= 499)

    @gen_test
    def test_missing_image(self):
        payload = dict(
            student_pipeline=[dict(
                env=dict(bar='yeet', student=""),
            )]
        )

        with self.assertRaises(tornado.httpclient.HTTPError) as context:
            yield self.send_initial_request(payload)

        self.assertTrue(400 <= context.exception.code <= 499)

    @gen_test
    def test_bad_pipeline(self):
        payload = dict(
            student_pipeline=[],
            students=[],
            postprocessing_pipeline=[dict(
                env=dict(bar='yeet', student=""),
            )]
        )

        with self.assertRaises(tornado.httpclient.HTTPError) as context:
            yield self.send_initial_request(payload)

        self.assertTrue(400 <= context.exception.code <= 499)

    @gen_test
    def send_malformed_request(self):
        with self.assertRaises(tornado.httpclient.HTTPError) as context:
            yield fetch(self.get_url('/api/v1/grading_run'), method='POST', headers=None, body='')

        self.assertTrue(400 <= context.exception.code <= 499)

    @gen_test
    def test_invalid_worker_register(self):
        wr_invalid_token = '/api/v1/worker_register?token=ofsomething'
        with self.assertRaises(tornado.httpclient.HTTPError) as context:
            yield fetch(self.get_url(wr_invalid_token), method='GET',
                    headers=None)
        self.assertTrue(400 <= context.exception.code <= 499)

    @gen_test
    def test_missing_worker_register(self):
        wr_missing_token = '/api/v1/worker_register'
        with self.assertRaises(tornado.httpclient.HTTPError) as context:
            yield fetch(self.get_url(wr_missing_token), method='GET',
                    headers=None)
        self.assertTrue(400 <= context.exception.code <= 499)

    @gen_test
    def test_invalid_worker_get_gj(self):
        with self.assertRaises(tornado.httpclient.HTTPError) as context:
            yield fetch(self.get_url('/api/v1/grading_run?worker_id=-1'), method='GET',
                    headers=None)
        self.assertTrue(400 <= context.exception.code <= 499)

    @gen_test
    def test_missing_worker_get_gj(self):
        with self.assertRaises(tornado.httpclient.HTTPError) as context:
            yield fetch(self.get_url('/api/v1/grading_run'), method='GET',
                    headers=None)
        self.assertTrue(400 <= context.exception.code <= 499)


    @gen_test
    def test_invalid_worker_job_post_gj(self):
        with self.assertRaises(tornado.httpclient.HTTPError) as context:
            job_id = -1
            grading_job_url = '/api/v1/grading_job/{0}'.format(job_id)
            yield fetch(self.get_url(grading_job_url), method='POST',
                    headers=None, body='')
            self.assertTrue(400 <= context.exception.code <= 499)

    @gen_test
    def test_invalid_worker_heartbeat(self):
        with self.assertRaises(tornado.httpclient.HTTPError) as context:
            yield fetch(self.get_url('/api/v1/heartbeat?worker_id=-1'), method='GET',
                    headers=None)
        self.assertTrue(400 <= context.exception.code <= 499)

    @gen_test
    def test_missing_worker_heartbeat(self):
        with self.assertRaises(tornado.httpclient.HTTPError) as context:
            yield fetch(self.get_url('/api/v1/heartbeat'), method='GET',
                    headers=None)
        self.assertTrue(400 <= context.exception.code <= 499)

