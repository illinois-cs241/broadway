from tornado.testing import AsyncHTTPTestCase, AsyncHTTPClient, gen_test
from tornado import gen
import json
from api import make_app
from database import DatabaseResolver

@gen.coroutine
def fetch(url, **kwargs):
    http_client = AsyncHTTPClient()
    response = yield http_client.fetch(url, **kwargs)
    raise gen.Return(response)

class AsyncHTTPTestCaseApp(AsyncHTTPTestCase):

    def get_app(self):
        db_resolver = DatabaseResolver(db_name='__test')
        return make_app(db_resolver)


class TestGradingRun(AsyncHTTPTestCaseApp):

    @gen_test
    def test_sanity(self):
        payload = dict(
                student_pipeline=[],
                students=[],
            )
        body = "{}={}".format('json_payload', json.dumps(payload))
        res = yield fetch(self.get_url('/api/v1/grading_run'), method='POST', headers=None, body=body)
        json_res = json.loads(res.body.decode('utf-8'))
        job_id = json_res['id']
        stat_url = '/api/v1/grading_run/{}'.format(job_id)
        res = yield fetch(self.get_url(stat_url), method='GET')
        student_statuses = json.loads(res.body.decode('UTF-8'))["student statuses"]

        self.assertEqual(student_statuses, [])
