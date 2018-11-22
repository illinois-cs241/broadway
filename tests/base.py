from tornado.testing import AsyncHTTPTestCase

from src.api import make_app
from src.database import DatabaseResolver

MOCK_TOKEN = "testing"


class BaseTest(AsyncHTTPTestCase):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.token = MOCK_TOKEN

    def get_app(self):
        self.db_resolver = DatabaseResolver(db_name='__test')
        return make_app(token=MOCK_TOKEN, db_object=self.db_resolver)

    def tearDown(self):
        super().tearDown()
        self.db_resolver.clear_db()
        # self.db_resolver.shutdown()
