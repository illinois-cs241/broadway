from tornado import gen, web
from tornado.iostream import StreamClosedError

from broadway.api.handlers.base import BaseAPIHandler
from broadway.api.decorators.auth import authenticate_course
from broadway.api.utils.streamqueue import StreamQueue


class GradingJobStreamHandler(BaseAPIHandler):
    def initialize(self):
        # Prepare for Server-Sent Events
        self.set_header("Content-Type", "text/event-stream")
        self.set_header("Cache-Control", "no-cache")

        self._id = id(self)

    @gen.coroutine
    def publish(self, event, data):
        try:
            self.write(f"event: {event}" f"data: {data}\n\n")
            yield self.flush()
        except StreamClosedError:
            raise web.Finish

    @authenticate_course
    @gen.coroutine
    def get(self, **kwargs):
        job_id = kwargs.get("job_id")

        sq = self.get_stream_queue()
        self._events = sq.register_stream(self._id, job_id)

        while True:
            if self._events.empty():
                yield gen.sleep(1)
            else:
                event, data = self._events.get()
                yield self.publish(event, data)
