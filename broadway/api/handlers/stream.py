from tornado import gen, web
from tornado.iostream import StreamClosedError

from broadway.api.handlers.base import BaseAPIHandler
from broadway.api.decorators.auth import authenticate_course


class GradingJobStreamHandler(BaseAPIHandler):
    def initialize(self):
        # Prepare for Server-Sent Events
        self.set_header("Content-Type", "text/event-stream")
        self.set_header("Cache-Control", "no-cache")

        self._id = id(self)

    def _stop_listening(self):
        self.get_stream_queue().unregister_stream(self._job_id, self._id)
        raise web.Finish

    @gen.coroutine
    def publish(self, event, data):
        try:
            self.write(f"event: {event}" f"data: {data}\n\n")
            yield self.flush()
        except StreamClosedError:
            self._stop_listening()

    @authenticate_course
    @gen.coroutine
    def get(self, **kwargs):
        self._job_id = kwargs.get("job_id")

        sq = self.get_stream_queue()
        sq.register_stream(self._job_id, self._id)

        # Add this line when manually testing. Add prints in the streamqueue to see
        # new updates.

        # raise web.Finish

        while True:
            # If we receive the sentinel value, stop listening
            res = sq.get(self._job_id, self._id)
            if res is None:
                self._stop_listening()
            yield self.publish(*res)
