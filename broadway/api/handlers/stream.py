import json
from tornado import gen, web
from tornado.iostream import StreamClosedError
from tornado.ioloop import PeriodicCallback

from broadway.api.handlers.base import BaseAPIHandler
from broadway.api.decorators.auth import authenticate_course_member_or_admin
from broadway.api.utils.streamqueue import StreamQueue

HEARTBEAT_TIME_MILLI = 20 * 1000  # 20 seconds


class GradingJobStreamHandler(BaseAPIHandler):
    def initialize(self):
        # Prepare for Server-Sent Events
        self.set_header("Content-Type", "text/event-stream")
        self.set_header("Cache-Control", "no-cache")

        self._id = id(self)
        self._callback = PeriodicCallback(
            callback=self._heartbeat, callback_time=HEARTBEAT_TIME_MILLI
        )
        self._callback.start()

    def _stop_listening(self):
        self.get_stream_queue().unregister_stream(self._job_id, self._id)
        self._callback.stop()
        raise web.Finish

    @gen.coroutine
    def _send_sse(self, message):
        try:
            self.write(message)
            yield self.flush()
        except StreamClosedError:
            self._stop_listening()

    @gen.coroutine
    def _heartbeat(self):
        yield self._send_sse(":\n\n")

    @gen.coroutine
    def publish(self, event, data):
        blob = json.dumps({"type": event, "data": data})
        yield self._send_sse(f"event: status_update\ndata: {blob}\n\n")

    @authenticate_course_member_or_admin
    @gen.coroutine
    def get(self, **kwargs):
        self._job_id = kwargs.get("job_id")

        sq = self.get_stream_queue()
        sq.register_stream(self._job_id, self._id)

        while True:
            res = yield sq.get(self._job_id, self._id)
            # If we receive the sentinel value, stop listening
            if res is StreamQueue.CLOSE_EVENT:
                self._stop_listening()
            yield self.publish(*res)
