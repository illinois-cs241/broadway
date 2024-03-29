import json
import logging
from jsonschema import validate

from tornado_json.requesthandlers import APIHandler
from tornado.websocket import WebSocketHandler

from broadway.api.definitions import ws_api_msg

logger = logging.getLogger(__name__)


class BaseAPIHandler(APIHandler):
    def set_default_headers(self, *args, **kwargs):
        self.set_header("Access-Control-Allow-Origin", "*")

    def options(self, *args, **kwargs):
        """
        For CORS, browsers will send a preflight request with "OPTIONS" method
        before the actual request. We want to respond with a status of "HTTP OK",
        a header with proper "Access-Control-Allow-Origin" field for all origins
        we allow and a proper "Access-Control-Allow-Headers" for all special
        header fields we allow, and no body.
        """
        self.set_header("Access-Control-Allow-Headers", "Authorization")
        self.set_status(204)
        self.finish()

    def abort(self, data, status=400):
        self.set_status(status)
        self.fail(data)

    def get_flags(self):
        return self.settings["FLAGS"]

    def get_token(self):
        return self.settings["FLAGS"]["token"]

    def get_queue(self):
        return self.settings["QUEUE"]

    def get_stream_queue(self):
        return self.settings["STREAM_QUEUE"]


class BaseWSAPIHandler(BaseAPIHandler, WebSocketHandler):
    msg_type_map = {}

    @staticmethod
    def msg_type(type_id, decl):
        def decor(handler):
            BaseWSAPIHandler.msg_type_map[type_id] = (decl, handler)
            return handler

        return decor

    def on_message(self, msg):
        try:
            data = json.loads(msg)

            # check msg decl
            validate(instance=data, schema=ws_api_msg)

            msg_type = data["type"]

            decl, handler = BaseWSAPIHandler.msg_type_map[msg_type]

            # check argument decl
            validate(instance=data["args"], schema=decl)

            return handler(self, **data["args"])

        except Exception as e:
            self.close(code=1011, reason="internal error")
            logger.warning("connection {} closed: {}".format(repr(self), repr(e)))

    def send(self, data):
        self.write_message(json.dumps(data))

    def get_ws_conn_map(self):
        return self.settings.get("WS_CONN_MAP")
