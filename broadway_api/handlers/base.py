import json
import logging
from jsonschema import validate

from tornado_json.requesthandlers import APIHandler
from tornado.websocket import WebSocketHandler

from broadway_api.definitions import ws_api_msg

logger = logging.getLogger("base")


class BaseAPIHandler(APIHandler):
    def abort(self, data, status=400):
        self.set_status(status)
        self.fail(data)

    def get_flags(self):
        return self.settings.get("FLAGS")

    def get_token(self):
        return self.settings.get("CLUSTER_TOKEN")

    def get_queue(self):
        return self.settings.get("QUEUE")


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
