from tornado_json.requesthandlers import APIHandler


class BaseAPIHandler(APIHandler):
    def abort(self, data, status=400):
        self.set_status(status)
        self.fail(data)

    def get_config(self):
        return self.settings.get("CONFIG")

    def get_token(self):
        return self.settings.get("CLUSTER_TOKEN")

    def get_queue(self):
        return self.settings.get("QUEUE")
