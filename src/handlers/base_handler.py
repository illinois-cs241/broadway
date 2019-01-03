from queue import Queue

from bson import ObjectId
from tornado_json.requesthandlers import APIHandler

import src.constants.api_keys as api_key
import src.constants.constants as consts
import src.constants.db_keys as db_key
from src.config import BAD_REQUEST_CODE
from src.database import DatabaseResolver
from src.utilities import resolve_env_vars


class BaseAPIHandler(APIHandler):
    def abort(self, data, status):
        self.set_status(status)
        self.fail(data)

    def is_id_valid(self, id_):
        if not ObjectId.is_valid(id_):
            self.abort({"message": "ID {} is not a valid bson ObjectId".format(id_)}, BAD_REQUEST_CODE)
            return False

        return True

    def get_db(self):
        # type: () -> DatabaseResolver
        return self.settings.get(consts.APP_DB)

    def get_queue(self):
        # type: () -> Queue
        return self.settings.get(consts.APP_QUEUE)

    def get_cluster_token(self):
        # type: () -> str
        return self.settings.get(consts.APP_TOKEN)

    def get_worker_node(self, id_):
        if not self.is_id_valid(id_):
            return None

        db_resolver = self.get_db()
        worker_node = db_resolver.get_worker_node_collection().find_one({db_key.ID: ObjectId(id_)})
        if worker_node is None:
            self.abort({"message": "Worker node with id {} does not exist".format(id_)}, BAD_REQUEST_CODE)
        else:
            return worker_node

    def get_grading_run(self, id_):
        if not self.is_id_valid(id_):
            return None

        db_resolver = self.get_db()
        grading_run = db_resolver.get_grading_run_collection().find_one({db_key.ID: ObjectId(id_)})
        if grading_run is None:
            self.abort({"message": "Grading run with id {} does not exist".format(id_)}, BAD_REQUEST_CODE)
        else:
            return grading_run

    def get_grading_job(self, id_):
        if not self.is_id_valid(id_):
            return None

        db_resolver = self.get_db()
        grading_job = db_resolver.get_grading_job_collection().find_one({db_key.ID: ObjectId(id_)})
        if grading_job is None:
            self.abort({"message": "Grading job with id {} does not exist".format(id_)}, BAD_REQUEST_CODE)
        else:
            return grading_job

    def create_job(self, pipeline_name, student_env_vars=None):
        cur_job = []
        for stage in self.body.get(pipeline_name):
            cur_stage = stage.copy()
            cur_stage[api_key.ENV] = resolve_env_vars(cur_stage.get(api_key.ENV, {}),
                                                      self.body.get(api_key.ENV, {}), student_env_vars)
            cur_job.append(cur_stage)

        return cur_job
