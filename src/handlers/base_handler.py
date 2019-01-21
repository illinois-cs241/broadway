from queue import Queue

from bson import ObjectId
from tornado_json.requesthandlers import APIHandler

import src.constants.keys as key
import src.constants.constants as consts
from src.config import BAD_REQUEST_CODE
from src.database import DatabaseResolver


class BaseAPIHandler(APIHandler):
    def abort(self, data, status):
        self.set_status(status)
        self.fail(data)

    def is_id_valid(self, id_):
        if not ObjectId.is_valid(id_):
            self.abort({"message": "ID {} is not a valid bson ObjectId".format(id_)}, BAD_REQUEST_CODE)
            return False

        return True

    @staticmethod
    def get_assignment_id(course_id, assignment_name):
        return "{}/{}".format(course_id, assignment_name)

    def get_db(self):
        """
        Extracts the DatabaseResolver instance attached to the application setting

        :rtype: DatabaseResolver
        :return: DatabaseResolver instance
        """
        return self.settings.get(consts.APP_DB)

    def get_queue(self):
        """
        Extracts the Queue attached to the application setting which contains all the grading jobs that are ready to be
        started

        :rtype: Queue
        :return: Grading Job Queue
        """
        return self.settings.get(consts.APP_QUEUE)

    def get_cluster_token(self):
        """
        Extracts the cluster token attached to the application setting which is used to authenticate grader requests

        :rtype: str
        :return: cluster token
        """
        return self.settings.get(consts.CLUSTER_TOKEN)

    def get_course_or_abort(self, id_):
        # does NOT have an auto id. So we will not be using ObjectId
        course = self.get_db().get_course_collection().find_one({key.ID: id_})
        if course is None:
            self.abort({"message": "Course with id {} does not exist".format(id_)}, BAD_REQUEST_CODE)
        else:
            return course

    def get_assignment_or_abort(self, id_):
        # does NOT have an auto id. So we will not be using ObjectId
        assignment = self.get_db().get_assignment_collection().find_one({key.ID: id_})

        if assignment is None:
            self.abort({"message": "Assignment with id {} does not exist".format(id_)}, BAD_REQUEST_CODE)
        else:
            return assignment

    def get_worker_node_or_abort(self, id_):
        if not self.is_id_valid(id_):
            return None

        worker_node = self.get_db().get_worker_node_collection().find_one({key.ID: ObjectId(id_)})
        if worker_node is None:
            self.abort({"message": "Worker node with id {} does not exist".format(id_)}, BAD_REQUEST_CODE)
        else:
            return worker_node

    def get_grading_run_or_abort(self, id_):
        if not self.is_id_valid(id_):
            return None

        grading_run = self.get_db().get_grading_run_collection().find_one({key.ID: ObjectId(id_)})
        if grading_run is None:
            self.abort({"message": "Grading run with id {} does not exist".format(id_)}, BAD_REQUEST_CODE)
        else:
            return grading_run

    def get_grading_job_or_abort(self, id_):
        if not self.is_id_valid(id_):
            return None

        grading_job = self.get_db().get_grading_job_collection().find_one({key.ID: ObjectId(id_)})
        if grading_job is None:
            self.abort({"message": "Grading job with id {} does not exist".format(id_)}, BAD_REQUEST_CODE)
        else:
            return grading_job
