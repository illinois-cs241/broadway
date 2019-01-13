import logging

from bson import ObjectId
from tornado_json import schema

import src.constants.keys as key
import src.constants.constants as consts
from src.auth import authenticate_cluster_token, authenticate_course
from src.config import BAD_REQUEST_CODE
from src.handlers.base_handler import BaseAPIHandler
from src.utilities import get_time, enqueue_job, enqueue_student_jobs

logger = logging.getLogger()


class GradingConfigHandler(BaseAPIHandler):
    @authenticate_course
    @schema.validate(
        input_schema=consts.GRADING_CONFIG_DEF
    )
    def post(self, *args, **kwargs):
        assignment_id = self.get_assignment_id(kwargs.get(key.COURSE_ID_PARAM),
                                               kwargs.get(key.ASSIGNMENT_NAME_PARAM))
        assignment_collection = self.get_db().get_assignment_collection()

        # if the assignment config already exists then delete it and replace it with the new one
        if assignment_collection.find_one({key.ID: assignment_id}) is not None:
            assignment_collection.delete_one({key.ID: assignment_id})

        assignment_collection.insert_one({key.ID: assignment_id, **self.body})

    @authenticate_course
    @schema.validate(
        on_empty_404=True,
        output_schema=consts.GRADING_CONFIG_DEF
    )
    def get(self, *args, **kwargs):
        assignment = self.get_assignment(kwargs.get(key.COURSE_ID_PARAM), kwargs.get(key.ASSIGNMENT_NAME_PARAM))
        if assignment is not None:
            del assignment[key.ID]
            return assignment


class StartGradingRunHandler(BaseAPIHandler):
    @authenticate_course
    @schema.validate(
        input_schema={
            "type": "object",
            "properties": {
                key.PRE_PROCESSING_ENV: {"type": "object"},
                key.STUDENTS_ENV: {"type": "array", "items": {"type": "object"}},
                key.POST_PROCESSING_ENV: {"type": "object"},
            },
            "required": [key.STUDENTS_ENV],
            "additionalProperties": False
        },
        output_schema={
            "type": "object",
            "properties": {
                key.GRADING_RUN_ID: {"type": "string"}
            },
            "required": [key.GRADING_RUN_ID],
            "additionalProperties": False
        },
        on_empty_404=True
    )
    def post(self, *args, **kwargs):
        course_id = kwargs.get(key.COURSE_ID_PARAM)
        assignment_name = kwargs.get(key.ASSIGNMENT_NAME_PARAM)
        assignment = self.get_assignment(course_id, assignment_name)
        if assignment is None:
            return

        # create grading run
        # pre_processing_env
        # post_processing_env
        # students_env

        grading_run = {key.ASSIGNMENT_ID: self.get_assignment_id(course_id, assignment_name),
                       key.STARTED: get_time(), key.STUDENT_JOBS_LEFT: len(self.body.get(key.STUDENTS_ENV))}
        # TODO start AG run. Schedule pre-processing if it exists otherwise schedule student jobs


class AddGradingRunHandler(BaseAPIHandler):
    @authenticate_cluster_token
    @schema.validate(
        input_schema={
            "type": "object",
            "properties": {
                key.PRE_PROCESSING_PIPELINE: {
                    "type": "array",
                    "items": {
                        "type": "object",
                        "properties": {
                            key.IMAGE: {"type": "string"},
                            key.ENV: {"type": "object"},
                            key.ENTRY_POINT: {"type": "array", "items": {"type": "string"}},
                            key.NETWORKING: {"type": "boolean"},
                            key.HOST_NAME: {"type": "string"},
                            key.TIMEOUT: {"type": "number"}
                        },
                        "required": [key.IMAGE],
                        "additionalProperties": False
                    },
                },
                key.STUDENT_PIPELINE: {
                    "type": "array",
                    "items": {
                        "type": "object",
                        "properties": {
                            key.IMAGE: {"type": "string"},
                            key.ENV: {"type": "object"},
                            key.ENTRY_POINT: {"type": "array", "items": {"type": "string"}},
                            key.NETWORKING: {"type": "boolean"},
                            key.HOST_NAME: {"type": "string"},
                            key.TIMEOUT: {"type": "number"}
                        },
                        "required": [key.IMAGE],
                        "additionalProperties": False
                    },
                },
                key.POST_PROCESSING_PIPELINE: {
                    "type": "array",
                    "items": {
                        "type": "object",
                        "properties": {
                            key.IMAGE: {"type": "string"},
                            key.ENV: {"type": "object"},
                            key.ENTRY_POINT: {"type": "array", "items": {"type": "string"}},
                            key.NETWORKING: {"type": "boolean"},
                            key.HOST_NAME: {"type": "string"},
                            key.TIMEOUT: {"type": "number"}
                        },
                        "required": [key.IMAGE],
                        "additionalProperties": False
                    },
                },
                key.STUDENTS: {
                    "type": "array",
                    "items": {"type": "object"},
                },
                key.ENV: {"type": "object"},
            },
            "required": [key.STUDENT_PIPELINE, key.STUDENTS],
            "additionalProperties": False
        },
        output_schema={
            "type": "object",
            "properties": {
                key.GRADING_RUN_ID: {"type": "string"}
            },
            "required": [key.GRADING_RUN_ID],
            "additionalProperties": False
        }
    )
    def post(self):
        # create grading run in DB
        db_handler = self.get_db()
        grading_runs_collection = db_handler.get_grading_run_collection()

        grading_run = {key.CREATED: get_time(), key.STUDENTS: self.body.get(key.STUDENTS)}
        grading_run_id = str(grading_runs_collection.insert_one(grading_run).inserted_id)

        jobs_collection = db_handler.get_grading_job_collection()

        # create all jobs in DB
        student_job_ids = []
        for student in self.body.get(key.STUDENTS):
            job = {key.CREATED: get_time(), key.GRADING_RUN_ID: grading_run_id,
                   key.STAGES: self.create_job(key.STUDENT_PIPELINE, student)}

            student_job_ids.append(str(jobs_collection.insert_one(job).inserted_id))

        grading_runs_collection.update_one({key.ID: ObjectId(grading_run_id)}, {
            "$set": {key.STUDENT_JOBS: student_job_ids, key.STUDENT_JOBS_LEFT: len(student_job_ids)}})

        # create pre processing stage if it exists
        if key.PRE_PROCESSING_PIPELINE in self.body:
            pre_processing_job = {key.CREATED: get_time(), key.GRADING_RUN_ID: grading_run_id,
                                  key.STAGES: self.create_job(key.PRE_PROCESSING_PIPELINE)}
            pre_processing_job_id = str(jobs_collection.insert_one(pre_processing_job).inserted_id)
            grading_runs_collection.update_one({key.ID: ObjectId(grading_run_id)},
                                               {"$set": {key.PRE_PROCESSING: pre_processing_job_id}})

        # create post processing stage if it exists
        if key.POST_PROCESSING_PIPELINE in self.body:
            post_processing_job = {key.CREATED: get_time(), key.GRADING_RUN_ID: grading_run_id,
                                   key.STAGES: self.create_job(key.POST_PROCESSING_PIPELINE)}
            post_processing_job_id = str(jobs_collection.insert_one(post_processing_job).inserted_id)
            grading_runs_collection.update_one({key.ID: ObjectId(grading_run_id)},
                                               {"$set": {key.POST_PROCESSING: post_processing_job_id}})

        # return the run id to user
        return {key.GRADING_RUN_ID: grading_run_id}


class GradingRunHandler(BaseAPIHandler):
    @authenticate_cluster_token
    def post(self, grading_run_id):
        db_handler = self.get_db()
        grading_runs_collection = db_handler.get_grading_run_collection()
        grading_run = self.get_grading_run(grading_run_id)
        if grading_run is None:
            return

        # check to see if grading run has already started
        if key.STARTED in grading_run:
            self.abort({"message": "Grading Run with id {} has already been queued in the past".format(grading_run_id)},
                       BAD_REQUEST_CODE)
            return

        # update grading run that it has started
        grading_runs_collection.update_one({key.ID: ObjectId(grading_run_id)},
                                           {"$set": {key.STARTED: get_time()}})

        # enqueue jobs
        if key.PRE_PROCESSING in grading_run:
            enqueue_job(db_handler, self.get_queue(), grading_run.get(key.PRE_PROCESSING),
                        grading_run.get(key.STUDENTS))
        else:
            enqueue_student_jobs(db_handler, self.get_queue(), grading_run)

    # TODO when building a web app around this
    @authenticate_cluster_token
    def get(self, grading_run_id):
        pass
