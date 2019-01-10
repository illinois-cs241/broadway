import logging

from bson import ObjectId
from tornado_json import schema

import src.constants.api_keys as api_key
import src.constants.db_keys as db_key
from src.auth import authenticate_cluster_token, authenticate_course
from src.config import BAD_REQUEST_CODE
from src.utilities import get_time, enqueue_job, enqueue_student_jobs
from src.handlers.base_handler import BaseAPIHandler

logger = logging.getLogger()

# constants
grading_stage_def = {
    "type": "object",
    "properties": {
        api_key.IMAGE: {"type": "string"},
        api_key.ENV: {"type": "object"},
        api_key.ENTRY_POINT: {"type": "array", "items": {"type": "string"}},
        api_key.NETWORKING: {"type": "boolean"},
        api_key.HOST_NAME: {"type": "string"},
        api_key.TIMEOUT: {"type": "number"}
    },
    "required": [api_key.IMAGE],
    "additionalProperties": False
}


class GradingConfigHandler(BaseAPIHandler):
    @authenticate_course
    @schema.validate(
        input_schema={
            "type": "object",
            "properties": {
                api_key.PRE_PROCESSING_PIPELINE: {
                    "type": "array",
                    "items": grading_stage_def,
                },
                api_key.STUDENT_PIPELINE: {
                    "type": "array",
                    "items": grading_stage_def,
                },
                api_key.POST_PROCESSING_PIPELINE: {
                    "type": "array",
                    "items": grading_stage_def,
                },
                api_key.ENV: {"type": "object"},
            },
            "required": [api_key.STUDENT_PIPELINE],
            "additionalProperties": False
        }
    )
    def post(self, *args, **kwargs):
        pass

    @authenticate_course
    @schema.validate(
        output_schema={
            "type": "object",
            "properties": {
                api_key.PRE_PROCESSING_PIPELINE: {
                    "type": "array",
                    "items": grading_stage_def,
                },
                api_key.STUDENT_PIPELINE: {
                    "type": "array",
                    "items": grading_stage_def,
                },
                api_key.POST_PROCESSING_PIPELINE: {
                    "type": "array",
                    "items": grading_stage_def,
                },
                api_key.ENV: {"type": "object"},
            },
            "required": [api_key.STUDENT_PIPELINE],
            "additionalProperties": False
        }
    )
    def get(self, *args, **kwargs):
        # TODO
        pass


class AddGradingRunHandler(BaseAPIHandler):
    @authenticate_cluster_token
    @schema.validate(
        input_schema={
            "type": "object",
            "properties": {
                api_key.PRE_PROCESSING_PIPELINE: {
                    "type": "array",
                    "items": grading_stage_def,
                },
                api_key.STUDENT_PIPELINE: {
                    "type": "array",
                    "items": grading_stage_def,
                },
                api_key.POST_PROCESSING_PIPELINE: {
                    "type": "array",
                    "items": grading_stage_def,
                },
                api_key.STUDENTS: {
                    "type": "array",
                    "items": {"type": "object"},
                },
                api_key.ENV: {"type": "object"},
            },
            "required": [api_key.STUDENT_PIPELINE, api_key.STUDENTS],
            "additionalProperties": False
        },
        output_schema={
            "type": "object",
            "properties": {
                api_key.RUN_ID: {"type": "string"}
            },
            "required": [api_key.RUN_ID],
            "additionalProperties": False
        }
    )
    def post(self):
        # create grading run in DB
        db_handler = self.get_db()
        grading_runs_collection = db_handler.get_grading_run_collection()

        grading_run = {db_key.CREATED: get_time(), db_key.STUDENTS: self.body.get(api_key.STUDENTS)}
        grading_run_id = str(grading_runs_collection.insert_one(grading_run).inserted_id)

        jobs_collection = db_handler.get_grading_job_collection()

        # create all jobs in DB
        student_job_ids = []
        for student in self.body.get(api_key.STUDENTS):
            job = {db_key.CREATED: get_time(), db_key.GRADING_RUN: grading_run_id,
                   db_key.STAGES: self.create_job(api_key.STUDENT_PIPELINE, student)}

            student_job_ids.append(str(jobs_collection.insert_one(job).inserted_id))

        grading_runs_collection.update_one({db_key.ID: ObjectId(grading_run_id)}, {
            "$set": {db_key.STUDENT_JOBS: student_job_ids, db_key.STUDENT_JOBS_LEFT: len(student_job_ids)}})

        # create pre processing stage if it exists
        if api_key.PRE_PROCESSING_PIPELINE in self.body:
            pre_processing_job = {db_key.CREATED: get_time(), db_key.GRADING_RUN: grading_run_id,
                                  db_key.STAGES: self.create_job(api_key.PRE_PROCESSING_PIPELINE)}
            pre_processing_job_id = str(jobs_collection.insert_one(pre_processing_job).inserted_id)
            grading_runs_collection.update_one({db_key.ID: ObjectId(grading_run_id)},
                                               {"$set": {db_key.PRE_PROCESSING: pre_processing_job_id}})

        # create post processing stage if it exists
        if api_key.POST_PROCESSING_PIPELINE in self.body:
            post_processing_job = {db_key.CREATED: get_time(), db_key.GRADING_RUN: grading_run_id,
                                   db_key.STAGES: self.create_job(api_key.POST_PROCESSING_PIPELINE)}
            post_processing_job_id = str(jobs_collection.insert_one(post_processing_job).inserted_id)
            grading_runs_collection.update_one({db_key.ID: ObjectId(grading_run_id)},
                                               {"$set": {db_key.POST_PROCESSING: post_processing_job_id}})

        # return the run id to user
        return {api_key.RUN_ID: grading_run_id}


class GradingRunHandler(BaseAPIHandler):
    @authenticate_cluster_token
    def post(self, grading_run_id):
        db_handler = self.get_db()
        grading_runs_collection = db_handler.get_grading_run_collection()
        grading_run = self.get_grading_run(grading_run_id)
        if grading_run is None:
            return

        # check to see if grading run has already started
        if db_key.STARTED in grading_run:
            self.abort({"message": "Grading Run with id {} has already been queued in the past".format(grading_run_id)},
                       BAD_REQUEST_CODE)
            return

        # update grading run that it has started
        grading_runs_collection.update_one({db_key.ID: ObjectId(grading_run_id)},
                                           {"$set": {db_key.STARTED: get_time()}})

        # enqueue jobs
        if db_key.PRE_PROCESSING in grading_run:
            enqueue_job(db_handler, self.get_queue(), grading_run.get(db_key.PRE_PROCESSING),
                        grading_run.get(db_key.STUDENTS))
        else:
            enqueue_student_jobs(db_handler, self.get_queue(), grading_run)

    # TODO when building a web app around this
    @authenticate_cluster_token
    def get(self, grading_run_id):
        pass
