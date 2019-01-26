import logging

from tornado_json import schema

import src.constants.constants as consts
import src.constants.keys as key
from src.auth import authenticate_course
from src.config import BAD_REQUEST_CODE
from src.handlers.base_handler import BaseAPIHandler
from src.handlers.schedulers import progress_grading_run
from src.utilities import get_time, get_job_status

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
        assignment_collection.delete_one({key.ID: assignment_id})
        assignment_collection.insert_one({key.ID: assignment_id, **self.body})

    @authenticate_course
    @schema.validate(
        on_empty_404=True,
        output_schema=consts.GRADING_CONFIG_DEF
    )
    def get(self, *args, **kwargs):
        assignment = self.get_assignment_or_abort(
            self.get_assignment_id(kwargs.get(key.COURSE_ID_PARAM), kwargs.get(key.ASSIGNMENT_NAME_PARAM)))

        if assignment is not None:
            del assignment[key.ID]
            return assignment


class GradingRunHandler(BaseAPIHandler):
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
        assignment_id = self.get_assignment_id(kwargs.get(key.COURSE_ID_PARAM), kwargs.get(key.ASSIGNMENT_NAME_PARAM))
        assignment = self.get_assignment_or_abort(assignment_id)
        if assignment is None:
            return

        if key.PRE_PROCESSING_ENV in self.body and key.PRE_PROCESSING_PIPELINE not in assignment:
            self.abort({"message": "Pre processing environment variables were provided when assignment config"},
                       BAD_REQUEST_CODE)
            return

        if key.POST_PROCESSING_ENV in self.body and key.POST_PROCESSING_PIPELINE not in assignment:
            self.abort({"message": "Post processing environment variables were provided when assignment config"},
                       BAD_REQUEST_CODE)
            return

        # create grading run document
        grading_run = {key.STATE: consts.GradingRunState.READY.value,
                       key.ASSIGNMENT_ID: assignment_id,
                       key.STARTED: get_time(),
                       key.STUDENT_JOBS_LEFT: len(self.body.get(key.STUDENTS_ENV)),
                       **self.body}
        grading_run_id = str(self.get_db().get_grading_run_collection().insert_one(grading_run).inserted_id)

        # schedule jobs
        progress_grading_run(self.get_db(), self.get_queue(), grading_run_id)
        return {key.GRADING_RUN_ID: grading_run_id}

    @authenticate_course
    @schema.validate(
        output_schema={
            "type": "object",
            "properties": {
                key.STATE: {"type": "string"},
                key.PRE_PROCESSING_STATE: {"type": "string"},
                key.POST_PROCESSING_STATE: {"type": "string"},
                key.STUDENTS_STATE: {"type": "array", "items": {"type": "string"}}
            },
            "required": [key.STATE],
            "additionalProperties": False
        },
        on_empty_404=True
    )
    def get(self, *args, **kwargs):
        assignment_id = self.get_assignment_id(kwargs.get(key.COURSE_ID_PARAM), kwargs.get(key.ASSIGNMENT_NAME_PARAM))
        grading_run_id = kwargs.get(key.RUN_ID_PARAM)

        grading_run = self.get_grading_run_or_abort(grading_run_id)
        if grading_run is None:
            return

        if grading_run.get(key.ASSIGNMENT_ID) != assignment_id:
            self.abort({"message": "Grading run {} does not belong to {}".format(grading_run_id, assignment_id)},
                       BAD_REQUEST_CODE)
            return

        grading_jobs = self.get_db().get_grading_job_collection().find({key.GRADING_RUN_ID: grading_run_id})
        run_state = {key.STUDENTS_STATE: [], key.STATE: grading_run.get(key.STATE)}

        for grading_job in grading_jobs:
            if grading_job[key.TYPE] == consts.GradingJobType.PRE_PROCESSING.value:
                run_state[key.PRE_PROCESSING_STATE] = get_job_status(grading_job).value
            elif grading_job[key.TYPE] == consts.GradingJobType.POST_PROCESSING.value:
                run_state[key.POST_PROCESSING_STATE] = get_job_status(grading_job).value
            else:
                run_state[key.STUDENTS_STATE].append(get_job_status(grading_job).value)

        return run_state
