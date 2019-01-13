import logging

from tornado_json import schema

import src.constants.constants as consts
import src.constants.keys as key
from src.auth import authenticate_course
from src.handlers.base_handler import BaseAPIHandler
from src.utilities import get_time

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
        course_id = kwargs.get(key.COURSE_ID_PARAM)
        assignment_name = kwargs.get(key.ASSIGNMENT_NAME_PARAM)
        assignment = self.get_assignment(course_id, assignment_name)
        if assignment is None:
            return

        # create grading run document
        grading_run = {key.ASSIGNMENT_ID: self.get_assignment_id(course_id, assignment_name),
                       key.STARTED: get_time(),
                       key.STUDENT_JOBS_LEFT: len(self.body.get(key.STUDENTS_ENV)),
                       **self.body}
        grading_run_id = str(self.get_db().get_grading_run_collection().insert_one(grading_run).inserted_id)

        # TODO start AG run. Schedule pre-processing if it exists otherwise schedule student jobs
