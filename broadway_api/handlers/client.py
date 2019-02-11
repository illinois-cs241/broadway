import logging

from tornado_json import schema

import broadway_api.daos as daos
import broadway_api.definitions as definitions
import broadway_api.models as models
from broadway_api.decorators.auth import authenticate_course
from broadway_api.handlers.base import BaseAPIHandler
from broadway_api.utils.run import continue_grading_run
from broadway_api.utils.time import get_time

logger = logging.getLogger("client")


class ClientAPIHandler(BaseAPIHandler):
    def get_assignment_id(self, **kwargs):
        return daos.AssignmentConfigDao.id_from(
            kwargs.get("course_id"), kwargs.get("assignment_name")
        )


class GradingConfigHandler(ClientAPIHandler):
    @authenticate_course
    @schema.validate(input_schema=definitions.grading_config)
    def post(self, *args, **kwargs):
        assignment_id = self.get_assignment_id(**kwargs)
        config = models.AssignmentConfig(id=assignment_id, **self.body)
        config.id = assignment_id

        config_dao = daos.AssignmentConfigDao(self.settings)
        config_dao.delete_by_id(assignment_id)
        config_dao.insert(config)

    @authenticate_course
    @schema.validate(on_empty_404=True, output_schema=definitions.grading_config)
    def get(self, *args, **kwargs):
        assignment_id = self.get_assignment_id(**kwargs)

        config_dao = daos.AssignmentConfigDao(self.settings)
        config = config_dao.find_by_id(assignment_id)
        if not config:
            self.abort({"message": "assignment configuration not found"})
            return

        return config.to_dict()


class GradingRunHandler(ClientAPIHandler):
    @authenticate_course
    @schema.validate(
        input_schema={
            "type": "object",
            "properties": {
                "pre_processing_env": {"type": "object"},
                "students_env": {"type": "array", "items": {"type": "object"}},
                "post_processing_env": {"type": "object"},
            },
            "required": ["students_env"],
            "additionalProperties": False,
        },
        output_schema={
            "type": "object",
            "properties": {"grading_run_id": {"type": "string"}},
            "required": ["grading_run_id"],
            "additionalProperties": False,
        },
        on_empty_404=True,
    )
    def post(self, *args, **kwargs):
        assignment_id = self.get_assignment_id(**kwargs)

        config_dao = daos.AssignmentConfigDao(self.settings)
        config = config_dao.find_by_id(assignment_id)
        if not config:
            self.abort({"message": "assignment configuration not found"})
            return

        if not self._assert_run_valid(config):
            # abort in valid-run method for conciseness
            return

        run_attrs = {
            **self.body,
            "assignment_id": assignment_id,
            "started_at": get_time(),
            "state": models.GradingRunState.READY,
            "student_jobs_left": len(self.body.get("students_env")),
        }
        run = models.GradingRun(**run_attrs)

        run_dao = daos.GradingRunDao(self.settings)
        run.id = str(run_dao.insert(run).inserted_id)

        if not continue_grading_run(self.settings, run):
            self.abort({"message": "failed to start grading run"}, status=500)
            return
        return {"grading_run_id": run.id}

    @authenticate_course
    @schema.validate(
        output_schema={
            "type": "object",
            "properties": {
                "state": {"type": "string"},
                "pre_processing_job_state": {"type": ["null", "string"]},
                "post_processing_job_state": {"type": ["null", "string"]},
                "student_jobs_state": {"type": "array", "items": {"type": "string"}},
            },
            "required": ["state"],
            "additionalProperties": False,
        },
        on_empty_404=True,
    )
    def get(self, *args, **kwargs):
        assignment_id = self.get_assignment_id(**kwargs)
        grading_run_id = kwargs.get("grading_run_id")

        grading_run_dao = daos.GradingRunDao(self.settings)
        grading_run = grading_run_dao.find_by_id(grading_run_id)
        if grading_run is None:
            self.abort({"message": "grading run with the given ID not found"})
            return

        if grading_run.assignment_id != assignment_id:
            self.abort(
                {"message": "grading run does not belong to specified assignment"}
            )
            return

        grading_job_dao = daos.GradingJobDao(self.settings)
        jobs = grading_job_dao.find_by_run_id(grading_run_id)
        pre_processing_job = next(
            filter(lambda j: j.type == models.GradingJobType.PRE_PROCESSING, jobs), None
        )
        post_processing_job = next(
            filter(lambda j: j.type == models.GradingJobType.POST_PROCESSING, jobs),
            None,
        )
        student_jobs = filter(lambda j: j.type == models.GradingJobType.STUDENT, jobs)

        return {
            "state": grading_run.state.value,
            "pre_processing_job_state": (
                pre_processing_job.get_state().value if pre_processing_job else None
            ),
            "post_processing_job_state": (
                post_processing_job.get_state().value if post_processing_job else None
            ),
            "student_jobs_state": [j.get_state().value for j in student_jobs],
        }

    def _assert_run_valid(self, config):
        if "pre_processing_env" in self.body and not config.pre_processing_pipeline:
            self.abort(
                {
                    "message": (
                        "pre-processing runtime environment provided, but no"
                        "pre-processing stage was associated with this assignment"
                    )
                },
                status=400,
            )
            return False

        if "post_processing_env" in self.body and not config.post_processing_pipeline:
            self.abort(
                {
                    "message": (
                        "post-processing runtime environment provided, but no"
                        "post-processing stage was associated with this assignment"
                    )
                },
                status=400,
            )
            return False
        return True
