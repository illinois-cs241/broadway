import logging
import tornado.ioloop

from tornado_json import schema

import broadway.api.daos as daos
import broadway.api.definitions as definitions
import broadway.api.models as models
from broadway.api.decorators.auth import authenticate_course
from broadway.api.handlers.base import BaseAPIHandler
from broadway.api.utils.run import continue_grading_run
from broadway.api.utils.time import get_time
from broadway.api.callbacks.worker import worker_schedule_job

logger = logging.getLogger(__name__)


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
        config = models.AssignmentConfig(id_=assignment_id, **self.body)
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

        # trigger schedule event
        tornado.ioloop.IOLoop.current().add_callback(worker_schedule_job, self.settings)

        return {"grading_run_id": run.id}

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


class GradingRunStatusHandler(ClientAPIHandler):
    @authenticate_course
    @schema.validate(
        output_schema={
            "type": "object",
            "properties": {
                "state": {"type": "string"},
                "pre_processing_job_state": {"type": ["null", "object"]},
                "post_processing_job_state": {"type": ["null", "object"]},
                "student_jobs_state": {"type": "object"},
            },
            "required": ["state"],
            "additionalProperties": False,
        },
        on_empty_404=True,
    )
    def get(self, *args, **kwargs):
        grading_run_id = kwargs.get("run_id")

        grading_run_dao = daos.GradingRunDao(self.settings)
        grading_run = grading_run_dao.find_by_id(grading_run_id)
        if grading_run is None:
            self.abort({"message": "grading run with the given ID not found"})
            return

        grading_job_dao = daos.GradingJobDao(self.settings)
        grading_jobs = grading_job_dao.find_by_run_id(grading_run_id)
        pre_processing_job = next(
            filter(
                lambda j: j.type == models.GradingJobType.PRE_PROCESSING, grading_jobs
            ),
            None,
        )
        post_processing_job = next(
            filter(
                lambda j: j.type == models.GradingJobType.POST_PROCESSING, grading_jobs
            ),
            None,
        )
        student_jobs = filter(
            lambda j: j.type == models.GradingJobType.STUDENT, grading_jobs
        )

        # [jobs] -> { job_id: job_state }
        def get_job_id_to_state_map(jobs):
            if jobs is None:
                return None
            else:
                return {job.id: job.get_state().value for job in jobs}

        return {
            "state": grading_run.state.value,
            "pre_processing_job_state": get_job_id_to_state_map(
                [pre_processing_job] if pre_processing_job else None
            ),
            "post_processing_job_state": get_job_id_to_state_map(
                [post_processing_job] if post_processing_job else None
            ),
            "student_jobs_state": get_job_id_to_state_map(student_jobs),
        }


class GradingJobLogHandler(ClientAPIHandler):
    @authenticate_course
    @schema.validate(
        output_schema={
            "type": "object",
            "properties": {"stderr": {"type": "string"}, "stdout": {"type": "string"}},
            "required": ["stderr", "stdout"],
            "additionalProperties": False,
        },
        on_empty_404=True,
    )
    def get(self, *args, **kwargs):
        job_id = kwargs["job_id"]

        job_log_dao = daos.GradingJobLogDao(self.settings)
        job_log = job_log_dao.find_by_job_id(job_id)

        if job_log is None:
            self.abort(
                {
                    "message": "grading job with the given ID"
                    "not found or has not finished"
                }
            )
            return

        return {"stderr": job_log.stderr, "stdout": job_log.stdout}


class CourseWorkerNodeHandler(ClientAPIHandler):
    @authenticate_course
    @schema.validate(
        output_schema={
            "type": "object",
            "properties": {
                "worker_nodes": {
                    "type": "array",
                    "items": {
                        "type": "object",
                        "properties": {
                            "hostname": {"type": "string"},
                            "jobs_processed": {"type": "number"},
                            "busy": {"type": "boolean"},
                            "alive": {"type": "boolean"},
                        },
                        "required": ["hostname", "jobs_processed", "busy", "alive"],
                        "additionalProperties": False,
                    },
                }
            },
            "required": ["worker_nodes"],
            "additionalProperties": False,
        },
        on_empty_404=True,
    )
    def get(self, *args, **kwargs):
        scope = kwargs.get("scope")
        worker_node_dao = daos.WorkerNodeDao(self.settings)

        if scope == "all":
            return {
                "worker_nodes": list(
                    map(
                        lambda worker_node: {
                            "hostname": worker_node.hostname,
                            "jobs_processed": worker_node.jobs_processed,
                            "busy": (worker_node.running_job_id is not None),
                            "alive": worker_node.is_alive,
                        },
                        worker_node_dao.find_all(),
                    )
                )
            }
        else:
            self.abort(
                {"message": "scope {} has not been implemented yet".format(scope)}, 404
            )
            return


class CourseQueueLengthHandler(ClientAPIHandler):
    @authenticate_course
    @schema.validate(
        output_schema={
            "type": "object",
            "properties": {"length": {"type": "number"}},
            "required": ["length"],
            "additionalProperties": False,
        },
        on_empty_404=True,
    )
    def get(self, *args, **kwargs):
        course_id = kwargs["course_id"]
        queue = self.settings["QUEUE"]

        length = 0
        if queue.contains_key(course_id):
            length = queue.get_queue_length(course_id)

        return {"length": length}


class GradingJobQueuePositionHandler(ClientAPIHandler):
    @authenticate_course
    @schema.validate(
        output_schema={
            "type": "object",
            "properties": {"position": {"type": "number"}},
            "required": ["position"],
            "additionalProperties": False,
        },
        on_empty_404=True,
    )
    def get(self, *args, **kwargs):

        grading_job_id = kwargs.get("job_id")

        grading_job_dao = daos.GradingJobDao(self.settings)
        if grading_job_dao.find_by_id(grading_job_id) is None:
            self.abort({"message": "grading job with the given ID not found"})
            return

        course_id = kwargs["course_id"]
        queue = self.settings["QUEUE"]

        if not queue.contains_key(course_id):
            self.abort(
                {"message": f"{course_id} does not exist as a course in the queue"}
            )
            return

        queue_position = queue.get_position_in_queue(course_id, grading_job_id)
        if queue_position == -1:
            self.abort(
                {"message": f"{grading_job_id} has already passed through the queue"}
            )

        return {"position": queue_position}
