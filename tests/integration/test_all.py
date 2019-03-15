import logging

from broadway_api.models import GradingRunState, GradingJobState

import tests._fixtures.grading_configs as grading_configs
import tests._fixtures.grading_runs as grading_runs
from tests.base import BaseTest

logging.disable(logging.WARNING)


# get status of the first job
def get_first_status(map):
    return list(map.values())[0]


class EndpointIntegrationTest(BaseTest):
    def test_single_student_job(self):
        worker_id = self.register_worker(self.get_header())
        self.upload_grading_config(
            self.course1,
            "assignment1",
            self.client_header1,
            grading_configs.only_student_config,
            200,
        )
        grading_run_id = self.start_grading_run(
            self.course1,
            "assignment1",
            self.client_header1,
            grading_runs.one_student_job,
            200,
        )

        self.check_grading_run_status(
            self.course1,
            "assignment1",
            grading_run_id,
            self.client_header1,
            200,
            GradingRunState.STUDENTS_STAGE.value,
        )
        run_state = self.get_grading_run_state(
            self.course1, "assignment1", grading_run_id, self.client_header1
        )
        self.assertEqual(
            get_first_status(run_state["student_jobs_state"]),
            GradingJobState.QUEUED.value,
        )

        # job logs should not exist at this point
        for job_id in run_state["student_jobs_state"]:
            self.get_grading_job_log(self.course1, job_id, self.client_header1, 400)

        student_job = self.poll_job(worker_id, self.get_header())
        self.check_grading_run_status(
            self.course1,
            "assignment1",
            grading_run_id,
            self.client_header1,
            200,
            GradingRunState.STUDENTS_STAGE.value,
        )
        self.assertEqual(self.poll_job(worker_id, self.get_header()), 498)
        run_state = self.get_grading_run_state(
            self.course1, "assignment1", grading_run_id, self.client_header1
        )

        self.assertEqual(
            get_first_status(run_state["student_jobs_state"]),
            GradingJobState.STARTED.value,
        )

        # job logs should not exist at this point
        for job_id in run_state["student_jobs_state"]:
            self.get_grading_job_log(self.course1, job_id, self.client_header1, 400)

        self.post_job_result(
            worker_id, self.get_header(), student_job.get("grading_job_id")
        )
        self.check_grading_run_status(
            self.course1,
            "assignment1",
            grading_run_id,
            self.client_header1,
            200,
            GradingRunState.FINISHED.value,
        )
        self.assertEqual(self.poll_job(worker_id, self.get_header()), 498)
        run_state = self.get_grading_run_state(
            self.course1, "assignment1", grading_run_id, self.client_header1
        )

        self.assertEqual(
            get_first_status(run_state["student_jobs_state"]),
            GradingJobState.SUCCEEDED.value,
        )

        for job_id in run_state["student_jobs_state"]:
            self.get_grading_job_log(self.course1, job_id, self.client_header1, 200)

    def test_pre_processing_job(self):
        worker_id = self.register_worker(self.get_header())
        self.upload_grading_config(
            self.course1,
            "assignment1",
            self.client_header1,
            grading_configs.pre_processing_config,
            200,
        )
        grading_run_id = self.start_grading_run(
            self.course1,
            "assignment1",
            self.client_header1,
            grading_runs.one_student_and_pre,
            200,
        )

        self.check_grading_run_status(
            self.course1,
            "assignment1",
            grading_run_id,
            self.client_header1,
            200,
            GradingRunState.PRE_PROCESSING_STAGE.value,
        )
        run_state = self.get_grading_run_state(
            self.course1, "assignment1", grading_run_id, self.client_header1
        )

        self.assertEqual(
            get_first_status(run_state["pre_processing_job_state"]),
            GradingJobState.QUEUED.value,
        )

        pre_processing_job = self.poll_job(worker_id, self.get_header())
        self.check_grading_run_status(
            self.course1,
            "assignment1",
            grading_run_id,
            self.client_header1,
            200,
            GradingRunState.PRE_PROCESSING_STAGE.value,
        )
        self.assertEqual(self.poll_job(worker_id, self.get_header()), 498)
        run_state = self.get_grading_run_state(
            self.course1, "assignment1", grading_run_id, self.client_header1
        )
        self.assertEqual(
            get_first_status(run_state["pre_processing_job_state"]),
            GradingJobState.STARTED.value,
        )

        self.post_job_result(
            worker_id, self.get_header(), pre_processing_job.get("grading_job_id")
        )
        self.check_grading_run_status(
            self.course1,
            "assignment1",
            grading_run_id,
            self.client_header1,
            200,
            GradingRunState.STUDENTS_STAGE.value,
        )
        student_job = self.poll_job(worker_id, self.get_header())
        self.check_grading_run_status(
            self.course1,
            "assignment1",
            grading_run_id,
            self.client_header1,
            200,
            GradingRunState.STUDENTS_STAGE.value,
        )
        self.assertEqual(self.poll_job(worker_id, self.get_header()), 498)
        run_state = self.get_grading_run_state(
            self.course1, "assignment1", grading_run_id, self.client_header1
        )
        self.assertEqual(
            get_first_status(run_state["pre_processing_job_state"]),
            GradingJobState.SUCCEEDED.value,
        )
        self.assertEqual(
            get_first_status(run_state["student_jobs_state"]),
            GradingJobState.STARTED.value,
        )

        self.post_job_result(
            worker_id, self.get_header(), student_job.get("grading_job_id")
        )
        self.check_grading_run_status(
            self.course1,
            "assignment1",
            grading_run_id,
            self.client_header1,
            200,
            GradingRunState.FINISHED.value,
        )
        self.assertEqual(self.poll_job(worker_id, self.get_header()), 498)
        run_state = self.get_grading_run_state(
            self.course1, "assignment1", grading_run_id, self.client_header1
        )
        self.assertEqual(
            get_first_status(run_state["student_jobs_state"]),
            GradingJobState.SUCCEEDED.value,
        )

    def test_post_processing_job(self):
        worker_id = self.register_worker(self.get_header())
        self.upload_grading_config(
            self.course1,
            "assignment1",
            self.client_header1,
            grading_configs.post_processing_config,
            200,
        )
        grading_run_id = self.start_grading_run(
            self.course1,
            "assignment1",
            self.client_header1,
            grading_runs.one_student_and_post,
            200,
        )

        self.check_grading_run_status(
            self.course1,
            "assignment1",
            grading_run_id,
            self.client_header1,
            200,
            GradingRunState.STUDENTS_STAGE.value,
        )
        run_state = self.get_grading_run_state(
            self.course1, "assignment1", grading_run_id, self.client_header1
        )

        self.assertEqual(
            get_first_status(run_state["student_jobs_state"]),
            GradingJobState.QUEUED.value,
        )

        student_job = self.poll_job(worker_id, self.get_header())
        self.check_grading_run_status(
            self.course1,
            "assignment1",
            grading_run_id,
            self.client_header1,
            200,
            GradingRunState.STUDENTS_STAGE.value,
        )
        self.assertEqual(self.poll_job(worker_id, self.get_header()), 498)

        self.post_job_result(
            worker_id, self.get_header(), student_job.get("grading_job_id")
        )
        self.check_grading_run_status(
            self.course1,
            "assignment1",
            grading_run_id,
            self.client_header1,
            200,
            GradingRunState.POST_PROCESSING_STAGE.value,
        )
        run_state = self.get_grading_run_state(
            self.course1, "assignment1", grading_run_id, self.client_header1
        )
        self.assertEqual(
            get_first_status(run_state["post_processing_job_state"]),
            GradingJobState.QUEUED.value,
        )
        self.assertEqual(
            get_first_status(run_state["student_jobs_state"]),
            GradingJobState.SUCCEEDED.value,
        )

        post_processing_job = self.poll_job(worker_id, self.get_header())
        self.check_grading_run_status(
            self.course1,
            "assignment1",
            grading_run_id,
            self.client_header1,
            200,
            GradingRunState.POST_PROCESSING_STAGE.value,
        )

        self.assertEqual(self.poll_job(worker_id, self.get_header()), 498)
        run_state = self.get_grading_run_state(
            self.course1, "assignment1", grading_run_id, self.client_header1
        )
        self.assertEqual(
            get_first_status(run_state["post_processing_job_state"]),
            GradingJobState.STARTED.value,
        )

        self.post_job_result(
            worker_id, self.get_header(), post_processing_job.get("grading_job_id")
        )
        self.check_grading_run_status(
            self.course1,
            "assignment1",
            grading_run_id,
            self.client_header1,
            200,
            GradingRunState.FINISHED.value,
        )
        self.assertEqual(self.poll_job(worker_id, self.get_header()), 498)
        run_state = self.get_grading_run_state(
            self.course1, "assignment1", grading_run_id, self.client_header1
        )
        self.assertEqual(
            get_first_status(run_state["post_processing_job_state"]),
            GradingJobState.SUCCEEDED.value,
        )

    def test_complete_run(self):
        worker_id = self.register_worker(self.get_header())
        self.upload_grading_config(
            self.course1,
            "assignment1",
            self.client_header1,
            grading_configs.both_config,
            200,
        )
        grading_run_id = self.start_grading_run(
            self.course1,
            "assignment1",
            self.client_header1,
            grading_runs.one_student_and_both,
            200,
        )

        self.check_grading_run_status(
            self.course1,
            "assignment1",
            grading_run_id,
            self.client_header1,
            200,
            GradingRunState.PRE_PROCESSING_STAGE.value,
        )
        run_state = self.get_grading_run_state(
            self.course1, "assignment1", grading_run_id, self.client_header1
        )
        self.assertEqual(
            get_first_status(run_state["pre_processing_job_state"]),
            GradingJobState.QUEUED.value,
        )

        pre_processing_job = self.poll_job(worker_id, self.get_header())
        self.check_grading_run_status(
            self.course1,
            "assignment1",
            grading_run_id,
            self.client_header1,
            200,
            GradingRunState.PRE_PROCESSING_STAGE.value,
        )
        self.assertEqual(self.poll_job(worker_id, self.get_header()), 498)
        run_state = self.get_grading_run_state(
            self.course1, "assignment1", grading_run_id, self.client_header1
        )

        self.assertEqual(
            get_first_status(run_state["pre_processing_job_state"]),
            GradingJobState.STARTED.value,
        )

        self.post_job_result(
            worker_id, self.get_header(), pre_processing_job.get("grading_job_id")
        )
        self.check_grading_run_status(
            self.course1,
            "assignment1",
            grading_run_id,
            self.client_header1,
            200,
            GradingRunState.STUDENTS_STAGE.value,
        )
        run_state = self.get_grading_run_state(
            self.course1, "assignment1", grading_run_id, self.client_header1
        )
        self.assertEqual(
            get_first_status(run_state["pre_processing_job_state"]),
            GradingJobState.SUCCEEDED.value,
        )
        self.assertEqual(
            get_first_status(run_state["student_jobs_state"]),
            GradingJobState.QUEUED.value,
        )

        student_job = self.poll_job(worker_id, self.get_header())
        self.check_grading_run_status(
            self.course1,
            "assignment1",
            grading_run_id,
            self.client_header1,
            200,
            GradingRunState.STUDENTS_STAGE.value,
        )
        self.assertEqual(self.poll_job(worker_id, self.get_header()), 498)
        run_state = self.get_grading_run_state(
            self.course1, "assignment1", grading_run_id, self.client_header1
        )
        self.assertEqual(
            get_first_status(run_state["student_jobs_state"]),
            GradingJobState.STARTED.value,
        )

        self.post_job_result(
            worker_id, self.get_header(), student_job.get("grading_job_id")
        )
        self.check_grading_run_status(
            self.course1,
            "assignment1",
            grading_run_id,
            self.client_header1,
            200,
            GradingRunState.POST_PROCESSING_STAGE.value,
        )
        run_state = self.get_grading_run_state(
            self.course1, "assignment1", grading_run_id, self.client_header1
        )
        self.assertEqual(
            get_first_status(run_state["post_processing_job_state"]),
            GradingJobState.QUEUED.value,
        )
        self.assertEqual(
            get_first_status(run_state["student_jobs_state"]),
            GradingJobState.SUCCEEDED.value,
        )

        post_processing_job = self.poll_job(worker_id, self.get_header())
        self.check_grading_run_status(
            self.course1,
            "assignment1",
            grading_run_id,
            self.client_header1,
            200,
            GradingRunState.POST_PROCESSING_STAGE.value,
        )
        self.assertEqual(self.poll_job(worker_id, self.get_header()), 498)
        run_state = self.get_grading_run_state(
            self.course1, "assignment1", grading_run_id, self.client_header1
        )
        self.assertEqual(
            get_first_status(run_state["post_processing_job_state"]),
            GradingJobState.STARTED.value,
        )

        self.post_job_result(
            worker_id, self.get_header(), post_processing_job.get("grading_job_id")
        )
        self.check_grading_run_status(
            self.course1,
            "assignment1",
            grading_run_id,
            self.client_header1,
            200,
            GradingRunState.FINISHED.value,
        )
        self.assertEqual(self.poll_job(worker_id, self.get_header()), 498)
        run_state = self.get_grading_run_state(
            self.course1, "assignment1", grading_run_id, self.client_header1
        )
        self.assertEqual(
            get_first_status(run_state["post_processing_job_state"]),
            GradingJobState.SUCCEEDED.value,
        )

    def test_stress(self):
        num_students = 20
        run_env_vars = {
            "pre_processing_env": {"type": "pre"},
            "post_processing_env": {"type": "post"},
            "students_env": [
                {"netid": "test net id {}".format(ind)} for ind in range(num_students)
            ],
        }

        self.upload_grading_config(
            self.course1,
            "assignment1",
            self.client_header1,
            grading_configs.complete_config,
            200,
        )
        grading_run_id = self.start_grading_run(
            self.course1, "assignment1", self.client_header1, run_env_vars, 200
        )

        # pre processing job stuff
        worker_id = self.register_worker(self.get_header())
        self.check_grading_run_status(
            self.course1,
            "assignment1",
            grading_run_id,
            self.client_header1,
            200,
            GradingRunState.PRE_PROCESSING_STAGE.value,
        )
        pre_processing_job = self.poll_job(worker_id, self.get_header())
        self.check_grading_run_status(
            self.course1,
            "assignment1",
            grading_run_id,
            self.client_header1,
            200,
            GradingRunState.PRE_PROCESSING_STAGE.value,
        )
        self.assertEqual(self.poll_job(worker_id, self.get_header()), 498)

        pre_processing_pipeline = grading_configs.complete_config.get(
            "pre_processing_pipeline"
        ).copy()
        for stage in pre_processing_pipeline:
            if "env" in stage:
                stage["env"].update(grading_configs.complete_config.get("env", {}))
                stage["env"].update({"type": "pre", "GRADING_RUN_ID": grading_run_id})
        self.assert_equal_grading_pipeline(
            pre_processing_job.get("stages"), pre_processing_pipeline
        )
        for stage in pre_processing_job.get("stages"):
            self.assertIn("GRADING_JOB_ID", stage["env"])

        self.post_job_result(
            worker_id, self.get_header(), pre_processing_job.get("grading_job_id")
        )
        self.check_grading_run_status(
            self.course1,
            "assignment1",
            grading_run_id,
            self.client_header1,
            200,
            GradingRunState.STUDENTS_STAGE.value,
        )

        # student jobs
        for ind in range(num_students):
            worker_id = self.register_worker(self.get_header())
            student_job = self.poll_job(worker_id, self.get_header())

            student_pipeline = grading_configs.complete_config.get(
                "student_pipeline"
            ).copy()
            for stage in student_pipeline:
                if "env" in stage:
                    stage["env"].update(grading_configs.complete_config.get("env", {}))
                    stage["env"].update(
                        {
                            "netid": "test net id {}".format(ind),
                            "GRADING_RUN_ID": grading_run_id,
                        }
                    )
            self.assert_equal_grading_pipeline(
                student_job.get("stages"), student_pipeline
            )
            for stage in student_job.get("stages"):
                self.assertIn("GRADING_JOB_ID", stage["env"])

            self.check_grading_run_status(
                self.course1,
                "assignment1",
                grading_run_id,
                self.client_header1,
                200,
                GradingRunState.STUDENTS_STAGE.value,
            )

            if ind == num_students - 1:
                self.assertEqual(self.poll_job(worker_id, self.get_header()), 498)

            self.post_job_result(
                worker_id, self.get_header(), student_job.get("grading_job_id")
            )

        # post processing job
        worker_id = self.register_worker(self.get_header())
        self.check_grading_run_status(
            self.course1,
            "assignment1",
            grading_run_id,
            self.client_header1,
            200,
            GradingRunState.POST_PROCESSING_STAGE.value,
        )
        post_processing_job = self.poll_job(worker_id, self.get_header())

        self.assertEqual(self.poll_job(worker_id, self.get_header()), 498)

        post_processing_pipeline = grading_configs.complete_config.get(
            "post_processing_pipeline"
        ).copy()
        for stage in post_processing_pipeline:
            if "env" in stage:
                stage["env"].update(grading_configs.complete_config.get("env", {}))
                stage["env"].update({"type": "post", "GRADING_RUN_ID": grading_run_id})
        self.assert_equal_grading_pipeline(
            post_processing_job.get("stages"), post_processing_pipeline
        )
        for stage in post_processing_job.get("stages"):
            self.assertIn("GRADING_JOB_ID", stage["env"])

        self.check_grading_run_status(
            self.course1,
            "assignment1",
            grading_run_id,
            self.client_header1,
            200,
            GradingRunState.POST_PROCESSING_STAGE.value,
        )
        self.assertEqual(self.poll_job(worker_id, self.get_header()), 498)

        self.post_job_result(
            worker_id, self.get_header(), post_processing_job.get("grading_job_id")
        )
        self.check_grading_run_status(
            self.course1,
            "assignment1",
            grading_run_id,
            self.client_header1,
            200,
            GradingRunState.FINISHED.value,
        )
        self.assertEqual(self.poll_job(worker_id, self.get_header()), 498)

    def test_abort_pre_processing_fail(self):
        worker_id = self.register_worker(self.get_header())
        self.upload_grading_config(
            self.course1,
            "assignment1",
            self.client_header1,
            grading_configs.pre_processing_config,
            200,
        )
        grading_run_id = self.start_grading_run(
            self.course1,
            "assignment1",
            self.client_header1,
            grading_runs.one_student_and_pre,
            200,
        )

        pre_processing_job = self.poll_job(worker_id, self.get_header())
        self.check_grading_run_status(
            self.course1,
            "assignment1",
            grading_run_id,
            self.client_header1,
            200,
            GradingRunState.PRE_PROCESSING_STAGE.value,
        )
        self.assertEqual(self.poll_job(worker_id, self.get_header()), 498)

        self.post_job_result(
            worker_id,
            self.get_header(),
            pre_processing_job.get("grading_job_id"),
            False,
        )
        self.check_grading_run_status(
            self.course1,
            "assignment1",
            grading_run_id,
            self.client_header1,
            200,
            GradingRunState.FAILED.value,
        )
        self.assertEqual(self.poll_job(worker_id, self.get_header()), 498)

    def test_no_abort_student_fail(self):
        worker_id = self.register_worker(self.get_header())
        self.upload_grading_config(
            self.course1,
            "assignment1",
            self.client_header1,
            grading_configs.both_config,
            200,
        )
        grading_run_id = self.start_grading_run(
            self.course1,
            "assignment1",
            self.client_header1,
            grading_runs.one_student_and_both,
            200,
        )

        pre_processing_job = self.poll_job(worker_id, self.get_header())
        self.check_grading_run_status(
            self.course1,
            "assignment1",
            grading_run_id,
            self.client_header1,
            200,
            GradingRunState.PRE_PROCESSING_STAGE.value,
        )
        self.assertEqual(self.poll_job(worker_id, self.get_header()), 498)

        self.post_job_result(
            worker_id, self.get_header(), pre_processing_job.get("grading_job_id")
        )
        self.check_grading_run_status(
            self.course1,
            "assignment1",
            grading_run_id,
            self.client_header1,
            200,
            GradingRunState.STUDENTS_STAGE.value,
        )

        student_job = self.poll_job(worker_id, self.get_header())
        self.assertEqual(self.poll_job(worker_id, self.get_header()), 498)
        self.post_job_result(
            worker_id, self.get_header(), student_job.get("grading_job_id"), False
        )

        self.check_grading_run_status(
            self.course1,
            "assignment1",
            grading_run_id,
            self.client_header1,
            200,
            GradingRunState.POST_PROCESSING_STAGE.value,
        )
        post_processing_job = self.poll_job(worker_id, self.get_header())
        self.assertEqual(self.poll_job(worker_id, self.get_header()), 498)

        self.post_job_result(
            worker_id, self.get_header(), post_processing_job.get("grading_job_id")
        )
        self.check_grading_run_status(
            self.course1,
            "assignment1",
            grading_run_id,
            self.client_header1,
            200,
            GradingRunState.FINISHED.value,
        )
        self.assertEqual(self.poll_job(worker_id, self.get_header()), 498)

    def test_abort_post_processing_fail(self):
        worker_id = self.register_worker(self.get_header())
        self.upload_grading_config(
            self.course1,
            "assignment1",
            self.client_header1,
            grading_configs.both_config,
            200,
        )
        grading_run_id = self.start_grading_run(
            self.course1,
            "assignment1",
            self.client_header1,
            grading_runs.one_student_and_both,
            200,
        )
        self.check_grading_run_status(
            self.course1,
            "assignment1",
            grading_run_id,
            self.client_header1,
            200,
            GradingRunState.PRE_PROCESSING_STAGE.value,
        )

        pre_processing_job = self.poll_job(worker_id, self.get_header())
        self.assertEqual(self.poll_job(worker_id, self.get_header()), 498)
        self.post_job_result(
            worker_id, self.get_header(), pre_processing_job.get("grading_job_id")
        )
        self.check_grading_run_status(
            self.course1,
            "assignment1",
            grading_run_id,
            self.client_header1,
            200,
            GradingRunState.STUDENTS_STAGE.value,
        )

        student_job = self.poll_job(worker_id, self.get_header())
        self.assertEqual(self.poll_job(worker_id, self.get_header()), 498)
        self.post_job_result(
            worker_id, self.get_header(), student_job.get("grading_job_id")
        )
        self.check_grading_run_status(
            self.course1,
            "assignment1",
            grading_run_id,
            self.client_header1,
            200,
            GradingRunState.POST_PROCESSING_STAGE.value,
        )

        post_processing_job = self.poll_job(worker_id, self.get_header())
        self.assertEqual(self.poll_job(worker_id, self.get_header()), 498)
        self.post_job_result(
            worker_id,
            self.get_header(),
            post_processing_job.get("grading_job_id"),
            False,
        )
        self.check_grading_run_status(
            self.course1,
            "assignment1",
            grading_run_id,
            self.client_header1,
            200,
            GradingRunState.FAILED.value,
        )
        self.assertEqual(self.poll_job(worker_id, self.get_header()), 498)
