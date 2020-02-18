import logging
import json

from broadway.api.models import GradingRunState, GradingJobState

import tests.api._fixtures.grading_configs as grading_configs
import tests.api._fixtures.grading_runs as grading_runs
from tests.api.base import BaseTest
from tests.api._utils.asyncio import to_sync

logging.disable(logging.WARNING)


# get status of the first job
def get_first_status(grading_job_map):
    return list(grading_job_map.values())[0]


class EndpointIntegrationTest(BaseTest):
    def test_course_worker_nodes(self):
        num_workers = 10
        num_jobs = num_workers // 2
        worker_node_ids = [
            self.register_worker(self.get_header()) for _ in range(num_workers)
        ]

        # all worker nodes should be free
        all_course_worker_nodes = self.get_course_worker_nodes(
            self.course1, "all", self.client_header1, 200
        )
        self.assertEqual(len(all_course_worker_nodes["worker_nodes"]), num_workers)
        for worker_node in all_course_worker_nodes["worker_nodes"]:
            self.assertTrue(worker_node["alive"])
            self.assertFalse(worker_node["busy"])
            self.assertEqual(worker_node["jobs_processed"], 0)

        # create some jobs
        self.upload_grading_config(
            self.course1,
            "assignment1",
            self.client_header1,
            grading_configs.only_student_config,
            200,
        )

        for _ in range(num_jobs):
            self.start_grading_run(
                self.course1,
                "assignment1",
                self.client_header1,
                grading_runs.one_student_job,
                200,
            )

        # workers poll those jobs
        jobs = [
            self.poll_job(worker_node_ids[i], self.get_header())
            for i in range(num_jobs)
        ]
        all_course_worker_nodes = self.get_course_worker_nodes(
            self.course1, "all", self.client_header1, 200
        )
        self.assertEqual(len(all_course_worker_nodes["worker_nodes"]), num_workers)

        # those workers should be busy
        num_workers_busy = 0
        for worker_node in all_course_worker_nodes["worker_nodes"]:
            self.assertTrue(worker_node["alive"])
            if worker_node["busy"]:
                num_workers_busy += 1
        self.assertEqual(num_workers_busy, num_jobs)

        # workers finish working on the jobs
        for i in range(num_jobs):
            self.post_job_result(
                worker_node_ids[i], self.get_header(), jobs[i].get("grading_job_id")
            )

        # all worker nodes should be free again
        all_course_worker_nodes = self.get_course_worker_nodes(
            self.course1, "all", self.client_header1, 200
        )

        num_jobs_processed = 0
        self.assertEqual(len(all_course_worker_nodes["worker_nodes"]), num_workers)
        for worker_node in all_course_worker_nodes["worker_nodes"]:
            self.assertTrue(worker_node["alive"])
            self.assertFalse(worker_node["busy"])
            num_jobs_processed += worker_node["jobs_processed"]
        self.assertEqual(num_jobs_processed, num_jobs)

    def test_job_logs(self):
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

        run_state = self.get_grading_run_state(
            self.course1, grading_run_id, self.client_header1
        )

        # job logs should not exist at this point
        for job_id in run_state["student_jobs_state"]:
            self.get_grading_job_log(self.course1, job_id, self.client_header1, 400)

        student_job = self.poll_job(worker_id, self.get_header())
        run_state = self.get_grading_run_state(
            self.course1, grading_run_id, self.client_header1
        )

        # job logs should not exist at this point
        for job_id in run_state["student_jobs_state"]:
            self.get_grading_job_log(self.course1, job_id, self.client_header1, 400)

        self.post_job_result(
            worker_id, self.get_header(), student_job.get("grading_job_id")
        )
        run_state = self.get_grading_run_state(
            self.course1, grading_run_id, self.client_header1
        )

        # logs should have been generated now
        for job_id in run_state["student_jobs_state"]:
            self.get_grading_job_log(self.course1, job_id, self.client_header1, 200)

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
            grading_run_id,
            self.client_header1,
            200,
            GradingRunState.STUDENTS_STAGE.value,
        )
        run_state = self.get_grading_run_state(
            self.course1, grading_run_id, self.client_header1
        )
        self.assertEqual(
            get_first_status(run_state["student_jobs_state"]),
            GradingJobState.QUEUED.value,
        )

        student_job = self.poll_job(worker_id, self.get_header())
        self.check_grading_run_status(
            self.course1,
            grading_run_id,
            self.client_header1,
            200,
            GradingRunState.STUDENTS_STAGE.value,
        )
        self.assertEqual(self.poll_job(worker_id, self.get_header()), 498)
        run_state = self.get_grading_run_state(
            self.course1, grading_run_id, self.client_header1
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
            grading_run_id,
            self.client_header1,
            200,
            GradingRunState.FINISHED.value,
        )
        self.assertEqual(self.poll_job(worker_id, self.get_header()), 498)
        run_state = self.get_grading_run_state(
            self.course1, grading_run_id, self.client_header1
        )

        self.assertEqual(
            get_first_status(run_state["student_jobs_state"]),
            GradingJobState.SUCCEEDED.value,
        )

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
            grading_run_id,
            self.client_header1,
            200,
            GradingRunState.PRE_PROCESSING_STAGE.value,
        )
        run_state = self.get_grading_run_state(
            self.course1, grading_run_id, self.client_header1
        )

        self.assertEqual(
            get_first_status(run_state["pre_processing_job_state"]),
            GradingJobState.QUEUED.value,
        )

        pre_processing_job = self.poll_job(worker_id, self.get_header())
        self.check_grading_run_status(
            self.course1,
            grading_run_id,
            self.client_header1,
            200,
            GradingRunState.PRE_PROCESSING_STAGE.value,
        )
        self.assertEqual(self.poll_job(worker_id, self.get_header()), 498)
        run_state = self.get_grading_run_state(
            self.course1, grading_run_id, self.client_header1
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
            grading_run_id,
            self.client_header1,
            200,
            GradingRunState.STUDENTS_STAGE.value,
        )
        student_job = self.poll_job(worker_id, self.get_header())
        self.check_grading_run_status(
            self.course1,
            grading_run_id,
            self.client_header1,
            200,
            GradingRunState.STUDENTS_STAGE.value,
        )
        self.assertEqual(self.poll_job(worker_id, self.get_header()), 498)
        run_state = self.get_grading_run_state(
            self.course1, grading_run_id, self.client_header1
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
            grading_run_id,
            self.client_header1,
            200,
            GradingRunState.FINISHED.value,
        )
        self.assertEqual(self.poll_job(worker_id, self.get_header()), 498)
        run_state = self.get_grading_run_state(
            self.course1, grading_run_id, self.client_header1
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
            grading_run_id,
            self.client_header1,
            200,
            GradingRunState.STUDENTS_STAGE.value,
        )
        run_state = self.get_grading_run_state(
            self.course1, grading_run_id, self.client_header1
        )

        self.assertEqual(
            get_first_status(run_state["student_jobs_state"]),
            GradingJobState.QUEUED.value,
        )

        student_job = self.poll_job(worker_id, self.get_header())
        self.check_grading_run_status(
            self.course1,
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
            grading_run_id,
            self.client_header1,
            200,
            GradingRunState.POST_PROCESSING_STAGE.value,
        )
        run_state = self.get_grading_run_state(
            self.course1, grading_run_id, self.client_header1
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
            grading_run_id,
            self.client_header1,
            200,
            GradingRunState.POST_PROCESSING_STAGE.value,
        )

        self.assertEqual(self.poll_job(worker_id, self.get_header()), 498)
        run_state = self.get_grading_run_state(
            self.course1, grading_run_id, self.client_header1
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
            grading_run_id,
            self.client_header1,
            200,
            GradingRunState.FINISHED.value,
        )
        self.assertEqual(self.poll_job(worker_id, self.get_header()), 498)
        run_state = self.get_grading_run_state(
            self.course1, grading_run_id, self.client_header1
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
            grading_run_id,
            self.client_header1,
            200,
            GradingRunState.PRE_PROCESSING_STAGE.value,
        )
        run_state = self.get_grading_run_state(
            self.course1, grading_run_id, self.client_header1
        )
        self.assertEqual(
            get_first_status(run_state["pre_processing_job_state"]),
            GradingJobState.QUEUED.value,
        )

        pre_processing_job = self.poll_job(worker_id, self.get_header())
        self.check_grading_run_status(
            self.course1,
            grading_run_id,
            self.client_header1,
            200,
            GradingRunState.PRE_PROCESSING_STAGE.value,
        )
        self.assertEqual(self.poll_job(worker_id, self.get_header()), 498)
        run_state = self.get_grading_run_state(
            self.course1, grading_run_id, self.client_header1
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
            grading_run_id,
            self.client_header1,
            200,
            GradingRunState.STUDENTS_STAGE.value,
        )
        run_state = self.get_grading_run_state(
            self.course1, grading_run_id, self.client_header1
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
            grading_run_id,
            self.client_header1,
            200,
            GradingRunState.STUDENTS_STAGE.value,
        )
        self.assertEqual(self.poll_job(worker_id, self.get_header()), 498)
        run_state = self.get_grading_run_state(
            self.course1, grading_run_id, self.client_header1
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
            grading_run_id,
            self.client_header1,
            200,
            GradingRunState.POST_PROCESSING_STAGE.value,
        )
        run_state = self.get_grading_run_state(
            self.course1, grading_run_id, self.client_header1
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
            grading_run_id,
            self.client_header1,
            200,
            GradingRunState.POST_PROCESSING_STAGE.value,
        )
        self.assertEqual(self.poll_job(worker_id, self.get_header()), 498)
        run_state = self.get_grading_run_state(
            self.course1, grading_run_id, self.client_header1
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
            grading_run_id,
            self.client_header1,
            200,
            GradingRunState.FINISHED.value,
        )
        self.assertEqual(self.poll_job(worker_id, self.get_header()), 498)
        run_state = self.get_grading_run_state(
            self.course1, grading_run_id, self.client_header1
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
            grading_run_id,
            self.client_header1,
            200,
            GradingRunState.PRE_PROCESSING_STAGE.value,
        )
        pre_processing_job = self.poll_job(worker_id, self.get_header())
        pre_processing_job_id = pre_processing_job["grading_job_id"]
        self.check_grading_run_status(
            self.course1,
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
                stage["env"].update(
                    {
                        "type": "pre",
                        "GRADING_RUN_ID": grading_run_id,
                        "GRADING_JOB_ID": pre_processing_job_id,
                    }
                )
        self.assert_equal_grading_pipeline(
            pre_processing_job.get("stages"), pre_processing_pipeline
        )

        self.post_job_result(
            worker_id, self.get_header(), pre_processing_job.get("grading_job_id")
        )
        self.check_grading_run_status(
            self.course1,
            grading_run_id,
            self.client_header1,
            200,
            GradingRunState.STUDENTS_STAGE.value,
        )

        # student jobs
        for ind in range(num_students):
            worker_id = self.register_worker(self.get_header())
            student_job = self.poll_job(worker_id, self.get_header())
            student_job_id = student_job["grading_job_id"]

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
                            "GRADING_JOB_ID": student_job_id,
                        }
                    )
            self.assert_equal_grading_pipeline(
                student_job.get("stages"), student_pipeline
            )

            self.check_grading_run_status(
                self.course1,
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
            grading_run_id,
            self.client_header1,
            200,
            GradingRunState.POST_PROCESSING_STAGE.value,
        )
        post_processing_job = self.poll_job(worker_id, self.get_header())
        post_processing_job_id = post_processing_job["grading_job_id"]

        self.assertEqual(self.poll_job(worker_id, self.get_header()), 498)

        post_processing_pipeline = grading_configs.complete_config.get(
            "post_processing_pipeline"
        ).copy()
        for stage in post_processing_pipeline:
            if "env" in stage:
                stage["env"].update(grading_configs.complete_config.get("env", {}))
                stage["env"].update(
                    {
                        "type": "post",
                        "GRADING_RUN_ID": grading_run_id,
                        "GRADING_JOB_ID": post_processing_job_id,
                    }
                )
        self.assert_equal_grading_pipeline(
            post_processing_job.get("stages"), post_processing_pipeline
        )

        self.check_grading_run_status(
            self.course1,
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
            grading_run_id,
            self.client_header1,
            200,
            GradingRunState.FAILED.value,
        )
        self.assertEqual(self.poll_job(worker_id, self.get_header()), 498)


class WSEndpointIntegrationTest(BaseTest):
    # basic sanity check
    def test_single_student_job_ws(self):
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
            grading_run_id,
            self.client_header1,
            200,
            GradingRunState.STUDENTS_STAGE.value,
        )

        run_state = self.get_grading_run_state(
            self.course1, grading_run_id, self.client_header1
        )

        self.assertEqual(
            get_first_status(run_state["student_jobs_state"]),
            GradingJobState.QUEUED.value,
        )

        conn = to_sync(self.worker_ws("test_worker", self.get_header()))

        student_job = json.loads(to_sync(conn.recv()))

        self.check_grading_run_status(
            self.course1,
            grading_run_id,
            self.client_header1,
            200,
            GradingRunState.STUDENTS_STAGE.value,
        )

        run_state = self.get_grading_run_state(
            self.course1, grading_run_id, self.client_header1
        )

        self.assertEqual(
            get_first_status(run_state["student_jobs_state"]),
            GradingJobState.STARTED.value,
        )

        to_sync(
            self.worker_ws_conn_reulst(conn, student_job.get("grading_job_id"), True)
        )

        self.check_grading_run_status(
            self.course1,
            grading_run_id,
            self.client_header1,
            200,
            GradingRunState.FINISHED.value,
        )

        run_state = self.get_grading_run_state(
            self.course1, grading_run_id, self.client_header1
        )

        self.assertEqual(
            get_first_status(run_state["student_jobs_state"]),
            GradingJobState.SUCCEEDED.value,
        )

        to_sync(conn.close())

    def test_worker_fail_midway(self):
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
            grading_run_id,
            self.client_header1,
            200,
            GradingRunState.STUDENTS_STAGE.value,
        )

        run_state = self.get_grading_run_state(
            self.course1, grading_run_id, self.client_header1
        )

        self.assertEqual(
            get_first_status(run_state["student_jobs_state"]),
            GradingJobState.QUEUED.value,
        )

        conn = to_sync(self.worker_ws("test_worker", self.get_header()))

        to_sync(conn.recv())

        self.check_grading_run_status(
            self.course1,
            grading_run_id,
            self.client_header1,
            200,
            GradingRunState.STUDENTS_STAGE.value,
        )

        run_state = self.get_grading_run_state(
            self.course1, grading_run_id, self.client_header1
        )

        self.assertEqual(
            get_first_status(run_state["student_jobs_state"]),
            GradingJobState.STARTED.value,
        )

        to_sync(conn.close())

        self.check_grading_run_status(
            self.course1,
            grading_run_id,
            self.client_header1,
            200,
            GradingRunState.FINISHED.value,
        )

        run_state = self.get_grading_run_state(
            self.course1, grading_run_id, self.client_header1
        )

        self.assertEqual(
            get_first_status(run_state["student_jobs_state"]),
            GradingJobState.FAILED.value,
        )

    # one ws worker vs two jobs
    def test_one_worker_two_jobs(self):
        self.upload_grading_config(
            self.course1,
            "assignment1",
            self.client_header1,
            grading_configs.only_student_config,
            200,
        )

        grading_run_ids = [
            self.start_grading_run(
                self.course1,
                "assignment1",
                self.client_header1,
                grading_runs.one_student_job,
                200,
            )
            for _ in range(2)
        ]

        conn = to_sync(self.worker_ws("test_worker", self.get_header()))

        student_job = json.loads(to_sync(conn.recv()))

        to_sync(
            self.worker_ws_conn_reulst(conn, student_job.get("grading_job_id"), False)
        )

        student_job = json.loads(to_sync(conn.recv()))

        to_sync(
            self.worker_ws_conn_reulst(conn, student_job.get("grading_job_id"), True)
        )

        self.check_grading_run_status(
            self.course1,
            grading_run_ids[0],
            self.client_header1,
            200,
            GradingRunState.FINISHED.value,
        )

        self.check_grading_run_status(
            self.course1,
            grading_run_ids[1],
            self.client_header1,
            200,
            GradingRunState.FINISHED.value,
        )

        run_state = self.get_grading_run_state(
            self.course1, grading_run_ids[0], self.client_header1
        )

        self.assertEqual(
            get_first_status(run_state["student_jobs_state"]),
            GradingJobState.FAILED.value,
        )

        run_state = self.get_grading_run_state(
            self.course1, grading_run_ids[1], self.client_header1
        )

        self.assertEqual(
            get_first_status(run_state["student_jobs_state"]),
            GradingJobState.SUCCEEDED.value,
        )

        to_sync(conn.close())

    # each worker should get one job only
    def test_two_workers_two_jobs(self):
        self.upload_grading_config(
            self.course1,
            "assignment1",
            self.client_header1,
            grading_configs.only_student_config,
            200,
        )

        grading_run_ids = [
            self.start_grading_run(
                self.course1,
                "assignment1",
                self.client_header1,
                grading_runs.one_student_job,
                200,
            )
            for _ in range(2)
        ]

        conn1 = to_sync(self.worker_ws("test_worker1", self.get_header()))
        conn2 = to_sync(self.worker_ws("test_worker2", self.get_header()))

        student_job = json.loads(to_sync(conn1.recv()))

        to_sync(
            self.worker_ws_conn_reulst(conn1, student_job.get("grading_job_id"), True)
        )

        student_job = json.loads(to_sync(conn2.recv()))

        to_sync(
            self.worker_ws_conn_reulst(conn2, student_job.get("grading_job_id"), True)
        )

        self.check_grading_run_status(
            self.course1,
            grading_run_ids[0],
            self.client_header1,
            200,
            GradingRunState.FINISHED.value,
        )

        self.check_grading_run_status(
            self.course1,
            grading_run_ids[1],
            self.client_header1,
            200,
            GradingRunState.FINISHED.value,
        )

        run_state = self.get_grading_run_state(
            self.course1, grading_run_ids[0], self.client_header1
        )

        self.assertEqual(
            get_first_status(run_state["student_jobs_state"]),
            GradingJobState.SUCCEEDED.value,
        )

        run_state = self.get_grading_run_state(
            self.course1, grading_run_ids[1], self.client_header1
        )

        self.assertEqual(
            get_first_status(run_state["student_jobs_state"]),
            GradingJobState.SUCCEEDED.value,
        )

        to_sync(conn1.close())
        to_sync(conn2.close())

    # both ws worker and normal worker
    # the first job should be actively pushed to the ws worker
    # and the second job should be queued until the normal worker
    # pulls it.
    def test_mix_worker_type(self):
        self.upload_grading_config(
            self.course1,
            "assignment1",
            self.client_header1,
            grading_configs.only_student_config,
            200,
        )

        # two grading jobs: the first one should be assigned to ws worker
        grading_run_ids = [
            self.start_grading_run(
                self.course1,
                "assignment1",
                self.client_header1,
                grading_runs.one_student_job,
                200,
            )
            for _ in range(2)
        ]

        conn = to_sync(self.worker_ws("test_worker", self.get_header()))
        worker_id = self.register_worker(self.get_header())

        student_job_1 = json.loads(to_sync(conn.recv()))
        student_job_2 = self.poll_job(worker_id, self.get_header())

        self.check_grading_run_status(
            self.course1,
            grading_run_ids[0],
            self.client_header1,
            200,
            GradingRunState.STUDENTS_STAGE.value,
        )

        self.check_grading_run_status(
            self.course1,
            grading_run_ids[1],
            self.client_header1,
            200,
            GradingRunState.STUDENTS_STAGE.value,
        )

        to_sync(
            self.worker_ws_conn_reulst(conn, student_job_1.get("grading_job_id"), False)
        )

        self.post_job_result(
            worker_id, self.get_header(), student_job_2.get("grading_job_id"), True
        )

        self.check_grading_run_status(
            self.course1,
            grading_run_ids[0],
            self.client_header1,
            200,
            GradingRunState.FINISHED.value,
        )

        self.check_grading_run_status(
            self.course1,
            grading_run_ids[1],
            self.client_header1,
            200,
            GradingRunState.FINISHED.value,
        )

        run_state = self.get_grading_run_state(
            self.course1, grading_run_ids[0], self.client_header1
        )

        self.assertEqual(
            get_first_status(run_state["student_jobs_state"]),
            GradingJobState.FAILED.value,
        )

        run_state = self.get_grading_run_state(
            self.course1, grading_run_ids[1], self.client_header1
        )

        self.assertEqual(
            get_first_status(run_state["student_jobs_state"]),
            GradingJobState.SUCCEEDED.value,
        )

        to_sync(conn.close())
