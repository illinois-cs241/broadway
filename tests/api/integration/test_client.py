import logging
from collections import deque

import tests.api._fixtures.grading_configs as grading_configs
import tests.api._fixtures.grading_runs as grading_runs

from tests.api.base import BaseTest

logging.disable(logging.WARNING)


class GradingConfigEndpointsTest(BaseTest):
    def test_no_token(self):
        self.upload_grading_config(
            self.course1, "assignment1", None, grading_configs.valid_configs[0], 401
        )
        self.get_grading_config(self.course1, "assignment1", None, 401)

    def test_wrong_token(self):
        # course 1 can only be authenticated with client header 1,
        # course 2 can be authenticated with either
        self.upload_grading_config(
            self.course1,
            "assignment1",
            self.client_header2,
            grading_configs.valid_configs[0],
            401,
        )
        self.get_grading_config(self.course1, "assignment1", self.client_header2, 401)

    def test_invalid_course_id(self):
        self.upload_grading_config(
            "wrong_id",
            "assignment1",
            self.client_header1,
            grading_configs.valid_configs[0],
            401,
        )
        self.get_grading_config("wrong_id", "assignment1", self.client_header1, 401)

    def test_update_old_config(self):
        self.get_grading_config(self.course1, "assignment1", self.client_header1, 400)
        self.upload_grading_config(
            self.course1,
            "assignment1",
            self.client_header1,
            grading_configs.valid_configs[0],
            200,
        )
        self.assert_equal_grading_config(
            self.get_grading_config(
                self.course1, "assignment1", self.client_header1, 200
            ),
            grading_configs.valid_configs[0],
        )

        self.upload_grading_config(
            self.course1,
            "assignment1",
            self.client_header1,
            grading_configs.valid_configs[1],
            200,
        )
        self.assert_equal_grading_config(
            self.get_grading_config(
                self.course1, "assignment1", self.client_header1, 200
            ),
            grading_configs.valid_configs[1],
        )

    def test_same_assignment_name(self):
        self.upload_grading_config(
            self.course1,
            "assignment1",
            self.client_header1,
            grading_configs.valid_configs[0],
            200,
        )
        self.assert_equal_grading_config(
            self.get_grading_config(
                self.course1, "assignment1", self.client_header1, 200
            ),
            grading_configs.valid_configs[0],
        )

        self.upload_grading_config(
            self.course2,
            "assignment1",
            self.client_header2,
            grading_configs.valid_configs[1],
            200,
        )
        self.assert_equal_grading_config(
            self.get_grading_config(
                self.course2, "assignment1", self.client_header2, 200
            ),
            grading_configs.valid_configs[1],
        )

    def test_multiple_tokens(self):
        # a course can use multiple tokens
        self.upload_grading_config(
            self.course2,
            "assignment1",
            self.client_header1,
            grading_configs.valid_configs[0],
            200,
        )
        self.assert_equal_grading_config(
            self.get_grading_config(
                self.course2, "assignment1", self.client_header1, 200
            ),
            grading_configs.valid_configs[0],
        )

        self.upload_grading_config(
            self.course2,
            "assignment2",
            self.client_header2,
            grading_configs.valid_configs[1],
            200,
        )
        self.assert_equal_grading_config(
            self.get_grading_config(
                self.course2, "assignment2", self.client_header2, 200
            ),
            grading_configs.valid_configs[1],
        )

    def test_shared_token(self):
        # the same token can be used by multiple courses
        self.upload_grading_config(
            self.course1,
            "assignment1",
            self.client_header1,
            grading_configs.valid_configs[0],
            200,
        )
        self.assert_equal_grading_config(
            self.get_grading_config(
                self.course1, "assignment1", self.client_header1, 200
            ),
            grading_configs.valid_configs[0],
        )

        self.upload_grading_config(
            self.course2,
            "assignment2",
            self.client_header1,
            grading_configs.valid_configs[1],
            200,
        )
        self.assert_equal_grading_config(
            self.get_grading_config(
                self.course2, "assignment2", self.client_header1, 200
            ),
            grading_configs.valid_configs[1],
        )

    def test_valid_configs(self):
        for idx, valid_config in enumerate(grading_configs.valid_configs):
            self.upload_grading_config(
                self.course1, str(idx), self.client_header1, valid_config, 200
            )
            self.assert_equal_grading_config(
                self.get_grading_config(
                    self.course1, str(idx), self.client_header1, 200
                ),
                valid_config,
            )

    def test_invalid_configs(self):
        for idx, invalid_config in enumerate(grading_configs.invalid_configs):
            self.upload_grading_config(
                self.course1, str(idx), self.client_header1, invalid_config, 400
            )
            self.get_grading_config(self.course1, str(idx), self.client_header1, 400)


class GradingRunEndpointsTest(BaseTest):
    def test_no_token(self):
        self.start_grading_run(
            self.course1, "assignment1", None, grading_runs.one_student_job, 401
        )
        self.check_grading_run_status(self.course1, "temp", None, 401)

    def test_wrong_token(self):
        # course 1 can only be authenticated with client header 1
        # course 2 can be authenticated with either
        self.start_grading_run(
            self.course1,
            "assignment1",
            self.client_header2,
            grading_runs.one_student_job,
            401,
        )
        self.check_grading_run_status(self.course1, "temp", self.client_header2, 401)

    def test_invalid_course_id(self):
        self.start_grading_run(
            "wrong_id",
            "assignment1",
            self.client_header1,
            grading_runs.one_student_job,
            401,
        )
        self.check_grading_run_status("wrong_id", "temp", self.client_header1, 401)

    def test_invalid_assignment_id(self):
        self.start_grading_run(
            self.course1,
            "assignment1",
            self.client_header1,
            grading_runs.one_student_job,
            400,
        )

    def test_invalid_run_id(self):
        self.upload_grading_config(
            self.course1,
            "assignment1",
            self.client_header1,
            grading_configs.valid_configs[0],
            200,
        )
        self.check_grading_run_status(self.course1, "temp", self.client_header1, 400)

    def test_invalid_run(self):
        # the config only has student env vars defined.
        # pre processing or post processing env vars are invalid
        self.upload_grading_config(
            self.course1,
            "assignment1",
            self.client_header1,
            grading_configs.only_student_config,
            200,
        )

        self.start_grading_run(
            self.course1,
            "assignment1",
            self.client_header1,
            grading_runs.one_student_and_pre,
            400,
        )
        self.start_grading_run(
            self.course1,
            "assignment1",
            self.client_header1,
            grading_runs.one_student_and_post,
            400,
        )
        self.start_grading_run(
            self.course1,
            "assignment1",
            self.client_header1,
            grading_runs.one_student_and_both,
            400,
        )

    def test_run_course_private(self):
        self.upload_grading_config(
            self.course1,
            "assignment1",
            self.client_header1,
            grading_configs.only_student_config,
            200,
        )
        self.start_grading_run(
            self.course2,
            "assignment1",
            self.client_header2,
            grading_runs.one_student_job,
            400,
        )


class GradingJobLogEndpointTest(BaseTest):
    def test_no_token(self):
        self.get_grading_job_log(self.course1, "weird", None, 401)

    def test_wrong_token(self):
        # course 1 can only be authenticated with client header 1,
        # course 2 can be authenticated with either
        self.get_grading_job_log(self.course1, "weird", self.client_header2, 401)

    def test_invalid_job_id(self):
        self.get_grading_job_log(self.course1, "weird", self.client_header1, 400)


class CourseWorkerNodeEndpointTest(BaseTest):
    def test_no_token(self):
        self.get_course_worker_nodes(self.course1, "all", None, 401)

    def test_wrong_token(self):
        # course 1 can only be authenticated with client header 1,
        # course 2 can be authenticated with either
        self.get_course_worker_nodes(self.course1, "all", self.client_header2, 401)

    def test_invalid_course_id(self):
        self.get_course_worker_nodes("wrong_id", "all", self.client_header1, 401)

    def test_invalid_scope(self):
        self.get_course_worker_nodes(
            self.course1, "invalid_scope", self.client_header1, 404
        )


class CourseQueueLengthEndpointTest(BaseTest):
    def assertLengthEquals(self, course_id, header, expected_len):
        length = self.get_course_queue_length(course_id, header, 200)["length"]
        self.assertEqual(expected_len, length)

    def test_single_course(self):
        num_students = 10

        # Upload the jobs
        self.upload_grading_config(
            self.course1,
            "assignment1",
            self.client_header1,
            grading_configs.only_student_config,
            200,
        )
        self.start_grading_run(
            self.course1,
            "assignment1",
            self.client_header1,
            grading_runs.generate_n_student_jobs(num_students),
            200,
        )

        self.assertLengthEquals(self.course1, self.client_header1, num_students)

        # Now, run those jobs
        worker_id = self.register_worker(self.get_header())
        for i in range(num_students):
            self.poll_job(worker_id, self.get_header())

        # No more jobs should be left
        self.assertLengthEquals(self.course1, self.client_header1, 0)

    def test_multiple_courses(self):
        num_students1 = 32
        num_students2 = 56

        # Upload the jobs
        self.upload_grading_config(
            self.course1,
            "assignment1",
            self.client_header1,
            grading_configs.only_student_config,
            200,
        )
        self.upload_grading_config(
            self.course2,
            "assignment2",
            self.client_header2,
            grading_configs.only_student_config,
            200,
        )
        self.start_grading_run(
            self.course1,
            "assignment1",
            self.client_header1,
            grading_runs.generate_n_student_jobs(num_students1),
            200,
        )
        self.start_grading_run(
            self.course2,
            "assignment2",
            self.client_header2,
            grading_runs.generate_n_student_jobs(num_students2),
            200,
        )

        self.assertLengthEquals(self.course1, self.client_header1, num_students1)
        self.assertLengthEquals(self.course2, self.client_header2, num_students2)

        # Now, run the jobs
        worker_id = self.register_worker(self.get_header())
        for i in range(num_students1 + num_students2):
            self.poll_job(worker_id, self.get_header())

        # No more jobs should be left.
        self.assertLengthEquals(self.course1, self.client_header1, 0)
        self.assertLengthEquals(self.course2, self.client_header2, 0)

    def test_invalid_course(self):
        self.upload_grading_config(
            self.course1,
            "assignment1",
            self.client_header1,
            grading_configs.only_student_config,
            200,
        )
        self.upload_grading_config(
            self.course2,
            "assignment2",
            self.client_header2,
            grading_configs.only_student_config,
            200,
        )

        # No jobs should have been pushed to the queue yet,
        # since we didn't start any grading runs.
        self.assertLengthEquals(self.course1, self.client_header1, 0)
        self.assertLengthEquals(self.course2, self.client_header2, 0)


class GradingJobQueuePositionEndpointTest(BaseTest):
    def assert_position_equals(self, course_id, grading_job_id, header, expected_pos):
        pos = self.get_grading_job_queue_position(
            course_id, grading_job_id, header, 200
        )["position"]
        self.assertEqual(expected_pos, pos)

    def test_single_job(self):

        # Upload the jobs
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

        # There should only be one job in the run
        job_id = list(run_state["student_jobs_state"].keys())[0]

        # There should be 0 jobs ahead of this run in the queue
        self.assert_position_equals(self.course1, job_id, self.client_header1, 0)

        # Now, run the job
        worker_id = self.register_worker(self.get_header())
        self.poll_job(worker_id, self.get_header())

        # The job should no longer be in the queue
        self.get_grading_job_queue_position(
            self.course1, job_id, self.client_header1, 400
        )

    def test_multiple_jobs(self):
        num_jobs = 10

        # Upload the jobs
        self.upload_grading_config(
            self.course1,
            "assignment1",
            self.client_header1,
            grading_configs.only_student_config,
            200,
        )

        job_ids = []
        for _ in range(num_jobs):
            grading_run_id = self.start_grading_run(
                self.course1,
                "assignment1",
                self.client_header1,
                grading_runs.one_student_job,
                200,
            )

            # Keep track of the job ids
            run_state = self.get_grading_run_state(
                self.course1, grading_run_id, self.client_header1
            )
            job_ids.append(list(run_state["student_jobs_state"].keys())[0])

        for ind, job_id in enumerate(job_ids):
            self.assert_position_equals(self.course1, job_id, self.client_header1, ind)

        # Now, run the job
        worker_id = self.register_worker(self.get_header())
        for starting_ind, job_id in enumerate(job_ids):
            # Run the job
            self.poll_job(worker_id, self.get_header())
            # Make sure the rest of the jobs have gone down 1 in position
            for expected_pos, waiting_job in enumerate(job_ids[starting_ind + 1 :]):
                self.assert_position_equals(
                    self.course1, waiting_job, self.client_header1, expected_pos
                )

        # The jobs should no longer be in the queue
        for job_id in job_ids:
            self.get_grading_job_queue_position(
                self.course1, job_id, self.client_header1, 400
            )


class StreamEndpointTest(BaseTest):
    def test_stream(self):
        num_jobs = 5

        # Upload the jobs
        self.upload_grading_config(
            self.course1,
            "assignment1",
            self.client_header1,
            grading_configs.only_student_config,
            200,
        )

        job_ids = []
        for _ in range(num_jobs):
            grading_run_id = self.start_grading_run(
                self.course1,
                "assignment1",
                self.client_header1,
                grading_runs.one_student_job,
                200,
            )

            # Keep track of the job ids
            run_state = self.get_grading_run_state(
                self.course1, grading_run_id, self.client_header1
            )
            job_ids.append(list(run_state["student_jobs_state"].keys())[0])

        for ind, job_id in enumerate(job_ids):

            def _create_callback(chunks):
                def _callback(chunk):
                    self.assertNotEquals(len(chunks), 0)
                    self.assertEquals(chunk, chunks.pop())

                return _callback

            chunks = deque()
            chunks.append(b"event: statedata: FINISHED\n\n")
            chunks.append(b"event: statedata: STARTED\n\n")
            for pos in range(ind):
                chunks.append(f"event: positiondata: {pos}\n\n".encode())

            self.get_grading_job_stream(
                self.course1, job_id, self.client_header1, _create_callback(chunks)
            )

        worker_id = self.register_worker(self.get_header())
        for job_id in job_ids:
            # Run and post the job
            self.poll_job(worker_id, self.get_header())
            self.post_job_result(worker_id, self.get_header(), job_id)
