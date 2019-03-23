import logging

import tests._fixtures.grading_configs as grading_configs
import tests._fixtures.grading_runs as grading_runs

from tests.base import BaseTest

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
