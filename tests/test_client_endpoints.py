import tests.dummy_grading_configs as dummy_configs
from src.config import BAD_REQUEST_CODE, UNAUTHORIZED_REQUEST_CODE, OK_REQUEST_CODE
from tests.base import BaseTest


class TestGradingConfig(BaseTest):
    def test_unauthorized(self):
        self.upload_grading_config(self.course1, "assignment1", None, dummy_configs.valid_configs[0], BAD_REQUEST_CODE)
        self.get_grading_config(self.course1, "assignment1", None, BAD_REQUEST_CODE)

    def test_wrong_token(self):
        # course 1 can only be authenticated with client header 1, course 2 can be authenticated with either
        self.upload_grading_config(self.course1, "assignment1", self.client_header2, dummy_configs.valid_configs[0],
                                   UNAUTHORIZED_REQUEST_CODE)
        self.get_grading_config(self.course1, "assignment1", self.client_header2, UNAUTHORIZED_REQUEST_CODE)

    def test_invalid_course_id(self):
        self.upload_grading_config("wrong_id", "assignment1", self.client_header1, dummy_configs.valid_configs[0],
                                   BAD_REQUEST_CODE)
        self.get_grading_config("wrong_id", "assignment1", self.client_header1, BAD_REQUEST_CODE)

    def test_update_old_config(self):
        self.get_grading_config(self.course1, "assignment1", self.client_header1, BAD_REQUEST_CODE)
        self.upload_grading_config(self.course1, "assignment1", self.client_header1, dummy_configs.valid_configs[0],
                                   OK_REQUEST_CODE)
        self.assert_equal_grading_config(
            self.get_grading_config(self.course1, "assignment1", self.client_header1, OK_REQUEST_CODE),
            dummy_configs.valid_configs[0])

        self.upload_grading_config(self.course1, "assignment1", self.client_header1, dummy_configs.valid_configs[1],
                                   OK_REQUEST_CODE)
        self.assert_equal_grading_config(
            self.get_grading_config(self.course1, "assignment1", self.client_header1, OK_REQUEST_CODE),
            dummy_configs.valid_configs[1])

    def test_same_assignment_name(self):
        self.upload_grading_config(self.course1, "assignment1", self.client_header1, dummy_configs.valid_configs[0],
                                   OK_REQUEST_CODE)
        self.assert_equal_grading_config(
            self.get_grading_config(self.course1, "assignment1", self.client_header1, OK_REQUEST_CODE),
            dummy_configs.valid_configs[0])

        self.upload_grading_config(self.course2, "assignment1", self.client_header2, dummy_configs.valid_configs[1],
                                   OK_REQUEST_CODE)
        self.assert_equal_grading_config(
            self.get_grading_config(self.course2, "assignment1", self.client_header2, OK_REQUEST_CODE),
            dummy_configs.valid_configs[1])

    def test_multiple_tokens(self):
        # a course can use multiple tokens
        self.upload_grading_config(self.course2, "assignment1", self.client_header1, dummy_configs.valid_configs[0],
                                   OK_REQUEST_CODE)
        self.assert_equal_grading_config(
            self.get_grading_config(self.course2, "assignment1", self.client_header1, OK_REQUEST_CODE),
            dummy_configs.valid_configs[0])

        self.upload_grading_config(self.course2, "assignment2", self.client_header2, dummy_configs.valid_configs[1],
                                   OK_REQUEST_CODE)
        self.assert_equal_grading_config(
            self.get_grading_config(self.course2, "assignment2", self.client_header2, OK_REQUEST_CODE),
            dummy_configs.valid_configs[1])

    def test_shared_token(self):
        # the same token can be used by multiple courses
        self.upload_grading_config(self.course1, "assignment1", self.client_header1, dummy_configs.valid_configs[0],
                                   OK_REQUEST_CODE)
        self.assert_equal_grading_config(
            self.get_grading_config(self.course1, "assignment1", self.client_header1, OK_REQUEST_CODE),
            dummy_configs.valid_configs[0])

        self.upload_grading_config(self.course2, "assignment2", self.client_header1, dummy_configs.valid_configs[1],
                                   OK_REQUEST_CODE)
        self.assert_equal_grading_config(
            self.get_grading_config(self.course2, "assignment2", self.client_header1, OK_REQUEST_CODE),
            dummy_configs.valid_configs[1])

    def test_valid_configs(self):
        for idx, valid_config in enumerate(dummy_configs.valid_configs):
            self.upload_grading_config(self.course1, str(idx), self.client_header1, valid_config, OK_REQUEST_CODE)
            self.assert_equal_grading_config(
                self.get_grading_config(self.course1, str(idx), self.client_header1, OK_REQUEST_CODE), valid_config)

    def test_invalid_configs(self):
        for idx, invalid_config in enumerate(dummy_configs.invalid_configs):
            self.upload_grading_config(self.course1, str(idx), self.client_header1, invalid_config, 400)
            self.get_grading_config(self.course1, str(idx), self.client_header1, BAD_REQUEST_CODE)
