import src.constants.keys as key
import tests.dummy_grading_configs as dummy_configs
import tests.dummy_grading_runs as dummy_runs
from src.config import BAD_REQUEST_CODE, OK_REQUEST_CODE, QUEUE_EMPTY_CODE
from src.constants.constants import GradingRunState

from tests.base import BaseTest


class TestIntegration(BaseTest):

    def test_grading_run_ownership(self):
        # courses can not check AG run status of other courses
        worker_id = self.register_worker(self.grader_header)
        self.upload_grading_config(self.course1, "assignment1", self.client_header1, dummy_configs.only_student_config,
                                   OK_REQUEST_CODE)
        grading_run_id = self.start_grading_run(self.course1, "assignment1", self.client_header1,
                                                dummy_runs.one_student_job, OK_REQUEST_CODE)

        self.check_grading_run_status(self.course2, "assignment1", grading_run_id, self.client_header2,
                                      BAD_REQUEST_CODE)
        job = self.poll_job(worker_id, self.grader_header)
        self.post_job_result(worker_id, self.grader_header, job.get(key.GRADING_JOB_ID))
        self.poll_job(worker_id, self.grader_header, QUEUE_EMPTY_CODE)

    def test_single_student_job(self):
        worker_id = self.register_worker(self.grader_header)
        self.upload_grading_config(self.course1, "assignment1", self.client_header1, dummy_configs.only_student_config,
                                   OK_REQUEST_CODE)
        grading_run_id = self.start_grading_run(self.course1, "assignment1", self.client_header1,
                                                dummy_runs.one_student_job, OK_REQUEST_CODE)

        self.check_grading_run_status(self.course1, "assignment1", grading_run_id, self.client_header1,
                                      OK_REQUEST_CODE, GradingRunState.STUDENTS_STAGE.value)

        job = self.poll_job(worker_id, self.grader_header)
        self.check_grading_run_status(self.course1, "assignment1", grading_run_id, self.client_header1,
                                      OK_REQUEST_CODE, GradingRunState.STUDENTS_STAGE.value)
        self.post_job_result(worker_id, self.grader_header, job.get(key.GRADING_JOB_ID))
        self.check_grading_run_status(self.course1, "assignment1", grading_run_id, self.client_header1,
                                      OK_REQUEST_CODE, GradingRunState.FINISHED.value)
        self.poll_job(worker_id, self.grader_header, QUEUE_EMPTY_CODE)
