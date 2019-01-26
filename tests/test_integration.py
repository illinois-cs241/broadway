import src.constants.keys as key
import tests.dummy_grading_configs as dummy_configs
import tests.dummy_grading_runs as dummy_runs
from src.config import BAD_REQUEST_CODE, OK_REQUEST_CODE, QUEUE_EMPTY_CODE
from src.constants.constants import GradingRunState, GradingJobState
from tests.base import BaseEndpointTest


class EndpointTestIntegration(BaseEndpointTest):

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
        self.assertEqual(self.poll_job(worker_id, self.grader_header), QUEUE_EMPTY_CODE)

    def test_single_student_job(self):
        worker_id = self.register_worker(self.grader_header)
        self.upload_grading_config(self.course1, "assignment1", self.client_header1, dummy_configs.only_student_config,
                                   OK_REQUEST_CODE)
        grading_run_id = self.start_grading_run(self.course1, "assignment1", self.client_header1,
                                                dummy_runs.one_student_job, OK_REQUEST_CODE)

        self.check_grading_run_status(self.course1, "assignment1", grading_run_id, self.client_header1,
                                      OK_REQUEST_CODE, GradingRunState.STUDENTS_STAGE.value)
        run_state = self.get_grading_run_state(self.course1, "assignment1", grading_run_id, self.client_header1)
        self.assertEqual(run_state[key.STUDENTS_STATE][0], GradingJobState.QUEUED.value)

        student_job = self.poll_job(worker_id, self.grader_header)
        self.assertNotIn(key.STUDENTS, student_job)
        self.check_grading_run_status(self.course1, "assignment1", grading_run_id, self.client_header1,
                                      OK_REQUEST_CODE, GradingRunState.STUDENTS_STAGE.value)
        self.assertEqual(self.poll_job(worker_id, self.grader_header), QUEUE_EMPTY_CODE)
        run_state = self.get_grading_run_state(self.course1, "assignment1", grading_run_id, self.client_header1)
        self.assertEqual(run_state[key.STUDENTS_STATE][0], GradingJobState.STARTED.value)

        self.post_job_result(worker_id, self.grader_header, student_job.get(key.GRADING_JOB_ID))
        self.check_grading_run_status(self.course1, "assignment1", grading_run_id, self.client_header1,
                                      OK_REQUEST_CODE, GradingRunState.FINISHED.value)
        self.assertEqual(self.poll_job(worker_id, self.grader_header), QUEUE_EMPTY_CODE)
        run_state = self.get_grading_run_state(self.course1, "assignment1", grading_run_id, self.client_header1)
        self.assertEqual(run_state[key.STUDENTS_STATE][0], GradingJobState.SUCCEEDED.value)

    def test_pre_processing_job(self):
        worker_id = self.register_worker(self.grader_header)
        self.upload_grading_config(self.course1, "assignment1", self.client_header1,
                                   dummy_configs.pre_processing_config, OK_REQUEST_CODE)
        grading_run_id = self.start_grading_run(self.course1, "assignment1", self.client_header1,
                                                dummy_runs.one_student_and_pre, OK_REQUEST_CODE)

        self.check_grading_run_status(self.course1, "assignment1", grading_run_id, self.client_header1,
                                      OK_REQUEST_CODE, GradingRunState.PRE_PROCESSING_STAGE.value)
        run_state = self.get_grading_run_state(self.course1, "assignment1", grading_run_id, self.client_header1)
        self.assertEqual(run_state[key.PRE_PROCESSING_STATE], GradingJobState.QUEUED.value)
        pre_processing_job = self.poll_job(worker_id, self.grader_header)
        self.assertIn(key.STUDENTS, pre_processing_job)
        self.check_grading_run_status(self.course1, "assignment1", grading_run_id, self.client_header1,
                                      OK_REQUEST_CODE, GradingRunState.PRE_PROCESSING_STAGE.value)
        self.assertEqual(self.poll_job(worker_id, self.grader_header), QUEUE_EMPTY_CODE)
        run_state = self.get_grading_run_state(self.course1, "assignment1", grading_run_id, self.client_header1)
        self.assertEqual(run_state[key.PRE_PROCESSING_STATE], GradingJobState.STARTED.value)

        self.post_job_result(worker_id, self.grader_header, pre_processing_job.get(key.GRADING_JOB_ID))
        self.check_grading_run_status(self.course1, "assignment1", grading_run_id, self.client_header1,
                                      OK_REQUEST_CODE, GradingRunState.STUDENTS_STAGE.value)
        student_job = self.poll_job(worker_id, self.grader_header)
        self.assertNotIn(key.STUDENTS, student_job)
        self.check_grading_run_status(self.course1, "assignment1", grading_run_id, self.client_header1,
                                      OK_REQUEST_CODE, GradingRunState.STUDENTS_STAGE.value)
        self.assertEqual(self.poll_job(worker_id, self.grader_header), QUEUE_EMPTY_CODE)
        run_state = self.get_grading_run_state(self.course1, "assignment1", grading_run_id, self.client_header1)
        self.assertEqual(run_state[key.PRE_PROCESSING_STATE], GradingJobState.SUCCEEDED.value)
        self.assertEqual(run_state[key.STUDENTS_STATE][0], GradingJobState.STARTED.value)

        self.post_job_result(worker_id, self.grader_header, student_job.get(key.GRADING_JOB_ID))
        self.check_grading_run_status(self.course1, "assignment1", grading_run_id, self.client_header1,
                                      OK_REQUEST_CODE, GradingRunState.FINISHED.value)
        self.assertEqual(self.poll_job(worker_id, self.grader_header), QUEUE_EMPTY_CODE)
        run_state = self.get_grading_run_state(self.course1, "assignment1", grading_run_id, self.client_header1)
        self.assertEqual(run_state[key.STUDENTS_STATE][0], GradingJobState.SUCCEEDED.value)

    def test_post_processing_job(self):
        worker_id = self.register_worker(self.grader_header)
        self.upload_grading_config(self.course1, "assignment1", self.client_header1,
                                   dummy_configs.post_processing_config, OK_REQUEST_CODE)
        grading_run_id = self.start_grading_run(self.course1, "assignment1", self.client_header1,
                                                dummy_runs.one_student_and_post, OK_REQUEST_CODE)

        self.check_grading_run_status(self.course1, "assignment1", grading_run_id, self.client_header1,
                                      OK_REQUEST_CODE, GradingRunState.STUDENTS_STAGE.value)
        run_state = self.get_grading_run_state(self.course1, "assignment1", grading_run_id, self.client_header1)
        self.assertEqual(run_state[key.STUDENTS_STATE][0], GradingJobState.QUEUED.value)
        student_job = self.poll_job(worker_id, self.grader_header)
        self.assertNotIn(key.STUDENTS, student_job)
        self.check_grading_run_status(self.course1, "assignment1", grading_run_id, self.client_header1,
                                      OK_REQUEST_CODE, GradingRunState.STUDENTS_STAGE.value)
        self.assertEqual(self.poll_job(worker_id, self.grader_header), QUEUE_EMPTY_CODE)

        self.post_job_result(worker_id, self.grader_header, student_job.get(key.GRADING_JOB_ID))
        self.check_grading_run_status(self.course1, "assignment1", grading_run_id, self.client_header1,
                                      OK_REQUEST_CODE, GradingRunState.POST_PROCESSING_STAGE.value)
        run_state = self.get_grading_run_state(self.course1, "assignment1", grading_run_id, self.client_header1)
        self.assertEqual(run_state[key.POST_PROCESSING_STATE], GradingJobState.QUEUED.value)
        self.assertEqual(run_state[key.STUDENTS_STATE][0], GradingJobState.SUCCEEDED.value)

        post_processing_job = self.poll_job(worker_id, self.grader_header)
        self.assertIn(key.STUDENTS, post_processing_job)
        self.check_grading_run_status(self.course1, "assignment1", grading_run_id, self.client_header1,
                                      OK_REQUEST_CODE, GradingRunState.POST_PROCESSING_STAGE.value)

        self.assertEqual(self.poll_job(worker_id, self.grader_header), QUEUE_EMPTY_CODE)
        run_state = self.get_grading_run_state(self.course1, "assignment1", grading_run_id, self.client_header1)
        self.assertEqual(run_state[key.POST_PROCESSING_STATE], GradingJobState.STARTED.value)

        self.post_job_result(worker_id, self.grader_header, post_processing_job.get(key.GRADING_JOB_ID))
        self.check_grading_run_status(self.course1, "assignment1", grading_run_id, self.client_header1,
                                      OK_REQUEST_CODE, GradingRunState.FINISHED.value)
        self.assertEqual(self.poll_job(worker_id, self.grader_header), QUEUE_EMPTY_CODE)
        run_state = self.get_grading_run_state(self.course1, "assignment1", grading_run_id, self.client_header1)
        self.assertEqual(run_state[key.POST_PROCESSING_STATE], GradingJobState.SUCCEEDED.value)

    def test_complete_run(self):
        worker_id = self.register_worker(self.grader_header)
        self.upload_grading_config(self.course1, "assignment1", self.client_header1,
                                   dummy_configs.both_config, OK_REQUEST_CODE)
        grading_run_id = self.start_grading_run(self.course1, "assignment1", self.client_header1,
                                                dummy_runs.one_student_and_both, OK_REQUEST_CODE)

        self.check_grading_run_status(self.course1, "assignment1", grading_run_id, self.client_header1,
                                      OK_REQUEST_CODE, GradingRunState.PRE_PROCESSING_STAGE.value)
        run_state = self.get_grading_run_state(self.course1, "assignment1", grading_run_id, self.client_header1)
        self.assertEqual(run_state[key.PRE_PROCESSING_STATE], GradingJobState.QUEUED.value)

        pre_processing_job = self.poll_job(worker_id, self.grader_header)
        self.assertIn(key.STUDENTS, pre_processing_job)
        self.check_grading_run_status(self.course1, "assignment1", grading_run_id, self.client_header1,
                                      OK_REQUEST_CODE, GradingRunState.PRE_PROCESSING_STAGE.value)
        self.assertEqual(self.poll_job(worker_id, self.grader_header), QUEUE_EMPTY_CODE)
        run_state = self.get_grading_run_state(self.course1, "assignment1", grading_run_id, self.client_header1)
        self.assertEqual(run_state[key.PRE_PROCESSING_STATE], GradingJobState.STARTED.value)

        self.post_job_result(worker_id, self.grader_header, pre_processing_job.get(key.GRADING_JOB_ID))
        self.check_grading_run_status(self.course1, "assignment1", grading_run_id, self.client_header1,
                                      OK_REQUEST_CODE, GradingRunState.STUDENTS_STAGE.value)
        run_state = self.get_grading_run_state(self.course1, "assignment1", grading_run_id, self.client_header1)
        self.assertEqual(run_state[key.STUDENTS_STATE][0], GradingJobState.QUEUED.value)

        student_job = self.poll_job(worker_id, self.grader_header)
        self.assertNotIn(key.STUDENTS, student_job)
        self.check_grading_run_status(self.course1, "assignment1", grading_run_id, self.client_header1,
                                      OK_REQUEST_CODE, GradingRunState.STUDENTS_STAGE.value)
        self.assertEqual(self.poll_job(worker_id, self.grader_header), QUEUE_EMPTY_CODE)
        run_state = self.get_grading_run_state(self.course1, "assignment1", grading_run_id, self.client_header1)
        self.assertEqual(run_state[key.STUDENTS_STATE][0], GradingJobState.STARTED.value)

        self.post_job_result(worker_id, self.grader_header, student_job.get(key.GRADING_JOB_ID))
        self.check_grading_run_status(self.course1, "assignment1", grading_run_id, self.client_header1,
                                      OK_REQUEST_CODE, GradingRunState.POST_PROCESSING_STAGE.value)
        post_processing_job = self.poll_job(worker_id, self.grader_header)
        self.assertIn(key.STUDENTS, post_processing_job)
        self.check_grading_run_status(self.course1, "assignment1", grading_run_id, self.client_header1,
                                      OK_REQUEST_CODE, GradingRunState.POST_PROCESSING_STAGE.value)
        self.assertEqual(self.poll_job(worker_id, self.grader_header), QUEUE_EMPTY_CODE)
        run_state = self.get_grading_run_state(self.course1, "assignment1", grading_run_id, self.client_header1)
        self.assertEqual(run_state[key.POST_PROCESSING_STATE], GradingJobState.STARTED.value)

        self.post_job_result(worker_id, self.grader_header, post_processing_job.get(key.GRADING_JOB_ID))
        self.check_grading_run_status(self.course1, "assignment1", grading_run_id, self.client_header1,
                                      OK_REQUEST_CODE, GradingRunState.FINISHED.value)
        self.assertEqual(self.poll_job(worker_id, self.grader_header), QUEUE_EMPTY_CODE)
        run_state = self.get_grading_run_state(self.course1, "assignment1", grading_run_id, self.client_header1)
        self.assertEqual(run_state[key.POST_PROCESSING_STATE], GradingJobState.SUCCEEDED.value)

    def test_stress(self):
        num_students = 20
        run_env_vars = {key.PRE_PROCESSING_ENV: {"type": "pre"}, key.POST_PROCESSING_ENV: {"type": "post"},
                        key.STUDENTS_ENV: [{"netid": "test net id {}".format(ind)} for ind in range(num_students)]}

        self.upload_grading_config(self.course1, "assignment1", self.client_header1, dummy_configs.complete_config,
                                   OK_REQUEST_CODE)
        grading_run_id = self.start_grading_run(self.course1, "assignment1", self.client_header1, run_env_vars,
                                                OK_REQUEST_CODE)

        # pre processing job stuff
        worker_id = self.register_worker(self.grader_header)
        self.check_grading_run_status(self.course1, "assignment1", grading_run_id, self.client_header1,
                                      OK_REQUEST_CODE, GradingRunState.PRE_PROCESSING_STAGE.value)
        pre_processing_job = self.poll_job(worker_id, self.grader_header)
        self.assertIn(key.STUDENTS, pre_processing_job)
        self.check_grading_run_status(self.course1, "assignment1", grading_run_id, self.client_header1,
                                      OK_REQUEST_CODE, GradingRunState.PRE_PROCESSING_STAGE.value)
        self.assertEqual(self.poll_job(worker_id, self.grader_header), QUEUE_EMPTY_CODE)

        pre_processing_pipeline = dummy_configs.complete_config.get(key.PRE_PROCESSING_PIPELINE).copy()
        for stage in pre_processing_pipeline:
            if key.ENV in stage:
                stage[key.ENV].update(dummy_configs.complete_config.get(key.ENV, {}))
                stage[key.ENV].update({"type": "pre"})
        self.assert_equal_grading_pipeline(pre_processing_job.get(key.STAGES), pre_processing_pipeline)

        self.post_job_result(worker_id, self.grader_header, pre_processing_job.get(key.GRADING_JOB_ID))
        self.check_grading_run_status(self.course1, "assignment1", grading_run_id, self.client_header1,
                                      OK_REQUEST_CODE, GradingRunState.STUDENTS_STAGE.value)

        # student jobs
        for ind in range(num_students):
            worker_id = self.register_worker(self.grader_header)
            student_job = self.poll_job(worker_id, self.grader_header)
            self.assertNotIn(key.STUDENTS, student_job)

            student_pipeline = dummy_configs.complete_config.get(key.STUDENT_PIPELINE).copy()
            for stage in student_pipeline:
                if key.ENV in stage:
                    stage[key.ENV].update(dummy_configs.complete_config.get(key.ENV, {}))
                    stage[key.ENV].update({"netid": "test net id {}".format(ind)})
            self.assert_equal_grading_pipeline(student_job.get(key.STAGES), student_pipeline)

            self.check_grading_run_status(self.course1, "assignment1", grading_run_id, self.client_header1,
                                          OK_REQUEST_CODE, GradingRunState.STUDENTS_STAGE.value)

            if ind == num_students - 1:
                self.assertEqual(self.poll_job(worker_id, self.grader_header), QUEUE_EMPTY_CODE)

            self.post_job_result(worker_id, self.grader_header, student_job.get(key.GRADING_JOB_ID))

        # post processing job
        worker_id = self.register_worker(self.grader_header)
        self.check_grading_run_status(self.course1, "assignment1", grading_run_id, self.client_header1,
                                      OK_REQUEST_CODE, GradingRunState.POST_PROCESSING_STAGE.value)
        post_processing_job = self.poll_job(worker_id, self.grader_header)

        self.assertEqual(self.poll_job(worker_id, self.grader_header), QUEUE_EMPTY_CODE)
        self.assertIn(key.STUDENTS, post_processing_job)

        post_processing_pipeline = dummy_configs.complete_config.get(key.POST_PROCESSING_PIPELINE).copy()
        for stage in post_processing_pipeline:
            if key.ENV in stage:
                stage[key.ENV].update(dummy_configs.complete_config.get(key.ENV, {}))
                stage[key.ENV].update({"type": "post"})
        self.assert_equal_grading_pipeline(post_processing_job.get(key.STAGES), post_processing_pipeline)

        self.assertIn(key.STUDENTS, post_processing_job)
        self.check_grading_run_status(self.course1, "assignment1", grading_run_id, self.client_header1,
                                      OK_REQUEST_CODE, GradingRunState.POST_PROCESSING_STAGE.value)
        self.assertEqual(self.poll_job(worker_id, self.grader_header), QUEUE_EMPTY_CODE)

        self.post_job_result(worker_id, self.grader_header, post_processing_job.get(key.GRADING_JOB_ID))
        self.check_grading_run_status(self.course1, "assignment1", grading_run_id, self.client_header1,
                                      OK_REQUEST_CODE, GradingRunState.FINISHED.value)
        self.assertEqual(self.poll_job(worker_id, self.grader_header), QUEUE_EMPTY_CODE)

    def test_abort_pre_processing_fail(self):
        worker_id = self.register_worker(self.grader_header)
        self.upload_grading_config(self.course1, "assignment1", self.client_header1,
                                   dummy_configs.pre_processing_config, OK_REQUEST_CODE)
        grading_run_id = self.start_grading_run(self.course1, "assignment1", self.client_header1,
                                                dummy_runs.one_student_and_pre, OK_REQUEST_CODE)

        pre_processing_job = self.poll_job(worker_id, self.grader_header)
        self.check_grading_run_status(self.course1, "assignment1", grading_run_id, self.client_header1,
                                      OK_REQUEST_CODE, GradingRunState.PRE_PROCESSING_STAGE.value)
        self.assertEqual(self.poll_job(worker_id, self.grader_header), QUEUE_EMPTY_CODE)

        self.post_job_result(worker_id, self.grader_header, pre_processing_job.get(key.GRADING_JOB_ID), False)
        self.check_grading_run_status(self.course1, "assignment1", grading_run_id, self.client_header1,
                                      OK_REQUEST_CODE, GradingRunState.FAILED.value)
        self.assertEqual(self.poll_job(worker_id, self.grader_header), QUEUE_EMPTY_CODE)

    def test_no_abort_student_fail(self):
        worker_id = self.register_worker(self.grader_header)
        self.upload_grading_config(self.course1, "assignment1", self.client_header1,
                                   dummy_configs.both_config, OK_REQUEST_CODE)
        grading_run_id = self.start_grading_run(self.course1, "assignment1", self.client_header1,
                                                dummy_runs.one_student_and_both, OK_REQUEST_CODE)

        pre_processing_job = self.poll_job(worker_id, self.grader_header)
        self.check_grading_run_status(self.course1, "assignment1", grading_run_id, self.client_header1,
                                      OK_REQUEST_CODE, GradingRunState.PRE_PROCESSING_STAGE.value)
        self.assertEqual(self.poll_job(worker_id, self.grader_header), QUEUE_EMPTY_CODE)

        self.post_job_result(worker_id, self.grader_header, pre_processing_job.get(key.GRADING_JOB_ID))
        self.check_grading_run_status(self.course1, "assignment1", grading_run_id, self.client_header1,
                                      OK_REQUEST_CODE, GradingRunState.STUDENTS_STAGE.value)

        student_job = self.poll_job(worker_id, self.grader_header)
        self.assertNotIn(key.STUDENTS, student_job)
        self.assertEqual(self.poll_job(worker_id, self.grader_header), QUEUE_EMPTY_CODE)
        self.post_job_result(worker_id, self.grader_header, student_job.get(key.GRADING_JOB_ID), False)

        self.check_grading_run_status(self.course1, "assignment1", grading_run_id, self.client_header1,
                                      OK_REQUEST_CODE, GradingRunState.POST_PROCESSING_STAGE.value)
        post_processing_job = self.poll_job(worker_id, self.grader_header)
        self.assertEqual(self.poll_job(worker_id, self.grader_header), QUEUE_EMPTY_CODE)

        self.post_job_result(worker_id, self.grader_header, post_processing_job.get(key.GRADING_JOB_ID))
        self.check_grading_run_status(self.course1, "assignment1", grading_run_id, self.client_header1,
                                      OK_REQUEST_CODE, GradingRunState.FINISHED.value)
        self.assertEqual(self.poll_job(worker_id, self.grader_header), QUEUE_EMPTY_CODE)

    def test_abort_post_processing_fail(self):
        worker_id = self.register_worker(self.grader_header)
        self.upload_grading_config(self.course1, "assignment1", self.client_header1,
                                   dummy_configs.both_config, OK_REQUEST_CODE)
        grading_run_id = self.start_grading_run(self.course1, "assignment1", self.client_header1,
                                                dummy_runs.one_student_and_both, OK_REQUEST_CODE)
        self.check_grading_run_status(self.course1, "assignment1", grading_run_id, self.client_header1, OK_REQUEST_CODE,
                                      GradingRunState.PRE_PROCESSING_STAGE.value)

        pre_processing_job = self.poll_job(worker_id, self.grader_header)
        self.assertEqual(self.poll_job(worker_id, self.grader_header), QUEUE_EMPTY_CODE)
        self.post_job_result(worker_id, self.grader_header, pre_processing_job.get(key.GRADING_JOB_ID))
        self.check_grading_run_status(self.course1, "assignment1", grading_run_id, self.client_header1, OK_REQUEST_CODE,
                                      GradingRunState.STUDENTS_STAGE.value)

        student_job = self.poll_job(worker_id, self.grader_header)
        self.assertEqual(self.poll_job(worker_id, self.grader_header), QUEUE_EMPTY_CODE)
        self.post_job_result(worker_id, self.grader_header, student_job.get(key.GRADING_JOB_ID))
        self.check_grading_run_status(self.course1, "assignment1", grading_run_id, self.client_header1, OK_REQUEST_CODE,
                                      GradingRunState.POST_PROCESSING_STAGE.value)

        post_processing_job = self.poll_job(worker_id, self.grader_header)
        self.assertEqual(self.poll_job(worker_id, self.grader_header), QUEUE_EMPTY_CODE)
        self.post_job_result(worker_id, self.grader_header, post_processing_job.get(key.GRADING_JOB_ID), False)
        self.check_grading_run_status(self.course1, "assignment1", grading_run_id, self.client_header1, OK_REQUEST_CODE,
                                      GradingRunState.FAILED.value)
        self.assertEqual(self.poll_job(worker_id, self.grader_header), QUEUE_EMPTY_CODE)
