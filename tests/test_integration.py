import src.constants.keys as api_key
import tests.configs
from tests.base import BaseTest


class TestJobPollOrder(BaseTest):
    def test_order_pre_and_post(self):
        worker_id = self.register_worker()
        self.assertIsNotNone(worker_id)

        run_id = self.add_grading_run()
        self.assertIsNotNone(run_id)
        self.start_run(run_id)

        # pre processing job
        pre_processing_job = self.poll_job(worker_id)
        self.assert_equal_job(pre_processing_job.get(api_key.STAGES), tests.configs.valid_jobs[0])
        self.assertIn(api_key.STUDENTS, pre_processing_job)
        self.assertEqual(pre_processing_job.get(api_key.STUDENTS), tests.configs.valid_config.get(api_key.STUDENTS))
        self.poll_job(worker_id, True)  # none of the student jobs should have been scheduled yet
        self.post_job_result(worker_id, pre_processing_job.get(api_key.GRADING_JOB_ID))

        # student jobs
        for i in range(1, len(tests.configs.valid_jobs) - 1):
            if i == 1:
                student_job = self.safe_poll_job(worker_id)
            else:
                student_job = self.poll_job(worker_id)

            self.assert_equal_job(student_job.get(api_key.STAGES), tests.configs.valid_jobs[i])

            if i == len(tests.configs.valid_jobs) - 2:
                # the post processing job should not have been scheduled yet
                # since we have to notified the API that the last student job finished
                self.poll_job(worker_id, True)

            self.post_job_result(worker_id, student_job.get(api_key.GRADING_JOB_ID))

        # post processing job
        post_processing_job = self.poll_job(worker_id)
        self.poll_job(worker_id, True)
        self.assert_equal_job(post_processing_job.get(api_key.STAGES), tests.configs.valid_jobs[-1])
        self.assertIn(api_key.STUDENTS, post_processing_job)
        self.assertEqual(post_processing_job.get(api_key.STUDENTS), tests.configs.valid_config.get(api_key.STUDENTS))
        self.post_job_result(worker_id, post_processing_job.get(api_key.GRADING_JOB_ID))
        self.poll_job(worker_id, True)
