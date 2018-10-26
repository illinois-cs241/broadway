from pymongo import MongoClient


class DatabaseResolver(object):

    def __init__(self, db_name='AG'):
        self.client = MongoClient()
        self.db = self.client[db_name]

    def get_workers_node_collection(self):
        # Worker Node:
        #   id (implicit)
        #   last_seen
        #   running_job_ids         None if not executing any job

        return self.db.worker_nodes

    def get_grading_run_collection(self):
        # Grading Run:
        #   id (implicit)
        #   created_at
        #   started_at
        #   finished_at
        #   student_job_ids = [id,...]
        #   postprocessing_job_id = None if no stage else id
        #   student_jobs_left

        return self.db.grading_runs

    def get_jobs_collection(self):
        # Job:
        #   id (implicit)
        #   created_at
        #   queued_at
        #   started_at
        #   finished_at
        #   result
        #   grading_run_id
        #   stages = [stage1, stage2, ...] with all environment variables expanded

        return self.db.jobs

# stage :
#  {
#    "image": <image name>,  REQUIRED
#    "environment":          OPTIONAL
#      {
#         <env var name>: <$env var name/value>, ...
#      }
#  }

# "student_pipeline":        REQUIRED
# [
#   stage1, stage2, ...
# ]

# "postprocessing_pipeline": OPTIONAL
# [
#   stage1, stage2, ...
# ]

# "environment":             OPTIONAL
#      {
#         <env var name>: <value>, ...
#      }

# "students":                REQUIRED
# [
#   { <env var name>: <value>, ...}, ...
# ]
