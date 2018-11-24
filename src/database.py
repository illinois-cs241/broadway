import logging
import os
from subprocess import Popen, DEVNULL

from src.config import DB_PATH
from pymongo import MongoClient
from pymongo import collection

logger = logging.getLogger()


class DatabaseResolver(object):

    def __init__(self, db_name="AG"):
        self.client = MongoClient()
        self.db_name = db_name
        self.db = self.client[self.db_name]

        logger.info("starting up Mongo daemon")
        os.makedirs(DB_PATH, exist_ok=True)
        self.mongo_daemon = Popen(["mongod", "--dbpath", DB_PATH], stdout=DEVNULL, stderr=DEVNULL)

    # Worker Node:
    #   _id (implicit)
    #   last_seen
    #   running_job_ids         None if not executing any job
    def get_worker_node_collection(self):
        # type: () -> collection.Collection
        return self.db.worker_nodes

    # Grading Run:
    #   _id (implicit)
    #   created_at
    #   started_at
    #   finished_at
    #   students
    #   student_jobs_left
    #   student_job_ids = [id,...]
    #   pre_processing_job_id  (if exists)
    #   post_processing_job_id  (if exists)
    #   success
    def get_grading_run_collection(self):
        # type: () -> collection.Collection
        return self.db.grading_runs

    # Job:
    #   _id (implicit)
    #   created_at
    #   queued_at
    #   started_at
    #   finished_at
    #   info
    #   success
    #   grading_run_id
    #   stages = [stage1, stage2, ...] with all environment variables expanded
    def get_grading_job_collection(self):
        # type: () -> collection.Collection
        return self.db.jobs

    def shutdown(self):
        logger.info("shutting down Mongo daemon")
        self.mongo_daemon.kill()

    def clear_db(self):
        logger.critical("Deleting the entire database")
        self.client.drop_database(self.db_name)
