import logging
import os
from subprocess import Popen, DEVNULL

from src.config import DB_PATH
from pymongo import MongoClient, collection

logger = logging.getLogger()


class DatabaseResolver(object):

    def __init__(self, db_name="AG", logs_db_name="logs"):
        self.client = MongoClient()
        self.db_name = db_name
        self.logs_db_name = logs_db_name
        self.db = self.client[self.db_name]
        self.logs_db = self.client[self.logs_db_name]

        logger.info("starting up Mongo daemon")
        os.makedirs(DB_PATH, exist_ok=True)
        self.mongo_daemon = Popen(["mongod", "--dbpath", DB_PATH], stdout=DEVNULL, stderr=DEVNULL)

    def get_job_logs_collection(self):
        """
        Returns a collection of Job logs produced by the containers when the job was run.
        Document format:
            _id (implicit)
            job_id
            stderr
            stdout

        :rtype: collection.Collection
        :return: collection of job log documents
        """
        return self.logs_db.job_logs

    def get_worker_node_collection(self):
        """
        Returns a collection of documents representing worker nodes currently online.
        Document format:
            _id (implicit)
            last_seen
            worker_hostname
            running_job_id         None if not executing any job

        :rtype: collection.Collection
        :return: collection of work node documents
        """
        return self.db.worker_nodes

    def get_courses_collection(self):
        """
        Returns a collection of documents representing all courses registered into the system.
        Document format:
            _id (implicit)
            auth_token
            course_id
            assignment_configs = [id,...]

        :rtype: collection.Collection
        :return: collection of work node documents
        """
        return self.db.courses

    def get_grading_run_collection(self):
        """
        Returns a collection of documents representing all grading runs that have been created. These might be in any
        state (created, running, finished).

        Document format:
            _id (implicit)
            created_at
            started_at
            finished_at
            students
            student_jobs_left
            student_job_ids = [id,...]
            pre_processing_job_id   (optional)
            post_processing_job_id  (optional)
            success

        :rtype: collection.Collection
        :return: collection of grading run documents
        """
        return self.db.grading_runs

    def get_grading_job_collection(self):
        """
        Returns a collection of documents representing all grading jobs that have been created. These might be in any
        state (created, queued, running, finished).

        Document format:
            _id (implicit)
            created_at
            queued_at
            started_at
            finished_at
            results
            success
            grading_run_id
            stages = [stage1, stage2, ...]

        :rtype: collection.Collection
        :return: collection of grading job documents
        """
        return self.db.grading_jobs

    def shutdown(self):
        logger.info("shutting down Mongo daemon")
        self.mongo_daemon.kill()

    def clear_db(self):
        logger.critical("Deleting the entire database")
        self.client.drop_database(self.db_name)
        self.client.drop_database(self.logs_db_name)
