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

    def get_job_log_collection(self):
        """
        Returns a collection of Job logs produced by the containers when the job was run. This is contained in a
        separate DB since this can be bulky.

        Document format:
            _id (implicit)
            job_id
            stderr
            stdout

        :rtype: collection.Collection
        :return: collection of job log documents
        """
        return self.logs_db.job_log

    def get_worker_node_collection(self):
        """
        Returns a collection of documents representing worker nodes currently online.
        Document format:
            _id (implicit)
            running_job_id (None if not executing any job)
            last_seen
            worker_hostname
            jobs_processed
            alive

        :rtype: collection.Collection
        :return: collection of work node documents
        """
        return self.db.worker_node

    def get_token_collection(self):
        """
        Returns a collection of tokens used to authenticate requests. A token can be owned by multiple courses and a
        course can own multiple tokens. Courses and tokens have a many to many relationship.
        Document format:
            _id (implicit)
            token

        :rtype: collection.Collection
        :return: collection of work node documents
        """
        return self.db.token

    def get_course_collection(self):
        """
        Returns a collection of documents representing all courses registered into the system.
        Document format:
            _id (unique, specified by clients)
            token_ids = [id,...]

        :rtype: collection.Collection
        :return: collection of work node documents
        """
        return self.db.course

    def get_assignment_collection(self):
        """
        Returns a collection of documents containing all assignment configs belonging to various courses.
        Document format:
            _id (unique: course_id + '/' + assignment_name)
            pre_processing_pipeline
            post_processing_pipeline
            student_pipeline
            global_env_vars

        :rtype: collection.Collection
        :return: collection of work node documents
        """
        return self.db.assigment_config

    def get_grading_run_collection(self):
        """
        Returns a collection of documents representing all grading runs that have been created. These might be in any
        state (created, running, finished).

        Document format:
            _id (auto)
            assignment_id
            created_at
            started_at
            finished_at
            students
            student_jobs_left
            success

        :rtype: collection.Collection
        :return: collection of grading run documents
        """
        return self.db.grading_run

    def get_grading_job_collection(self):
        """
        Returns a collection of documents representing all grading jobs that have been created. These might be in any
        state (created, queued, running, finished).

        Document format:
            _id (auto)
            grading_run_id
            worker_id (once started)
            created_at
            queued_at
            started_at
            finished_at
            results
            success
            stages = [stage1, stage2, ...]

        :rtype: collection.Collection
        :return: collection of grading job documents
        """
        return self.db.grading_job

    def shutdown(self):
        logger.info("shutting down Mongo daemon")
        self.mongo_daemon.kill()

    def clear_db(self):
        logger.critical("Deleting the entire database")
        self.client.drop_database(self.db_name)
        self.client.drop_database(self.logs_db_name)
