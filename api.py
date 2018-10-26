import json
from threading import Condition
from queue import Queue
from threading import Thread
import tornado.ioloop
import tornado.web
import datetime as dt
import random
import string
import logging
import time
import os

# constants
from bson import ObjectId
from tornado import escape

from database import DatabaseResolver
from settings import LOGS_DIR_NAME, ID_REGEX, TIMESTAMP_FORMAT, PORT, HEARTBEAT_INTERVAL, BAD_REQUEST_CODE, \
    EMPTY_QUEUE_CODE

# globals
heartbeat_validator_thread = None
cluster_token = None
job_queue = Queue()
heartbeat = True
heartbeat_cv = Condition()

def get_time():
    return dt.datetime.fromtimestamp(time.time()).strftime(TIMESTAMP_FORMAT)

# TODO
def handle_lost_worker_node(worker_node):
    logging.critical("Worker node {} executing {} went offline unexpectedly".format(worker_node["_id"],
                                                                                    worker_node["running_job_ids"]))


def heartbeat_validator(db_resolver):
    worker_nodes_collection = db_resolver.get_workers_node_collection()
    while heartbeat:
        cur_time = dt.datetime.fromtimestamp(time.time())
        for worker_node in worker_nodes_collection.find():
            last_seen_time = dt.datetime.strptime(worker_node['last_seen'], TIMESTAMP_FORMAT)

            # the worker node dead if it does not send a heartbeat for 2 intervals
            if (cur_time - last_seen_time).total_seconds() >= 2 * HEARTBEAT_INTERVAL:
                handle_lost_worker_node(worker_node)
                worker_nodes_collection.delete_one({"_id": worker_node["_id"]})

        heartbeat_cv.acquire()
        heartbeat_cv.wait(timeout=HEARTBEAT_INTERVAL)
        heartbeat_cv.release()


def get_status(student_job):
    if 'finished_at' in student_job:
        return "Finished"
    elif 'started_at' in student_job:
        return "Running"
    elif 'queued_at' in student_job:
        return "Queued"
    else:
        return "Created"

def expand_env_vars(vars_to_fill, global_env_vars, student_env_vars=None):
    if student_env_vars is None:
        student_env_vars = {}

    res_vars = []
    for var in vars_to_fill:
        # value is not specified: if the env var is defined anywhere else, replace it
        if len(vars_to_fill[var]) == 0:
            if var in global_env_vars:
                res_vars.append(var + "=" + global_env_vars[var])
            elif var in student_env_vars:
                res_vars.append(var + "=" + student_env_vars[var])
            else:
                raise Exception(
                    "Could not find environment variable {} using student env vars or global env vars".format(var))
        # if the env var is dependent on another, substitute appropriately
        elif vars_to_fill[var][0] == '$':
            var_to_sub = vars_to_fill[var][1:]
            if var_to_sub in global_env_vars:
                res_vars.append(var + "=" + global_env_vars[var_to_sub])
            elif var_to_sub in student_env_vars:
                res_vars.append(var + "=" + student_env_vars[var_to_sub])
            else:
                raise Exception(
                    "Could not substitude environment variable {} using student env vars or global env vars".format(
                        vars_to_fill[var]))
        # if the value is independent, just copy it over
        else:
            res_vars.append(var + "=" + vars_to_fill[var])

    return res_vars


def make_app(db_resolver):
    return tornado.web.Application([
        # ---------Client Endpoints---------
        # POST to add grading run
        (r"/api/v1/grading_run", AddGradingRunHandler, {"db_object": db_resolver}),

        # POST to start grading run.
        # GET to get statuses of all jobs
        (r"/api/v1/grading_run/{}".format(ID_REGEX), GradingRunHandler, {"db_object": db_resolver}),
        # ----------------------------------

        # -----Worker Node Endpoints--------
        # GET to register node and get worked ID
        (r"/api/v1/worker_register", WorkerRegisterHandler, {"db_object": db_resolver}),

        # GET to get a grading job
        (r"/api/v1/grading_job", GradingJobHandler, {"db_object": db_resolver}),

        # TODO POST to update status of job
        (r"/api/v1/grading_job/{}".format(ID_REGEX), JobUpdateHandler, {"db_object": db_resolver}),

        # POST to register heartbeat
        (r"/api/v1/heartbeat", HeartBeatHandler, {"db_object": db_resolver}),
        # ----------------------------------
    ])


def generate_random_key(length):
    return ''.join(random.choice(string.ascii_lowercase + string.digits) for _ in range(length))

def enqueue_job(job_id, db_resolver):
    jobs_collection = db_resolver.get_jobs_collection()
    cur_job = {}
    job = jobs_collection.find_one({'_id': ObjectId(job_id)})
    assert job is not None
    cur_job['stages'] = job['stages']
    cur_job['job_id'] = job_id
    job_queue.put(cur_job, block=False)
    jobs_collection.update_one({'_id': ObjectId(job_id)}, {"$set": {'queued_at': get_time()}})


class RequestHandlerBase(tornado.web.RequestHandler):
    def initialize(self, db_object: DatabaseResolver):
        self.db_handler = db_object


class AddGradingRunHandler(RequestHandlerBase):
    def bad_request(self, error_message):
        self.clear()
        self.set_status(BAD_REQUEST_CODE)
        self.finish(json.dumps({'error': error_message}))

    def is_valid(self, args):
        if "student_pipeline" not in args:
            return False, "missing argument \'student_pipeline\'"

        for student_pipeline_stage in args["student_pipeline"]:
            if "image" not in student_pipeline_stage:
                return False, "missing argument \'image\' in student pipeline stages"

        if "postprocessing_pipeline" in args:
            for postprocessing_pipeline_stage in args["postprocessing_pipeline"]:
                if "image" not in postprocessing_pipeline_stage:
                    return False, "missing argument \'image\' in postprocessing pipeline stage"

        if "students" not in args:
            return False, "missing argument \'students\'"

        return True, None

    # adds grading pipeline to DB and returns its ID
    def post(self):
        if 'json_payload' not in self.request.arguments:
            self.bad_request('\'json_payload\' field missing in request')
            return

        json_payload = json.loads(escape.to_basestring(self.request.arguments['json_payload'][0]))
        valid, error_message = self.is_valid(json_payload)
        if not valid:
            self.bad_request(error_message)
            return

        # create all student jobs
        student_jobs = []
        for student in json_payload["students"]:
            cur_job = []
            for stage in json_payload["student_pipeline"]:
                cur_stage = {"image": stage["image"]}
                if "entrypoint" in stage:
                    cur_stage["entrypoint"] = stage["entrypoint"]
                if "env" in stage:
                    try:
                        cur_stage["env"] = expand_env_vars(stage["env"], json_payload["env"], student)
                    except Exception as error:
                        self.bad_request("Student Pipeline: {}".format(str(error)))
                        return
                cur_job.append(cur_stage)

            student_jobs.append(cur_job)

        # create post processing stage if it exists
        postprocessing_job = []
        if "postprocessing_pipeline" in json_payload:
            for stage in json_payload["postprocessing_pipeline"]:
                cur_stage = {"image": stage["image"]}
                if "entrypoint" in stage:
                    cur_stage["entrypoint"] = stage["entrypoint"]
                if "env" in stage:
                    try:
                        cur_stage["env"] = expand_env_vars(stage["env"], json_payload["env"])
                    except Exception as error:
                        self.bad_request("Postprocessing Pipeline: {}".format(str(error)))
                        return
                postprocessing_job.append(cur_stage)

        jobs_collection = self.db_handler.get_jobs_collection()
        grading_runs_collection = self.db_handler.get_grading_run_collection()
        # arguments are valid so feed into DB and return run id
        grading_run = {'created_at': get_time(), 'student_jobs_left': len(student_jobs)}
        grading_run_id = str(grading_runs_collection.insert_one(grading_run).inserted_id)

        student_job_ids = []
        for student_job in student_jobs:
            job = {'created_at': get_time(), 'grading_run_id': grading_run_id, 'stages': student_job}
            student_job_ids.append(str(jobs_collection.insert_one(job).inserted_id))

        postprocessing_job_id = None
        if len(postprocessing_job) > 0:
            job = {'created_at': get_time(), 'grading_run_id': grading_run_id, 'stages': postprocessing_job}
            postprocessing_job_id = str(jobs_collection.insert_one(job).inserted_id)

        grading_runs_collection.update_one({'_id': ObjectId(grading_run_id)}, {
            "$set": {'student_job_ids': student_job_ids, 'postprocessing_job_id': postprocessing_job_id}})

        # return the run id to user
        self.write(json.dumps({'id': grading_run_id}))


class GradingRunHandler(RequestHandlerBase):
    def bad_request(self, error_message):
        self.clear()
        self.set_status(BAD_REQUEST_CODE)
        self.finish(json.dumps({'error': error_message}))

    # adds grading jobs to queue
    def post(self, id_):
        grading_runs_collection = self.db_handler.get_grading_run_collection()
        grading_run_id = id_
        grading_run = grading_runs_collection.find_one({'_id': ObjectId(grading_run_id)})
        if grading_run is None:
            self.bad_request("Grading Run with id {} does not exist".format(grading_run_id))
            return

        if 'started_at' in grading_run:
            self.bad_request("Grading Run with id {} has already been queued in the past".format(grading_run_id))
            return

        assert "student_job_ids" in grading_run
        for student_job_id in grading_run['student_job_ids']:
            enqueue_job(student_job_id)

        grading_runs_collection.update_one({'_id': ObjectId(grading_run_id)}, {"$set": {'started_at': get_time()}})

    # get statuses of all jobs
    def get(self, id_):
        grading_run_id = id_
        grading_runs_collection = self.db_handler.get_grading_run_collection()
        jobs_collection = self.db_handler.get_jobs_collection()
        grading_run = grading_runs_collection.find_one({'_id': ObjectId(grading_run_id)})
        if grading_run is None:
            self.bad_request("Grading Run with id {} does not exist".format(grading_run_id))
            return

        res = {'student statuses': []}
        for student_job_id in grading_run['student_job_ids']:
            student_job = jobs_collection.find_one({'_id': ObjectId(student_job_id)})
            assert student_job is not None
            res['student statuses'].append(
                {'job_id': student_job_id, 'status': get_status(student_job)})

        if grading_run['postprocessing_job_id'] is not None:
            postprocessing_job = jobs_collection.find_one({'_id': ObjectId(grading_run['postprocessing_job_id'])})
            assert postprocessing_job is not None
            res['postprocessing status'] = {'job_id': grading_run['postprocessing_job_id'],
                                            'status': get_status(postprocessing_job)}

        self.write(json.dumps(res))


class GradingJobHandler(RequestHandlerBase):
    def bad_request(self, error_message):
        self.clear()
        self.set_status(BAD_REQUEST_CODE)
        self.finish(json.dumps({'error': error_message}))

    # get a grading job from the queue
    def get(self):
        worker_nodes_collection = self.db_handler.get_worker_nodes_collection()
        jobs_collection = self.db_handler.get_jobs_collection()
        # authenticate this get call with the worker id
        if 'worker_id' not in self.request.arguments:
            self.bad_request('\'worker_id\' field missing in request')
            return

        worker_id = escape.to_basestring(self.request.arguments['worker_id'][0])
        worker_node = worker_nodes_collection.find_one({'_id': ObjectId(worker_id)})
        if worker_node is None:
            self.bad_request("Worker node with id {} does not exist".format(worker_id))
            return

        # poll from queue, updated job's start time and update worker node's running_job_ids list
        try:
            job = job_queue.get_nowait()
            jobs_collection.update_one({'_id': ObjectId(job['job_id'])}, {"$set": {'started_at': get_time()}})
            worker_node["running_job_ids"].append(job['job_id'])
            worker_nodes_collection.update_one({'_id': ObjectId(worker_id)},
                                               {"$set": {"running_job_ids": worker_node["running_job_ids"]}})
            self.write(json.dumps(job))
        except:
            self.clear()
            self.set_status(EMPTY_QUEUE_CODE)
            self.finish(json.dumps({'error': "The queue is empty"}))


class JobUpdateHandler(RequestHandlerBase):
    def bad_request(self, error_message):
        self.clear()
        self.set_status(BAD_REQUEST_CODE)
        self.finish(json.dumps({'error': error_message}))

    def post(self, id_):
        worker_nodes_collection = self.db_handler.get_worker_nodes_collection()
        jobs_collection = self.db_handler.get_jobs_collection()
        grading_runs_collection = self.db_handler.get_grading_run_collection()
        job_id = id_
        # authenticate this get call with the worker id
        if 'worker_id' not in self.request.arguments:
            self.bad_request('\'worker_id\' field missing in request')
            return

        worker_id = escape.to_basestring(self.request.arguments['worker_id'][0])
        worker_node = worker_nodes_collection.find_one({'_id': ObjectId(worker_id)})
        if worker_node is None:
            self.bad_request("Worker node with id {} does not exist".format(worker_id))
            return

        # check if the job exists
        job = jobs_collection.find_one({'_id': ObjectId(job_id)})
        if job is None:
            self.bad_request("Job with id {} does not exist".format(job_id))
            return

        # update job: finished_at and result
        assert "created_at" in job
        assert "queued_at" in job
        assert "started_at" in job
        assert "finished_at" not in job
        jobs_collection.update_one({'_id': ObjectId(job_id)}, {"$set": {"finished_at": get_time(),
                                                                        "result": escape.to_basestring(
                                                                            self.request.arguments['result'][0])
                                                                        if 'result' in self.request.arguments else None}})

        # update worker node: remove this job from its currently running jobs
        assert job_id in worker_node["running_job_ids"]
        worker_node["running_job_ids"].remove(job_id)
        worker_nodes_collection.update_one({'_id': ObjectId(worker_id)},
                                           {"$set": {"running_job_ids": worker_node["running_job_ids"]}})

        # update grading run: if last job finished then update finished_at. Update student_jobs_left if student job.
        # enqueue post processing if all student jobs finished
        assert "grading_run_id" in job
        grading_run = grading_runs_collection.find_one({'_id': ObjectId(job["grading_run_id"])})

        assert "created_at" in grading_run
        assert "started_at" in grading_run
        assert "finished_at" not in grading_run
        assert "postprocessing_job_id" in grading_run

        if grading_run["postprocessing_job_id"] == job_id:
            assert grading_run["student_jobs_left"] == 0
            grading_runs_collection.update_one({'_id': ObjectId(job["grading_run_id"])},
                                               {"$set": {"finished_at": get_time()}})
        else:
            # this is a student's job
            assert grading_run is not None
            assert grading_run["student_jobs_left"] > 0
            grading_runs_collection.update_one({'_id': ObjectId(job["grading_run_id"])},
                                               {"$set": {"student_jobs_left": grading_run["student_jobs_left"] - 1}})

            if grading_run["student_jobs_left"] == 1:
                # this was the last student job which finished so if post processing exists then schedule it
                if grading_run["postprocessing_job_id"] is None:
                    grading_runs_collection.update_one({'_id': ObjectId(job["grading_run_id"])},
                                                       {"$set": {"finished_at": get_time()}})
                else:
                    enqueue_job(grading_run["postprocessing_job_id"])


class WorkerRegisterHandler(RequestHandlerBase):
    def bad_request(self, error_message):
        self.clear()
        self.set_status(BAD_REQUEST_CODE)
        self.finish(json.dumps({'error': error_message}))

    def get(self):
        worker_nodes_collection = self.db_handler.get_worker_nodes_collection()
        if 'token' not in self.request.arguments:
            self.bad_request('\'token\' field missing in request')
            return

        token = escape.to_basestring(self.request.arguments['token'][0])

        if token != cluster_token:
            self.bad_request('wrong token')
            return

        worker_node = {'last_seen': get_time(), 'running_job_ids': []}
        worker_node_id = str(worker_nodes_collection.insert_one(worker_node).inserted_id)
        logging.info("Worker {} joined".format(worker_node_id))
        self.write(json.dumps({'worker_id': worker_node_id, 'heartbeat': HEARTBEAT_INTERVAL}))


class HeartBeatHandler(RequestHandlerBase):
    def bad_request(self, error_message):
        self.clear()
        self.set_status(BAD_REQUEST_CODE)
        self.finish(json.dumps({'error': error_message}))

    def post(self):
        worker_nodes_collection = self.db_handler.get_worker_nodes_collection()

        if 'worker_id' not in self.request.arguments:
            self.bad_request('\'worker_id\' field missing in request')
            return

        worker_id = escape.to_basestring(self.request.arguments['worker_id'][0])

        worker_node = worker_nodes_collection.find_one({'_id': ObjectId(worker_id)})
        if worker_node is None:
            self.bad_request("Worker node with id {} does not exist".format(worker_id))
            return

        worker_nodes_collection.update_one({'_id': ObjectId(worker_id)}, {"$set": {'last_seen': get_time()}})
        logging.info("Heartbeat from {}".format(worker_id))


if __name__ == "__main__":
    # set up logger
    if not os.path.exists(LOGS_DIR_NAME):
        os.makedirs(LOGS_DIR_NAME)
    logging.basicConfig(filename='{}/{}.log'.format(LOGS_DIR_NAME, get_time()), level=logging.DEBUG)

    # set up cluster token
    cluster_token = generate_random_key(30)
    print("Nodes can join the cluster using token: {}".format(cluster_token))

    # start heartbeat checker as a background task
    heartbeat_validator_thread = Thread(target=heartbeat_validator)
    heartbeat_validator_thread.start()

    # start the API
    db_resolver = DatabaseResolver()
    app = make_app(db_resolver)
    app.listen(PORT)

    try:
        tornado.ioloop.IOLoop.current().start()
    except KeyboardInterrupt:
        tornado.ioloop.IOLoop.current().stop()

    # kill heartbeat thread
    heartbeat = False
    heartbeat_cv.acquire()
    heartbeat_cv.notify()
    heartbeat_cv.release()
    heartbeat_validator_thread.join()