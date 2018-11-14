import json
from threading import Condition
from threading import Lock
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
from tornado import escape, gen

from src.database import DatabaseResolver
from src.settings import LOGS_DIR_NAME, ID_REGEX, TIMESTAMP_FORMAT, PORT, HEARTBEAT_INTERVAL, BAD_REQUEST_CODE

# globals
cluster_token = None
job_queue = Queue()
heartbeat = True
heartbeat_cv = Condition()
job_update_lock = Lock()


def get_time():
    return dt.datetime.fromtimestamp(time.time()).strftime(TIMESTAMP_FORMAT)


def handle_lost_worker_node(worker_node, db_resolver):
    logging.critical("Worker node {} executing {} went offline unexpectedly".format(worker_node["_id"],
                                                                                    worker_node["running_job_ids"]))
    for job_id in worker_node["running_job_ids"]:
        job = db_resolver.get_grading_job(job_id)
        db_resolver.get_grading_run_collection().update_one({'_id': ObjectId(job["grading_run_id"])},
                                                            {"$inc": {"student_jobs_left": -1}})


def heartbeat_validator(db_resolver):
    worker_nodes_collection = db_resolver.get_workers_node_collection()
    while heartbeat:
        cur_time = dt.datetime.fromtimestamp(time.time())
        for worker_node in worker_nodes_collection.find():
            last_seen_time = dt.datetime.strptime(worker_node['last_seen'], TIMESTAMP_FORMAT)

            # the worker node dead if it does not send a heartbeat for 2 intervals
            if (cur_time - last_seen_time).total_seconds() >= 2 * HEARTBEAT_INTERVAL:
                handle_lost_worker_node(worker_node, db_resolver)
                worker_nodes_collection.delete_one({"_id": ObjectId(worker_node["_id"])})

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

    if any(global_env_vars):
        for global_var, global_value in global_env_vars.items():
            res_vars.append(global_var + "=" + global_value)

    if any(student_env_vars):
        for student_var, student_value in student_env_vars.items():
            res_vars.append(student_var + "=" + student_value)

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
                    "Could not substitute environment variable {} using student env vars or global env vars".format(
                        vars_to_fill[var]))
        # if the value is independent, just copy it over
        else:
            res_vars.append(var + "=" + vars_to_fill[var])

    return res_vars


def make_app(db_resolver):
    return tornado.web.Application([
        # ---------Client Endpoints---------
        # POST to add grading run
        (r"/api/v1/grading_run", AddGradingRunHandler),

        # POST to start grading run.
        # GET to get statuses of all jobs
        (r"/api/v1/grading_run/{}".format(ID_REGEX), GradingRunHandler),
        # ----------------------------------

        # -----Grader Endpoints--------
        # GET to register node and get worked ID
        (r"/api/v1/worker_register", WorkerRegisterHandler),

        # GET to get a grading job
        (r"/api/v1/grading_job", GradingJobHandler),

        # POST to update status of job
        (r"/api/v1/grading_job/{}".format(ID_REGEX), JobUpdateHandler),

        # POST to register heartbeat
        (r"/api/v1/heartbeat", HeartBeatHandler),
        # ----------------------------------
    ], db_object=db_resolver)


def generate_random_key(length):
    return ''.join(random.choice(string.ascii_lowercase + string.digits) for _ in range(length))


def enqueue_job(job_id, db_resolver):
    jobs_collection = db_resolver.get_jobs_collection()
    cur_job = {}
    job = db_resolver.get_grading_job(job_id)
    assert job is not None
    cur_job['stages'] = job['stages']
    cur_job['job_id'] = job_id
    job_queue.put(cur_job)
    jobs_collection.update_one({'_id': ObjectId(job_id)}, {"$set": {'queued_at': get_time()}})


# ----------------- Clients -----------------

class RequestHandlerBase(tornado.web.RequestHandler):
    def bad_request(self, error_message):
        self.clear()
        self.set_status(BAD_REQUEST_CODE)
        self.finish(json.dumps({'error': error_message}))


class AddGradingRunHandler(RequestHandlerBase):
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

        if "preprocessing_pipeline" in args:
            for preprocessing_pipeline_stage in args["preprocessing_pipeline"]:
                if "image" not in preprocessing_pipeline_stage:
                    return False, "missing argument \'image\' in preprocessing pipeline stage"

        if "students" not in args:
            return False, "missing argument \'students\'"

        return True, None

    # stage is defined as:
    #  {
    #    "image": <image name>,  REQUIRED
    #    "environment":          OPTIONAL
    #      {
    #         <env var name>: <$env var name/value>, ...
    #      },
    #    "entrypoint": [command, args...], OPTIONAL
    #    "enable_networking": true / false(default), OPTIONAL
    #    "host_name": < hostname of container > OPTIONAL
    #  }
    #
    # Json payload is supposed to have the following contents:
    #
    # "student_pipeline":        REQUIRED
    # [
    #   stage1, stage2, ...
    # ]
    #
    # "students":                REQUIRED
    # [
    #   { <env var name>: <value>, ...}, ...
    # ]
    #
    # "postprocessing_pipeline": OPTIONAL
    # [
    #   stage1, stage2, ...
    # ]
    #
    # "preprocessing_pipeline":  OPTIONAL
    # [
    #   stage1, stage2, ...
    # ]
    #
    # "environment":             OPTIONAL
    #  {
    #   <env var name>: <value>, ...
    #  }
    #
    # adds grading pipeline to DB and returns its ID
    @gen.coroutine
    def post(self):
        if 'json_payload' not in self.request.arguments:
            self.bad_request('\'json_payload\' field missing in request')
            return

        try:
            json_payload = json.loads(escape.to_basestring(self.request.arguments['json_payload'][0]))
        except Exception as ex:
            logging.critical("Decoding exception: {}".format(str(ex)))
            self.bad_request('\'json_payload\' could not be decoded')
            return

        valid, error_message = self.is_valid(json_payload)
        if not valid:
            self.bad_request(error_message)
            return

        # create all student jobs
        student_jobs = []
        for student in json_payload["students"]:
            student_jobs.append(self.create_job("student_pipeline", json_payload, student))

        # create post processing stage if it exists
        postprocessing_job = self.create_job("postprocessing_pipeline",
                                             json_payload) if "postprocessing_pipeline" in json_payload else None

        # create pre processing stage if it exists
        preprocessing_job = self.create_job("preprocessing_pipeline",
                                            json_payload) if "preprocessing_pipeline" in json_payload else None

        # arguments are valid so feed into DB and return run id
        db_handler = self.settings['db_object']
        jobs_collection = db_handler.get_jobs_collection()
        grading_runs_collection = db_handler.get_grading_run_collection()

        grading_run = {'created_at': get_time(), 'student_jobs_left': len(student_jobs)}
        grading_run_id = str(grading_runs_collection.insert_one(grading_run).inserted_id)

        student_job_ids = []
        for student_job in student_jobs:
            job = {'created_at': get_time(), 'grading_run_id': grading_run_id, 'stages': student_job}
            student_job_ids.append(str(jobs_collection.insert_one(job).inserted_id))

        postprocessing_job_id = None
        if postprocessing_job is not None:
            job = {'created_at': get_time(), 'grading_run_id': grading_run_id, 'stages': postprocessing_job}
            postprocessing_job_id = str(jobs_collection.insert_one(job).inserted_id)

        preprocessing_job_id = None
        if preprocessing_job is not None:
            job = {'created_at': get_time(), 'grading_run_id': grading_run_id, 'stages': preprocessing_job}
            preprocessing_job_id = str(jobs_collection.insert_one(job).inserted_id)

        grading_runs_collection.update_one({'_id': ObjectId(grading_run_id)}, {
            "$set": {'student_job_ids': student_job_ids, 'postprocessing_job_id': postprocessing_job_id,
                     'preprocessing_job_id': preprocessing_job_id}})

        # return the run id to user
        self.write(json.dumps({'id': grading_run_id}))
        yield self.flush()

    def create_job(self, pipeline_name, json_payload, job_specific_env=None):
        cur_job = []
        for stage in json_payload[pipeline_name]:
            cur_stage = stage.copy()

            try:
                cur_stage["env"] = expand_env_vars(stage.get("env", {}), json_payload.get("env", {}), job_specific_env)

            except Exception as error:
                self.bad_request("{}: {}".format(pipeline_name, str(error)))
                return
            cur_job.append(cur_stage)
        return cur_job


class GradingRunHandler(RequestHandlerBase):
    # adds grading jobs to queue
    @gen.coroutine
    def post(self, id_):
        db_handler = self.settings['db_object']
        grading_runs_collection = db_handler.get_grading_run_collection()
        grading_run_id = id_
        grading_run = db_handler.get_grading_run(grading_run_id)
        if grading_run is None:
            self.bad_request("Grading Run with id {} does not exist".format(grading_run_id))
            return

        if 'started_at' in grading_run:
            self.bad_request("Grading Run with id {} has already been queued in the past".format(grading_run_id))
            return

        assert "preprocessing_job_id" in grading_run
        if grading_run["preprocessing_job_id"] is None:
            assert "student_job_ids" in grading_run
            for student_job_id in grading_run['student_job_ids']:
                enqueue_job(student_job_id, db_handler)
        else:
            enqueue_job(grading_run["preprocessing_job_id"], db_handler)

        grading_runs_collection.update_one({'_id': ObjectId(grading_run_id)}, {"$set": {'started_at': get_time()}})

    # get statuses of all jobs
    @gen.coroutine
    def get(self, id_):
        db_handler = self.settings['db_object']
        grading_run_id = id_
        grading_run = db_handler.get_grading_run(grading_run_id)
        if grading_run is None:
            self.bad_request("Grading Run with id {} does not exist".format(grading_run_id))
            return

        res = {'student_statuses': []}

        for student_job_id in grading_run['student_job_ids']:
            student_job = db_handler.get_grading_job(student_job_id)
            stages = []
            for stage in student_job['stages']:
                stage_temp = {'image': stage['image'], 'env': dict([env.split("=") for env in stage['env']])}
                stages.append(stage_temp)
            assert student_job is not None
            res['student_statuses'].append(
                {'job_id': student_job_id, 'status': get_status(student_job), 'stages': stages})

        if grading_run['postprocessing_job_id'] is not None:
            postprocessing_job = db_handler.get_grading_job(grading_run['postprocessing_job_id'])
            assert postprocessing_job is not None
            res['postprocessing_status'] = {'job_id': grading_run['postprocessing_job_id'],
                                            'status': get_status(postprocessing_job)}

        if grading_run['preprocessing_job_id'] is not None:
            preprocessing_job = db_handler.get_grading_job(grading_run['preprocessing_job_id'])
            assert preprocessing_job is not None
            res['preprocessing_status'] = {'job_id': grading_run['preprocessing_job_id'],
                                           'status': get_status(preprocessing_job)}

        self.write(json.dumps(res))
        yield self.flush()


# -------------------------------------------


# -------------- Worker Nodes ---------------

class WorkerRegisterHandler(RequestHandlerBase):
    @gen.coroutine
    def get(self):
        db_handler = self.settings['db_object']
        worker_nodes_collection = db_handler.get_workers_node_collection()
        if 'token' not in self.request.arguments:
            self.bad_request('\'token\' field missing in request')
            return

        token = escape.to_basestring(self.request.arguments['token'][0])

        if token != cluster_token:
            self.bad_request('wrong token')
            return

        worker_node = {'last_seen': get_time(), 'running_job_ids': []}
        worker_id = str(worker_nodes_collection.insert_one(worker_node).inserted_id)
        logging.info("Worker {} joined".format(worker_id))

        try:
            self.write(json.dumps({'worker_id': worker_id, 'heartbeat': HEARTBEAT_INTERVAL}))
            yield self.flush()
        except Exception as ex:
            logging.critical(
                "Worker Node {} possibly disconnected. Could not write its worker id to it when grader tried to "
                "register. Error: {}".format(worker_id, str(ex)))


class HeartBeatHandler(RequestHandlerBase):
    @gen.coroutine
    def post(self):
        db_handler = self.settings['db_object']
        worker_nodes_collection = db_handler.get_workers_node_collection()

        if 'worker_id' not in self.request.arguments:
            self.bad_request('\'worker_id\' field missing in request')
            return

        worker_id = escape.to_basestring(self.request.arguments['worker_id'][0])
        worker_node = db_handler.get_worker_node(worker_id)
        if worker_node is None:
            self.bad_request("Worker node with id {} does not exist".format(worker_id))
            return

        worker_nodes_collection.update_one({'_id': ObjectId(worker_id)}, {"$set": {'last_seen': get_time()}})
        logging.info("Heartbeat from {}".format(worker_id))
        yield self.flush()
        self.finish()


class GradingJobHandler(RequestHandlerBase):
    # get a grading job from the queue
    @gen.coroutine
    def get(self):
        # authenticate this get call with the worker id
        if 'worker_id' not in self.request.arguments:
            self.bad_request('\'worker_id\' field missing in request')
            return

        db_handler = self.settings['db_object']
        worker_nodes_collection = db_handler.get_workers_node_collection()
        jobs_collection = db_handler.get_jobs_collection()

        worker_id = escape.to_basestring(self.request.arguments['worker_id'][0])
        worker_node = db_handler.get_worker_node(worker_id)
        if worker_node is None:
            self.bad_request("Worker node with id {} does not exist".format(worker_id))
            return

        # poll from queue, updated job's start time and update worker node's running_job_ids list
        # this will block until a job is available. This might take a while so check if the connection is still alive
        # this is done by self.flush()
        try:
            job = yield tornado.ioloop.IOLoop.current().run_in_executor(None,
                                                                        lambda: job_queue.get(block=True, timeout=18))
        except Exception as e:
            self.write("")  # signals the grader that the queue is empty
            self.finish()
            return

        try:
            self.write(json.dumps(job))
            yield self.flush()
            jobs_collection.update_one({'_id': ObjectId(job['job_id'])}, {"$set": {'started_at': get_time()}})
            worker_node["running_job_ids"].append(job['job_id'])
            worker_nodes_collection.update_one({'_id': ObjectId(worker_id)},
                                               {"$set": {"running_job_ids": worker_node["running_job_ids"]}})
        except Exception as ex:
            # queue the job again if this worker node could not take the job
            job_queue.put(job)
            logging.critical(
                "Worker Node {} possibly disconnected. Could not write polled job to it. Error: {}".format(worker_id,
                                                                                                           str(ex)))


class JobUpdateHandler(RequestHandlerBase):
    def post(self, id_):
        # authenticate this get call with the worker id
        if 'worker_id' not in self.request.arguments:
            self.bad_request('\'worker_id\' field missing in request')
            return

        db_handler = self.settings['db_object']
        worker_nodes_collection = db_handler.get_workers_node_collection()
        jobs_collection = db_handler.get_jobs_collection()
        grading_runs_collection = db_handler.get_grading_run_collection()

        job_id = id_
        worker_id = escape.to_basestring(self.request.arguments['worker_id'][0])
        worker_node = db_handler.get_worker_node(worker_id)
        if worker_node is None:
            self.bad_request("Worker node with id {} does not exist".format(worker_id))
            return

        # check if the job exists
        job = db_handler.get_grading_job(job_id)
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
                                                                        if 'result' in self.request.arguments else None}
                                                               })

        # update worker node: remove this job from its currently running jobs
        assert job_id in worker_node["running_job_ids"]
        worker_node["running_job_ids"].remove(job_id)
        worker_nodes_collection.update_one({'_id': ObjectId(worker_id)},
                                           {"$set": {"running_job_ids": worker_node["running_job_ids"]}})

        # update grading run: if last job finished then update finished_at. Update student_jobs_left if student job.
        # enqueue post processing if all student jobs finished
        job_update_lock.acquire()

        assert "grading_run_id" in job
        grading_run = db_handler.get_grading_run(job["grading_run_id"])

        assert grading_run is not None
        assert "created_at" in grading_run
        assert "started_at" in grading_run
        assert "finished_at" not in grading_run
        assert "student_job_ids" in grading_run
        assert "student_jobs_left" in grading_run
        assert "postprocessing_job_id" in grading_run
        assert "preprocessing_job_id" in grading_run

        if grading_run["preprocessing_job_id"] == job_id:
            for student_job_id in grading_run['student_job_ids']:
                enqueue_job(student_job_id, db_handler)
        elif grading_run["postprocessing_job_id"] == job_id:
            assert grading_run["student_jobs_left"] == 0
            grading_runs_collection.update_one({'_id': ObjectId(job["grading_run_id"])},
                                               {"$set": {"finished_at": get_time()}})
        else:
            # this is a student's job
            assert grading_run["student_jobs_left"] > 0
            grading_runs_collection.update_one({'_id': ObjectId(job["grading_run_id"])},
                                               {"$inc": {"student_jobs_left": -1}})

            if grading_run["student_jobs_left"] == 1:
                # this was the last student job which finished so if post processing exists then schedule it
                if grading_run["postprocessing_job_id"] is None:
                    grading_runs_collection.update_one({'_id': ObjectId(job["grading_run_id"])},
                                                       {"$set": {"finished_at": get_time()}})
                else:
                    enqueue_job(grading_run["postprocessing_job_id"], db_handler)

        job_update_lock.release()


# -------------------------------------------

def set_up_logger():
    if not os.path.exists(LOGS_DIR_NAME):
        os.makedirs(LOGS_DIR_NAME)
    logging.basicConfig(filename='{}/{}.log'.format(LOGS_DIR_NAME, get_time()), level=logging.DEBUG)


def initialize_cluster_token():
    global cluster_token
    cluster_token = generate_random_key(30)
    print("Nodes can join the cluster using token: {}".format(cluster_token))


if __name__ == "__main__":
    # set up logger
    set_up_logger()

    # set up cluster token
    initialize_cluster_token()

    app = make_app(DatabaseResolver())
    app.listen(PORT)

    # start heartbeat checker as a background task
    heartbeat_validator_thread = Thread(target=heartbeat_validator, args=[app.settings["db_object"]])
    heartbeat_validator_thread.start()

    # start the API
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
