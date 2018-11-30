# Broadway API
[![Build Status](https://www.travis-ci.com/illinois-cs241/broadway-api.svg?branch=master)](https://www.travis-ci.com/illinois-cs241/broadway-api)
[![Coverage Status](https://coveralls.io/repos/github/illinois-cs241/broadway-api/badge.svg?branch=master)](https://coveralls.io/github/illinois-cs241/broadway-api?branch=master)
![License](https://img.shields.io/badge/license-NCSA%2FIllinois-blue.svg)
![Python Versions](https://img.shields.io/badge/python-3.5%20%7C%203.6-blue.svg)
[![Maintainability](https://api.codeclimate.com/v1/badges/490d3846207832df47fd/maintainability)](https://codeclimate.com/github/illinois-cs241/broadway-api/maintainability)

Broadway API is a webserver that receives, distributes, and keeps track of grading jobs and runs.
The overarching aim of this project is to provide a generic interface to a distributed autograding system which multiple courses can use. We aim at providing the following benefits:
* System which is reliable and easy to scale
* Faster AG runs
* More stable and reliable AG runs. No one student can break the entire AG run.
* Easier to track and debug failed grading jobs

## Terminology
### Grading Pipeline
Consists of one or more pipeline stages which are run sequentially (as per order specified in the config). Each pipeline stage consists of a Docker image and zero or more environment variables, some of which might consist of templates. A pipeline could either be used for the grading of a single student’s work, or be a (pre/post)-processing pipeline (described below).

An instance of a grading pipeline is called a **grading job**. For example, there will be many instances of student grading pipeline where the pipeline stages will be templated with the different NetIDs, the current assignment ID and so on. Worker nodes will be executing these grading jobs.

The entire grading job is aborted if a stage fails (container timeout or non-zero exit code). In case of failure, subsequent stages in the pipeline sequence are not executed and the grading job itself fails.

### Grading Run
Usually represents the grading of a single assignment. Consists of the following:
* **Pre-processing grading job**: This is optional. Has access to the grading run roster. This is executed before any student grading job is scheduled. Student jobs are only scheduled if this succeeds. If not defined, student jobs are scheduled right away.
* **Student grading jobs**: Consists of many grading jobs which will be executed simultaneously across many graders. Ideally each job grades one student. The grading run is unaffected by the failure of any of these jobs (since student's code might break things, timeout the containers, etc).
* **Post-processing grading job**: This is optional. Has access to the grading run roster. This is executed after all student jobs have finished.

## Config File (JSON)
We can define a grading run entirely through this one config file in JSON format. [Example](utils/grading_run.json)

### Grading Run Config
```
{
    'students':                 [ { <env var name>: <value>, ...}, ... ],   REQUIRED
    'student_pipeline':         [pipeline stage 1, ...],                    REQUIRED
    'pre_processing_pipeline':  [pipeline stage 1, ...]                     OPTIONAL
    'post_processing_pipeline': [pipeline stage 1, ...],                    OPTIONAL
    'env': {  <env var name>: <value>, ...  },                              OPTIONAL
}
```
**Notes**
* `students` - This field contains a list of dictionaries. Each dictionary contains environment variables which probably describe template arguments for that student job. Those environment variables are private to the respective student grading job. This is a good place to put student job specific templates, like `NET_ID`, etc.  Number of student jobs = the length of this list. This is also acts as the grading run roster (since it contains all NetIDs or any form of student ID).
* `env` - This field contains a dictionary of environment variables. These will be *global*, i.e. they will be available to all grading jobs (including pre and post processing jobs). This is a good place to add templates like `DUE_DATE`, `ASSIGNMENT_ID`, etc.
* `pipeline` - Pipeline above is just a list of one or more pipeline stages (their JSON representation is described below).

### Pipeline Stage Config
```
{
    'image': <image name>,                                      REQUIRED
    'env': { <env var name>: <$env var name/value>, ... },      OPTIONAL
    'entry_point': [command, args...],                          OPTIONAL
    'enable_networking': true / false                           OPTIONAL
    'host_name': <hostname for the container>                   OPTIONAL
    'timeout': <timeout for the container in seconds>           OPTIONAL
}
```
**Notes**
The following settings are configurable for the containers
* `image` - This image should be public on Docker Hub. It would be impractical to build them manually on every grading machine as these will keep changing as we scale up or as machines break.
* `env` - This field contains a dictionary of environment variables. These are private to that specific pipeline stage. This is a good place to specify stage specific environment variables.
* `entry_point` - Sets the command and parameters that will be executed first when a container is run. No commands are run if not specified.
* `enable_networking` - Completely disables the networking stack on a container if set to false. Defaults to false if not specified. Good security feature when running student's code.
* `host_name` - Gives the flexibility to specify hostname of the container running the stage. Helpful in case any grading step (like compilation) is dependent on the hostname. Possible security feature.
* `timeout` - The default timeout is **30 seconds**. The grading job fails if any stage times out.

## Running API Server
Make sure that [MongoDB](https://www.mongodb.com/) is installed. [Installation guide](https://docs.mongodb.com/manual/tutorial/install-mongodb-on-ubuntu/)
Requires Python 3.5+ Install the required packages specified in [requirements.txt](requirements.txt) using:
```shell
pip install -r requirements.txt
```

Then start up the server:
```shell
python -m src.api
```

To use the MongoDB we need the Mongo Daemon running in the background. Currently the API server starts up the Mongo Daemon as a background process on start up but if you want to access the DB when the API is down you will have to start Mongo Daemon manually. You can do so using:
```shell
mkdir -p <DB_PATH>
mongod --dbpath <DB_PATH> 2>&1 > /dev/null &
```
`DB_PATH` can be found in the [config file](src/config.py).

## Starting a Grading Run
We provide a [sample script](utils/start_run_script.py) to start a grading run. Make sure `HOST` and `PORT` are set correctly. Usage:
```shell
python start_run_script.py <path to config json> <cluster token>
```
It is recommended to build a CLI which can generate the required config files and start the grading run (so that AG run scheduling can be automated).

## Design Considerations

### File sharing between stages
All containers (representing grading stages) in a grading job can share files between each other using the `/job/` directory inside the container. Before the start of any grading job, we create a temporary directory (which is completely destroyed once the job is over) on the local FS and mount that directory onto all the containers for the grading job at path `/job/` in the container. So for example if the first container writes to `/job/file.txt`, subsequent containers should be able to see that file at the same path.

### Roster
The roster is defined as contents of `students` field in the grading run config. Pre and post processing job containers will have the roster available as a JSON file at path `/job/roster.json` in the containers.

### Publishing grades at the same time
A course might want to publish grades for all students at the same time as opposed to releasing a student's grade as soon as their grading job finishing. Sometimes grading runs might take really long and it will be unfair to some students who will get their results much later than others.

A course can build a service (which is customized for them) which is responsible for collecting results and publishing them. We could use the *pre-processing grading job* to register a grading run to that service along with the roster (so the service only accepts results for students in the roster). Each *student grading job* can generate a result (in any form) and post it to the service. The *post processing stage* can just signal the service to publish the grades then.

### Blocking calls
Please be cautious of adding blocking calls in the application logic because Tornado uses a single-threaded event loop. Therefore, one blocking call will prevent the API from serving requests and hence tamper with the entire distributed system. For instance, it might prevent the API from listening to heartbeats and as a result the server will consider worker nodes to be dead.

If you want to use blocking calls, please make them asynchronous. [Asynchronous and non-blocking IO guide for Tornado](http://www.tornadoweb.org/en/stable/guide/async.html)

## Client Endpoints
These are the endpoints used by courses to schedule AG runs and check statuses.

### Auth
All client requests have to be authorized via the cluster token (as for now, later we could integrate this with Shib).

All client request headers should have the format:
```
{
    'Authorization': <cluster token>
}
```

### POST api/v1/grading_run
Used to add a grading run. Responds with the newly generated grading run ID.

**Request Body** - JSON string of the grading run config

**Returns** - JSON string of:
```
{
    'status':  'description of status of request',
    'data': {
        'grading_run_id': <grading run id>
    }
}
```

### GET api/v1/grading_run/{grading run id}
Returns the statuses of all grading job under the grading run of given id.

Not implemented yet. Will be useful when building a web app around this system to schedule runs.

### POST api/v1/grading_run/{id}
Starts the grading run of the given id. Queues pre-processing job if it exists otherwise queues all the student grading jobs.

## Grader Endpoints
These endpoints are only meant for worker nodes ([Graders](https://github.com/illinois-cs241/broadway-grader)) in the cluster.

### Auth
All grader request headers should have the format (except register endpoint which does not require worker id since you only get the worker id after registration). Describes a two layer authentication since workers need both the cluster token and a worker ID which is an [ObjectId](https://docs.mongodb.com/manual/reference/method/ObjectId/).
```
{
    'Authorization': <cluster token>,
    'worker_id': <worker id>
}
```

### GET api/v1/worker_register
This endpoint is used by workers to register themselves into the cluster and get their worker id. Needs the cluster token for auth.

**Returns** - JSON string of:
```
{
    'status':  'description of status of request',
    'data': {
        'worker_id': <worker id>,
        'heartbeat': <required heartbeat interval (secs)>
    }
}
```

### GET api/v1/grading_job
Used to poll the queue for a job. If the queue is empty, sets the status code to `QUEUE_EMPTY_CODE` mentioned in the [config file](src/config.py).

**Returns** - JSON string if successful in polling the queue:
```
{
    'status':  'description of status of request',
    'data': {
        'job_id':     <job id>,
        'stages':     [ stage1, stage2, ... ]
        'students':   [ { <env var name>: <value>, ...}, ... ],   OPTIONAL (this is the roster which is only provided for pre/post processing jobs)
    }
}
```

### POST api/v1/grading_job/{job id}
Used to update the server when grading a job has finished (successfully or unsuccessfully).

**Request Body** - JSON string of
```
{
    'status':  'description of status of request',
    'data': {
        'success': true/false (if the job successfully completed or had to be aborted),
        'info': [{stage result info object}, ...]
    }
}
```

### POST api/v1/heartbeat
Used to register a heartbeat.


## Testing
Please run/modify the [tests](tests) each time a change is made to the logic or structure. You can run tests using [pytest](https://docs.pytest.org/en/latest/).