# Broadway API
[![Build Status](https://www.travis-ci.com/illinois-cs241/broadway-api.svg?branch=master)](https://www.travis-ci.com/illinois-cs241/broadway-api)
[![Coverage Status](https://coveralls.io/repos/github/illinois-cs241/broadway-api/badge.svg?branch=master)](https://coveralls.io/github/illinois-cs241/broadway-api?branch=master)
## Running API Server
Requires Python 3.5+ Install the required packages specified in [requirements.txt](requirements.txt) by:
```shell
pip install -r requirements.txt
```

Set up Mongo DB and start the Mongo Daemon:
```shell
mkdir -p /tmp/mongo/data
mongod --dbpath /tmp/mongo/data 2>&1 > /dev/null &
```

Then start up the server:
```shell
python -m src.api
```

## Starting a Grading Run
Provided a [simple script](utils/start_run_script.py) to start a grading run. Make sure `HOST` and `PORT` are set correctly. Usage:
```shell
python start_run_script.py <path to valid pipeline json>
```

## Note
Almost none of the functions are thread-safe since we have used Tornado's [Asynchronous and non-blocking IO](http://www.tornadoweb.org/en/stable/guide/async.html). Every function which has the `@gen.coroutine` decorator is a [Generator-based coroutine](https://www.tornadoweb.org/en/stable/gen.html). This has been done because Tornado uses a single-threaded event loop. This means that all application code **must** aim to be asynchronous and non-blocking because only one operation can be active at a time. Therefore, please consult the above mentioned guides before injecting a blocking call as that might tamper with the entire distributed system. For instance, it will prevent the API from listening to heartbeats and hence misclassifying worked nodes to be dead.

## Client Endpoints
Clients might be students if we give them the freedom to create their own AG runs at their will through a web app we provide. Clients can be other courses too. (Hopefully other universities too!)

### POST api/v1/grading_run
Used to add a grading run. Returns ID of the grading run.

Arguments:
```
'json_payload': <JSON string of grading run json object>
```
Grading Run JSON object:
```
{
     'student_pipeline':  [ stage1, stage2, ... ],          REQUIRED
     'students': [ { <env var name>: <value>, ...}, ... ],  REQUIRED
     'postprocessing_pipeline':  [  stage1, stage2, ... ],  OPTIONAL
     'env': {  <env var name>: <value>, ...  },             OPTIONAL
}
```
Stage object:
```
{
  'image': <image name>,                                    REQUIRED
  'env': { <env var name>: <$env var name/value>, ... },    OPTIONAL
  'entrypoint': [command, args...],                         OPTIONAL
  'enable_networking': true / false(default),               OPTIONAL
  'host_name': <hostname of container>                      OPTIONAL
}
```
Returns JSON string of:
```
{
  'id': <grading run id>
}
```

### GET api/v1/grading_run/{grading run id}
Returns the statuses of all grading job under the grading run of given id.

Returns JSON string of:
```
{
  'student_statuses': 
    [
      {
        'job_id': <student job id>, 
        'stages': [stage1, stage2, ... ],
        'status': <status of job>
      }, ...
    ], 
  'postprocessing_status':                                 OPTIONAL
    { 
      'job_id': <post processing job id>, 
      'status': <status of job> 
    } 
}
```

### POST api/v1/grading_run/{id}
Starts the grading run of the given id. Queues all the internal student grading jobs. If the postprocessing job exists, it will be queued after all student jobs have completed.

## Grader Endpoints
Worker nodes ([Graders](https://github.com/illinois-cs241/broadway-grader)) join this system using the cluster token the API spits out when it starts. They can only use these endpoints using their worker ids for auth.

### GET api/v1/worker_register
This endpoint is used by workers to register themselves in the system and get their worker id. Needs the cluster token for auth.

Arguments:
```
token: <cluster token>
```
Returns the JSON string of:
```
{
     'worker_id': <worker id>,
     'heartbeat': <required heartbeat interval (secs)>
}
```

### GET api/v1/grading_job
Used to poll the queue for a job. If the queue is empty, sets the status code to [this](src/settings.py#L5)

Arguments:
```
worker_id: <worker id>
```

Returns the JSON string if successful in polling the quue:
```
{
  'job_id': <job id>,
  'stages': [ stage1, stage2, ... ]
}
```
Note that all environment variables which could be expanded have been expanded here. This is all the information the grader needs to grade.

### POST api/v1/grading_job/{job id}
Used to update the server when grading a job has finished successfully or unsuccessfully. If this update was for the last student job then starts the postprocessing job if available. If last job finished then concludes that grading run.

Arguments:
```
worker_id: <worker id>
result: <any string describing the result, error messages, etc>
```

### POST api/v1/heartbeat
Used to register a heartbeat.

Arguments:
```
worker_id: <worker id>
```

## Testing
Please run/modify the [tests](tests) each time a change is made to the logic or structure. Mongo deamon needs to be up. Run tests using:
```shell
pytest
```
