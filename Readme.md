# Broadway API
[![Build Status](https://www.travis-ci.com/illinois-cs241/broadway-api.svg?branch=master)](https://www.travis-ci.com/illinois-cs241/broadway-api)
[![Coverage Status](https://coveralls.io/repos/github/illinois-cs241/broadway-api/badge.svg?branch=master)](https://coveralls.io/github/illinois-cs241/broadway-api?branch=master)
## Running API Server
Install the required packages specified in [requirements.txt](requirements.txt) by:
```shell
pip install -r requirements.txt
```

Run the Mongo Daemon as a background process:
```shell
mongod &
```
Then start up the server:
```shell
python src/api.py
```

## Endpoints

### POST api/v1/grading_run
Used to add a grading run. Returns ID of the grading run.

Data body information:
```
{
  'json_payload': <JSON string of grading run json object>
}
```
Grading Run JSON object:
```
{
     'student_pipeline':  [ stage1, stage2, ... ],              REQUIRED
     'students': [ { <env var name>: <value>, ...}, ... ],       REQUIRED
     'postprocessing_pipeline':  [  stage1, stage2, ... ],      OPTIONAL
     'env': {  <env var name>: <value>, ...  },         OPTIONAL
}
```
Stage object:
```
{
  'image': <image name>,                                        REQUIRED
  'env': { <env var name>: <$env var name/value>, ... } OPTIONAL
}
```
Returns JSON string of:
```
{
  'id': <grading run id as hex string>
}
```

### GET api/v1/grading_run/{id}
Returns the statuses of all grading job under the grading run of given id.

Returns JSON string of:
```
{
  'student_statuses': 
    [
      {
        'id': <student job id>, 
        'student': [stage1, stage2, ... ],
        'status': <status of job>
      }, ...
    ], 
  'postprocessing status':                                      OPTIONAL
    { 
      'id': <post processing job id>, 
      'status': <status of job> 
    } 
}
```

### POST api/v1/grading_run/{id}
Starts the grading run with the given ID. Queues all the internal student grading jobs.

### GET api/v1/grading_job
This endpoint is used by the worker nodes (graders). Returns a job from the queue. Error if queue is empty

Returns JSON string of:
```
{
  'id': <job id>,
  'stages': [ stage1, stage2, ... ]
}
```
Note that all environment variables which could be expanded have been expanded here. This is all the information the grader needs to grade.

### GET /api/v1/worker_register
This endpoint is used by the worker nodes to register themselves and get the worker id

Data body information:
```
{
    'token': <cluster token>
}
```

Returns JSON string of:
```
{
     'id':  <worker id>
}
```

## Testing
Please run/modify the [tests](test_api.py) each time a change is made to the logic or structure. Run tests by:
```shell
pytest
```
