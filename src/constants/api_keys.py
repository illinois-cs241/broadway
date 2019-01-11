# these constants are used in the header and body of requests

AUTH = "Authorization"
WORKER_ID = "worker_id"
HEARTBEAT = "heartbeat"
JOB_ID = "job_id"
STAGES = "stages"
STUDENTS = "students"
PRE_PROCESSING_PIPELINE = "pre_processing_pipeline"
POST_PROCESSING_PIPELINE = "post_processing_pipeline"
STUDENT_PIPELINE = "student_pipeline"
RUN_ID = "grading_run_id"
RESULTS = "results"
SUCCESS = "success"
LOGS = "logs"

# these define url parameters
HOSTNAME_PARAM = "hostname"
WORKER_ID_PARAM = "worker_id"
COURSE_ID_PARAM = "course_id"
ASSIGNMENT_NAME_PARAM = "assignment_name"


# these let us specify the stage specs
IMAGE = "image"
ENV = "env"
ENTRY_POINT = "entry_point"
NETWORKING = "enable_networking"
HOST_NAME = "host_name"
TIMEOUT = "timeout"
