### General ###
# the port to which the API will bind
PORT = 1470
# whether or not to use debug mode
DEBUG = False

### Logging ###
# the path in which logs will be stored
LOGS_DIR = "logs"
# the time of day when logs will be rotated
LOGS_ROTATE_WHEN = 'midnight'
# the number of logs to keep on disk
LOGS_BACKUP_COUNT = 7

### Database ###
# the MongoDB host
DB_HOST = "127.0.0.1"
# the MongoDB port
DB_PORT = 27017
# the name of the MongoDB database where all API metadata are kept
DB_PRIMARY = "AG"
# the name of the MongoDB database where all grading logs are kept
DB_LOGS = "logs"
# the MongoDB filesystem path
DB_PATH = "/data/db"

### Networking ###
# the number of seconds until a heartbeat is sent to a worker
HEARTBEAT_INTERVAL = 10