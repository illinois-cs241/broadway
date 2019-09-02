from queue import Queue

from broadway_api.utils import flag


def gen_global_settings(flags):
    return {
        "CLUSTER_TOKEN": flags["token"],
        "FLAGS": flags,
        "DB": None,
        "QUEUE": Queue(),
        "WS_CONN_MAP": {},
    }


def gen_flags():
    fset = flag.FlagSet()

    fset.add_flag(
        "debug",
        bool,
        default=False,
        env="BROADWAY_DEBUG",
        config_name="debug",
        help="debug mode",
    )

    fset.add_flag(
        "token",
        str,
        required=True,
        env="BROADWAY_TOKEN",
        config_name="token",
        help="access token for communication between workers and api",
    )
    fset.add_flag(
        "heartbeat-interval",
        int,
        default=10,
        config_name="heartbeat_interval",
        help="heartbeat interval",
    )
    fset.add_flag(
        "course-config",
        str,
        config_name="course_config",
        help="optional course config file",
    )

    # web app flags
    fset.add_flag(
        "bind-addr",
        str,
        default="localhost",
        env="BROADWAY_BIND_ADDR",
        config_name="bind_addr",
        help="web app bind address",
    )
    fset.add_flag(
        "bind-port",
        str,
        default="1470",
        env="BROADWAY_BIND_PORT",
        config_name="bind_port",
        help="web app bind port",
    )

    # log flags
    fset.add_flag(
        "log-dir",
        str,
        default="logs",
        env="BROADWAY_LOG_DIR",
        config_name="log_dir",
        help="directory for logs",
    )
    fset.add_flag(
        "log-level",
        str,
        default="INFO",
        env="BROADWAY_LOG_LEVEL",
        config_name="log_level",
        help="logging level",
    )
    fset.add_flag(
        "log-rotate",
        str,
        default="midnight",
        env="BROADWAY_LOG_ROTATE",
        config_name="log_rotate",
        help="time for log rotate",
    )
    fset.add_flag(
        "log-backup",
        int,
        default=7,
        env="BROADWAY_LOG_BACKUP",
        config_name="log_backup",
        help="backup count for logs",
    )

    # mongo db flags
    fset.add_flag(
        "mongodb-dsn",
        str,
        default="mongodb://localhost:27017",
        env="BROADWAY_MONGODB_DSN",
        config_name="mongodb_dsn",
        help="data source name for mongodb",
    )
    fset.add_flag(
        "mongodb-primary",
        str,
        default="AG",
        env="BROADWAY_MONGODB_PRIMARY",
        config_name="mongodb_primary",
        help="name of the primary database",
    )
    fset.add_flag(
        "mongodb-logs",
        str,
        default="logs",
        env="BROADWAY_MONGODB_LOGS",
        config_name="mongodb_logs",
        help="name of the logging database",
    )
    fset.add_flag(
        "mongodb-timeout",
        int,
        default=5,
        env="BROADWAY_MONGODB_TIMEOUT",
        config_name="mongodb_timeout",
        help="timeout for mongodb connection",
    )

    return fset
