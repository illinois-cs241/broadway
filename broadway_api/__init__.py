from queue import Queue
from flagset import Flag, FlagSet


def gen_global_settings(flags):
    return {
        "CLUSTER_TOKEN": flags["token"],
        "FLAGS": flags,
        "DB": None,
        "QUEUE": Queue(),
        "WS_CONN_MAP": {},
    }


def gen_flags():
    fset = FlagSet(
        {
            "debug": Flag(
                bool,
                default=False,
                cmdline_name=["-d", "--debug"],
                help="enable debug mode",
            ),
            "token": Flag(
                str,
                required=True,
                cmdline_name="--token",
                help="access token for communication between workers and api",
            ),
            "heartbeat_interval": Flag(
                int,
                default=10,
                config_name="heartbeat_interval",
                help="heartbeat interval",
            ),
            "course_config": Flag(
                str, cmdline_name="--course-config", help="optional course config file"
            ),
            # web app flags
            "bind_addr": Flag(
                str,
                default="localhost",
                cmdline_name="--bind-addr",
                help="web app bind address",
            ),
            "bind_port": Flag(
                str,
                default="1470",
                cmdline_name="--bind-port",
                help="web app bind port",
            ),
            # log flags
            "log_dir": Flag(
                str, default="logs", config_name="log.dir", help="directory for logs"
            ),
            "log_level": Flag(
                str, default="INFO", config_name="log.level", help="logging level"
            ),
            "log_rotate": Flag(
                str,
                default="midnight",
                config_name="log.rotate",
                help="time for log rotate",
            ),
            "log_backup": Flag(
                int, default=7, config_name="log_backup", help="backup count for logs"
            ),
            # mongo db flags
            "mongodb_dsn": Flag(
                str,
                default="mongodb://localhost:27017",
                cmdline_name="--mongodb-dsn",
                help="data source name for mongodb",
            ),
            "mongodb_primary": Flag(
                str,
                default="AG",
                config_name="mongodb.primary",
                help="name of the primary database",
            ),
            "mongodb_logs": Flag(
                str,
                default="logs",
                config_name="mongodb.logs",
                help="name of the logging database",
            ),
            "mongodb_timeout": Flag(
                int,
                default=5,
                config_name="mongodb.timeout",
                help="timeout for mongodb connection",
            ),
        }
    )

    return fset
