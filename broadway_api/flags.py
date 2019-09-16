from flagset import Flag, FlagSet

app_flags = FlagSet(
    {
        "debug": Flag(
            bool,
            default=False,
            cmdline_name=["-d", "--debug"],
            env_name="BROADWAY_DEBUG",
            config_name="debug",
            help="enable debug mode",
        ),
        "token": Flag(
            str,
            cmdline_name="--token",
            env_name="BROADWAY_TOKEN",
            config_name="token",
            help="access token for communication between workers and api",
        ),
        "heartbeat_interval": Flag(
            int,
            default=10,
            cmdline_name="--heartbeat-interval",
            config_name="heartbeat_interval",
            help="heartbeat interval in seconds",
        ),
        "course_config": Flag(
            str,
            cmdline_name="--course-config",
            config_name="course_config",
            help="optional course config file."
            + "if given, the existing config will be overwritten;"
            + "otherwise, the existing config will be used",
        ),
        # web app flags
        "bind_addr": Flag(
            str,
            default="localhost",
            cmdline_name="--bind-addr",
            env_name="BROADWAY_BIND_ADDR",
            config_name="bind_addr",
            help="web app bind address",
        ),
        "bind_port": Flag(
            str,
            default="1470",
            cmdline_name="--bind-port",
            env_name="BROADWAY_BIND_PORT",
            config_name="bind_port",
            help="web app bind port",
        ),
        # log flags
        "log_dir": Flag(
            str,
            default="logs",
            cmdline_name="--log-dir",
            env_name="BROADWAY_LOG_DIR",
            config_name="log.dir",
            help="directory for logs",
        ),
        "log_level": Flag(
            str,
            default="INFO",
            cmdline_name="--log-level",
            env_name="BROADWAY_LOG_LEVEL",
            config_name="log.level",
            help="logging level, e.g. INFO, DEBUG",
        ),
        "log_timestamps": Flag(
            bool,
            default=True,
            cmdline_name="--log-timestamps",
            env_name="BROADWAY_LOG_TIMESTAMPS",
            config_name="log.timestamps",
            help="whether to include timestamps in logs",
        ),
        "log_rotate": Flag(
            str,
            default="midnight",
            cmdline_name="--log-rotate",
            env_name="BROADWAY_LOG_ROTATE",
            config_name="log.rotate",
            help="time for log rotate",
        ),
        "log_backup": Flag(
            int,
            default=7,
            cmdline_name="--log-backup",
            env_name="BROADWAY_LOG_BACKUP",
            config_name="log.backup",
            help="backup count for logs",
        ),
        # mongo db flags
        "mongodb_dsn": Flag(
            str,
            default="mongodb://localhost:27017",
            cmdline_name="--mongodb-dsn",
            env_name="BROADWAY_MONGODB_DSN",
            config_name="mongodb.dsn",
            help="data source name for mongodb",
        ),
        "mongodb_primary": Flag(
            str,
            default="AG",
            cmdline_name="--mongodb-primary",
            env_name="BROADWAY_MONGODB_PRIMARY",
            config_name="mongodb.primary",
            help="name of the primary database",
        ),
        "mongodb_logs": Flag(
            str,
            default="logs",
            cmdline_name="--mongodb-logs",
            env_name="BROADWAY_MONGODB_LOGS",
            config_name="mongodb.logs",
            help="name of the logging database",
        ),
        "mongodb_timeout": Flag(
            int,
            default=5,
            cmdline_name="--mongodb-timeout",
            env_name="BROADWAY_MONGODB_TIMEOUT",
            config_name="mongodb.timeout",
            help="timeout for mongodb connection",
        ),
    }
)
