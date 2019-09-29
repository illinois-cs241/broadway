from flagset import Flag, FlagSet

fset = FlagSet(
    {
        "token": Flag(
            str,
            cmdline_name="token",
            env_name="BROADWAY_TOKEN",
            config_name="token",
            help="cluster token",
        ),
        "grader_id": Flag(
            str,
            cmdline_name="grader-id",
            env_name="BROADWAY_GRADER_ID",
            config_name="id",
            help="unique identifier of the grader instance",
        ),
        "api_host": Flag(
            str,
            default="http://127.0.0.1:1470",
            cmdline_name="--api-host",
            env_name="BROADWAY_API_HOST",
            config_name="api_host",
            help="api host. no slash in the end. "
            + "supported protocols: ws(s) and http(s)",
        ),
        "verbose": Flag(
            bool,
            default=False,
            cmdline_name=["-v", "--verbose"],
            env_name="BROADWAY_VERBOSE",
            config_name="verbose",
            help="verbose mode",
        ),
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
    }
)
