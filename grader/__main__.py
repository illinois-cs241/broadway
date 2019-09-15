import os
import logging

from logging.handlers import TimedRotatingFileHandler

from grader.flags import fset
from grader.ws import run_ws_grader
from grader.http import run_http_grader


def _initialize_logger(flags):
    log_dir = flags["log_dir"]
    log_level = flags["log_level"]
    log_rotate = flags["log_rotate"]
    log_backup = flags["log_backup"]

    os.makedirs(log_dir, exist_ok=True)

    rotating_handler = TimedRotatingFileHandler(
        "{}/log".format(log_dir), when=log_rotate, backupCount=log_backup
    )

    logging.basicConfig(
        handlers=[rotating_handler, logging.StreamHandler()],
        format="%(asctime)s %(levelname)s %(module)s.%(funcName)s: %(message)s",
        level=log_level,
    )


def __main__():
    flags = fset.parse()
    _initialize_logger(flags)

    api_host = flags["api_host"]

    if api_host.startswith("wss://") or api_host.startswith("ws://"):
        run_ws_grader(flags)
    elif api_host.startswith("https://") or api_host.startswith("http://"):
        run_http_grader(flags)
    else:
        raise RuntimeError("unsupported protocol: {}".format(api_host))


if __name__ == "__main__":
    __main__()
