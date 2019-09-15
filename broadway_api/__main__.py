import logging

import tornado.ioloop

from flagset import FlagSet

from broadway_api.utils.bootstrap import (
    initialize_global_settings,
    initialize_database,
    initialize_logger,
    initialize_course_tokens,
    initialize_signal_handler,
    initialize_app,
)

from broadway_api.flags import app_flags

logger = logging.getLogger(__name__)


def __main__():
    flags = FlagSet(app_flags).parse()
    settings = initialize_global_settings(flags)

    initialize_logger(settings, flags)
    initialize_database(settings, flags)
    initialize_course_tokens(settings, flags)
    initialize_signal_handler(settings, flags)
    initialize_app(settings, flags)

    logger.info("ready to serve")

    tornado.ioloop.IOLoop.current().start()

    tornado.ioloop.IOLoop.current().close(all_fds=True)
    settings["DB"].close()

    logger.info("shutted down")


if __name__ == "__main__":
    __main__()
