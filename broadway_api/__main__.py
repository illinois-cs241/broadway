import logging

import tornado.ioloop

from broadway_api.bootstrap import (
    initialize_logger,
    initialize_database,
    initialize_course_tokens,
    initialize_signal_handler,
    initialize_app,
)

from broadway_api import gen_global_settings, gen_flags

logger = logging.getLogger("broadway.main")


def __main__():
    flags = gen_flags().parse()
    settings = gen_global_settings(flags)

    initialize_logger(settings, flags)
    initialize_database(settings, flags)
    initialize_course_tokens(settings, flags)
    initialize_signal_handler(settings, flags)
    initialize_app(settings, flags)

    logger.info("ready to serve")
    tornado.ioloop.IOLoop.instance().start()


if __name__ == "__main__":
    __main__()
