import logging

from broadway.api.daos.base import BaseDao

logger = logging.getLogger(__name__)


def validate_obj_size(func):
    def wrapper(*args, **kwargs):
        base_dao: BaseDao = args[0]
        obj = args[1]

        if base_dao.is_obj_size_valid(obj):
            return func(*args, **kwargs)

        logger.critical(
            "Bson document larger than the maximum bson size as specified by the mongo"
            "client. Not saving the object."
        )
        return

    return wrapper
