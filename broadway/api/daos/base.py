from typing import Optional

import bson

from pymongo import MongoClient
from pymongo.collection import Collection


class BaseDao:
    ID = "_id"

    def __init__(self, settings):
        self._config: dict = settings["FLAGS"]
        self._client: MongoClient = settings["DB"]

    def _get_log_collection(self, collection_name) -> Collection:
        return self._client[self._config["mongodb_logs"]][collection_name]

    def _get_primary_collection(self, collection_name) -> Collection:
        return self._client[self._config["mongodb_primary"]][collection_name]

    def _to_store(self, obj) -> dict:
        raise NotImplementedError("_to_store not implemented")

    def _from_store(self, obj) -> Optional["BaseDao"]:
        raise NotImplementedError("_from_store not implemented")

    def is_obj_size_valid(self, obj) -> bool:
        return len(bson.BSON.encode(self._to_store(obj))) <= self._client.max_bson_size
