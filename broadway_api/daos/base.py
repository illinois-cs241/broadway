class BaseDao:
    ID = "_id"

    def __init__(self, app):
        self._config = app["CONFIG"]
        self._client = app["DB"]

    def _get_collection(self, db_name, collection_name):
        return self._client[db_name][collection_name]

    def _to_store(self, obj):
        raise NotImplementedError("_to_store not implemented")

    def _from_store(self, obj):
        raise NotImplementedError("_from_store not implemented")
