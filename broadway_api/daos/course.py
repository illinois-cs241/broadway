from broadway_api.daos.base import BaseDao
from broadway_api.models import Course


class CourseDao(BaseDao):
    ID = "_id"
    TOKENS = "tokens"
    _COLLECTION = "course"

    def __init__(self, app):
        super().__init__(app)
        self._collection = self._get_collection(
            self._config["DB_PRIMARY"], CourseDao._COLLECTION
        )

    def insert_or_update(self, obj):
        document = self._to_store(obj)
        return self._collection.update_one(
            {CourseDao.ID: obj.id}, {"$set": document}, upsert=True
        )

    def find_by_id(self, id_):
        return self._from_store(self._collection.find_one({CourseDao.ID: id_}))

    def drop_all(self):
        return self._collection.delete_many({})

    def _from_store(self, obj):
        if obj is None:
            return None
        attrs = {"id_": obj.get(CourseDao.ID), "tokens": obj.get(CourseDao.TOKENS)}
        return Course(**attrs)

    def _to_store(self, obj):
        return {CourseDao.ID: obj.id, CourseDao.TOKENS: obj.tokens}
