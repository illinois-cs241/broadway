from broadway_api.daos.base import BaseDao
from broadway_api.models.assignment_config import AssignmentConfig


class AssignmentConfigDao(BaseDao):
    ID = "_id"
    ENV = "env"
    STUDENT_PIPELINE = "student_pipeline"
    PRE_PROCESSING_PIPELINE = "pre_processing_pipeline"
    POST_PROCESSING_PIPELINE = "post_processing_pipeline"
    _COLLECTION = "assignment_config"

    def __init__(self, app):
        super().__init__(app)
        self._collection = self._get_collection(
            self._config["DB_PRIMARY"], AssignmentConfigDao._COLLECTION
        )

    @staticmethod
    def id_from(course_id, assignment_name):
        return "{}/{}".format(course_id, assignment_name)

    def insert(self, obj):
        return self._collection.insert_one(self._to_store(obj))

    def find_by_id(self, id):
        return self._from_store(self._collection.find_one({AssignmentConfigDao.ID: id}))

    def delete_by_id(self, id):
        return self._collection.delete_one({AssignmentConfigDao.ID: id})

    def _from_store(self, obj):
        if obj is None:
            return obj
        attrs = {
            "id": obj.get(AssignmentConfigDao.ID),
            "env": obj.get(AssignmentConfigDao.ENV),
            "student_pipeline": obj.get(AssignmentConfigDao.STUDENT_PIPELINE),
            "pre_processing_pipeline": obj.get(
                AssignmentConfigDao.PRE_PROCESSING_PIPELINE
            ),
            "post_processing_pipeline": obj.get(
                AssignmentConfigDao.POST_PROCESSING_PIPELINE
            ),
        }
        return AssignmentConfig(**attrs)

    def _to_store(self, obj):
        return {
            AssignmentConfigDao.ID: obj.id,
            AssignmentConfigDao.ENV: obj.env,
            AssignmentConfigDao.STUDENT_PIPELINE: obj.student_pipeline,
            AssignmentConfigDao.PRE_PROCESSING_PIPELINE: obj.pre_processing_pipeline,
            AssignmentConfigDao.POST_PROCESSING_PIPELINE: obj.post_processing_pipeline,
        }
