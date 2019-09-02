from typing import Optional

from broadway_api.daos.base import BaseDao
from broadway_api.daos.decorators import validate_obj_size
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
        self._collection = self._get_primary_collection(AssignmentConfigDao._COLLECTION)

    @staticmethod
    def id_from(course_id, assignment_name):
        return "{}/{}".format(course_id, assignment_name)

    @validate_obj_size
    def insert(self, obj):
        return self._collection.insert_one(self._to_store(obj))

    def find_by_id(self, id_):
        return self._from_store(
            self._collection.find_one({AssignmentConfigDao.ID: id_})
        )

    def delete_by_id(self, id_):
        return self._collection.delete_one({AssignmentConfigDao.ID: id_})

    def _from_store(self, obj) -> Optional[AssignmentConfig]:
        if obj is None:
            return None
        attrs = {
            "id_": obj.get(AssignmentConfigDao.ID),
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

    def _to_store(self, obj) -> dict:
        return {
            AssignmentConfigDao.ID: obj.id,
            AssignmentConfigDao.ENV: obj.env,
            AssignmentConfigDao.STUDENT_PIPELINE: obj.student_pipeline,
            AssignmentConfigDao.PRE_PROCESSING_PIPELINE: obj.pre_processing_pipeline,
            AssignmentConfigDao.POST_PROCESSING_PIPELINE: obj.post_processing_pipeline,
        }
