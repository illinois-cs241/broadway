from typing import Optional

from bson import ObjectId

from broadway_api.daos.base import BaseDao
from broadway_api.daos.decorators import validate_obj_size
from broadway_api.models import GradingRun, GradingRunState


class GradingRunDao(BaseDao):
    ID = "_id"
    STATE = "state"
    ASSIGNMENT_ID = "assignment_id"
    STARTED_AT = "started_at"
    FINISHED_AT = "finished_at"
    PRE_PROCESSING_ENV = "pre_processing_env"
    POST_PROCESSING_ENV = "post_processing_env"
    STUDENTS_ENV = "students_env"
    STUDENT_JOBS_LEFT = "students_jobs_left"
    SUCCESS = "success"
    _COLLECTION = "grading_run"

    def __init__(self, app):
        super().__init__(app)
        self._collection = self._get_collection(
            self._config["DB_PRIMARY"], GradingRunDao._COLLECTION
        )

    @validate_obj_size
    def insert(self, obj):
        document = self._to_store(obj)
        del document[GradingRunDao.ID]
        return self._collection.insert_one(document)

    def find_by_id(self, id_):
        if not ObjectId.is_valid(id_):
            return None
        return self._from_store(
            self._collection.find_one({GradingRunDao.ID: ObjectId(id_)})
        )

    @validate_obj_size
    def update(self, obj):
        return self._collection.update_one(
            {GradingRunDao.ID: ObjectId(obj.id)}, {"$set": self._to_store(obj)}
        )

    def _from_store(self, obj) -> Optional[GradingRun]:
        if obj is None:
            return None
        attrs = {
            "id_": str(obj.get(GradingRunDao.ID)),
            "state": GradingRunState(obj.get(GradingRunDao.STATE)),
            "assignment_id": obj.get(GradingRunDao.ASSIGNMENT_ID),
            "started_at": obj.get(GradingRunDao.STARTED_AT),
            "finished_at": obj.get(GradingRunDao.FINISHED_AT),
            "pre_processing_env": obj.get(GradingRunDao.PRE_PROCESSING_ENV),
            "post_processing_env": obj.get(GradingRunDao.POST_PROCESSING_ENV),
            "students_env": obj.get(GradingRunDao.STUDENTS_ENV),
            "student_jobs_left": obj.get(GradingRunDao.STUDENT_JOBS_LEFT),
            "success": obj.get(GradingRunDao.SUCCESS),
        }
        return GradingRun(**attrs)

    def _to_store(self, obj) -> dict:
        return {
            GradingRunDao.ID: ObjectId(obj.id) if obj.id is not None else None,
            GradingRunDao.STATE: obj.state.value,
            GradingRunDao.ASSIGNMENT_ID: obj.assignment_id,
            GradingRunDao.STARTED_AT: obj.started_at,
            GradingRunDao.FINISHED_AT: obj.finished_at,
            GradingRunDao.PRE_PROCESSING_ENV: obj.pre_processing_env,
            GradingRunDao.POST_PROCESSING_ENV: obj.post_processing_env,
            GradingRunDao.STUDENTS_ENV: obj.students_env,
            GradingRunDao.STUDENT_JOBS_LEFT: obj.student_jobs_left,
            GradingRunDao.SUCCESS: obj.success,
        }
