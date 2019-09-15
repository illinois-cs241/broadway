from typing import Optional

from bson import ObjectId

from broadway.api.daos.base import BaseDao
from broadway.api.daos.decorators import validate_obj_size
from broadway.api.models.grading_job import GradingJob, GradingJobType


class GradingJobDao(BaseDao):
    ID = "_id"
    TYPE = "type"
    RUN_ID = "grading_run_id"
    WORKER_ID = "worker_id"
    QUEUED_AT = "queued_at"
    STARTED_AT = "started_at"
    FINISHED_AT = "finished_at"
    RESULTS = "results"
    SUCCESS = "success"
    STAGES = "stages"
    STUDENTS = "students"
    _COLLECTION = "grading_job"

    def __init__(self, app):
        super().__init__(app)
        self._collection = self._get_primary_collection(GradingJobDao._COLLECTION)

    @validate_obj_size
    def insert(self, obj):
        document = self._to_store(obj)
        del document[GradingJobDao.ID]
        return self._collection.insert_one(document)

    def find_by_id(self, id_):
        if not ObjectId.is_valid(id_):
            return None
        return self._from_store(
            self._collection.find_one({GradingJobDao.ID: ObjectId(id_)})
        )

    def find_by_run_id(self, run_id):
        return list(
            map(self._from_store, self._collection.find({GradingJobDao.RUN_ID: run_id}))
        )

    @validate_obj_size
    def update(self, obj):
        return self._collection.update_one(
            {GradingJobDao.ID: ObjectId(obj.id)}, {"$set": self._to_store(obj)}
        )

    def _from_store(self, obj) -> Optional[GradingJob]:
        if obj is None:
            return None
        attrs = {
            "id_": str(obj.get(GradingJobDao.ID)),
            "job_type": GradingJobType(obj.get(GradingJobDao.TYPE)),
            "run_id": obj.get(GradingJobDao.RUN_ID),
            "worker_id": obj.get(GradingJobDao.WORKER_ID),
            "queued_at": obj.get(GradingJobDao.QUEUED_AT),
            "started_at": obj.get(GradingJobDao.STARTED_AT),
            "finished_at": obj.get(GradingJobDao.FINISHED_AT),
            "results": obj.get(GradingJobDao.RESULTS),
            "success": obj.get(GradingJobDao.SUCCESS),
            "stages": obj.get(GradingJobDao.STAGES),
            "students": obj.get(GradingJobDao.STUDENTS),
        }
        return GradingJob(**attrs)

    def _to_store(self, obj) -> dict:
        return {
            GradingJobDao.ID: ObjectId(obj.id) if obj.id is not None else obj.id,
            GradingJobDao.TYPE: obj.type.value,
            GradingJobDao.RUN_ID: obj.run_id,
            GradingJobDao.WORKER_ID: obj.worker_id,
            GradingJobDao.QUEUED_AT: obj.queued_at,
            GradingJobDao.STARTED_AT: obj.started_at,
            GradingJobDao.FINISHED_AT: obj.finished_at,
            GradingJobDao.RESULTS: obj.results,
            GradingJobDao.SUCCESS: obj.success,
            GradingJobDao.STAGES: obj.stages,
            GradingJobDao.STUDENTS: obj.students,
        }
