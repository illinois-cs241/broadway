from typing import Optional

from bson import ObjectId

from broadway_api.daos.base import BaseDao
from broadway_api.daos.decorators import validate_obj_size
from broadway_api.models import GradingJobLog


class GradingJobLogDao(BaseDao):
    ID = "_id"
    GRADING_JOB_ID = "grading_job_id"
    STDOUT = "stdout"
    STDERR = "stderr"
    _COLLECTION = "job_log"

    def __init__(self, app):
        super().__init__(app)
        self._collection = self._get_log_collection(GradingJobLogDao._COLLECTION)

    @validate_obj_size
    def insert(self, obj):
        document = self._to_store(obj)
        del document[GradingJobLogDao.ID]
        return self._collection.insert_one(document)

    def find_by_id(self, id_):
        if not ObjectId.is_valid(id_):
            return None
        return self._from_store(
            self._collection.find_one({GradingJobLogDao.ID: ObjectId(id_)})
        )

    def find_by_job_id(self, job_id):
        found = self._collection.find_one({GradingJobLogDao.GRADING_JOB_ID: job_id})
        return self._from_store(found)

    def _from_store(self, obj) -> Optional[GradingJobLog]:
        if obj is None:
            return None
        attrs = {
            "id_": str(obj.get(GradingJobLogDao.ID)),
            "job_id": obj.get(GradingJobLogDao.GRADING_JOB_ID),
            "stdout": obj.get(GradingJobLogDao.STDOUT),
            "stderr": obj.get(GradingJobLogDao.STDERR),
        }
        return GradingJobLog(**attrs)

    def _to_store(self, obj) -> dict:
        return {
            GradingJobLogDao.ID: ObjectId(obj.id) if obj.id is not None else obj.id,
            GradingJobLogDao.GRADING_JOB_ID: obj.job_id,
            GradingJobLogDao.STDOUT: obj.stdout,
            GradingJobLogDao.STDERR: obj.stderr,
        }
