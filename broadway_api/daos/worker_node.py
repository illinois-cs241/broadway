from broadway_api.daos.base import BaseDao
from broadway_api.models import WorkerNode


class WorkerNodeDao(BaseDao):
    ID = "_id"
    RUNNING_JOB_ID = "running_job_id"
    LAST_SEEN = "last_seen"
    WORKER_HOSTNAME = "worker_hostname"
    JOBS_PROCESSED = "jobs_processed"
    ALIVE = "alive"
    _COLLECTION = "worker_node"

    def __init__(self, app):
        super().__init__(app)
        self._collection = self._get_collection(
            self._config["DB_PRIMARY"], WorkerNodeDao._COLLECTION
        )

    def insert(self, obj):
        document = self._to_store(obj)
        return self._collection.insert_one(document)

    def update(self, obj):
        document = self._to_store(obj)
        return self._collection.update_one(
            {WorkerNodeDao.ID: obj.id}, {"$set": document}
        )

    def find_all(self):
        return list(map(self._from_store, self._collection.find()))

    def find_by_id(self, id_):
        return self._from_store(self._collection.find_one({WorkerNodeDao.ID: id_}))

    def find_by_hostname(self, hostname):
        return self._from_store(
            self._collection.find_one({WorkerNodeDao.WORKER_HOSTNAME: hostname})
        )

    def find_by_liveness(self, alive):
        return list(
            map(self._from_store, self._collection.find({WorkerNodeDao.ALIVE: alive}))
        )

    def _from_store(self, obj):
        if obj is None:
            return None
        attrs = {
            "id_": obj.get(WorkerNodeDao.ID),
            "running_job_id": obj.get(WorkerNodeDao.RUNNING_JOB_ID),
            "last_seen": obj.get(WorkerNodeDao.LAST_SEEN),
            "hostname": obj.get(WorkerNodeDao.WORKER_HOSTNAME),
            "jobs_processed": obj.get(WorkerNodeDao.JOBS_PROCESSED),
            "is_alive": obj.get(WorkerNodeDao.ALIVE),
        }
        return WorkerNode(**attrs)

    def _to_store(self, obj):
        return {
            WorkerNodeDao.ID: obj.id,
            WorkerNodeDao.RUNNING_JOB_ID: obj.running_job_id,
            WorkerNodeDao.LAST_SEEN: obj.last_seen,
            WorkerNodeDao.WORKER_HOSTNAME: obj.hostname,
            WorkerNodeDao.JOBS_PROCESSED: obj.jobs_processed,
            WorkerNodeDao.ALIVE: obj.is_alive,
        }
