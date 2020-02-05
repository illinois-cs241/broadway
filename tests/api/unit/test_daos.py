import datetime as dt
import logging

import broadway.api.daos as daos
import broadway.api.models as models

from tests.api.base import BaseTest

logging.disable(logging.WARNING)


class AssignmentConfigDaoTest(BaseTest):

    DEFAULT_OBJECT = models.AssignmentConfig(
        id_="obj",
        env={"key": "value"},
        pre_processing_pipeline=[{"image": "alpine"}],
        student_pipeline=[{"image": "alpine"}],
    )

    def setUp(self):
        super().setUp()
        self.dao = daos.AssignmentConfigDao(self.app.settings)

    def _insert_obj(self):
        return self.dao.insert(AssignmentConfigDaoTest.DEFAULT_OBJECT)

    def test_id_from(self):
        _id = self.dao.id_from("course", "assignment")
        self.assertEqual("course/assignment", _id)

    def test_insert(self):
        result = self._insert_obj()
        self.assertIsNotNone(result.inserted_id)

    def test_find_by_id(self):
        result = self._insert_obj()
        obj = self.dao.find_by_id(result.inserted_id)

        self.assertIsNotNone(obj)
        for var in vars(obj):
            self.assertEqual(
                getattr(obj, var), getattr(AssignmentConfigDaoTest.DEFAULT_OBJECT, var)
            )

    def test_delete(self):
        insert_result = self._insert_obj()
        delete_result = self.dao.delete_by_id(insert_result.inserted_id)
        self.assertGreater(delete_result.deleted_count, 0)


class CourseDaoTest(BaseTest):

    DEFAULT_OBJECT = models.Course(id_="course", tokens=["value"])

    def setUp(self):
        super().setUp()
        self.dao = daos.CourseDao(self.app.settings)

    def _insert_obj(self):
        return self.dao.insert_or_update(CourseDaoTest.DEFAULT_OBJECT)

    def test_insert(self):
        result = self._insert_obj()
        self.assertIsNotNone(result.upserted_id)

    def test_find_by_id(self):
        result = self._insert_obj()
        obj = self.dao.find_by_id(result.upserted_id)

        self.assertIsNotNone(obj)
        for var in vars(obj):
            self.assertEqual(
                getattr(obj, var), getattr(CourseDaoTest.DEFAULT_OBJECT, var)
            )

    def test_update(self):
        insert_result = self._insert_obj()
        obj = self.dao.find_by_id(insert_result.upserted_id)
        obj.tokens = ["new_value"]
        update_result = self.dao.insert_or_update(obj)

        self.assertGreater(update_result.matched_count, 0)


class GradingJobLogDaoTest(BaseTest):

    DEFAULT_OBJECT = models.GradingJobLog(
        job_id="job_id", stdout="output", stderr="errors"
    )

    def setUp(self):
        super().setUp()
        self.dao = daos.GradingJobLogDao(self.app.settings)

    def _insert_obj(self):
        return self.dao.insert(GradingJobLogDaoTest.DEFAULT_OBJECT)

    def test_insert(self):
        result = self._insert_obj()
        self.assertIsNotNone(result.inserted_id)

    def test_find_by_id(self):
        result = self._insert_obj()
        obj = self.dao.find_by_id(result.inserted_id)

        self.assertIsNotNone(obj)
        for var in vars(obj):
            if var == "id":
                self.assertIsNotNone(obj.id)
            else:
                self.assertEqual(
                    getattr(obj, var), getattr(GradingJobLogDaoTest.DEFAULT_OBJECT, var)
                )

    def test_find_by_invalid_id(self):
        result = self.dao.find_by_id("$$$")
        self.assertIsNone(result)


class GradingJobDaoTest(BaseTest):

    DEFAULT_OBJECT = models.GradingJob(
        job_type=models.GradingJobType.STUDENT,
        run_id="run123",
        worker_id="worker123",
        queued_at=dt.datetime.utcnow(),
        success=True,
        stages=[{"image": "alpine"}],
        students=[{"STUDENT_ID": "example"}],
    )

    def setUp(self):
        super().setUp()
        self.dao = daos.GradingJobDao(self.app.settings)

    def _insert_obj(self):
        return self.dao.insert(GradingJobDaoTest.DEFAULT_OBJECT)

    def test_insert(self):
        result = self._insert_obj()
        self.assertIsNotNone(result.inserted_id)

    def test_find_by_id(self):
        result = self._insert_obj()
        obj = self.dao.find_by_id(result.inserted_id)

        self.assertIsNotNone(obj)
        for var in vars(obj):
            if var == "id":
                self.assertIsNotNone(obj.id)
            elif var == "queued_at":
                delta = GradingJobDaoTest.DEFAULT_OBJECT.queued_at - obj.queued_at
                self.assertEqual(delta.seconds, 0)
            else:
                self.assertEqual(
                    getattr(obj, var), getattr(GradingJobDaoTest.DEFAULT_OBJECT, var)
                )

    def test_find_by_run_id(self):
        result = self._insert_obj()
        objs = self.dao.find_by_run_id(GradingJobDaoTest.DEFAULT_OBJECT.run_id)

        self.assertEqual(len(objs), 1)
        self.assertEqual(objs[0].id, str(result.inserted_id))

    def test_update(self):
        insert_result = self._insert_obj()
        obj = self.dao.find_by_id(insert_result.inserted_id)
        obj.success = False
        update_result = self.dao.update(obj)

        self.assertGreater(update_result.matched_count, 0)


class GradingRunDaoTest(BaseTest):

    DEFAULT_OBJECT = models.GradingRun(
        assignment_id="assignment123",
        course_id="cs241",
        state=models.GradingRunState.READY,
        students_env=[{"override": "1"}],
        student_jobs_left=1,
    )

    def setUp(self):
        super().setUp()
        self.dao = daos.GradingRunDao(self.app.settings)

    def _insert_obj(self):
        return self.dao.insert(GradingRunDaoTest.DEFAULT_OBJECT)

    def test_insert(self):
        result = self._insert_obj()
        self.assertIsNotNone(result.inserted_id)

    def test_find_by_id(self):
        result = self._insert_obj()
        obj = self.dao.find_by_id(result.inserted_id)

        self.assertIsNotNone(obj)
        for var in vars(obj):
            if var == "id":
                self.assertIsNotNone(obj.id)
            else:
                self.assertEqual(
                    getattr(obj, var), getattr(GradingRunDaoTest.DEFAULT_OBJECT, var)
                )

    def test_update(self):
        insert_result = self._insert_obj()
        obj = self.dao.find_by_id(insert_result.inserted_id)
        obj.student_jobs_left = 0
        update_result = self.dao.update(obj)

        self.assertGreater(update_result.matched_count, 0)


class WorkerNodeDaoTest(BaseTest):

    DEFAULT_OBJECT = models.WorkerNode(id_="holygoply", hostname="example.com")

    def setUp(self):
        super().setUp()
        self.dao = daos.WorkerNodeDao(self.app.settings)

    def _insert_obj(self):
        self.dao.insert(WorkerNodeDaoTest.DEFAULT_OBJECT)
        return WorkerNodeDaoTest.DEFAULT_OBJECT.id

    def test_insert(self):
        worker_id = self._insert_obj()
        self.assertIsNotNone(worker_id)

    def test_find_by_hostname(self):
        self._insert_obj()
        obj = self.dao.find_by_hostname(WorkerNodeDaoTest.DEFAULT_OBJECT.hostname)
        self.assertIsNotNone(obj)

    def test_find_by_liveness(self):
        worker_id = self._insert_obj()
        obj_list = self.dao.find_by_liveness(alive=True)
        no_obj_list = self.dao.find_by_liveness(alive=False)
        self.assertEqual(len(obj_list), 1)
        self.assertEqual(len(no_obj_list), 0)
        self.assertEqual(obj_list[0].id, worker_id)

    def test_update(self):
        worker_id = self._insert_obj()
        obj = self.dao.find_by_id(worker_id)
        obj.is_alive = False
        update_result = self.dao.update(obj)

        self.assertGreater(update_result.matched_count, 0)
