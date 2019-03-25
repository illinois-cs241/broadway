from enum import Enum
from typing import Any, Dict, List, Optional
from datetime import datetime

from broadway_api.models.base import BaseModel


class GradingJobType(Enum):
    PRE_PROCESSING = "pre processing job"
    STUDENT = "student grading job"
    POST_PROCESSING = "post processing job"


class GradingJobState(Enum):
    QUEUED = "grading job has been scheduled"
    STARTED = "grading job is running"
    FAILED = "grading job failed"
    SUCCEEDED = "grading job was successful"


class GradingJob(BaseModel):
    def __init__(
        self,
        job_type: GradingJobType,
        run_id: str,
        stages: List[Any] = [],
        students: List[Dict[str, str]] = None,
        id_: Optional[str] = None,
        worker_id: Optional[str] = None,
        queued_at: Optional[datetime] = None,
        started_at: Optional[datetime] = None,
        finished_at: Optional[datetime] = None,
        results: Optional[List[Any]] = None,
        success: Optional[bool] = None,
    ) -> None:
        self.id = id_
        self.type = job_type
        self.run_id = run_id
        self.worker_id = worker_id
        self.queued_at = queued_at
        self.started_at = started_at
        self.finished_at = finished_at
        self.results = results
        self.success = success
        self.stages = stages
        self.students = students

    def get_state(self) -> GradingJobState:
        if self.finished_at is not None:
            if self.success:
                return GradingJobState.SUCCEEDED
            return GradingJobState.FAILED
        if self.started_at is not None:
            return GradingJobState.STARTED
        if self.queued_at is not None:
            return GradingJobState.QUEUED
        raise ValueError("Invalid state")

    def set_stages(self, stages, global_environ, run_environ):
        # there are three types of environments: global, stage, and run
        # (with precedence in that order)

        # we need to make sure that the incoming stages have these environments
        # merged correctly before setting them
        stages_copy = stages.copy()
        for stage in stages_copy:
            stage["env"] = {**global_environ, **stage.get("env", {}), **run_environ}
        self.stages = stages_copy
