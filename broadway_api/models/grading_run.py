from enum import Enum
from typing import Dict, List, Optional

from broadway_api.models.base import BaseModel


class GradingRunState(Enum):
    READY = "ready to be started"
    PRE_PROCESSING_STAGE = "pre processing job has been scheduled"
    STUDENTS_STAGE = "students grading jobs have been scheduled"
    POST_PROCESSING_STAGE = "post processing job has been scheduled"
    FINISHED = "grading run is complete"
    FAILED = "grading run failed"


class GradingRun(BaseModel):
    def __init__(
        self,
        assignment_id: str,
        state: GradingRunState,
        id: Optional[str] = None,
        started_at: Optional[str] = None,
        finished_at: Optional[str] = None,
        pre_processing_env: Optional[List[Dict[str, str]]] = None,
        post_processing_env: Optional[List[Dict[str, str]]] = None,
        students_env: List[Dict[str, str]] = [],
        student_jobs_left: int = 0,
        success: Optional[bool] = None,
    ) -> None:
        self.id = id
        self.state = state
        self.assignment_id = assignment_id
        self.started_at = started_at
        self.finished_at = finished_at
        self.pre_processing_env = pre_processing_env
        self.post_processing_env = post_processing_env
        self.students_env = students_env
        self.student_jobs_left = student_jobs_left
        self.success = success
