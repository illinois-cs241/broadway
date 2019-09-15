from typing import Any, Dict, List, Optional

from broadway.api.models.base import BaseModel


class AssignmentConfig(BaseModel):
    def __init__(
        self,
        id_: str,
        env: Optional[Dict[str, str]] = None,
        student_pipeline: List[Dict[str, Any]] = [],
        pre_processing_pipeline: Optional[List[Dict[str, Any]]] = None,
        post_processing_pipeline: Optional[List[Dict[str, Any]]] = None,
    ) -> None:
        self.id = id_
        self.env = env
        self.student_pipeline = student_pipeline
        self.pre_processing_pipeline = pre_processing_pipeline
        self.post_processing_pipeline = post_processing_pipeline

    def to_dict(self):
        result = {}
        for var in vars(self):
            if var != "id":
                result[var] = getattr(self, var)
        return result
