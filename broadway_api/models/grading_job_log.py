from typing import Optional


class GradingJobLog:
    def __init__(
        self,
        job_id: str,
        id: Optional[str] = None,
        stdout: Optional[str] = "",
        stderr: Optional[str] = "",
    ):
        self.id = id
        self.job_id = job_id
        self.stdout = stdout
        self.stderr = stderr
