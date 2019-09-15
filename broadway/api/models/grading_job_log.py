from typing import Optional


class GradingJobLog:
    def __init__(
        self,
        job_id: str,
        id_: Optional[str] = None,
        stdout: Optional[str] = "",
        stderr: Optional[str] = "",
    ):
        self.id = id_
        self.job_id = job_id
        self.stdout = stdout
        self.stderr = stderr
