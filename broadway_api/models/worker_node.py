from typing import Optional


class WorkerNode:
    def __init__(
        self,
        hostname: str,
        id: Optional[str] = None,
        running_job_id: Optional[str] = None,
        last_seen: Optional[str] = None,
        jobs_processed: int = 0,
        is_alive: bool = True,
    ):
        self.id = id
        self.running_job_id = running_job_id
        self.last_seen = last_seen
        self.hostname = hostname
        self.jobs_processed = jobs_processed
        self.is_alive = is_alive
