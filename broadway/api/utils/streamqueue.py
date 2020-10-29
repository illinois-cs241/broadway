from queue import Queue
from collections import defaultdict
import json

"""
Used in conjunction with server-sent events (SSE) to service updates about grading jobs.
Updates on a job's queue position and status are saved here.
"""


class StreamQueue:
    POSITION_EVENT = 'position'
    STATUS_EVENT = 'status'

    def __init__(self):
        self._streams = defaultdict(lambda: defaultdict(lambda: Queue()))

    def register_stream(self, job_id, iid) -> Queue:
        # Create and return the queue
        return self._streams[job_id][iid]

    def unregister_stream(self, job_id, iid) -> None:
        del self._streams[job_id][iid]
        # Clean up job_id entry if there are no more listeners
        if not self._streams[job_id]:
            del self._streams[job_id]

    def update_queue_position(self, job_id, position) -> None:
        """
        Add the queue position event to all queues for this job
        """
        if job_id not in self._streams:
            return
        event = (self.POSITION_EVENT, position)
        for iid in self._streams[job_id]:
            self._streams[job_id][iid].put(event)

    def update_job_status(self, job_id, status) -> None:
        """
        Add the new job status to all queues for this job
        """
        if job_id not in self._streams:
            return
        event = (self.STATUS_EVENT, status)
        for iid in self._streams[job_id]:
            self._streams[job_id][iid].put(event)
