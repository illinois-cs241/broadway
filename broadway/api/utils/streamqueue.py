from queue import Queue
from collections import defaultdict

"""
Used in conjunction with server-sent events (SSE) to service updates about grading jobs.
Updates on a job's queue position and state are saved here.
"""


class StreamQueue:
    POSITION_EVENT = "position"
    STATE_EVENT = "state"

    def __init__(self):
        self._streams = defaultdict(lambda: defaultdict(lambda: Queue()))

    def _ensure_stream_exists(self, job_id, iid) -> bool:
        """
        Raise an exception if there is no corresponding listener.

        :param job_id: Target job ID.
        :param iid: ID of the listener.
        :raises Exception: If there is no corresponding listener.
        """
        if job_id not in self._streams or iid not in self._streams[job_id]:
            raise Exception(f"KeyError: ({job_id}:{iid}) is not in the StreamQueue")

    def register_stream(self, job_id, iid) -> None:
        """
        Register a new stream to listen for events for the given job ID.

        :param job_id: Target job ID.
        :param iid: A unique identifier for the listener (Using `id(self)` in handlers).
        """
        self._streams[job_id][iid] = Queue()

    def unregister_stream(self, job_id, iid) -> None:
        """
        Remove a listener for the given job ID.

        :param job_id: Target job ID.
        :param iid: ID of the listener.
        :raises Exception: If there is no corresponding listener.
        """
        self._ensure_stream_exists(job_id, iid)
        del self._streams[job_id][iid]
        if not self._streams[job_id]:
            del self._streams[job_id]

    def has_update(self, job_id, iid) -> bool:
        """
        Returns whether a listener has any new events.

        :param job_id: Target job ID.
        :param iid: ID of the listener.
        :raises Exception: If there is no corresponding listener.
        """
        self._ensure_stream_exists(job_id, iid)
        return not self._streams[job_id][iid].empty()

    def get(self, job_id, iid):
        """
        Pops and returns the next message from the listener's event queue.

        :param job_id: Target job ID.
        :param iid: ID of the listener.
        :raises Exception: If there is no corresponding listener.
        """
        self._ensure_stream_exists(job_id, iid)
        return self._streams[job_id][iid].get()

    def update_queue_position(self, job_id, position) -> None:
        """
        Add a queue position change event to all listeners of the given job ID.

        :param job_id: Target job ID.
        :param position: New position of the job.
        """
        if job_id not in self._streams:
            return
        event = (self.POSITION_EVENT, position)
        for iid in self._streams[job_id]:
            self._streams[job_id][iid].put(event)

    def update_job_state(self, job_id, state) -> None:
        """
        Add a job state change event to all listeners of the given job ID.

        :param job_id: Target job ID.
        :param state: New state of the job.
        """
        if job_id not in self._streams:
            return
        event = (self.STATE_EVENT, state)
        for iid in self._streams[job_id]:
            self._streams[job_id][iid].put(event)
