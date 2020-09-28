from queue import Queue, Empty

"""
A wrapper around Python's queue module that contains multiple queues
and round-robins between them when pulling.
"""


class MultiQueue:
    def __init__(self):
        self.queues = {}
        self.keys = []
        self.round_robin_idx = 0

    def _add_queue(self, queue_id):
        if queue_id in self.queues:
            raise Exception(f"{queue_id} already exists in the MultiQueue.")

        self.queues[queue_id] = Queue()
        self.keys.append(queue_id)

    def _ensure_queue_exists(self, queue_id):
        if queue_id not in self.queues:
            raise Exception(f"{queue_id} does not exist in the MultiQueue.")

    def push(self, queue_id, elem):
        if queue_id not in self.queues:
            self._add_queue(queue_id)

        self.queues[queue_id].put(elem)

    def pull(self):
        N = len(self.keys)
        if N == 0:
            raise Empty("MultiQueue has no queues in it.")

        for i in range(N):
            idx = (self.round_robin_idx + i) % N

            try:
                rv = self.queues[self.keys[idx]].get_nowait()
                self.round_robin_idx = (idx + 1) % N
                return rv

            except Empty:
                # continue to next queue
                pass

        raise Empty("All the queues in the MultiQueue are empty.")

    def contains_key(self, key):
        return key in self.queues

    def get_queue_length(self, queue_id):
        self._ensure_queue_exists(queue_id)
        return self.queues[queue_id].qsize()

    def get_position_in_queue(self, queue_id, key):
        self._ensure_queue_exists(queue_id)
        try:
            return self.queues[queue_id].queue.index(key)
        except ValueError:
            return -1
