from collections import defaultdict, deque
from threading import Lock

class EventBus:
    def __init__(self):
        self._subs = defaultdict(list)
        self._queue = deque()
        self._lock = Lock()

    def publish(self, topic: str, message: dict):
        with self._lock:
            for cb in self._subs[topic]:
                cb(message)
            self._queue.append((topic, message))

    def subscribe(self, topic: str, callback):
        with self._lock:
            self._subs[topic].append(callback)

    def get_recent(self, limit=50):
        with self._lock:
            return list(self._queue)[-limit:]

BUS = EventBus()
