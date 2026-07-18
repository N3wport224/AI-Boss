"""In-memory pub/sub so the dashboard can watch a run happen via Server-Sent Events.

Each run gets its own `stream_id` (unrelated to the StateStore's persisted `run_id` —
this one is just an ephemeral handle for the live SSE channel). A background thread
publishes events into a queue as the orchestrator executes; the SSE endpoint drains
that same queue for whichever client is watching. Because it's a queue, a client that
connects a moment late still gets everything from the start — nothing is missed.
"""
import itertools
import queue
import threading
from typing import Iterator, Optional

_DONE = object()


class RunEventBus:
    def __init__(self) -> None:
        self._queues: dict[int, "queue.Queue"] = {}
        self._lock = threading.Lock()
        self._ids = itertools.count(1)

    def create(self) -> int:
        with self._lock:
            stream_id = next(self._ids)
            self._queues[stream_id] = queue.Queue()
        return stream_id

    def publish(self, stream_id: int, event: dict) -> None:
        q = self._queues.get(stream_id)
        if q is not None:
            q.put(event)

    def close(self, stream_id: int) -> None:
        q = self._queues.get(stream_id)
        if q is not None:
            q.put(_DONE)

    def stream(self, stream_id: int, timeout: float = 30.0) -> Iterator[dict]:
        q: Optional["queue.Queue"] = self._queues.get(stream_id)
        if q is None:
            return
        while True:
            try:
                event = q.get(timeout=timeout)
            except queue.Empty:
                break
            if event is _DONE:
                break
            yield event
        with self._lock:
            self._queues.pop(stream_id, None)
