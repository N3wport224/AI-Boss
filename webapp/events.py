"""In-memory pub/sub so the dashboard can watch a run happen via Server-Sent Events.

Each run gets its own `stream_id` (unrelated to the StateStore's persisted `run_id` —
this one is just an ephemeral handle for the live SSE channel). Events are recorded
in an append-only list per stream_id rather than drained from a `queue.Queue`, so a
client that reconnects mid-run (a dropped connection, a browser reload) can replay
everything it missed by resuming from wherever it left off, instead of the events
being gone the moment the first connection's `get()` pulled them off a shared queue.
The `/api/stream/{id}` endpoint pairs this with SSE's native `id:`/`Last-Event-ID`
reconnect mechanism, so a plain `EventSource` gets this for free without any custom
client-side retry logic.
"""
import itertools
import threading
import time
from typing import Iterator

# How long a finished stream's event log is kept around after close(), so a
# client that reconnects shortly after a run ends can still replay it. Swept
# lazily on the next create() call rather than a background timer.
_RETENTION_SECONDS = 300.0


class RunEventBus:
    def __init__(self) -> None:
        self._lock = threading.Condition()
        self._events: dict[int, list[dict]] = {}
        self._done: dict[int, bool] = {}
        self._closed_at: dict[int, float] = {}
        self._ids = itertools.count(1)

    def create(self) -> int:
        with self._lock:
            self._prune_expired_locked()
            stream_id = next(self._ids)
            self._events[stream_id] = []
            self._done[stream_id] = False
        return stream_id

    def _prune_expired_locked(self) -> None:
        now = time.monotonic()
        expired = [sid for sid, closed_at in self._closed_at.items() if now - closed_at > _RETENTION_SECONDS]
        for sid in expired:
            self._events.pop(sid, None)
            self._done.pop(sid, None)
            self._closed_at.pop(sid, None)

    def publish(self, stream_id: int, event: dict) -> None:
        with self._lock:
            events = self._events.get(stream_id)
            if events is None:
                return
            events.append(event)
            self._lock.notify_all()

    def close(self, stream_id: int) -> None:
        with self._lock:
            if stream_id in self._done:
                self._done[stream_id] = True
                self._closed_at[stream_id] = time.monotonic()
                self._lock.notify_all()

    def stream(self, stream_id: int, from_index: int = 0, timeout: float = 30.0) -> Iterator[tuple[int, dict]]:
        """Yield `(index, event)` pairs starting at `from_index` — replaying
        anything already published before waiting for anything new. Stops once
        the stream is closed and fully drained, or after `timeout` seconds
        pass with no new event (the caller — an EventSource — is expected to
        reconnect and resume from the last `id` it saw)."""
        index = from_index
        while True:
            with self._lock:
                events = self._events.get(stream_id)
                if events is None:
                    return
                if index < len(events):
                    event = events[index]
                elif self._done.get(stream_id, True):
                    return
                else:
                    notified = self._lock.wait(timeout=timeout)
                    if not notified:
                        return
                    continue
            yield index, event
            index += 1
