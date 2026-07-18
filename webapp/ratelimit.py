"""A simple in-memory, single-process rate limiter for run-triggering endpoints.

Not meant as multi-tenant abuse prevention (this app has no auth/identity) —
just a guard against an accidental request storm (a buggy script, a stuck
retry loop, a runaway scheduled task) overwhelming the background thread
pool. Keyed by client IP, which in the single-user/local deployment this app
targets is effectively one bucket; there's no Redis or shared state, so this
resets on every process restart and doesn't coordinate across processes.
"""
import threading
import time
from collections import defaultdict, deque


class RateLimiter:
    def __init__(self, max_requests: int, window_seconds: float):
        self.max_requests = max_requests
        self.window_seconds = window_seconds
        self._hits: dict[str, deque] = defaultdict(deque)
        self._lock = threading.Lock()

    def allow(self, key: str) -> bool:
        """Record a hit for `key` and return whether it's still within the
        limit — a sliding window over the last `window_seconds`."""
        now = time.monotonic()
        with self._lock:
            hits = self._hits[key]
            while hits and now - hits[0] > self.window_seconds:
                hits.popleft()
            if len(hits) >= self.max_requests:
                return False
            hits.append(now)
            return True
