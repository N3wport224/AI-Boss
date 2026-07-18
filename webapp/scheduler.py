"""In-process recurring-run scheduler.

Deliberately not a real cron-expression engine (no croniter dependency, no
external job queue) — this is a single-process local tool, so a plain "run
every N seconds, starting now" timer covers the actual use case (rerun this
module or pipeline on a recurring interval) without pretending to support
minute/hour/day-of-week cron syntax nothing here would ever parse correctly.

A background thread polls the schedules table every `poll_interval` seconds
and fires anything whose `next_run_at` has passed. Firing calls back into a
caller-supplied `trigger(schedule)` — this module knows nothing about how a
module or pipeline is actually launched, only when to launch it.
"""
import threading
from datetime import datetime, timedelta, timezone
from typing import Callable

from engine.state_store import StateStore


class Scheduler:
    def __init__(self, state_store: StateStore, trigger: Callable[[dict], None], poll_interval: float = 5.0):
        self.state_store = state_store
        self.trigger = trigger
        self.poll_interval = poll_interval
        self._stop = threading.Event()
        self._thread: threading.Thread | None = None

    def start(self) -> None:
        if self._thread is not None:
            return
        self._thread = threading.Thread(target=self._loop, daemon=True)
        self._thread.start()

    def stop(self) -> None:
        self._stop.set()

    def _loop(self) -> None:
        while not self._stop.is_set():
            self._tick()
            self._stop.wait(self.poll_interval)

    def _tick(self) -> None:
        now = datetime.now(timezone.utc)
        for schedule in self.state_store.due_schedules(now.isoformat()):
            try:
                self.trigger(schedule)
                status = "triggered"
            except Exception as exc:
                status = f"error: {exc}"
            next_run_at = now + timedelta(seconds=schedule["interval_seconds"])
            self.state_store.record_schedule_run(schedule["id"], next_run_at.isoformat(), status)
