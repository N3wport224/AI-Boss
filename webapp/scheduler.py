"""In-process recurring-run scheduler.

Deliberately not a real cron-expression engine (no croniter dependency, no
external job queue) — this is a single-process local tool, so two schedule
kinds cover the actual use cases: "run every N seconds, starting now" for a
recurring interval, and "run once a day at HH:MM" for automations that should
fire at a particular time of day rather than on a fixed cadence. Neither
pretends to support full minute/hour/day-of-week cron syntax.

A background thread polls the schedules table every `poll_interval` seconds
and fires anything whose `next_run_at` has passed. Firing calls back into a
caller-supplied `trigger(schedule)` — this module knows nothing about how a
module or pipeline is actually launched, only when to launch it.
"""
import threading
from datetime import datetime, timedelta, timezone
from typing import Callable

from engine.state_store import StateStore


def next_daily_run_at(daily_time: str, after: datetime) -> datetime:
    """Next UTC instant a "HH:MM" daily schedule should fire, strictly after
    `after`. HH:MM is interpreted in the machine's local timezone (the one a
    user picking "09:00" actually means), then converted back to UTC so it
    compares directly against every other `next_run_at` in the schedules
    table — those are always UTC regardless of schedule kind."""
    hour, minute = (int(part) for part in daily_time.split(":"))
    local_after = after.astimezone()
    candidate = local_after.replace(hour=hour, minute=minute, second=0, microsecond=0)
    if candidate <= local_after:
        candidate += timedelta(days=1)
    return candidate.astimezone(timezone.utc)


class Scheduler:
    def __init__(self, state_store: StateStore, trigger: Callable[[dict], None], poll_interval: float = 5.0):
        self.state_store = state_store
        self.trigger = trigger
        self.poll_interval = poll_interval
        self._stop = threading.Event()
        self._thread: threading.Thread | None = None
        # A master pause, distinct from any individual schedule's own
        # `enabled` flag — for a maintenance window where nothing should
        # fire regardless of what each schedule is otherwise set to, without
        # touching (and having to remember to restore) every schedule's own
        # enabled state. In-memory only, like `poll_interval` — resets to
        # unpaused on a server restart.
        self.paused = False

    def pause(self) -> None:
        self.paused = True

    def resume(self) -> None:
        self.paused = False

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
        if self.paused:
            return
        now = datetime.now(timezone.utc)
        for schedule in self.state_store.due_schedules(now.isoformat()):
            try:
                self.trigger(schedule)
                status = "triggered"
            except Exception as exc:
                status = f"error: {exc}"
            if schedule.get("schedule_type") == "daily":
                next_run_at = next_daily_run_at(schedule["daily_time"], now)
            else:
                next_run_at = now + timedelta(seconds=schedule["interval_seconds"])
            self.state_store.record_schedule_run(schedule["id"], next_run_at.isoformat(), status)
