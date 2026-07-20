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

from engine.logging_config import get_logger
from engine.state_store import StateStore

logger = get_logger(__name__)


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


def next_weekly_run_at(day_of_week: int, daily_time: str, after: datetime) -> datetime:
    """Next UTC instant a weekly schedule (a single day of the week, 0=Monday
    ... 6=Sunday to match datetime.weekday(), plus an "HH:MM" time of day)
    should fire, strictly after `after`. Same local-time interpretation as
    next_daily_run_at() -- a user picking "Monday 09:00" means their own
    machine's local Monday 9am, converted back to UTC to compare against
    next_run_at like every other schedule kind."""
    hour, minute = (int(part) for part in daily_time.split(":"))
    local_after = after.astimezone()
    candidate = local_after.replace(hour=hour, minute=minute, second=0, microsecond=0)
    days_ahead = (day_of_week - candidate.weekday()) % 7
    candidate += timedelta(days=days_ahead)
    if candidate <= local_after:
        candidate += timedelta(days=7)
    return candidate.astimezone(timezone.utc)


def next_n_daily_run_ats(daily_time: str, after: datetime, count: int) -> list[datetime]:
    """The next `count` fire times for a daily schedule, each strictly after
    the previous -- chains next_daily_run_at() forward one day at a time
    (feeding each result back in as the new `after` rolls it forward by
    exactly one day, since the hour/minute already match)."""
    results = []
    current = after
    for _ in range(count):
        current = next_daily_run_at(daily_time, current)
        results.append(current)
    return results


def next_n_weekly_run_ats(day_of_week: int, daily_time: str, after: datetime, count: int) -> list[datetime]:
    """Same idea as next_n_daily_run_ats() but for a weekly schedule --
    chains next_weekly_run_at() forward one week at a time."""
    results = []
    current = after
    for _ in range(count):
        current = next_weekly_run_at(day_of_week, daily_time, current)
        results.append(current)
    return results


class Scheduler:
    def __init__(
        self,
        state_store: StateStore,
        trigger: Callable[[dict], None],
        poll_interval: float = 5.0,
        on_error: Callable[[dict, str], None] | None = None,
        on_once_fired: Callable[[dict], None] | None = None,
    ):
        self.state_store = state_store
        self.trigger = trigger
        self.poll_interval = poll_interval
        # Optional (schedule, error_message) callback fired whenever a
        # schedule's trigger raises -- lets a caller surface a durable
        # notification without this module needing to know what a
        # notification even is. None by default so every existing caller
        # (and every prior test) keeps working unchanged.
        self.on_error = on_error
        # Optional (schedule) callback fired only when a "once" schedule's
        # trigger succeeds -- a one-time schedule is set up ahead of time
        # and easy to forget about, unlike a recurring one the user is
        # likely to check on repeatedly, so this is the one case worth a
        # dedicated success notification. None by default, same as on_error.
        self.on_once_fired = on_once_fired
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
        # Join so a caller tearing down (and about to close the shared
        # StateStore) knows no tick is still mid-flight on the connection.
        if self._thread is not None:
            self._thread.join(timeout=5.0)

    def is_alive(self) -> bool:
        """Whether the polling thread is up and running -- used by the
        liveness health check to catch a scheduler thread that's died
        without anyone tearing it down deliberately."""
        return self._thread is not None and self._thread.is_alive()

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
                # "schedule_name", not "name" -- the latter is a reserved
                # LogRecord attribute (the logger's own name) that extra={}
                # can't override.
                logger.info(
                    "schedule triggered",
                    extra={"schedule_id": schedule["id"], "kind": schedule["kind"], "schedule_name": schedule["name"]},
                )
            except Exception as exc:
                status = f"error: {exc}"
                logger.warning(
                    "schedule trigger failed",
                    extra={
                        "schedule_id": schedule["id"],
                        "kind": schedule["kind"],
                        "schedule_name": schedule["name"],
                        "error": str(exc),
                    },
                )
                if self.on_error is not None:
                    self.on_error(schedule, str(exc))
            schedule_type = schedule.get("schedule_type")
            if schedule_type == "daily":
                next_run_at = next_daily_run_at(schedule["daily_time"], now)
            elif schedule_type == "weekly":
                next_run_at = next_weekly_run_at(schedule["day_of_week"], schedule["daily_time"], now)
            elif schedule_type == "once":
                # Never recomputed -- disabling it below is what actually
                # stops it firing again; the exact value left in
                # next_run_at from here on is never read again.
                next_run_at = now
            else:
                next_run_at = now + timedelta(seconds=schedule["interval_seconds"])
            self.state_store.record_schedule_run(schedule["id"], next_run_at.isoformat(), status)
            if schedule_type == "once":
                self.state_store.set_schedule_enabled(schedule["id"], False)
                if status == "triggered" and self.on_once_fired is not None:
                    self.on_once_fired(schedule)
