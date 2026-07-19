import time
from datetime import datetime, timedelta, timezone

from engine.state_store import StateStore
from webapp.scheduler import Scheduler, next_daily_run_at, next_weekly_run_at


def test_scheduler_fires_due_schedules_and_reschedules_next_run(tmp_path):
    store = StateStore(str(tmp_path / "test.db"))
    calls = []
    scheduler = Scheduler(store, trigger=lambda schedule: calls.append(schedule["id"]), poll_interval=0.05)

    store.create_schedule(
        kind="module", name="fake", interval_seconds=0.05,
        next_run_at=datetime.now(timezone.utc).isoformat(), tier="automation", inputs={},
    )

    scheduler.start()
    time.sleep(0.3)
    scheduler.stop()

    assert len(calls) >= 2  # short interval -> should have fired more than once
    updated = store.list_schedules()[0]
    assert updated["last_status"] == "triggered"
    assert updated["last_run_at"] is not None
    store.close()


def test_scheduler_records_trigger_errors_without_crashing(tmp_path):
    store = StateStore(str(tmp_path / "test2.db"))

    def boom(schedule):
        raise RuntimeError("trigger failed")

    scheduler = Scheduler(store, trigger=boom, poll_interval=0.05)
    store.create_schedule(
        kind="module", name="fake", interval_seconds=1.0,
        next_run_at=datetime.now(timezone.utc).isoformat(), tier="automation", inputs={},
    )
    scheduler.start()
    time.sleep(0.2)
    scheduler.stop()

    updated = store.list_schedules()[0]
    assert "trigger failed" in updated["last_status"]
    store.close()


def test_scheduler_calls_on_error_with_the_schedule_and_error_message(tmp_path):
    store = StateStore(str(tmp_path / "test_on_error.db"))

    def boom(schedule):
        raise RuntimeError("kaboom")

    calls = []
    scheduler = Scheduler(
        store, trigger=boom, poll_interval=0.05,
        on_error=lambda schedule, message: calls.append((schedule["name"], message)),
    )
    store.create_schedule(
        kind="module", name="fake_on_error", interval_seconds=1.0,
        next_run_at=datetime.now(timezone.utc).isoformat(), tier="automation", inputs={},
    )
    scheduler.start()
    time.sleep(0.2)
    scheduler.stop()

    assert calls
    name, message = calls[0]
    assert name == "fake_on_error"
    assert "kaboom" in message
    store.close()


def test_scheduler_without_on_error_never_raises_on_trigger_failure(tmp_path):
    """on_error defaults to None -- every caller from before this feature
    existed (including every other test in this file) must keep working."""
    store = StateStore(str(tmp_path / "test_no_on_error.db"))

    def boom(schedule):
        raise RuntimeError("still fine")

    scheduler = Scheduler(store, trigger=boom, poll_interval=0.05)
    store.create_schedule(
        kind="module", name="fake_no_handler", interval_seconds=1.0,
        next_run_at=datetime.now(timezone.utc).isoformat(), tier="automation", inputs={},
    )
    scheduler.start()
    time.sleep(0.2)
    scheduler.stop()

    updated = store.list_schedules()[0]
    assert "still fine" in updated["last_status"]
    store.close()


def test_next_daily_run_at_stays_today_when_time_is_still_ahead():
    after = datetime.now(timezone.utc)
    local_after = after.astimezone()
    target = (local_after + timedelta(hours=2)).strftime("%H:%M")

    result = next_daily_run_at(target, after)

    delta = result - after
    assert timedelta(hours=1, minutes=58) <= delta <= timedelta(hours=2, minutes=1)


def test_next_daily_run_at_rolls_to_tomorrow_when_time_already_passed():
    after = datetime.now(timezone.utc)
    local_after = after.astimezone()
    target = (local_after - timedelta(hours=1)).strftime("%H:%M")

    result = next_daily_run_at(target, after)

    delta = result - after
    assert timedelta(hours=22, minutes=58) <= delta <= timedelta(hours=23, minutes=1)


def test_next_weekly_run_at_stays_this_week_when_day_and_time_are_still_ahead():
    # Anchored to local noon rather than the real current time -- the plain
    # datetime.now() version of this test was flaky whenever it happened to
    # run within ~2 hours of local midnight, since adding 2 hours would spill
    # into the next calendar day while target_day stayed pinned to "today",
    # making next_weekly_run_at correctly (but unexpectedly, for this test)
    # roll a full week forward instead of staying "later today".
    after = datetime.now(timezone.utc).astimezone().replace(hour=12, minute=0, second=0, microsecond=0).astimezone(timezone.utc)
    local_after = after.astimezone()
    # Same weekday, two hours from now -- should fire later today, not roll a week.
    target_time = (local_after + timedelta(hours=2)).strftime("%H:%M")
    target_day = local_after.weekday()

    result = next_weekly_run_at(target_day, target_time, after)

    delta = result - after
    assert timedelta(hours=1, minutes=58) <= delta <= timedelta(hours=2, minutes=1)


def test_next_weekly_run_at_rolls_to_next_week_when_same_day_but_time_already_passed():
    # Same local-noon anchor as the test above, for the same reason.
    after = datetime.now(timezone.utc).astimezone().replace(hour=12, minute=0, second=0, microsecond=0).astimezone(timezone.utc)
    local_after = after.astimezone()
    target_time = (local_after - timedelta(hours=1)).strftime("%H:%M")
    target_day = local_after.weekday()

    result = next_weekly_run_at(target_day, target_time, after)

    delta = result - after
    assert timedelta(days=6, hours=22) <= delta <= timedelta(days=7, hours=1)


def test_next_weekly_run_at_picks_the_correct_day_of_week():
    after = datetime.now(timezone.utc)
    local_after = after.astimezone()
    target_day = (local_after.weekday() + 3) % 7  # three days from now
    target_time = local_after.strftime("%H:%M")

    result = next_weekly_run_at(target_day, target_time, after)

    assert result.astimezone().weekday() == target_day
    delta = result - after
    assert timedelta(days=2, hours=23) <= delta <= timedelta(days=3, hours=1)


def test_scheduler_fires_a_weekly_schedule_whose_day_and_time_are_already_due(tmp_path):
    store = StateStore(str(tmp_path / "weekly_due.db"))
    calls = []
    scheduler = Scheduler(store, trigger=lambda schedule: calls.append(schedule["id"]), poll_interval=0.05)

    now = datetime.now(timezone.utc)
    store.create_schedule(
        kind="module", name="fake_weekly", interval_seconds=None,
        next_run_at=now.isoformat(), tier="automation", inputs={},
        schedule_type="weekly", daily_time="09:00", day_of_week=now.astimezone().weekday(),
    )

    scheduler.start()
    time.sleep(0.2)
    scheduler.stop()

    assert calls
    updated = store.list_schedules()[0]
    assert updated["last_status"] == "triggered"
    assert updated["schedule_type"] == "weekly"
    # Rescheduled a full week out since it just fired.
    next_run = datetime.fromisoformat(updated["next_run_at"])
    assert timedelta(days=6) <= (next_run - now) <= timedelta(days=7, hours=1)
    store.close()


def test_scheduler_fires_a_once_schedule_exactly_once_then_disables_it(tmp_path):
    store = StateStore(str(tmp_path / "once.db"))
    calls = []
    scheduler = Scheduler(store, trigger=lambda schedule: calls.append(schedule["id"]), poll_interval=0.05)

    now = datetime.now(timezone.utc)
    store.create_schedule(
        kind="module", name="fake_once", interval_seconds=None,
        next_run_at=now.isoformat(), tier="automation", inputs={},
        schedule_type="once",
    )

    scheduler.start()
    time.sleep(0.3)  # several poll intervals -- a bug here would fire more than once
    scheduler.stop()

    assert len(calls) == 1
    updated = store.list_schedules()[0]
    assert updated["last_status"] == "triggered"
    assert updated["enabled"] is False
    store.close()


def test_scheduler_pause_stops_all_schedules_from_firing_regardless_of_their_own_enabled_state(tmp_path):
    store = StateStore(str(tmp_path / "paused.db"))
    calls = []
    scheduler = Scheduler(store, trigger=lambda s: calls.append(s["id"]), poll_interval=0.05)

    store.create_schedule(
        kind="module", name="fake", interval_seconds=0.05,
        next_run_at=datetime.now(timezone.utc).isoformat(), tier="automation", inputs={},
    )
    assert scheduler.paused is False

    scheduler.pause()
    assert scheduler.paused is True

    scheduler.start()
    time.sleep(0.3)
    scheduler.stop()

    assert calls == []  # the schedule is enabled and due, but the master pause wins
    store.close()


def test_scheduler_resume_lets_schedules_fire_again(tmp_path):
    store = StateStore(str(tmp_path / "resumed.db"))
    calls = []
    scheduler = Scheduler(store, trigger=lambda s: calls.append(s["id"]), poll_interval=0.05)

    store.create_schedule(
        kind="module", name="fake", interval_seconds=0.05,
        next_run_at=datetime.now(timezone.utc).isoformat(), tier="automation", inputs={},
    )

    scheduler.pause()
    scheduler.start()
    time.sleep(0.15)
    assert calls == []

    scheduler.resume()
    assert scheduler.paused is False
    time.sleep(0.3)
    scheduler.stop()

    assert len(calls) >= 1
    store.close()


def test_scheduler_reschedules_daily_schedule_about_24h_out(tmp_path):
    store = StateStore(str(tmp_path / "daily.db"))
    calls = []
    scheduler = Scheduler(store, trigger=lambda s: calls.append(s["id"]), poll_interval=0.05)

    now = datetime.now(timezone.utc)
    due_time = now.astimezone().strftime("%H:%M")  # "now", so it's already due
    store.create_schedule(
        kind="module", name="fake", interval_seconds=None,
        next_run_at=now.isoformat(), tier="automation", inputs={},
        schedule_type="daily", daily_time=due_time,
    )

    scheduler.start()
    time.sleep(0.3)
    scheduler.stop()

    assert len(calls) == 1  # fires once; the rescheduled next_run_at is ~24h out
    updated = store.list_schedules()[0]
    assert updated["schedule_type"] == "daily"
    next_run = datetime.fromisoformat(updated["next_run_at"])
    assert next_run - now >= timedelta(hours=23)
    store.close()


def test_disabled_schedule_is_never_triggered(tmp_path):
    store = StateStore(str(tmp_path / "test3.db"))
    calls = []
    scheduler = Scheduler(store, trigger=lambda s: calls.append(s["id"]), poll_interval=0.05)

    schedule = store.create_schedule(
        kind="module", name="fake", interval_seconds=0.05,
        next_run_at=datetime.now(timezone.utc).isoformat(), tier="automation", inputs={},
    )
    store.set_schedule_enabled(schedule["id"], False)

    scheduler.start()
    time.sleep(0.2)
    scheduler.stop()

    assert calls == []
    store.close()


def test_scheduler_calls_on_once_fired_only_for_a_successful_once_schedule(tmp_path):
    store = StateStore(str(tmp_path / "once_fired.db"))
    fired = []
    scheduler = Scheduler(
        store, trigger=lambda schedule: None, poll_interval=0.05,
        on_once_fired=lambda schedule: fired.append(schedule["name"]),
    )

    now = datetime.now(timezone.utc)
    store.create_schedule(
        kind="module", name="fake_once_fired", interval_seconds=None,
        next_run_at=now.isoformat(), tier="automation", inputs={},
        schedule_type="once",
    )

    scheduler.start()
    time.sleep(0.3)
    scheduler.stop()

    assert fired == ["fake_once_fired"]
    store.close()


def test_scheduler_does_not_call_on_once_fired_when_the_once_schedule_errors(tmp_path):
    store = StateStore(str(tmp_path / "once_fired_error.db"))
    fired = []

    def boom(schedule):
        raise RuntimeError("nope")

    scheduler = Scheduler(
        store, trigger=boom, poll_interval=0.05,
        on_once_fired=lambda schedule: fired.append(schedule["name"]),
    )

    now = datetime.now(timezone.utc)
    store.create_schedule(
        kind="module", name="fake_once_error", interval_seconds=None,
        next_run_at=now.isoformat(), tier="automation", inputs={},
        schedule_type="once",
    )

    scheduler.start()
    time.sleep(0.3)
    scheduler.stop()

    assert fired == []
    updated = store.list_schedules()[0]
    assert "nope" in updated["last_status"]
    assert updated["enabled"] is False  # still disabled after firing once, even on error
    store.close()


def test_scheduler_without_on_once_fired_never_raises(tmp_path):
    """on_once_fired defaults to None -- every caller from before this
    feature existed must keep working."""
    store = StateStore(str(tmp_path / "once_no_hook.db"))
    scheduler = Scheduler(store, trigger=lambda schedule: None, poll_interval=0.05)

    now = datetime.now(timezone.utc)
    store.create_schedule(
        kind="module", name="fake_once_no_hook", interval_seconds=None,
        next_run_at=now.isoformat(), tier="automation", inputs={},
        schedule_type="once",
    )

    scheduler.start()
    time.sleep(0.2)
    scheduler.stop()

    updated = store.list_schedules()[0]
    assert updated["last_status"] == "triggered"
    store.close()
