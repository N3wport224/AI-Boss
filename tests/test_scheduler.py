import time
from datetime import datetime, timezone

from engine.state_store import StateStore
from webapp.scheduler import Scheduler


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
