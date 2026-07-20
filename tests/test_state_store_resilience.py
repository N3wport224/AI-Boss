"""Retry-with-backoff behavior for StateStore's initial connection, and the
liveness helpers (is_alive()) added to the scheduler/watcher/auto-backup
background threads for the /healthz endpoint."""
import sqlite3
import threading
import time

import pytest

from engine.state_store import StateStore


def _hold_exclusive_lock(path, hold_seconds, ready_event):
    """Open its own connection, take an EXCLUSIVE lock (blocking every other
    writer, including a fresh StateStore's own CREATE TABLE statements),
    hold it for `hold_seconds`, then release. `ready_event` is set once the
    lock is actually held, so the caller never races starting its own
    connection attempt before there's anything to contend with."""
    conn = sqlite3.connect(str(path), timeout=0)
    conn.execute("BEGIN EXCLUSIVE")
    conn.execute("CREATE TABLE IF NOT EXISTS lock_holder_marker (a INTEGER)")
    ready_event.set()
    time.sleep(hold_seconds)
    conn.commit()
    conn.close()


def test_state_store_retries_past_a_transient_lock_and_succeeds(tmp_path):
    path = tmp_path / "locked.db"
    sqlite3.connect(str(path)).close()  # the file must already exist to lock it

    # Held well past StateStore's own short internal connect timeout
    # (CONNECT_TIMEOUT_SECONDS = 0.1s), so at least one attempt genuinely
    # hits "database is locked" and falls through to our retry loop instead
    # of just quietly succeeding inside sqlite3's own busy-wait.
    ready = threading.Event()
    holder = threading.Thread(target=_hold_exclusive_lock, args=(path, 0.5, ready))
    holder.start()
    ready.wait(timeout=2.0)

    sleeps = []
    store = StateStore(str(path), max_retries=10, backoff_seconds=0.05, _sleep=sleeps.append)
    holder.join()

    assert store.recent_runs(1) == []
    assert len(sleeps) >= 1, "expected at least one retry against the held lock"
    # Exponential: each recorded delay should be double the previous one.
    for prev, nxt in zip(sleeps, sleeps[1:]):
        assert nxt == pytest.approx(prev * 2)
    store.close()


def test_state_store_gives_up_after_max_retries_against_a_persistent_lock(tmp_path):
    path = tmp_path / "locked2.db"
    sqlite3.connect(str(path)).close()

    ready = threading.Event()
    holder = threading.Thread(target=_hold_exclusive_lock, args=(path, 3.0, ready))
    holder.start()
    ready.wait(timeout=2.0)

    sleeps = []
    try:
        with pytest.raises(sqlite3.OperationalError, match="locked"):
            StateStore(str(path), max_retries=2, backoff_seconds=0.01, _sleep=sleeps.append)
        assert len(sleeps) == 2
    finally:
        # Don't leave a long-sleeping thread behind for the next test.
        holder.join(timeout=3.5)


def test_state_store_does_not_retry_a_non_lock_error(tmp_path):
    """A directory path can never be opened as a SQLite file -- this must
    fail immediately, not be mistaken for a transient lock and retried."""
    sleeps = []
    with pytest.raises(sqlite3.OperationalError):
        StateStore(str(tmp_path), max_retries=5, backoff_seconds=0.01, _sleep=sleeps.append)
    assert sleeps == []


def test_scheduler_watcher_auto_backup_report_alive_once_started():
    from webapp.auto_backup import AutoBackup
    from webapp.scheduler import Scheduler
    from webapp.watcher import FilesystemWatcher

    store = StateStore(":memory:")
    scheduler = Scheduler(store, trigger=lambda schedule: None, poll_interval=10.0)
    assert scheduler.is_alive() is False
    scheduler.start()
    try:
        assert scheduler.is_alive() is True
    finally:
        scheduler.stop()
    assert scheduler.is_alive() is False

    watcher = FilesystemWatcher(on_new_file=lambda path: None, interval=10.0)
    assert watcher.is_alive() is False
    watcher.start()
    try:
        assert watcher.is_alive() is True
    finally:
        watcher.stop()
    assert watcher.is_alive() is False

    auto_backup = AutoBackup(backups_dir=None, build_snapshot=lambda: {}, poll_interval=10.0)
    assert auto_backup.is_alive() is False
    auto_backup.start()
    try:
        assert auto_backup.is_alive() is True
    finally:
        auto_backup.stop()
    assert auto_backup.is_alive() is False

    store.close()
