import time

from webapp.auto_backup import AutoBackup


def _dummy_snapshot():
    return {"runs": [], "marker": "test-snapshot"}


def test_run_now_writes_a_snapshot_file_and_updates_last_backup_at(tmp_path):
    backup = AutoBackup(tmp_path / "backups", _dummy_snapshot)
    assert backup.last_backup_at is None

    timestamp = backup.run_now()
    assert timestamp == backup.last_backup_at

    files = list((tmp_path / "backups").glob("backup_*.json"))
    assert len(files) == 1
    assert '"marker": "test-snapshot"' in files[0].read_text()


def test_prune_keeps_only_the_most_recent_keep_count_files(tmp_path):
    backup = AutoBackup(tmp_path / "backups", _dummy_snapshot)
    backup.configure(enabled=True, interval_hours=24.0, keep_count=2)

    backup.run_now()
    time.sleep(1.1)  # filenames are second-resolution timestamps -- force distinct names
    backup.run_now()
    time.sleep(1.1)
    backup.run_now()

    files = sorted((tmp_path / "backups").glob("backup_*.json"))
    assert len(files) == 2


def test_status_reports_enabled_state_and_computes_next_backup_at(tmp_path):
    backup = AutoBackup(tmp_path / "backups", _dummy_snapshot)
    status_before = backup.status()
    assert status_before == {
        "enabled": False,
        "interval_hours": 24.0,
        "keep_count": 7,
        "last_backup_at": None,
        "next_backup_at": None,
    }

    backup.configure(enabled=True, interval_hours=1.0, keep_count=3)
    backup.run_now()
    status_after = backup.status()
    assert status_after["enabled"] is True
    assert status_after["interval_hours"] == 1.0
    assert status_after["keep_count"] == 3
    assert status_after["last_backup_at"] is not None
    assert status_after["next_backup_at"] is not None
    assert status_after["next_backup_at"] > status_after["last_backup_at"]


def test_background_loop_fires_when_enabled_and_due(tmp_path):
    backup = AutoBackup(tmp_path / "backups", _dummy_snapshot, poll_interval=0.05)
    backup.configure(enabled=True, interval_hours=0.0000001, keep_count=5)  # effectively "always due"

    backup.start()
    time.sleep(0.3)
    backup.stop()

    files = list((tmp_path / "backups").glob("backup_*.json"))
    assert len(files) >= 1


def test_disabled_backup_never_writes_from_the_background_loop(tmp_path):
    backup = AutoBackup(tmp_path / "backups", _dummy_snapshot, poll_interval=0.05)
    backup.configure(enabled=False, interval_hours=0.0000001, keep_count=5)

    backup.start()
    time.sleep(0.2)
    backup.stop()

    assert not (tmp_path / "backups").exists() or not list((tmp_path / "backups").glob("backup_*.json"))


def test_background_loop_reports_a_failing_snapshot_via_on_failure_and_keeps_running(tmp_path):
    def _broken_snapshot():
        raise RuntimeError("disk is full")

    failures = []
    backup = AutoBackup(
        tmp_path / "backups", _broken_snapshot, poll_interval=0.05, on_failure=failures.append
    )
    backup.configure(enabled=True, interval_hours=0.0000001, keep_count=5)

    backup.start()
    time.sleep(0.3)
    backup.stop()

    assert failures, "expected on_failure to be called at least once"
    assert isinstance(failures[0], RuntimeError)
    assert backup.last_backup_at is None, "a failed write must not be recorded as a successful backup"


def test_run_now_still_raises_directly_when_called_outside_the_background_loop(tmp_path):
    def _broken_snapshot():
        raise RuntimeError("disk is full")

    failures = []
    backup = AutoBackup(tmp_path / "backups", _broken_snapshot, on_failure=failures.append)

    try:
        backup.run_now()
        assert False, "run_now() should propagate the exception when called directly"
    except RuntimeError:
        pass

    assert not failures, "on_failure is only for the background loop's own tick, not a direct run_now() call"
