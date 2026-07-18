import shutil
import time

from webapp import ingestion, watcher
from webapp.main import app  # noqa: F401  (starts the module-level watcher thread)


def _wait_until(predicate, timeout=5.0, interval=0.05):
    deadline = time.time() + timeout
    while time.time() < deadline:
        if predicate():
            return True
        time.sleep(interval)
    return False


def test_watcher_detects_a_new_file_and_ignores_ones_already_present(tmp_path):
    seen = []
    watch_dir = tmp_path / "watched"
    watch_dir.mkdir()
    (watch_dir / "already-here.csv").write_text("a,b\n1,2\n")

    original_watch_dir = watcher.WATCH_DIR
    watcher.WATCH_DIR = watch_dir
    try:
        fs_watcher = watcher.FilesystemWatcher(on_new_file=lambda p: seen.append(p.name), interval=0.05)
        fs_watcher.start()
        try:
            time.sleep(0.2)
            assert seen == []  # pre-existing file must not be replayed as "new"

            (watch_dir / "new-file.csv").write_text("x,y\n3,4\n")
            assert _wait_until(lambda: "new-file.csv" in seen)
        finally:
            fs_watcher.stop()
    finally:
        watcher.WATCH_DIR = original_watch_dir


def test_app_level_watcher_skips_a_file_over_the_size_limit(monkeypatch):
    import webapp.main as main

    if ingestion.ARTIFACTS_DIR.exists():
        shutil.rmtree(ingestion.ARTIFACTS_DIR)
    watcher.ensure_watch_dir()
    monkeypatch.setattr(ingestion, "MAX_UPLOAD_BYTES", 10)

    filename = f"oversized_test_{int(time.time() * 1000)}.csv"
    (watcher.WATCH_DIR / filename).write_text("name,value\nfoo,1\nbar,2\n")  # well over 10 bytes

    assert _wait_until(lambda: any(entry["filename"] == filename for entry in main._watcher_log))
    entry = next(e for e in main._watcher_log if e["filename"] == filename)
    assert "error" in entry and "auto-ingest limit" in entry["error"]
    assert ingestion.list_artifacts() == []

    (watcher.WATCH_DIR / filename).unlink(missing_ok=True)
    shutil.rmtree(ingestion.ARTIFACTS_DIR, ignore_errors=True)


def test_app_level_watcher_auto_ingests_a_dropped_json_file():
    if ingestion.ARTIFACTS_DIR.exists():
        shutil.rmtree(ingestion.ARTIFACTS_DIR)
    watcher.ensure_watch_dir()

    filename = f"watched_test_{int(time.time() * 1000)}.json"
    (watcher.WATCH_DIR / filename).write_text('[{"name": "foo", "value": 1}]')

    from webapp.main import _watcher_log

    assert _wait_until(lambda: any(entry["filename"] == filename for entry in _watcher_log), timeout=6.0)
    entry = next(e for e in _watcher_log if e["filename"] == filename)
    assert "error" not in entry

    saved = list(ingestion.ARTIFACTS_DIR.glob(f"*{filename}"))
    assert len(saved) == 1

    (watcher.WATCH_DIR / filename).unlink(missing_ok=True)
    shutil.rmtree(ingestion.ARTIFACTS_DIR, ignore_errors=True)


def test_app_level_watcher_auto_ingests_a_dropped_csv():
    if ingestion.ARTIFACTS_DIR.exists():
        shutil.rmtree(ingestion.ARTIFACTS_DIR)
    watcher.ensure_watch_dir()

    filename = f"watched_test_{int(time.time() * 1000)}.csv"
    (watcher.WATCH_DIR / filename).write_text("name,value\nfoo,1\n")

    from webapp.main import _watcher_log

    assert _wait_until(lambda: any(entry["filename"] == filename for entry in _watcher_log), timeout=6.0)

    entry = next(e for e in _watcher_log if e["filename"] == filename)
    assert "error" not in entry

    json_artifacts = list(ingestion.ARTIFACTS_DIR.glob(f"*{filename.replace('.csv', '.json')}"))
    assert len(json_artifacts) == 1

    (watcher.WATCH_DIR / filename).unlink(missing_ok=True)
    shutil.rmtree(ingestion.ARTIFACTS_DIR, ignore_errors=True)
