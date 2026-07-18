import json
import shutil

import pytest
from fastapi.testclient import TestClient

from webapp import pipelines as pipeline_store
from webapp.main import app

client = TestClient(app)


@pytest.fixture(autouse=True)
def clean_pipelines_dir():
    if pipeline_store.PIPELINES_DIR.exists():
        shutil.rmtree(pipeline_store.PIPELINES_DIR)
    yield
    if pipeline_store.PIPELINES_DIR.exists():
        shutil.rmtree(pipeline_store.PIPELINES_DIR)


def test_health_check_reports_ready_for_the_bundled_modules():
    res = client.get("/api/health")
    assert res.status_code == 200

    body = res.json()
    assert body["status"] == "ready"
    names = {check["name"] for check in body["checks"]}
    assert names == {"state_store", "manifests", "entrypoints", "environment"}
    assert all(check["ok"] for check in body["checks"])


def test_performance_reports_a_live_process_snapshot():
    res = client.get("/api/performance")
    assert res.status_code == 200

    body = res.json()
    assert body["pid"] > 0
    assert body["memory_rss_mb"] > 0
    assert body["thread_count"] >= 1
    assert body["active_run_threads"] >= 0
    assert body["uptime_seconds"] >= 0


def test_memory_endpoints_list_delete_and_redact():
    from webapp.main import store

    store.set_memory("last_risk_level", "high")
    store.set_memory("agent_secret_token", "sk-should-be-hidden")

    listed = client.get("/api/memory").json()
    entries = {entry["key"]: entry["value"] for entry in listed}
    assert entries["last_risk_level"] == "high"
    assert entries["agent_secret_token"] == "***REDACTED***"

    res = client.delete("/api/memory/last_risk_level")
    assert res.status_code == 200
    assert res.json() == {"deleted": "last_risk_level"}
    remaining_keys = {entry["key"] for entry in client.get("/api/memory").json()}
    assert "last_risk_level" not in remaining_keys

    clear_res = client.delete("/api/memory")
    assert clear_res.status_code == 200
    assert client.get("/api/memory").json() == []


def test_prune_runs_removes_only_finished_runs_older_than_cutoff(tmp_path):
    from datetime import datetime, timedelta, timezone

    from engine.context import StepRecord
    from engine.state_store import StateStore

    isolated_store = StateStore(str(tmp_path / "prune_test.db"))

    # An old, finished run -> should be pruned.
    old_run_id = isolated_store.start_run()
    now = datetime.now(timezone.utc)
    isolated_store.log_step(old_run_id, StepRecord("a", "automation", now, now, True, {}))
    isolated_store.finish_run(old_run_id, "completed")
    old_cutoff = (now - timedelta(hours=1)).isoformat()
    isolated_store._conn.execute("UPDATE runs SET finished_at = ? WHERE id = ?", (old_cutoff, old_run_id))
    isolated_store._conn.commit()

    # A recent, finished run -> should survive.
    recent_run_id = isolated_store.start_run()
    isolated_store.finish_run(recent_run_id, "completed")

    removed = isolated_store.prune_runs(older_than_hours=0.01)
    isolated_store.close()

    assert removed == 1

    reopened = StateStore(str(tmp_path / "prune_test.db"))
    remaining_ids = {run["id"] for run in reopened.recent_runs(limit=10)}
    assert old_run_id not in remaining_ids
    assert recent_run_id in remaining_ids
    assert reopened.steps_for_run(old_run_id) == []  # its steps were removed too
    reopened.close()


def test_purge_runs_endpoint_removes_old_runs():
    res = client.post("/api/pipeline/run", json={"inputs": {}})
    with client.stream("GET", f"/api/stream/{res.json()['stream_id']}") as response:
        for line in response.iter_lines():
            if line.startswith("data: ") and '"run_completed"' in line:
                break

    before = client.get("/api/metrics").json()["total_runs"]
    purge_res = client.post("/api/runs/purge", params={"older_than_hours": 0})
    assert purge_res.status_code == 200
    assert purge_res.json()["removed_count"] >= 1

    after = client.get("/api/metrics").json()["total_runs"]
    assert after < before


def test_restore_snapshot_is_additive_and_idempotent(tmp_path):
    from engine.state_store import StateStore

    source_store = StateStore(str(tmp_path / "source.db"))
    run_id = source_store.start_run()
    source_store.finish_run(run_id, "completed")
    source_store.set_memory("some_key", {"nested": "value"})
    snapshot = source_store.export_snapshot()
    source_store.close()

    target_store = StateStore(str(tmp_path / "target.db"))
    first_counts = target_store.restore_snapshot(snapshot)
    assert first_counts["runs"] == 1
    assert first_counts["memory"] == 1

    assert target_store.get_memory("some_key") == {"nested": "value"}
    assert {r["id"] for r in target_store.recent_runs(10)} == {run_id}

    # Restoring the exact same snapshot again inserts nothing new.
    second_counts = target_store.restore_snapshot(snapshot)
    assert second_counts == {"runs": 0, "steps": 0, "schedules": 0, "ingested_files": 0, "memory": 0}

    target_store.close()


def test_restore_snapshot_never_overwrites_an_existing_memory_key():
    from webapp.main import store

    store.set_memory("protected_key", "original_value")
    fake_snapshot = {"memory": [{"key": "protected_key", "value": "attempted_overwrite", "updated_at": None}]}

    store.restore_snapshot(fake_snapshot)
    assert store.get_memory("protected_key") == "original_value"


def test_backup_restore_endpoint_fills_in_missing_runs_and_memory():
    from webapp.main import store

    fake_run_id = 987654321
    fake_snapshot = {
        "runs": [{"id": fake_run_id, "started_at": "2020-01-01T00:00:00+00:00", "finished_at": "2020-01-01T00:00:01+00:00", "status": "completed"}],
        "steps": [],
        "schedules": [],
        "ingested_files": [],
        "memory": [{"key": "restored_test_key", "value": "restored_value", "updated_at": None}],
        "pipelines": [],
    }
    snapshot_bytes = json.dumps(fake_snapshot).encode()

    res = client.post("/api/backup/restore", files={"file": ("backup.json", snapshot_bytes, "application/json")})
    assert res.status_code == 200
    body = res.json()
    assert body["runs"] == 1
    assert body["memory"] == 1
    assert body["pipelines"] == 0

    assert store.get_memory("restored_test_key") == "restored_value"
    run_ids = {r["id"] for r in client.get("/api/runs?limit=1000").json()}
    assert fake_run_id in run_ids

    # Restoring the same snapshot again is a no-op — nothing new inserted.
    res2 = client.post("/api/backup/restore", files={"file": ("backup.json", snapshot_bytes, "application/json")})
    assert res2.json()["runs"] == 0
    assert res2.json()["memory"] == 0


def test_backup_restore_rejects_malformed_json():
    res = client.post("/api/backup/restore", files={"file": ("bad.json", b"{not valid", "application/json")})
    assert res.status_code == 400


def test_state_store_export_snapshot_reflects_runs_and_steps(tmp_path):
    from datetime import datetime, timezone

    from engine.context import StepRecord
    from engine.state_store import StateStore

    isolated_store = StateStore(str(tmp_path / "isolated.db"))
    run_id = isolated_store.start_run()
    now = datetime.now(timezone.utc)
    isolated_store.log_step(run_id, StepRecord("fetch_raw_metrics", "automation", now, now, True, {"raw_metrics": {"signups": 5}}))
    isolated_store.finish_run(run_id, "completed")

    snapshot = isolated_store.export_snapshot()
    isolated_store.close()

    assert len(snapshot["runs"]) == 1
    assert snapshot["runs"][0]["status"] == "completed"
    assert len(snapshot["steps"]) == 1
    assert snapshot["steps"][0]["output"] == {"raw_metrics": {"signups": 5}}
    assert snapshot["steps"][0]["success"] is True
    assert snapshot["schedules"] == []
    assert snapshot["ingested_files"] == []


def test_backup_export_includes_a_completed_run_and_saved_pipelines():
    res = client.post("/api/pipeline/run", json={"inputs": {}})
    with client.stream("GET", f"/api/stream/{res.json()['stream_id']}") as response:
        for line in response.iter_lines():
            if line.startswith("data: ") and '"run_completed"' in line:
                break

    client.post(
        "/api/pipelines",
        json={"name": "Backup Test", "steps": [{"tier": "automation", "name": "fetch_raw_metrics"}]},
    )

    res = client.get("/api/backup/export")
    assert res.status_code == 200
    body = res.json()

    assert "exported_at" in body
    assert len(body["runs"]) >= 1
    assert len(body["steps"]) >= 1
    # A step's output is parsed back into a real dict, not left as a JSON string.
    assert isinstance(body["steps"][0]["output"], dict)
    assert any(p["slug"] == "backup_test" for p in body["pipelines"])

    from webapp import pipelines as pipeline_store
    import shutil

    shutil.rmtree(pipeline_store.PIPELINES_DIR, ignore_errors=True)


def test_backup_db_download_is_a_valid_sqlite_file(tmp_path):
    res = client.get("/api/backup/db")
    assert res.status_code == 200
    assert res.headers["content-disposition"].endswith('filename="orchestrator.db"')

    downloaded = tmp_path / "downloaded.db"
    downloaded.write_bytes(res.content)

    import sqlite3

    conn = sqlite3.connect(str(downloaded))
    tables = {row[0] for row in conn.execute("SELECT name FROM sqlite_master WHERE type='table'")}
    conn.close()
    assert {"runs", "steps", "schedules"}.issubset(tables)


def test_metrics_reflect_a_completed_run():
    before = client.get("/api/metrics").json()

    res = client.post("/api/pipeline/run", json={"inputs": {}})
    stream_id = res.json()["stream_id"]
    with client.stream("GET", f"/api/stream/{stream_id}") as response:
        for line in response.iter_lines():
            if line.startswith("data: ") and '"run_completed"' in line:
                break

    after = client.get("/api/metrics").json()
    assert after["total_runs"] == before["total_runs"] + 1
    assert after["completed"] >= 1
    assert after["success_rate"] is not None
    assert after["avg_duration_seconds"] is not None


def test_runs_csv_export_has_a_header_and_rows():
    client.post("/api/pipeline/run", json={"inputs": {}})
    res = client.get("/api/runs.csv")
    assert res.status_code == 200
    assert res.headers["content-type"].startswith("text/csv")

    lines = res.text.strip().splitlines()
    assert lines[0] == "id,started_at,finished_at,status,duration_seconds"
    assert len(lines) >= 2


def test_duplicate_pipeline_creates_a_distinct_copy():
    client.post(
        "/api/pipelines",
        json={"name": "Original", "steps": [{"tier": "automation", "name": "fetch_raw_metrics"}]},
    )

    res = client.post("/api/pipelines/original/duplicate")
    assert res.status_code == 200
    duplicated = res.json()["pipeline"]
    assert duplicated["slug"] == "original_copy"
    assert duplicated["name"] == "Original (copy)"
    assert duplicated["steps"] == [
        {"tier": "automation", "name": "fetch_raw_metrics", "inputs": {}, "mappings": {}}
    ]

    slugs = {p["slug"] for p in client.get("/api/pipelines").json()}
    assert slugs == {"original", "original_copy"}


def test_duplicate_unknown_pipeline_404s():
    res = client.post("/api/pipelines/does_not_exist/duplicate")
    assert res.status_code == 404
