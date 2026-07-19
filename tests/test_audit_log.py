"""Batch 10: recent actions audit trail -- a lightweight, state-store-backed
log of destructive/administrative actions (run purge, bulk-delete runs,
artifact purge, backup restore, circuit breaker reset, schedule pause/resume,
pipeline version restore) so a user can see what happened and when without
guessing.
"""
import json
import shutil

import pytest
from fastapi.testclient import TestClient

from engine.state_store import StateStore
from webapp import pipelines as pipeline_store
from webapp.main import app, store

client = TestClient(app)


@pytest.fixture(autouse=True)
def clean_pipelines_dir():
    if pipeline_store.PIPELINES_DIR.exists():
        shutil.rmtree(pipeline_store.PIPELINES_DIR)
    yield
    if pipeline_store.PIPELINES_DIR.exists():
        shutil.rmtree(pipeline_store.PIPELINES_DIR)


# ---- Store-level behavior ----

def test_record_and_list_events_newest_first(tmp_path):
    s = StateStore(str(tmp_path / "audit.db"))
    first = s.record_audit_event("run_purge", "Purged 3 run(s).")
    second = s.record_audit_event("artifact_purge", "Purged 1 artifact(s).")

    events = s.list_audit_events()
    assert events[0]["id"] == second["id"]
    assert events[1]["id"] == first["id"]
    assert events[0]["action"] == "artifact_purge"
    assert events[0]["created_at"]
    s.close()


def test_list_respects_limit(tmp_path):
    s = StateStore(str(tmp_path / "audit_limit.db"))
    for i in range(5):
        s.record_audit_event("run_purge", f"event {i}")

    events = s.list_audit_events(limit=2)
    assert len(events) == 2
    assert events[0]["detail"] == "event 4"
    assert events[1]["detail"] == "event 3"
    s.close()


# ---- End-to-end through the webapp ----

def test_circuit_breaker_reset_is_logged():
    res = client.post("/api/breakers/automation/fetch_raw_metrics/reset")
    assert res.status_code == 200

    events = client.get("/api/audit-log").json()
    assert any(
        e["action"] == "circuit_breaker_reset" and "automation/fetch_raw_metrics" in e["detail"]
        for e in events
    )


def test_run_purge_is_logged():
    res = client.post("/api/runs/purge", params={"older_than_hours": 999999})
    assert res.status_code == 200

    events = client.get("/api/audit-log").json()
    assert any(e["action"] == "run_purge" for e in events)


def test_run_bulk_delete_is_logged():
    res = client.post("/api/runs/bulk-delete", json={"run_ids": [999999999]})
    assert res.status_code == 200

    events = client.get("/api/audit-log").json()
    assert any(e["action"] == "run_bulk_delete" and "999999999" in e["detail"] for e in events)


def test_artifact_purge_is_logged():
    res = client.post("/api/artifacts/purge", params={"older_than_hours": 999999})
    assert res.status_code == 200

    events = client.get("/api/audit-log").json()
    assert any(e["action"] == "artifact_purge" for e in events)


def test_scheduler_pause_and_resume_are_logged():
    try:
        pause_res = client.post("/api/scheduler/pause")
        assert pause_res.status_code == 200
        resume_res = client.post("/api/scheduler/resume")
        assert resume_res.status_code == 200
    finally:
        client.post("/api/scheduler/resume")  # never leave the shared scheduler paused for later tests

    events = client.get("/api/audit-log").json()
    assert any(e["action"] == "scheduler_pause" for e in events)
    assert any(e["action"] == "scheduler_resume" for e in events)


def test_pipeline_version_restore_is_logged():
    client.post(
        "/api/pipelines",
        json={
            "name": "Audit Restore Target",
            "steps": [{"tier": "automation", "name": "fetch_raw_metrics", "inputs": {"signups": 1, "churn": 1, "revenue": 1}}],
        },
    )
    client.post(
        "/api/pipelines",
        json={
            "name": "Audit Restore Target",
            "steps": [{"tier": "automation", "name": "fetch_raw_metrics", "inputs": {"signups": 999, "churn": 1, "revenue": 1}}],
        },
    )
    old_version_id = client.get("/api/pipelines/audit_restore_target/versions").json()[0]["version_id"]

    res = client.post(f"/api/pipelines/audit_restore_target/versions/{old_version_id}/restore")
    assert res.status_code == 200

    events = client.get("/api/audit-log").json()
    assert any(
        e["action"] == "pipeline_version_restore" and "audit_restore_target" in e["detail"]
        for e in events
    )


def test_backup_restore_is_logged():
    fake_snapshot = {
        "runs": [], "steps": [], "schedules": [], "ingested_files": [], "memory": [], "pipelines": [],
    }
    snapshot_bytes = json.dumps(fake_snapshot).encode()

    res = client.post("/api/backup/restore", files={"file": ("audit_backup.json", snapshot_bytes, "application/json")})
    assert res.status_code == 200

    events = client.get("/api/audit-log").json()
    assert any(e["action"] == "backup_restore" for e in events)


def test_audit_log_endpoint_orders_newest_first_and_respects_limit():
    client.post("/api/artifacts/purge", params={"older_than_hours": 999999})
    client.post("/api/runs/purge", params={"older_than_hours": 999999})

    events = client.get("/api/audit-log", params={"limit": 2}).json()
    assert len(events) == 2
    assert events[0]["action"] == "run_purge"
    assert events[1]["action"] == "artifact_purge"


# ---- Batch 11: CSV export ----

def test_audit_log_csv_export_has_a_header_and_rows():
    client.post("/api/artifacts/purge", params={"older_than_hours": 999999})

    res = client.get("/api/audit-log.csv")
    assert res.status_code == 200
    assert res.headers["content-type"].startswith("text/csv")

    lines = res.text.strip().splitlines()
    assert lines[0] == "id,action,detail,created_at"
    assert len(lines) >= 2
    assert "artifact_purge" in res.text


def test_audit_log_csv_export_respects_limit():
    client.post("/api/artifacts/purge", params={"older_than_hours": 999999})
    client.post("/api/runs/purge", params={"older_than_hours": 999999})

    res = client.get("/api/audit-log.csv", params={"limit": 1})
    lines = res.text.strip().splitlines()
    assert len(lines) == 2  # header + exactly one row
    assert "run_purge" in lines[1]


# ---- Batch 20: search across the audit log ----

def test_search_audit_log_finds_a_keyword_in_the_detail():
    unique_marker = "batch20auditmarker9192"
    client.post("/api/artifacts/purge", params={"older_than_hours": 999999})

    res = client.get("/api/audit-log/search", params={"q": "artifact_purge"})
    assert res.status_code == 200
    body = res.json()
    assert body["query"] == "artifact_purge"
    assert any(r["action"] == "artifact_purge" for r in body["results"])


def test_search_audit_log_is_case_insensitive():
    client.post("/api/artifacts/purge", params={"older_than_hours": 999999})
    res = client.get("/api/audit-log/search", params={"q": "ARTIFACT_PURGE"})
    assert any(r["action"] == "artifact_purge" for r in res.json()["results"])


def test_search_audit_log_returns_empty_for_a_blank_query():
    res = client.get("/api/audit-log/search", params={"q": ""})
    assert res.status_code == 200
    assert res.json()["results"] == []


def test_search_audit_log_finds_nothing_for_an_unmatched_keyword():
    res = client.get("/api/audit-log/search", params={"q": "zzz_never_used_audit_action_zzz"})
    assert res.json()["results"] == []


def test_clear_audit_log_deletes_every_entry():
    client.post("/api/artifacts/purge", params={"older_than_hours": 999999})
    assert client.get("/api/audit-log").json()

    res = client.post("/api/audit-log/clear")
    assert res.status_code == 200
    assert res.json()["deleted"] > 0

    assert client.get("/api/audit-log").json() == []


def test_clear_audit_log_is_a_no_op_when_already_empty():
    client.post("/api/audit-log/clear")
    res = client.post("/api/audit-log/clear")
    assert res.status_code == 200
    assert res.json() == {"deleted": 0}


def test_purge_audit_log_removes_only_entries_older_than_cutoff():
    from datetime import datetime, timedelta, timezone

    old_event = store.record_audit_event("test_old_event", "This one should be purged.")
    now = datetime.now(timezone.utc)
    old_cutoff = (now - timedelta(hours=1)).isoformat()
    store._conn.execute("UPDATE audit_log SET created_at = ? WHERE id = ?", (old_cutoff, old_event["id"]))
    store._conn.commit()

    recent_event = store.record_audit_event("test_recent_event", "This one should survive.")

    res = client.post("/api/audit-log/purge", params={"older_than_hours": 0.01})
    assert res.status_code == 200
    assert res.json()["deleted"] >= 1

    events = client.get("/api/audit-log?limit=1000").json()
    ids = {e["id"] for e in events}
    assert old_event["id"] not in ids
    assert recent_event["id"] in ids


def test_purge_audit_log_defaults_to_24_hours():
    res = client.post("/api/audit-log/purge")
    assert res.status_code == 200
    assert "deleted" in res.json()


def test_purge_audit_log_is_a_no_op_when_nothing_is_old_enough():
    client.post("/api/audit-log/clear")
    store.record_audit_event("test_fresh_event", "Just created.")
    res = client.post("/api/audit-log/purge", params={"older_than_hours": 999999})
    assert res.status_code == 200
    assert res.json()["deleted"] == 0
