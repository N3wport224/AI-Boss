"""Batch 15: quick-fill a module's inputs from its last run -- a one-click
"whatever I ran last time" shortcut, distinct from a named saved preset
(Batch 12): no saving required ahead of time.
"""
import json
from datetime import datetime, timezone

from fastapi.testclient import TestClient

from engine.context import StepRecord
from engine.state_store import StateStore
from webapp.main import app

client = TestClient(app)


def _log(store, name, tier, inputs):
    now = datetime.now(timezone.utc)
    store.log_step(
        run_id=1,
        step=StepRecord(name=name, tier=tier, started_at=now, finished_at=now, success=True, output={}, inputs=inputs),
    )


def _collect_stream(stream_id):
    events = []
    with client.stream("GET", f"/api/stream/{stream_id}") as response:
        for line in response.iter_lines():
            if not line.startswith("data: "):
                continue
            event = json.loads(line[len("data: "):])
            events.append(event)
            if event["kind"] in ("run_completed", "run_failed"):
                break
    return events


# ---- Store-level behavior ----

def test_latest_step_inputs_returns_none_when_never_run(tmp_path):
    s = StateStore(str(tmp_path / "last_run.db"))
    assert s.latest_step_inputs("automation", "never_run_module") is None
    s.close()


def test_latest_step_inputs_returns_the_most_recent_runs_inputs(tmp_path):
    s = StateStore(str(tmp_path / "last_run2.db"))
    _log(s, "m", "automation", {"signups": 1})
    _log(s, "m", "automation", {"signups": 2})
    assert s.latest_step_inputs("automation", "m") == {"signups": 2}
    s.close()


def test_latest_step_inputs_is_scoped_to_tier_and_name(tmp_path):
    s = StateStore(str(tmp_path / "last_run3.db"))
    _log(s, "shared_name", "automation", {"a": "automation"})
    _log(s, "shared_name", "workflow", {"a": "workflow"})
    assert s.latest_step_inputs("automation", "shared_name") == {"a": "automation"}
    assert s.latest_step_inputs("workflow", "shared_name") == {"a": "workflow"}
    s.close()


# ---- End-to-end through the webapp ----

def test_last_run_inputs_404s_when_never_run():
    res = client.get("/api/modules/agent/escalation_agent/last-run-inputs")
    assert res.status_code == 404


def test_last_run_inputs_reflects_the_most_recent_run():
    res1 = client.post(
        "/api/modules/automation/fetch_raw_metrics/run",
        json={"inputs": {"signups": 111, "churn": 1, "revenue": 1}, "force_refresh": True},
    )
    _collect_stream(res1.json()["stream_id"])

    res2 = client.post(
        "/api/modules/automation/fetch_raw_metrics/run",
        json={"inputs": {"signups": 222, "churn": 2, "revenue": 2}, "force_refresh": True},
    )
    _collect_stream(res2.json()["stream_id"])

    last = client.get("/api/modules/automation/fetch_raw_metrics/last-run-inputs")
    assert last.status_code == 200
    assert last.json()["inputs"]["signups"] == 222.0
