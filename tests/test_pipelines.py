import json
import shutil

import pytest
from fastapi.testclient import TestClient

from webapp import pipelines as pipeline_store
from webapp.main import app

client = TestClient(app)


@pytest.fixture(autouse=True)
def clean_pipelines_dir():
    """Every test in this file saves real pipeline YAML files to disk; make sure
    none of that leaks between tests or into the repo working tree."""
    if pipeline_store.PIPELINES_DIR.exists():
        shutil.rmtree(pipeline_store.PIPELINES_DIR)
    yield
    if pipeline_store.PIPELINES_DIR.exists():
        shutil.rmtree(pipeline_store.PIPELINES_DIR)


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


def test_save_and_launch_pipeline_persists_to_disk_and_runs():
    payload = {
        "name": "Custom Chain",
        "description": "A hand-built 3-step chain",
        "steps": [
            {
                "tier": "automation",
                "name": "fetch_raw_metrics",
                "inputs": {"signups": 40, "churn": 20, "revenue": 500},
            },
            {"tier": "workflow", "name": "analyze_metrics", "inputs": {"risk_threshold": 0.1}},
            {"tier": "agent", "name": "churn_response_agent", "inputs": {"notify_slack": False}},
        ],
    }

    res = client.post("/api/pipelines", json=payload)
    assert res.status_code == 200

    body = res.json()
    assert body["pipeline"]["slug"] == "custom_chain"
    assert (pipeline_store.PIPELINES_DIR / "custom_chain.yaml").exists()

    events = _collect_stream(body["stream_id"])
    assert [e["kind"] for e in events if e["kind"] in ("step_started", "step_completed")] == [
        "step_started", "step_completed",
        "step_started", "step_completed",
        "step_started", "step_completed",
    ]
    assert events[-1]["kind"] == "run_completed"
    assert "agent_decision" in events[-1]["context"]

    listed = client.get("/api/pipelines").json()
    assert [p["slug"] for p in listed] == ["custom_chain"]


def test_mapping_wires_an_earlier_steps_output_into_a_later_field():
    # Map analyze_metrics' declared "insight" output straight into the agent's
    # "notify_slack" field instead of leaving it at its static default (False) —
    # proves a dropdown-picked mapping actually changes what the module sees at
    # runtime, not just that it's accepted and ignored.
    payload = {
        "name": "Mapped Notify",
        "steps": [
            {"tier": "automation", "name": "fetch_raw_metrics", "inputs": {"signups": 10, "churn": 9, "revenue": 1}},
            {"tier": "workflow", "name": "analyze_metrics", "inputs": {"risk_threshold": 0.1}},
            {
                "tier": "agent",
                "name": "churn_response_agent",
                "inputs": {},
                "mappings": {"notify_slack": {"step": 1, "output": "insight"}},
            },
        ],
    }

    res = client.post("/api/pipelines", json=payload)
    assert res.status_code == 200
    events = _collect_stream(res.json()["stream_id"])
    assert events[-1]["kind"] == "run_completed"

    agent_step = next(e for e in events if e["kind"] == "step_completed" and e["name"] == "churn_response_agent")
    # insight is a non-empty dict, so as a mapped-in "notify_slack" value it's
    # truthy — the agent's own notify_slack branch fires, proving the mapped
    # value (not the static default False) reached the module.
    assert "Slack notification queued" in agent_step["output"]["agent_decision"]["message"]


def test_template_field_interpolates_against_an_earlier_steps_flat_context_keys():
    # custom_note is a "template" field — {signups}/{churn} should resolve using
    # whatever the automation step put in context, not stay literal placeholders.
    payload = {
        "name": "Templated Note",
        "steps": [
            {"tier": "automation", "name": "fetch_raw_metrics", "inputs": {"signups": 77, "churn": 3, "revenue": 500}},
            {"tier": "workflow", "name": "analyze_metrics", "inputs": {"risk_threshold": 0.1}},
            {
                "tier": "agent",
                "name": "churn_response_agent",
                "inputs": {"custom_note": "Custom: {signups} signups vs {churn} churn."},
            },
        ],
    }

    res = client.post("/api/pipelines", json=payload)
    assert res.status_code == 200
    events = _collect_stream(res.json()["stream_id"])

    agent_step = next(e for e in events if e["kind"] == "step_completed" and e["name"] == "churn_response_agent")
    message = agent_step["output"]["agent_decision"]["message"]
    assert "Custom: 77.0 signups vs 3.0 churn." in message


def test_save_pipeline_rejects_reference_to_a_nonexistent_module():
    res = client.post(
        "/api/pipelines",
        json={"name": "Bad Module", "steps": [{"tier": "automation", "name": "does_not_exist"}]},
    )
    assert res.status_code == 400
    assert "does_not_exist" in res.json()["detail"]


def test_save_pipeline_rejects_mapping_to_an_undeclared_output():
    payload = {
        "name": "Bad Mapping",
        "steps": [
            {"tier": "automation", "name": "fetch_raw_metrics"},
            {
                "tier": "workflow",
                "name": "analyze_metrics",
                "mappings": {"risk_threshold": {"step": 0, "output": "not_a_real_output"}},
            },
        ],
    }
    res = client.post("/api/pipelines", json=payload)
    assert res.status_code == 400
    assert "not_a_real_output" in res.json()["detail"]


def test_save_pipeline_rejects_mapping_from_a_step_that_is_not_earlier():
    payload = {
        "name": "Forward Mapping",
        "steps": [
            {
                "tier": "automation",
                "name": "fetch_raw_metrics",
                "mappings": {"signups": {"step": 0, "output": "raw_metrics"}},
            },
        ],
    }
    res = client.post("/api/pipelines", json=payload)
    assert res.status_code == 400


def test_run_saved_pipeline_by_slug_then_unknown_slug_404s():
    client.post(
        "/api/pipelines",
        json={"name": "Rerunnable", "steps": [{"tier": "automation", "name": "fetch_raw_metrics"}]},
    )

    res = client.post("/api/pipelines/rerunnable/run")
    assert res.status_code == 200
    events = _collect_stream(res.json()["stream_id"])
    assert events[-1]["kind"] == "run_completed"

    missing = client.post("/api/pipelines/does_not_exist/run")
    assert missing.status_code == 404
