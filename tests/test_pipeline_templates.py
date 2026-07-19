"""Batch 10: built-in pipeline starter templates -- curated, static content a
user can clone into their own saved pipelines with one click, instead of
always building from a single blank step in the visual builder.
"""
import json
import shutil

import pytest
from fastapi.testclient import TestClient

from webapp import pipelines as pipeline_store
from webapp.main import app
from webapp.templates import PIPELINE_TEMPLATES

client = TestClient(app)


@pytest.fixture(autouse=True)
def clean_pipelines_dir():
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


def test_list_pipeline_templates_returns_every_built_in_template():
    res = client.get("/api/pipeline-templates")
    assert res.status_code == 200
    body = res.json()
    ids = {t["id"] for t in body}
    assert ids == {t["id"] for t in PIPELINE_TEMPLATES}
    assert len(body) == 3


def test_clone_template_creates_a_saved_pipeline_with_matching_steps():
    res = client.post("/api/pipeline-templates/basic-churn-response/clone")
    assert res.status_code == 200
    pipeline = res.json()["pipeline"]
    assert pipeline["name"] == "Basic Churn Response"
    assert [s["name"] for s in pipeline["steps"]] == [
        "fetch_raw_metrics", "analyze_metrics", "churn_response_agent",
    ]

    listed = client.get("/api/pipelines").json()
    assert any(p["slug"] == pipeline["slug"] for p in listed)


def test_cloning_the_same_template_twice_gets_distinct_names():
    first = client.post("/api/pipeline-templates/basic-churn-response/clone").json()["pipeline"]
    second = client.post("/api/pipeline-templates/basic-churn-response/clone").json()["pipeline"]

    assert first["name"] == "Basic Churn Response"
    assert second["name"] == "Basic Churn Response (2)"
    assert first["slug"] != second["slug"]


def test_clone_unknown_template_404s():
    res = client.post("/api/pipeline-templates/does-not-exist/clone")
    assert res.status_code == 404


def test_escalation_template_clone_preserves_the_handoff_mapping_and_runs():
    res = client.post("/api/pipeline-templates/churn-response-with-escalation/clone")
    pipeline = res.json()["pipeline"]
    assert pipeline["steps"][3]["name"] == "escalation_agent"
    assert pipeline["steps"][3]["mappings"] == {"handoff": {"step": 2, "output": "handoff"}}

    # Run it for real with the template's own default inputs -- 14 churn events
    # against 128 signups is already just above analyze_metrics' default 0.1
    # risk_threshold, so this genuinely exercises the high-risk/escalation path.
    run_res = client.post(f"/api/pipelines/{pipeline['slug']}/run")
    assert run_res.status_code == 200
    events = _collect_stream(run_res.json()["stream_id"])
    final = events[-1]["context"]
    assert final["handoff"]["requested"] is True
    assert final["escalation_result"]["status"] == "escalated"


def test_webhook_notifier_template_keeps_its_placeholder_url():
    res = client.post("/api/pipeline-templates/metrics-webhook-notifier/clone")
    pipeline = res.json()["pipeline"]
    http_step = next(s for s in pipeline["steps"] if s["name"] == "http_request")
    assert http_step["inputs"]["url"] == "https://example.com/webhook"
    assert http_step["inputs"]["method"] == "POST"
