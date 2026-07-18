import json

from fastapi.testclient import TestClient

from webapp.main import app

client = TestClient(app)


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


def test_list_modules_groups_by_tier_and_includes_input_schema():
    res = client.get("/api/modules")
    assert res.status_code == 200

    body = res.json()
    assert set(body.keys()) == {"automation", "workflow", "agent"}

    automation = body["automation"][0]
    assert automation["name"] == "fetch_raw_metrics"
    assert {f["name"] for f in automation["inputs"]} == {"signups", "churn", "revenue"}


def test_run_single_module_streams_progress_and_completion():
    res = client.post(
        "/api/modules/automation/fetch_raw_metrics/run",
        json={"inputs": {"signups": 200, "churn": 5, "revenue": 999}},
    )
    assert res.status_code == 200
    stream_id = res.json()["stream_id"]

    events = _collect_stream(stream_id)
    kinds = [e["kind"] for e in events]
    assert kinds == ["step_started", "step_completed", "run_completed"]

    completed = events[1]
    assert completed["tier"] == "automation"
    assert completed["name"] == "fetch_raw_metrics"
    assert completed["output"]["raw_metrics"] == {"signups": 200.0, "churn": 5.0, "revenue": 999.0}


def test_run_unknown_module_returns_404():
    res = client.post("/api/modules/automation/does_not_exist/run", json={"inputs": {}})
    assert res.status_code == 404


def test_run_full_pipeline_streams_agent_thoughts_and_tool_calls():
    res = client.post("/api/pipeline/run", json={"inputs": {}})
    assert res.status_code == 200
    stream_id = res.json()["stream_id"]

    events = _collect_stream(stream_id)

    step_kinds = [e["kind"] for e in events if e["kind"] in ("step_started", "step_completed")]
    assert step_kinds == [
        "step_started", "step_completed",
        "step_started", "step_completed",
        "step_started", "step_completed",
    ]

    agent_thoughts = [e for e in events if e["kind"] == "thought" and e.get("name") == "churn_response_agent"]
    agent_tool_calls = [e for e in events if e["kind"] == "tool_call" and e.get("name") == "churn_response_agent"]
    assert len(agent_thoughts) > 0
    assert len(agent_tool_calls) > 0

    final = events[-1]
    assert final["kind"] == "run_completed"
    assert "agent_decision" in final["context"]
    # The agent's custom_note template default interpolates real upstream values.
    assert "128" in final["context"]["custom_note"]
    assert "14" in final["context"]["custom_note"]
    assert "{signups}" not in final["context"]["custom_note"]


def test_run_module_failure_streams_step_failed_then_run_failed(monkeypatch):
    import automations.example_automation as example_automation

    def boom(self, context):
        raise RuntimeError("simulated failure")

    monkeypatch.setattr(example_automation.DataFetchAutomation, "run", boom)

    res = client.post("/api/modules/automation/fetch_raw_metrics/run", json={"inputs": {}})
    stream_id = res.json()["stream_id"]

    events = _collect_stream(stream_id)
    kinds = [e["kind"] for e in events]
    assert kinds == ["step_started", "step_failed", "run_failed"]
    assert "simulated failure" in events[1]["error"]
