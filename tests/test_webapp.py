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


def test_stream_reconnect_with_last_event_id_resumes_from_where_it_left_off():
    res = client.post(
        "/api/modules/automation/fetch_raw_metrics/run",
        json={"inputs": {"signups": 1, "churn": 1, "revenue": 1}, "force_refresh": True},
    )
    stream_id = res.json()["stream_id"]

    # Drain the whole stream once, recording each SSE "id:" line alongside its event.
    ids_and_kinds = []
    with client.stream("GET", f"/api/stream/{stream_id}") as response:
        current_id = None
        for line in response.iter_lines():
            if line.startswith("id: "):
                current_id = int(line[len("id: "):])
            elif line.startswith("data: "):
                event = json.loads(line[len("data: "):])
                ids_and_kinds.append((current_id, event["kind"]))
                if event["kind"] in ("run_completed", "run_failed"):
                    break

    assert [kind for _, kind in ids_and_kinds] == ["step_started", "step_completed", "run_completed"]
    first_id = ids_and_kinds[0][0]

    # Reconnect claiming we already saw the first event — the browser's own
    # Last-Event-ID reconnect behavior — and confirm we resume after it
    # instead of replaying the whole stream again.
    resumed_kinds = []
    with client.stream(
        "GET", f"/api/stream/{stream_id}", headers={"Last-Event-ID": str(first_id)}
    ) as response:
        for line in response.iter_lines():
            if not line.startswith("data: "):
                continue
            event = json.loads(line[len("data: "):])
            resumed_kinds.append(event["kind"])
            if event["kind"] in ("run_completed", "run_failed"):
                break

    assert resumed_kinds == ["step_completed", "run_completed"]


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


def test_run_module_result_is_cached_and_force_refresh_bypasses_it(monkeypatch):
    import automations.example_automation as example_automation

    call_count = {"n": 0}
    original_run = example_automation.DataFetchAutomation.run

    def counting_run(self, context):
        call_count["n"] += 1
        return original_run(self, context)

    monkeypatch.setattr(example_automation.DataFetchAutomation, "run", counting_run)

    payload = {"inputs": {"signups": 321, "churn": 9, "revenue": 555}}

    res1 = client.post("/api/modules/automation/fetch_raw_metrics/run", json=payload)
    events1 = _collect_stream(res1.json()["stream_id"])
    assert events1[-1]["kind"] == "run_completed"
    assert call_count["n"] == 1

    res2 = client.post("/api/modules/automation/fetch_raw_metrics/run", json=payload)
    body2 = res2.json()
    assert body2.get("cached") is True
    events2 = _collect_stream(body2["stream_id"])
    assert call_count["n"] == 1  # cache hit — the module never ran again
    completed2 = next(e for e in events2 if e["kind"] == "step_completed")
    assert completed2.get("cached") is True
    assert completed2["output"] == events1[1]["output"]

    res3 = client.post(
        "/api/modules/automation/fetch_raw_metrics/run",
        json={**payload, "force_refresh": True},
    )
    body3 = res3.json()
    assert "cached" not in body3
    _collect_stream(body3["stream_id"])
    assert call_count["n"] == 2  # force_refresh skipped the cache


def test_schedule_crud_and_validation():
    res = client.post(
        "/api/schedules",
        json={
            "kind": "module",
            "tier": "automation",
            "name": "fetch_raw_metrics",
            "inputs": {"signups": 1, "churn": 1, "revenue": 1},
            "interval_seconds": 30,
        },
    )
    assert res.status_code == 200
    schedule = res.json()
    assert schedule["enabled"] is True
    schedule_id = schedule["id"]

    listed = client.get("/api/schedules").json()
    assert any(s["id"] == schedule_id for s in listed)

    patched = client.patch(f"/api/schedules/{schedule_id}", json={"enabled": False})
    assert patched.json()["enabled"] is False

    deleted = client.delete(f"/api/schedules/{schedule_id}")
    assert deleted.json() == {"deleted": schedule_id}
    assert not any(s["id"] == schedule_id for s in client.get("/api/schedules").json())


def test_schedule_rejects_too_short_interval():
    res = client.post(
        "/api/schedules",
        json={"kind": "module", "tier": "automation", "name": "fetch_raw_metrics", "inputs": {}, "interval_seconds": 1},
    )
    assert res.status_code == 400


def test_schedule_rejects_unknown_module():
    res = client.post(
        "/api/schedules",
        json={"kind": "module", "tier": "automation", "name": "does_not_exist", "inputs": {}, "interval_seconds": 30},
    )
    assert res.status_code == 404


def test_schedule_rejects_unknown_pipeline():
    res = client.post(
        "/api/schedules",
        json={"kind": "pipeline", "name": "does-not-exist", "inputs": {}, "interval_seconds": 30},
    )
    assert res.status_code == 404


def test_run_detail_and_compare_endpoints():
    res1 = client.post(
        "/api/modules/automation/fetch_raw_metrics/run",
        json={"inputs": {"signups": 10, "churn": 1, "revenue": 100}, "force_refresh": True},
    )
    _collect_stream(res1.json()["stream_id"])

    res2 = client.post(
        "/api/modules/automation/fetch_raw_metrics/run",
        json={"inputs": {"signups": 20, "churn": 2, "revenue": 200}, "force_refresh": True},
    )
    _collect_stream(res2.json()["stream_id"])

    runs = client.get("/api/runs?limit=2").json()
    run_b_id, run_a_id = runs[0]["id"], runs[1]["id"]  # newest first

    detail = client.get(f"/api/runs/{run_a_id}")
    assert detail.status_code == 200
    assert detail.json()["steps"][0]["output"]["raw_metrics"]["signups"] == 10.0

    compare = client.get("/api/runs/compare", params={"a": run_a_id, "b": run_b_id})
    assert compare.status_code == 200
    body = compare.json()
    assert len(body["steps"]) == 1
    diff = body["steps"][0]["output_diff"]
    assert "raw_metrics" in diff
    assert diff["raw_metrics"]["a"]["signups"] == 10.0
    assert diff["raw_metrics"]["b"]["signups"] == 20.0


def test_run_detail_404s_for_unknown_run():
    assert client.get("/api/runs/999999").status_code == 404


def test_compare_404s_when_a_run_has_no_steps():
    res = client.post(
        "/api/modules/automation/fetch_raw_metrics/run",
        json={"inputs": {"signups": 1, "churn": 1, "revenue": 1}, "force_refresh": True},
    )
    _collect_stream(res.json()["stream_id"])
    run_id = client.get("/api/runs?limit=1").json()[0]["id"]

    assert client.get("/api/runs/compare", params={"a": run_id, "b": 999999}).status_code == 404


def test_run_endpoints_are_rate_limited_per_client():
    from webapp.main import _run_rate_limiter

    payload = {"inputs": {"signups": 1, "churn": 1, "revenue": 1}, "force_refresh": True}
    for _ in range(_run_rate_limiter.max_requests):
        res = client.post("/api/modules/automation/fetch_raw_metrics/run", json=payload)
        assert res.status_code == 200

    over_limit = client.post("/api/modules/automation/fetch_raw_metrics/run", json=payload)
    assert over_limit.status_code == 429
    assert "Too many run requests" in over_limit.json()["detail"]


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
