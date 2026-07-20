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


def test_save_without_launch_persists_but_never_runs():
    payload = {
        "name": "Draft Only",
        "steps": [{"tier": "automation", "name": "fetch_raw_metrics", "inputs": {"signups": 1, "churn": 1, "revenue": 1}}],
        "launch": False,
    }

    res = client.post("/api/pipelines", json=payload)
    assert res.status_code == 200

    body = res.json()
    assert body["pipeline"]["slug"] == "draft_only"
    assert "stream_id" not in body
    assert (pipeline_store.PIPELINES_DIR / "draft_only.yaml").exists()

    runs_before = client.get("/api/runs?limit=10000").json()
    # A second save-without-launch of the same pipeline must still not run it.
    client.post("/api/pipelines", json=payload)
    runs_after = client.get("/api/runs?limit=10000").json()
    assert len(runs_after) == len(runs_before)


def test_save_without_launch_still_validates_and_blocks_disabled_modules():
    bad_payload = {
        "name": "Draft With Bad Reference",
        "steps": [{"tier": "automation", "name": "does_not_exist"}],
        "launch": False,
    }
    res = client.post("/api/pipelines", json=bad_payload)
    assert res.status_code == 400
    assert not (pipeline_store.PIPELINES_DIR / "draft_with_bad_reference.yaml").exists()


def test_save_and_launch_is_still_the_default_when_launch_is_omitted():
    payload = {
        "name": "Default Launch",
        "steps": [{"tier": "automation", "name": "fetch_raw_metrics", "inputs": {"signups": 1, "churn": 1, "revenue": 1}}],
    }
    res = client.post("/api/pipelines", json=payload)
    assert res.status_code == 200
    assert "stream_id" in res.json()


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


def test_mapping_reaches_into_a_nested_key_of_an_earlier_steps_output():
    # analyze_metrics' declared output is the whole "insight" dict; a dotted
    # path lets a later step map just its nested "risk_level" string instead
    # of the whole dict.
    payload = {
        "name": "Nested Mapped Notify",
        "steps": [
            {"tier": "automation", "name": "fetch_raw_metrics", "inputs": {"signups": 10, "churn": 9, "revenue": 1}},
            {"tier": "workflow", "name": "analyze_metrics", "inputs": {"risk_threshold": 0.1}},
            {
                "tier": "agent",
                "name": "churn_response_agent",
                "inputs": {},
                "mappings": {"notify_slack": {"step": 1, "output": "insight.risk_level"}},
            },
        ],
    }

    res = client.post("/api/pipelines", json=payload)
    assert res.status_code == 200
    events = _collect_stream(res.json()["stream_id"])
    assert events[-1]["kind"] == "run_completed"

    agent_step = next(e for e in events if e["kind"] == "step_completed" and e["name"] == "churn_response_agent")
    # churn=9 out of signups=10 is a 90% churn rate, well above the 0.1 threshold -> "high",
    # a truthy non-empty string -> the mapped-in notify_slack branch fires.
    assert "Slack notification queued" in agent_step["output"]["agent_decision"]["message"]


def test_pipeline_graph_reports_nodes_sequence_and_mapping_edges():
    payload = {
        "name": "Graph Test",
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
    launch = client.post("/api/pipelines", json=payload)
    _collect_stream(launch.json()["stream_id"])  # wait for the launch to finish before moving on

    res = client.get("/api/pipelines/graph_test/graph")
    assert res.status_code == 200
    body = res.json()

    assert [n["name"] for n in body["nodes"]] == ["fetch_raw_metrics", "analyze_metrics", "churn_response_agent"]

    sequence_edges = [e for e in body["edges"] if e["kind"] == "sequence"]
    assert sequence_edges == [{"from": 0, "to": 1, "kind": "sequence"}, {"from": 1, "to": 2, "kind": "sequence"}]

    mapping_edges = [e for e in body["edges"] if e["kind"] == "mapping"]
    assert mapping_edges == [{"from": 1, "to": 2, "kind": "mapping", "field": "notify_slack", "output": "insight"}]


def test_pipeline_graph_includes_a_steps_note():
    res = client.post(
        "/api/pipelines",
        json={
            "name": "Graph Note Test",
            "steps": [
                {"tier": "automation", "name": "fetch_raw_metrics", "note": "Documented for the graph tooltip."},
            ],
        },
    )
    _collect_stream(res.json()["stream_id"])

    graph = client.get("/api/pipelines/graph_note_test/graph").json()
    assert graph["nodes"][0]["note"] == "Documented for the graph tooltip."


def test_pipeline_graph_404s_for_unknown_slug():
    assert client.get("/api/pipelines/does-not-exist/graph").status_code == 404


def test_agent_remembers_risk_level_trend_across_separate_pipeline_runs():
    from webapp.main import store

    store.set_memory("last_risk_level", None)  # known baseline: nothing remembered yet

    def run_and_get_message(signups, churn):
        payload = {
            "name": f"Trend Test {signups}-{churn}",
            "steps": [
                {"tier": "automation", "name": "fetch_raw_metrics", "inputs": {"signups": signups, "churn": churn, "revenue": 1}},
                {"tier": "workflow", "name": "analyze_metrics", "inputs": {"risk_threshold": 0.1}},
                {"tier": "agent", "name": "churn_response_agent", "inputs": {}},
            ],
        }
        res = client.post("/api/pipelines", json=payload)
        events = _collect_stream(res.json()["stream_id"])
        agent_step = next(e for e in events if e["kind"] == "step_completed" and e["name"] == "churn_response_agent")
        return agent_step["output"]["agent_decision"]["message"]

    # First run: churn_rate = 1/100 = 1% -> "low" risk. Nothing was remembered before it.
    first_message = run_and_get_message(100, 1)
    assert "Risk moved" not in first_message

    # A second, completely separate pipeline run: churn_rate = 9/10 = 90% -> "high" risk.
    # The trend note can only appear if the *first* run's "low" survived in memory
    # across these two independent Orchestrator.run() calls.
    second_message = run_and_get_message(10, 9)
    assert "Risk moved from low to high since the last run." in second_message


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


def _conditional_pipeline_payload(name):
    """3-step chain whose agent step only runs when the workflow's nested
    insight.risk_level lands on "high" — churn/signups decide which way it goes."""
    return {
        "name": name,
        "steps": [
            {"tier": "automation", "name": "fetch_raw_metrics", "inputs": {"signups": 40, "churn": 20, "revenue": 500}},
            {"tier": "workflow", "name": "analyze_metrics", "inputs": {"risk_threshold": 0.1}},
            {
                "tier": "agent",
                "name": "churn_response_agent",
                "inputs": {"notify_slack": False},
                "condition": {"source": "insight.risk_level", "operator": "equals", "value": "high"},
            },
        ],
    }


def test_conditional_step_runs_when_condition_on_an_earlier_output_is_met():
    payload = _conditional_pipeline_payload("Gated High")
    # churn 20 of 40 signups = 0.5 churn rate > 0.1 threshold -> "high" -> agent runs

    res = client.post("/api/pipelines", json=payload)
    assert res.status_code == 200

    events = _collect_stream(res.json()["stream_id"])
    assert events[-1]["kind"] == "run_completed"
    assert "agent_decision" in events[-1]["context"]
    assert not any(e["kind"] == "step_skipped" for e in events)


def test_conditional_step_is_skipped_when_condition_is_not_met():
    payload = _conditional_pipeline_payload("Gated Low")
    payload["steps"][0]["inputs"] = {"signups": 1000, "churn": 1, "revenue": 500}
    # churn rate 0.001 < 0.1 -> "low" -> the agent step must be skipped

    res = client.post("/api/pipelines", json=payload)
    assert res.status_code == 200

    events = _collect_stream(res.json()["stream_id"])
    assert events[-1]["kind"] == "run_completed"  # skipping is not a failure
    assert "agent_decision" not in events[-1]["context"]

    skipped = next(e for e in events if e["kind"] == "step_skipped")
    assert skipped["name"] == "churn_response_agent"
    assert skipped["index"] == 2
    assert "insight.risk_level equals high" == skipped["condition"]


def test_condition_survives_the_saved_yaml_round_trip():
    payload = _conditional_pipeline_payload("Gated Persisted")
    res = client.post("/api/pipelines", json=payload)
    assert res.status_code == 200
    _collect_stream(res.json()["stream_id"])

    saved = pipeline_store.load_pipeline("gated_persisted")
    assert saved["steps"][2]["condition"] == {
        "source": "insight.risk_level",
        "operator": "equals",
        "value": "high",
    }
    # Steps without a condition don't carry a `condition: null` key in the YAML.
    assert "condition" not in saved["steps"][0]


def test_step_note_survives_the_saved_yaml_round_trip():
    res = client.post(
        "/api/pipelines",
        json={
            "name": "Step Note Persisted",
            "steps": [
                {
                    "tier": "automation",
                    "name": "fetch_raw_metrics",
                    "note": "  Retry policy tuned for a flaky upstream API.  ",
                },
                {"tier": "automation", "name": "http_request", "inputs": {"url": "https://example.com", "method": "GET"}},
            ],
        },
    )
    assert res.status_code == 200
    _collect_stream(res.json()["stream_id"])

    saved = pipeline_store.load_pipeline("step_note_persisted")
    assert saved["steps"][0]["note"] == "Retry policy tuned for a flaky upstream API."
    # A step without a note doesn't carry a `note: ''` key in the YAML.
    assert "note" not in saved["steps"][1]


def test_blank_step_note_is_not_saved():
    res = client.post(
        "/api/pipelines",
        json={
            "name": "Blank Step Note",
            "steps": [{"tier": "automation", "name": "fetch_raw_metrics", "note": "   "}],
        },
    )
    assert res.status_code == 200
    _collect_stream(res.json()["stream_id"])

    saved = pipeline_store.load_pipeline("blank_step_note")
    assert "note" not in saved["steps"][0]


def test_parallel_group_note_survives_the_saved_yaml_round_trip():
    res = client.post(
        "/api/pipelines",
        json={
            "name": "Group Note Persisted",
            "steps": [
                {
                    "type": "parallel",
                    "name": "",
                    "note": "Both branches hit the same flaky vendor -- run concurrently to save time.",
                    "branches": [
                        {"tier": "automation", "name": "fetch_raw_metrics"},
                        {"tier": "automation", "name": "http_request", "inputs": {"url": "https://example.com", "method": "GET"}},
                    ],
                }
            ],
        },
    )
    assert res.status_code == 200
    _collect_stream(res.json()["stream_id"])

    saved = pipeline_store.load_pipeline("group_note_persisted")
    assert saved["steps"][0]["note"] == "Both branches hit the same flaky vendor -- run concurrently to save time."
    # A branch itself never carries its own note -- only the group slot does.
    assert "note" not in saved["steps"][0]["branches"][0]


def test_condition_with_unknown_operator_is_rejected():
    payload = _conditional_pipeline_payload("Bad Operator")
    payload["steps"][2]["condition"]["operator"] = "matches_regex"

    res = client.post("/api/pipelines", json=payload)
    assert res.status_code == 400
    assert "operator" in res.json()["detail"]


def test_condition_without_a_source_key_is_rejected():
    payload = _conditional_pipeline_payload("No Source")
    payload["steps"][2]["condition"]["source"] = "  "

    res = client.post("/api/pipelines", json=payload)
    assert res.status_code == 400
    assert "context key" in res.json()["detail"]


def _parallel_pipeline_payload(name):
    return {
        "name": name,
        "steps": [
            {"tier": "automation", "name": "fetch_raw_metrics", "inputs": {"signups": 40, "churn": 20, "revenue": 500}},
            {
                "type": "parallel",
                "name": "fanout",
                "branches": [
                    {"tier": "workflow", "name": "analyze_metrics", "inputs": {"risk_threshold": 0.1}},
                    {"tier": "automation", "name": "http_request", "inputs": {"url": "http://127.0.0.1:9/x", "timeout": 1}},
                ],
            },
        ],
    }


def test_parallel_group_pipeline_saves_runs_and_emits_branch_tagged_events():
    payload = _parallel_pipeline_payload("Fanout Chain")
    # Replace the failing http_request branch with a second deterministic module.
    payload["steps"][1]["branches"][1] = {
        "tier": "automation", "name": "fetch_raw_metrics",
        "inputs": {"signups": 1, "churn": 0, "revenue": 1},
    }
    payload["steps"].append(
        {
            "tier": "agent",
            "name": "churn_response_agent",
            "inputs": {"notify_slack": False},
            "mappings": {"notify_slack": {"step": 1, "output": "insight"}},  # maps FROM the group slot
        }
    )

    res = client.post("/api/pipelines", json=payload)
    assert res.status_code == 200

    events = _collect_stream(res.json()["stream_id"])
    assert events[-1]["kind"] == "run_completed"

    group_started = next(e for e in events if e["kind"] == "group_started")
    assert group_started["name"] == "fanout"
    assert group_started["branch_count"] == 2

    branch_events = [e for e in events if e.get("parallel") and e["kind"] == "step_completed"]
    assert {e["branch_index"] for e in branch_events} == {0, 1}
    assert all(e["index"] == 1 for e in branch_events)

    # The agent's mapped field read the analyze branch's output from the group.
    assert "agent_decision" in events[-1]["context"]

    # Saved YAML round-trips the group structure.
    saved = pipeline_store.load_pipeline("fanout_chain")
    assert saved["steps"][1]["type"] == "parallel"
    assert len(saved["steps"][1]["branches"]) == 2
    assert "condition" not in saved["steps"][0]


def test_parallel_group_with_fewer_than_two_branches_is_rejected():
    payload = _parallel_pipeline_payload("Lonely Group")
    payload["steps"][1]["branches"] = payload["steps"][1]["branches"][:1]

    res = client.post("/api/pipelines", json=payload)
    assert res.status_code == 400
    assert "two branches" in res.json()["detail"]


def test_parallel_branch_mapping_may_only_reference_steps_before_the_group():
    payload = _parallel_pipeline_payload("Bad Branch Mapping")
    # Branch tries to map from the group's own slot (index 1) — not allowed.
    payload["steps"][1]["branches"][0]["mappings"] = {"risk_threshold": {"step": 1, "output": "insight"}}

    res = client.post("/api/pipelines", json=payload)
    assert res.status_code == 400
    assert "branch 1" in res.json()["detail"]
    assert "earlier step" in res.json()["detail"]


def test_pipeline_graph_renders_a_parallel_group_as_one_slot_with_branch_detail():
    payload = _parallel_pipeline_payload("Graphed Fanout")
    payload["steps"][1]["branches"][1] = {
        "tier": "automation", "name": "fetch_raw_metrics", "inputs": {},
        "mappings": {"signups": {"step": 0, "output": "raw_metrics.signups"}},
    }
    res = client.post("/api/pipelines", json=payload)
    assert res.status_code == 200
    _collect_stream(res.json()["stream_id"])

    graph = client.get("/api/pipelines/graphed_fanout/graph").json()
    group_node = graph["nodes"][1]
    assert group_node["tier"] == "parallel"
    assert [b["name"] for b in group_node["branches"]] == ["analyze_metrics", "fetch_raw_metrics"]

    mapping_edges = [e for e in graph["edges"] if e["kind"] == "mapping"]
    assert {"from": 0, "to": 1, "kind": "mapping", "field": "signups", "output": "raw_metrics.signups"} in mapping_edges


def test_pipeline_step_retries_a_transient_failure_and_succeeds(monkeypatch):
    import automations.example_automation as example_automation

    call_count = {"n": 0}
    original_run = example_automation.DataFetchAutomation.run

    def flaky_run(self, context):
        call_count["n"] += 1
        if call_count["n"] <= 2:
            raise RuntimeError(f"transient failure {call_count['n']}")
        return original_run(self, context)

    monkeypatch.setattr(example_automation.DataFetchAutomation, "run", flaky_run)

    payload = {
        "name": "Retry Demo",
        "steps": [
            {
                "tier": "automation",
                "name": "fetch_raw_metrics",
                "inputs": {"signups": 10, "churn": 1, "revenue": 5},
                "retry": {"max_retries": 3, "backoff_seconds": 0.01},
            }
        ],
    }
    res = client.post("/api/pipelines", json=payload)
    assert res.status_code == 200

    events = _collect_stream(res.json()["stream_id"])
    assert events[-1]["kind"] == "run_completed"
    assert call_count["n"] == 3

    retrying = [e for e in events if e["kind"] == "step_retrying"]
    assert len(retrying) == 2
    assert [r["attempt"] for r in retrying] == [1, 2]

    saved = pipeline_store.load_pipeline("retry_demo")
    assert saved["steps"][0]["retry"] == {"max_retries": 3, "backoff_seconds": 0.01}


def test_pipeline_step_fails_for_real_once_retries_are_exhausted(monkeypatch):
    import automations.example_automation as example_automation

    def always_fails(self, context):
        raise RuntimeError("permanent failure")

    monkeypatch.setattr(example_automation.DataFetchAutomation, "run", always_fails)

    payload = {
        "name": "Retry Exhausted",
        "steps": [
            {
                "tier": "automation",
                "name": "fetch_raw_metrics",
                "retry": {"max_retries": 2, "backoff_seconds": 0.01},
            }
        ],
    }
    res = client.post("/api/pipelines", json=payload)
    assert res.status_code == 200

    try:
        events = _collect_stream(res.json()["stream_id"])
        assert events[-1]["kind"] == "run_failed"
        assert len([e for e in events if e["kind"] == "step_retrying"]) == 2
        assert any(e["kind"] == "step_failed" for e in events)
    finally:
        # 3 consecutive failures (1 attempt + 2 retries) trips fetch_raw_metrics'
        # breaker on the shared app-level StateStore — reset it so it doesn't
        # block this same module in other test files sharing the same store.
        from webapp.main import store

        store.reset_breaker("automation", "fetch_raw_metrics")


def test_pipeline_step_without_retry_key_omits_it_from_saved_yaml():
    payload = {
        "name": "No Retry Here",
        "steps": [{"tier": "automation", "name": "fetch_raw_metrics"}],
    }
    res = client.post("/api/pipelines", json=payload)
    assert res.status_code == 200
    saved = pipeline_store.load_pipeline("no_retry_here")
    assert "retry" not in saved["steps"][0]


def test_pipeline_step_retry_with_negative_max_retries_is_rejected():
    payload = {
        "name": "Bad Retry Count",
        "steps": [
            {
                "tier": "automation",
                "name": "fetch_raw_metrics",
                "retry": {"max_retries": -1, "backoff_seconds": 1},
            }
        ],
    }
    res = client.post("/api/pipelines", json=payload)
    assert res.status_code == 400
    assert "max_retries" in res.json()["detail"]


def test_pipeline_step_retry_with_negative_backoff_is_rejected():
    payload = {
        "name": "Bad Retry Backoff",
        "steps": [
            {
                "tier": "automation",
                "name": "fetch_raw_metrics",
                "retry": {"max_retries": 1, "backoff_seconds": -0.5},
            }
        ],
    }
    res = client.post("/api/pipelines", json=payload)
    assert res.status_code == 400
    assert "backoff_seconds" in res.json()["detail"]


def _webhook_pipeline_payload(name):
    return {
        "name": name,
        "steps": [
            {"tier": "automation", "name": "fetch_raw_metrics", "inputs": {"signups": 10, "churn": 1, "revenue": 5}},
            {"tier": "workflow", "name": "analyze_metrics", "inputs": {"risk_threshold": 0.1}},
        ],
    }


def test_webhook_overrides_the_first_steps_inputs():
    payload = _webhook_pipeline_payload("Webhook Override")
    res = client.post("/api/pipelines", json=payload)
    assert res.status_code == 200
    _collect_stream(res.json()["stream_id"])

    res2 = client.post("/api/pipelines/webhook_override/webhook", json={"signups": 1000, "churn": 900})
    assert res2.status_code == 200
    events = _collect_stream(res2.json()["stream_id"])

    final = events[-1]
    assert final["kind"] == "run_completed"
    assert final["context"]["signups"] == 1000
    assert final["context"]["churn"] == 900
    assert final["context"]["insight"]["risk_level"] == "high"  # 900/1000 churn rate triggers "high"


def test_webhook_with_no_body_runs_pipeline_with_its_saved_defaults():
    payload = _webhook_pipeline_payload("Webhook No Body")
    res = client.post("/api/pipelines", json=payload)
    _collect_stream(res.json()["stream_id"])

    res2 = client.post("/api/pipelines/webhook_no_body/webhook")
    assert res2.status_code == 200
    events = _collect_stream(res2.json()["stream_id"])
    assert events[-1]["kind"] == "run_completed"
    assert events[-1]["context"]["signups"] == 10  # the pipeline's own saved default


def test_webhook_ignores_unknown_keys_in_the_body():
    payload = _webhook_pipeline_payload("Webhook Unknown Keys")
    res = client.post("/api/pipelines", json=payload)
    _collect_stream(res.json()["stream_id"])

    res2 = client.post("/api/pipelines/webhook_unknown_keys/webhook", json={"not_a_real_field": "whatever"})
    assert res2.status_code == 200
    events = _collect_stream(res2.json()["stream_id"])
    assert events[-1]["kind"] == "run_completed"
    assert events[-1]["context"]["signups"] == 10  # unaffected by the unrecognized key


def test_webhook_rejects_a_non_object_json_body():
    payload = _webhook_pipeline_payload("Webhook Bad Body")
    res = client.post("/api/pipelines", json=payload)
    _collect_stream(res.json()["stream_id"])

    res2 = client.post("/api/pipelines/webhook_bad_body/webhook", content=b"[1, 2, 3]", headers={"Content-Type": "application/json"})
    assert res2.status_code == 400
    assert "JSON object" in res2.json()["detail"]


def test_webhook_rejects_malformed_json():
    payload = _webhook_pipeline_payload("Webhook Malformed")
    res = client.post("/api/pipelines", json=payload)
    _collect_stream(res.json()["stream_id"])

    res2 = client.post("/api/pipelines/webhook_malformed/webhook", content=b"{not valid", headers={"Content-Type": "application/json"})
    assert res2.status_code == 400
    assert "valid JSON" in res2.json()["detail"]


def test_webhook_for_an_unknown_pipeline_is_a_404():
    res = client.post("/api/pipelines/does_not_exist_at_all/webhook", json={})
    assert res.status_code == 404


def test_webhook_overrides_a_branchs_inputs_inside_a_parallel_first_step():
    payload = {
        "name": "Webhook Parallel Entry",
        "steps": [
            {
                "type": "parallel",
                "branches": [
                    {"tier": "automation", "name": "fetch_raw_metrics", "inputs": {"signups": 10, "churn": 1, "revenue": 5}},
                    {"tier": "automation", "name": "fetch_raw_metrics", "inputs": {"signups": 1, "churn": 1, "revenue": 1}},
                ],
            }
        ],
    }
    res = client.post("/api/pipelines", json=payload)
    assert res.status_code == 200
    _collect_stream(res.json()["stream_id"])

    res2 = client.post("/api/pipelines/webhook_parallel_entry/webhook", json={"signups": 777})
    assert res2.status_code == 200
    events = _collect_stream(res2.json()["stream_id"])
    assert events[-1]["kind"] == "run_completed"
    # Both branches run the same module name, so context.signups reflects
    # whichever branch's output merged in last — either way it's the override.
    assert events[-1]["context"]["signups"] == 777


def test_resaving_a_pipeline_under_the_same_name_overwrites_it_in_place():
    """The builder's "Edit" flow re-POSTs the same pipeline name after
    changes — this must overwrite the existing file/slug, not create a
    second entry alongside it."""
    first = {
        "name": "Editable Pipeline",
        "description": "original description",
        "steps": [{"tier": "automation", "name": "fetch_raw_metrics", "inputs": {"signups": 1, "churn": 1, "revenue": 1}}],
    }
    res = client.post("/api/pipelines", json=first)
    assert res.status_code == 200
    _collect_stream(res.json()["stream_id"])

    second = {
        "name": "Editable Pipeline",
        "description": "updated description",
        "steps": [
            {"tier": "automation", "name": "fetch_raw_metrics", "inputs": {"signups": 99, "churn": 2, "revenue": 3}},
            {"tier": "workflow", "name": "analyze_metrics", "inputs": {"risk_threshold": 0.5}},
        ],
    }
    res2 = client.post("/api/pipelines", json=second)
    assert res2.status_code == 200
    assert res2.json()["pipeline"]["slug"] == "editable_pipeline"
    _collect_stream(res2.json()["stream_id"])

    listed = client.get("/api/pipelines").json()
    matching = [p for p in listed if p["slug"] == "editable_pipeline"]
    assert len(matching) == 1  # no duplicate left behind
    assert matching[0]["description"] == "updated description"
    assert len(matching[0]["steps"]) == 2
    assert matching[0]["steps"][0]["inputs"]["signups"] == 99


def test_export_pipeline_returns_its_own_yaml_file():
    payload = {
        "name": "Export Target",
        "steps": [{"tier": "automation", "name": "fetch_raw_metrics", "inputs": {"signups": 10, "churn": 1, "revenue": 5}}],
    }
    res = client.post("/api/pipelines", json=payload)
    _collect_stream(res.json()["stream_id"])

    export_res = client.get("/api/pipelines/export_target/export")
    assert export_res.status_code == 200
    assert "yaml" in export_res.headers["content-type"]

    import yaml

    exported = yaml.safe_load(export_res.content)
    assert exported["name"] == "Export Target"
    assert exported["slug"] == "export_target"
    assert exported["steps"][0]["inputs"]["signups"] == 10


def test_export_missing_pipeline_is_a_404():
    res = client.get("/api/pipelines/totally_missing/export")
    assert res.status_code == 404


def test_export_pipeline_json_returns_the_same_definition_as_json():
    payload = {
        "name": "Export Target JSON",
        "steps": [{"tier": "automation", "name": "fetch_raw_metrics", "inputs": {"signups": 10, "churn": 1, "revenue": 5}}],
    }
    res = client.post("/api/pipelines", json=payload)
    _collect_stream(res.json()["stream_id"])

    export_res = client.get("/api/pipelines/export_target_json/export.json")
    assert export_res.status_code == 200
    assert export_res.headers["content-type"].startswith("application/json")
    assert "attachment; filename=export_target_json.json" in export_res.headers["content-disposition"]

    exported = export_res.json()
    assert exported["name"] == "Export Target JSON"
    assert exported["slug"] == "export_target_json"
    assert exported["steps"][0]["inputs"]["signups"] == 10


def test_export_pipeline_json_missing_pipeline_is_a_404():
    res = client.get("/api/pipelines/totally_missing/export.json")
    assert res.status_code == 404


def test_export_all_pipelines_returns_a_zip_with_every_saved_pipeline():
    import io
    import zipfile

    client.post(
        "/api/pipelines",
        json={"name": "Export All A", "steps": [{"tier": "automation", "name": "fetch_raw_metrics"}]},
    )
    client.post(
        "/api/pipelines",
        json={"name": "Export All B", "steps": [{"tier": "automation", "name": "fetch_raw_metrics"}]},
    )

    res = client.get("/api/pipelines/export-all")
    assert res.status_code == 200
    assert res.headers["content-type"] == "application/zip"

    zf = zipfile.ZipFile(io.BytesIO(res.content))
    names = zf.namelist()
    assert "export_all_a.yaml" in names
    assert "export_all_b.yaml" in names

    import yaml

    exported_a = yaml.safe_load(zf.read("export_all_a.yaml"))
    assert exported_a["name"] == "Export All A"


def test_export_all_pipelines_404s_when_there_are_none():
    res = client.get("/api/pipelines/export-all")
    assert res.status_code == 404


def test_bulk_export_pipelines_zip_contains_only_the_selected_pipelines():
    import io
    import zipfile

    client.post(
        "/api/pipelines",
        json={"name": "Bulk Export Zip A", "steps": [{"tier": "automation", "name": "fetch_raw_metrics"}]},
    )
    client.post(
        "/api/pipelines",
        json={"name": "Bulk Export Zip B", "steps": [{"tier": "automation", "name": "fetch_raw_metrics"}]},
    )
    client.post(
        "/api/pipelines",
        json={"name": "Bulk Export Zip Not Selected", "steps": [{"tier": "automation", "name": "fetch_raw_metrics"}]},
    )

    res = client.post(
        "/api/pipelines/bulk-export-zip",
        json={"slugs": ["bulk_export_zip_a", "bulk_export_zip_b", "totally_made_up_slug"]},
    )
    assert res.status_code == 200
    assert res.headers["content-type"] == "application/zip"
    assert "attachment; filename=pipelines_selected.zip" in res.headers["content-disposition"]

    zf = zipfile.ZipFile(io.BytesIO(res.content))
    names = zf.namelist()
    assert "bulk_export_zip_a.yaml" in names
    assert "bulk_export_zip_b.yaml" in names
    assert "bulk_export_zip_not_selected.yaml" not in names


def test_bulk_export_pipelines_zip_404s_when_no_selected_slug_exists():
    res = client.post("/api/pipelines/bulk-export-zip", json={"slugs": ["totally_made_up_slug"]})
    assert res.status_code == 404

    res2 = client.post("/api/pipelines/bulk-export-zip", json={"slugs": []})
    assert res2.status_code == 404


def test_import_pipeline_creates_a_new_saved_pipeline():
    yaml_text = (
        "name: Imported Fresh\n"
        "steps:\n"
        "  - tier: automation\n"
        "    name: fetch_raw_metrics\n"
        "    inputs:\n"
        "      signups: 42\n"
        "      churn: 3\n"
        "      revenue: 7\n"
    )
    res = client.post(
        "/api/pipelines/import",
        files={"file": ("imported_fresh.yaml", yaml_text.encode(), "application/x-yaml")},
    )
    assert res.status_code == 200
    assert res.json()["pipeline"]["slug"] == "imported_fresh"

    listed = client.get("/api/pipelines").json()
    match = next(p for p in listed if p["slug"] == "imported_fresh")
    assert match["steps"][0]["inputs"]["signups"] == 42


def test_import_round_trips_an_exported_pipeline():
    payload = {
        "name": "Round Trip Source",
        "steps": [
            {"tier": "automation", "name": "fetch_raw_metrics", "inputs": {"signups": 11, "churn": 2, "revenue": 3}},
            {"tier": "workflow", "name": "analyze_metrics", "inputs": {"risk_threshold": 0.2}},
        ],
    }
    res = client.post("/api/pipelines", json=payload)
    _collect_stream(res.json()["stream_id"])
    exported = client.get("/api/pipelines/round_trip_source/export").content

    # Import the exact same YAML back under a renamed identity so it's a genuinely new pipeline.
    renamed = exported.decode().replace("name: Round Trip Source", "name: Round Trip Copy").replace(
        "slug: round_trip_source", "slug: round_trip_copy"
    )
    res2 = client.post(
        "/api/pipelines/import",
        files={"file": ("copy.yaml", renamed.encode(), "application/x-yaml")},
    )
    assert res2.status_code == 200
    copy = res2.json()["pipeline"]
    assert copy["slug"] == "round_trip_copy"
    assert len(copy["steps"]) == 2
    assert copy["steps"][1]["tier"] == "workflow"


def test_import_rejects_malformed_yaml():
    res = client.post(
        "/api/pipelines/import",
        files={"file": ("bad.yaml", b": : : not valid yaml", "application/x-yaml")},
    )
    assert res.status_code == 400
    assert "parse" in res.json()["detail"].lower()


def test_import_rejects_a_non_object_yaml_document():
    res = client.post(
        "/api/pipelines/import",
        files={"file": ("list.yaml", b"- one\n- two\n", "application/x-yaml")},
    )
    assert res.status_code == 400
    assert "single object" in res.json()["detail"]


# ---- Batch 19: bulk import pipelines from a zip bundle ----

def _build_pipelines_zip(entries):
    import io as _io
    import zipfile as _zipfile

    buffer = _io.BytesIO()
    with _zipfile.ZipFile(buffer, "w") as zf:
        for filename, content in entries.items():
            zf.writestr(filename, content)
    return buffer.getvalue()


def test_import_zip_imports_every_yaml_file_inside():
    zip_bytes = _build_pipelines_zip({
        "zip_import_a.yaml": "name: Zip Import A\nsteps:\n  - tier: automation\n    name: fetch_raw_metrics\n",
        "zip_import_b.yaml": "name: Zip Import B\nsteps:\n  - tier: automation\n    name: fetch_raw_metrics\n",
    })
    res = client.post("/api/pipelines/import-zip", files={"file": ("bundle.zip", zip_bytes, "application/zip")})
    assert res.status_code == 200
    body = res.json()
    assert set(body["imported"]) == {"zip_import_a", "zip_import_b"}
    assert body["failed"] == []

    listed = {p["slug"] for p in client.get("/api/pipelines").json()}
    assert "zip_import_a" in listed
    assert "zip_import_b" in listed


def test_import_zip_ignores_non_yaml_members():
    zip_bytes = _build_pipelines_zip({
        "zip_import_c.yaml": "name: Zip Import C\nsteps:\n  - tier: automation\n    name: fetch_raw_metrics\n",
        "readme.txt": "not a pipeline",
    })
    res = client.post("/api/pipelines/import-zip", files={"file": ("bundle.zip", zip_bytes, "application/zip")})
    assert res.status_code == 200
    assert res.json()["imported"] == ["zip_import_c"]


def test_import_zip_reports_per_file_failures_without_blocking_the_rest():
    zip_bytes = _build_pipelines_zip({
        "zip_import_good.yaml": "name: Zip Import Good\nsteps:\n  - tier: automation\n    name: fetch_raw_metrics\n",
        "zip_import_bad.yaml": "name: Zip Import Bad\nsteps:\n  - tier: automation\n    name: totally_fake_module\n",
    })
    res = client.post("/api/pipelines/import-zip", files={"file": ("bundle.zip", zip_bytes, "application/zip")})
    assert res.status_code == 200
    body = res.json()
    assert body["imported"] == ["zip_import_good"]
    assert len(body["failed"]) == 1
    assert body["failed"][0]["name"] == "zip_import_bad.yaml"


def test_import_zip_rejects_a_non_zip_file():
    res = client.post("/api/pipelines/import-zip", files={"file": ("not_a_zip.zip", b"just some bytes", "application/zip")})
    assert res.status_code == 400
    assert "zip" in res.json()["detail"].lower()


def test_import_zip_400s_when_no_yaml_files_are_present():
    zip_bytes = _build_pipelines_zip({"readme.txt": "nothing pipeline-shaped here"})
    res = client.post("/api/pipelines/import-zip", files={"file": ("bundle.zip", zip_bytes, "application/zip")})
    assert res.status_code == 400


def test_import_rejects_a_pipeline_referencing_an_unknown_module():
    yaml_text = "name: Bad Module Ref\nsteps:\n  - tier: automation\n    name: totally_fake_module\n"
    res = client.post(
        "/api/pipelines/import",
        files={"file": ("bad.yaml", yaml_text.encode(), "application/x-yaml")},
    )
    assert res.status_code == 400
    assert "totally_fake_module" in res.json()["detail"]


def test_import_under_an_existing_name_overwrites_it():
    first = {
        "name": "Import Overwrite Target",
        "steps": [{"tier": "automation", "name": "fetch_raw_metrics", "inputs": {"signups": 1, "churn": 1, "revenue": 1}}],
    }
    res = client.post("/api/pipelines", json=first)
    _collect_stream(res.json()["stream_id"])

    yaml_text = (
        "name: Import Overwrite Target\n"
        "steps:\n"
        "  - tier: automation\n"
        "    name: fetch_raw_metrics\n"
        "    inputs:\n"
        "      signups: 555\n"
        "      churn: 1\n"
        "      revenue: 1\n"
    )
    res2 = client.post(
        "/api/pipelines/import",
        files={"file": ("overwrite.yaml", yaml_text.encode(), "application/x-yaml")},
    )
    assert res2.status_code == 200

    listed = client.get("/api/pipelines").json()
    matching = [p for p in listed if p["slug"] == "import_overwrite_target"]
    assert len(matching) == 1
    assert matching[0]["steps"][0]["inputs"]["signups"] == 555


# ---- Pipeline version history: archive-on-overwrite, diff, restore ----

def test_first_save_of_a_new_pipeline_creates_no_version():
    client.post(
        "/api/pipelines",
        json={"name": "Fresh Pipeline", "steps": [{"tier": "automation", "name": "fetch_raw_metrics"}]},
    )
    res = client.get("/api/pipelines/fresh_pipeline/versions")
    assert res.status_code == 200
    assert res.json() == []


def test_resaving_a_pipeline_archives_the_previous_definition():
    client.post(
        "/api/pipelines",
        json={
            "name": "Versioned Pipeline",
            "steps": [{"tier": "automation", "name": "fetch_raw_metrics", "inputs": {"signups": 1, "churn": 1, "revenue": 1}}],
        },
    )
    client.post(
        "/api/pipelines",
        json={
            "name": "Versioned Pipeline",
            "steps": [{"tier": "automation", "name": "fetch_raw_metrics", "inputs": {"signups": 2, "churn": 2, "revenue": 2}}],
        },
    )

    versions = client.get("/api/pipelines/versioned_pipeline/versions").json()
    assert len(versions) == 1
    assert versions[0]["saved_at"]

    version_detail = client.get(f"/api/pipelines/versioned_pipeline/versions/{versions[0]['version_id']}").json()
    assert version_detail["version"]["steps"][0]["inputs"]["signups"] == 1  # the OLD value, archived pre-overwrite
    assert "steps" in version_detail["diff"]  # current vs. archived differ on the steps key


def test_saving_a_third_time_keeps_both_older_versions():
    for signups in (1, 2, 3):
        client.post(
            "/api/pipelines",
            json={
                "name": "Triple Saved",
                "steps": [{"tier": "automation", "name": "fetch_raw_metrics", "inputs": {"signups": signups, "churn": 1, "revenue": 1}}],
            },
        )
    versions = client.get("/api/pipelines/triple_saved/versions").json()
    assert len(versions) == 2  # the first two saves, not the current (3rd) one
    # newest archived version first
    saved_signups = [
        client.get(f"/api/pipelines/triple_saved/versions/{v['version_id']}").json()["version"]["steps"][0]["inputs"]["signups"]
        for v in versions
    ]
    assert saved_signups == [2, 1]


def test_prune_pipeline_versions_keeps_only_the_latest_n():
    for signups in (1, 2, 3, 4):
        client.post(
            "/api/pipelines",
            json={
                "name": "Prune Me",
                "steps": [{"tier": "automation", "name": "fetch_raw_metrics", "inputs": {"signups": signups, "churn": 1, "revenue": 1}}],
            },
        )
    versions_before = client.get("/api/pipelines/prune_me/versions").json()
    assert len(versions_before) == 3  # saves 1-3 archived, save 4 is current

    res = client.post("/api/pipelines/prune_me/versions/prune", params={"keep": 1})
    assert res.status_code == 200
    assert res.json()["deleted"] == 2

    versions_after = client.get("/api/pipelines/prune_me/versions").json()
    assert len(versions_after) == 1
    assert versions_after[0]["version_id"] == versions_before[0]["version_id"]  # kept the newest


def test_prune_pipeline_versions_defaults_to_keeping_five():
    for signups in range(1, 8):
        client.post(
            "/api/pipelines",
            json={
                "name": "Prune Default",
                "steps": [{"tier": "automation", "name": "fetch_raw_metrics", "inputs": {"signups": signups, "churn": 1, "revenue": 1}}],
            },
        )
    versions_before = client.get("/api/pipelines/prune_default/versions").json()
    assert len(versions_before) == 6

    res = client.post("/api/pipelines/prune_default/versions/prune")
    assert res.status_code == 200
    assert res.json()["deleted"] == 1

    assert len(client.get("/api/pipelines/prune_default/versions").json()) == 5


def test_prune_pipeline_versions_is_a_no_op_when_under_the_keep_count():
    client.post(
        "/api/pipelines",
        json={"name": "Prune Sparse", "steps": [{"tier": "automation", "name": "fetch_raw_metrics"}]},
    )
    res = client.post("/api/pipelines/prune_sparse/versions/prune", params={"keep": 5})
    assert res.status_code == 200
    assert res.json()["deleted"] == 0


def test_prune_pipeline_versions_rejects_a_negative_keep():
    client.post(
        "/api/pipelines",
        json={"name": "Prune Negative", "steps": [{"tier": "automation", "name": "fetch_raw_metrics"}]},
    )
    res = client.post("/api/pipelines/prune_negative/versions/prune", params={"keep": -1})
    assert res.status_code == 400


def test_prune_pipeline_versions_for_unknown_slug_404s():
    res = client.post("/api/pipelines/does_not_exist/versions/prune")
    assert res.status_code == 404


def test_compare_two_pipeline_versions_reports_differing_steps():
    for signups in (1, 2, 3):
        client.post(
            "/api/pipelines",
            json={
                "name": "Compare Versions",
                "steps": [{"tier": "automation", "name": "fetch_raw_metrics", "inputs": {"signups": signups, "churn": 1, "revenue": 1}}],
            },
        )
    versions = client.get("/api/pipelines/compare_versions/versions").json()
    assert len(versions) == 2
    newer_id = versions[0]["version_id"]  # signups=2
    older_id = versions[1]["version_id"]  # signups=1

    res = client.get(f"/api/pipelines/compare_versions/versions/{older_id}/compare/{newer_id}")
    assert res.status_code == 200
    body = res.json()
    assert body["a"]["steps"][0]["inputs"]["signups"] == 1
    assert body["b"]["steps"][0]["inputs"]["signups"] == 2
    assert "steps" in body["diff"]


def test_compare_two_pipeline_versions_is_empty_diff_for_identical_content():
    client.post(
        "/api/pipelines",
        json={
            "name": "Compare Identical",
            "steps": [{"tier": "automation", "name": "fetch_raw_metrics", "inputs": {"signups": 5, "churn": 1, "revenue": 1}}],
        },
    )
    # Re-save with the exact same steps (name change alone still archives a version).
    client.post(
        "/api/pipelines",
        json={
            "name": "Compare Identical",
            "steps": [{"tier": "automation", "name": "fetch_raw_metrics", "inputs": {"signups": 5, "churn": 1, "revenue": 1}}],
        },
    )
    client.post(
        "/api/pipelines",
        json={
            "name": "Compare Identical",
            "steps": [{"tier": "automation", "name": "fetch_raw_metrics", "inputs": {"signups": 5, "churn": 1, "revenue": 1}}],
        },
    )
    versions = client.get("/api/pipelines/compare_identical/versions").json()
    assert len(versions) == 2

    res = client.get(
        f"/api/pipelines/compare_identical/versions/{versions[0]['version_id']}/compare/{versions[1]['version_id']}"
    )
    assert res.status_code == 200
    assert res.json()["diff"] == {}


def test_compare_pipeline_versions_404s_on_an_unknown_version_on_either_side():
    client.post(
        "/api/pipelines",
        json={"name": "Compare Unknown", "steps": [{"tier": "automation", "name": "fetch_raw_metrics"}]},
    )
    client.post(
        "/api/pipelines",
        json={
            "name": "Compare Unknown",
            "steps": [{"tier": "automation", "name": "fetch_raw_metrics", "inputs": {"signups": 2, "churn": 1, "revenue": 1}}],
        },
    )
    real_id = client.get("/api/pipelines/compare_unknown/versions").json()[0]["version_id"]

    assert client.get(f"/api/pipelines/compare_unknown/versions/does-not-exist/compare/{real_id}").status_code == 404
    assert client.get(f"/api/pipelines/compare_unknown/versions/{real_id}/compare/does-not-exist").status_code == 404


def test_restore_pipeline_version_brings_back_the_old_definition():
    client.post(
        "/api/pipelines",
        json={
            "name": "Restore Target",
            "steps": [{"tier": "automation", "name": "fetch_raw_metrics", "inputs": {"signups": 1, "churn": 1, "revenue": 1}}],
        },
    )
    client.post(
        "/api/pipelines",
        json={
            "name": "Restore Target",
            "steps": [{"tier": "automation", "name": "fetch_raw_metrics", "inputs": {"signups": 999, "churn": 1, "revenue": 1}}],
        },
    )
    old_version_id = client.get("/api/pipelines/restore_target/versions").json()[0]["version_id"]

    res = client.post(f"/api/pipelines/restore_target/versions/{old_version_id}/restore")
    assert res.status_code == 200
    restored = res.json()["pipeline"]
    assert restored["steps"][0]["inputs"]["signups"] == 1

    current = client.get("/api/pipelines/restore_target/graph")  # loads current definition indirectly
    assert current.status_code == 200

    # restoring is itself undoable: the pre-restore (signups=999) definition
    # is now archived as a new version
    versions_after_restore = client.get("/api/pipelines/restore_target/versions").json()
    assert len(versions_after_restore) == 2


def test_pipeline_versions_for_unknown_slug_404s():
    assert client.get("/api/pipelines/does_not_exist/versions").status_code == 404


def test_get_unknown_pipeline_version_404s():
    client.post(
        "/api/pipelines",
        json={"name": "No Versions Yet", "steps": [{"tier": "automation", "name": "fetch_raw_metrics"}]},
    )
    res = client.get("/api/pipelines/no_versions_yet/versions/20200101T000000000000")
    assert res.status_code == 404


def test_restore_unknown_pipeline_version_404s():
    client.post(
        "/api/pipelines",
        json={"name": "No Versions To Restore", "steps": [{"tier": "automation", "name": "fetch_raw_metrics"}]},
    )
    res = client.post("/api/pipelines/no_versions_to_restore/versions/20200101T000000000000/restore")
    assert res.status_code == 404


# ---- Branch a pipeline version into a new pipeline (distinct from in-place restore) ----

def test_branch_pipeline_version_creates_a_new_pipeline_without_touching_the_original():
    client.post(
        "/api/pipelines",
        json={
            "name": "Branch Source",
            "steps": [{"tier": "automation", "name": "fetch_raw_metrics", "inputs": {"signups": 1, "churn": 1, "revenue": 1}}],
        },
    )
    client.post(
        "/api/pipelines",
        json={
            "name": "Branch Source",
            "steps": [{"tier": "automation", "name": "fetch_raw_metrics", "inputs": {"signups": 999, "churn": 1, "revenue": 1}}],
        },
    )
    old_version_id = client.get("/api/pipelines/branch_source/versions").json()[0]["version_id"]

    res = client.post(
        f"/api/pipelines/branch_source/versions/{old_version_id}/branch",
        json={"new_name": "Branch Source (from old version)"},
    )
    assert res.status_code == 200
    branched = res.json()["pipeline"]
    assert branched["slug"] == "branch_source_from_old_version"
    assert branched["steps"][0]["inputs"]["signups"] == 1

    # The original pipeline's own current definition is untouched.
    original = pipeline_store.load_pipeline("branch_source")
    assert original["steps"][0]["inputs"]["signups"] == 999


def test_branch_pipeline_version_rejects_a_colliding_name():
    client.post(
        "/api/pipelines",
        json={"name": "Branch Collide Source", "steps": [{"tier": "automation", "name": "fetch_raw_metrics"}]},
    )
    client.post(
        "/api/pipelines",
        json={"name": "Branch Collide Source", "steps": [{"tier": "automation", "name": "http_request", "inputs": {"url": "https://example.com", "method": "GET"}}]},
    )
    client.post(
        "/api/pipelines",
        json={"name": "Branch Collide Target", "steps": [{"tier": "automation", "name": "fetch_raw_metrics"}]},
    )
    version_id = client.get("/api/pipelines/branch_collide_source/versions").json()[0]["version_id"]

    res = client.post(
        f"/api/pipelines/branch_collide_source/versions/{version_id}/branch",
        json={"new_name": "Branch Collide Target"},
    )
    assert res.status_code == 400


def test_branch_unknown_pipeline_version_404s():
    client.post(
        "/api/pipelines",
        json={"name": "No Versions To Branch", "steps": [{"tier": "automation", "name": "fetch_raw_metrics"}]},
    )
    res = client.post(
        "/api/pipelines/no_versions_to_branch/versions/20200101T000000000000/branch",
        json={"new_name": "Whatever"},
    )
    assert res.status_code == 404


def test_branch_pipeline_version_rejects_a_blank_name():
    client.post(
        "/api/pipelines",
        json={"name": "Branch Blank Name Source", "steps": [{"tier": "automation", "name": "fetch_raw_metrics"}]},
    )
    client.post(
        "/api/pipelines",
        json={"name": "Branch Blank Name Source", "steps": [{"tier": "automation", "name": "http_request", "inputs": {"url": "https://example.com", "method": "GET"}}]},
    )
    version_id = client.get("/api/pipelines/branch_blank_name_source/versions").json()[0]["version_id"]

    res = client.post(
        f"/api/pipelines/branch_blank_name_source/versions/{version_id}/branch",
        json={"new_name": "   "},
    )
    assert res.status_code == 400


# ---- Delete a saved pipeline ----

def test_delete_pipeline_removes_it_from_the_saved_list():
    client.post(
        "/api/pipelines",
        json={"name": "Delete Me", "steps": [{"tier": "automation", "name": "fetch_raw_metrics"}]},
    )
    assert any(p["slug"] == "delete_me" for p in client.get("/api/pipelines").json())

    res = client.delete("/api/pipelines/delete_me")
    assert res.status_code == 200
    assert res.json() == {"deleted": "delete_me"}
    assert not any(p["slug"] == "delete_me" for p in client.get("/api/pipelines").json())


def test_delete_unknown_pipeline_404s():
    res = client.delete("/api/pipelines/does_not_exist")
    assert res.status_code == 404


def test_deleted_pipelines_version_history_survives_and_can_be_restored():
    """The archived versions directory is deliberately left in place on
    delete -- recreating a pipeline under the same name lets its History
    panel see (and restore) whatever was archived before the delete."""
    client.post(
        "/api/pipelines",
        json={
            "name": "Recoverable",
            "steps": [{"tier": "automation", "name": "fetch_raw_metrics", "inputs": {"signups": 1, "churn": 1, "revenue": 1}}],
        },
    )
    client.post(
        "/api/pipelines",
        json={
            "name": "Recoverable",
            "steps": [{"tier": "automation", "name": "fetch_raw_metrics", "inputs": {"signups": 999, "churn": 1, "revenue": 1}}],
        },
    )
    versions_before_delete = client.get("/api/pipelines/recoverable/versions").json()
    assert len(versions_before_delete) == 1

    assert client.delete("/api/pipelines/recoverable").status_code == 200

    # Gone from the saved list...
    assert not any(p["slug"] == "recoverable" for p in client.get("/api/pipelines").json())
    # ...but its version history is still there.
    versions_after_delete = client.get("/api/pipelines/recoverable/versions").json()
    assert versions_after_delete == versions_before_delete

    restored = client.post(
        f"/api/pipelines/recoverable/versions/{versions_before_delete[0]['version_id']}/restore"
    )
    assert restored.status_code == 200
    assert restored.json()["pipeline"]["steps"][0]["inputs"]["signups"] == 1
    assert any(p["slug"] == "recoverable" for p in client.get("/api/pipelines").json())


# ---- Batch 17: bulk-delete saved pipelines ----

def test_bulk_delete_pipelines_removes_every_selected_one():
    client.post("/api/pipelines", json={"name": "Bulk Del A", "steps": [{"tier": "automation", "name": "fetch_raw_metrics"}]})
    client.post("/api/pipelines", json={"name": "Bulk Del B", "steps": [{"tier": "automation", "name": "fetch_raw_metrics"}]})
    client.post("/api/pipelines", json={"name": "Bulk Del Keep", "steps": [{"tier": "automation", "name": "fetch_raw_metrics"}]})

    res = client.post("/api/pipelines/bulk-delete", json={"slugs": ["bulk_del_a", "bulk_del_b"]})
    assert res.status_code == 200
    assert set(res.json()["deleted"]) == {"bulk_del_a", "bulk_del_b"}

    remaining_slugs = {p["slug"] for p in client.get("/api/pipelines").json()}
    assert "bulk_del_a" not in remaining_slugs
    assert "bulk_del_b" not in remaining_slugs
    assert "bulk_del_keep" in remaining_slugs


def test_bulk_delete_pipelines_skips_unknown_slugs_without_failing():
    client.post("/api/pipelines", json={"name": "Bulk Del Solo", "steps": [{"tier": "automation", "name": "fetch_raw_metrics"}]})

    res = client.post("/api/pipelines/bulk-delete", json={"slugs": ["bulk_del_solo", "does_not_exist_at_all"]})
    assert res.status_code == 200
    assert res.json()["deleted"] == ["bulk_del_solo"]


def test_bulk_delete_pipelines_is_a_no_op_on_an_empty_selection():
    res = client.post("/api/pipelines/bulk-delete", json={"slugs": []})
    assert res.status_code == 200
    assert res.json()["deleted"] == []


def test_bulk_delete_pipelines_also_clears_their_tags():
    client.post("/api/pipelines", json={"name": "Bulk Del Tagged", "steps": [{"tier": "automation", "name": "fetch_raw_metrics"}]})
    client.put("/api/pipelines/bulk_del_tagged/tags", json={"tags": ["important"]})

    client.post("/api/pipelines/bulk-delete", json={"slugs": ["bulk_del_tagged"]})

    tagged = client.get("/api/pipelines", params={"tag": "important"}).json()
    assert not any(p["slug"] == "bulk_del_tagged" for p in tagged)


# ---- Diff two saved pipelines ----

def test_compare_two_pipelines_reports_differing_keys():
    client.post(
        "/api/pipelines",
        json={
            "name": "Compare A",
            "description": "First one",
            "steps": [{"tier": "automation", "name": "fetch_raw_metrics", "inputs": {"signups": 1, "churn": 1, "revenue": 1}}],
            "launch": False,
        },
    )
    client.post(
        "/api/pipelines",
        json={
            "name": "Compare B",
            "description": "Second one",
            "steps": [{"tier": "automation", "name": "fetch_raw_metrics", "inputs": {"signups": 999, "churn": 1, "revenue": 1}}],
            "launch": False,
        },
    )

    res = client.get("/api/pipelines/compare", params={"a": "compare_a", "b": "compare_b"})
    assert res.status_code == 200
    body = res.json()
    assert body["a"]["name"] == "Compare A"
    assert body["b"]["name"] == "Compare B"
    assert "name" in body["diff"]
    assert "description" in body["diff"]
    assert "steps" in body["diff"]
    assert body["diff"]["name"] == {"a": "Compare A", "b": "Compare B"}


def test_compare_identical_pipelines_reports_no_differences():
    payload = {
        "name": "Identical Twin",
        "steps": [{"tier": "automation", "name": "fetch_raw_metrics"}],
        "launch": False,
    }
    client.post("/api/pipelines", json=payload)
    # Save a second pipeline under a different name but identical steps/description.
    client.post("/api/pipelines", json={**payload, "name": "Identical Twin 2"})

    res = client.get("/api/pipelines/compare", params={"a": "identical_twin", "b": "identical_twin_2"})
    diff = res.json()["diff"]
    assert set(diff.keys()) <= {"name", "slug"}  # only the name/slug should ever differ here


def test_compare_pipelines_404s_when_either_slug_is_unknown():
    client.post("/api/pipelines", json={"name": "Solo", "steps": [{"tier": "automation", "name": "fetch_raw_metrics"}], "launch": False})

    assert client.get("/api/pipelines/compare", params={"a": "solo", "b": "does_not_exist"}).status_code == 404
    assert client.get("/api/pipelines/compare", params={"a": "does_not_exist", "b": "solo"}).status_code == 404


# ---- Batch 12: validate a pipeline without saving or launching ----

def test_validate_accepts_a_well_formed_definition_without_persisting_it():
    payload = {
        "name": "Validate Only Should Not Persist",
        "steps": [{"tier": "automation", "name": "fetch_raw_metrics", "inputs": {"signups": 1, "churn": 1, "revenue": 1}}],
    }
    res = client.post("/api/pipelines/validate", json=payload)
    assert res.status_code == 200
    body = res.json()
    assert body["valid"] is True
    assert body["pipeline"]["slug"] == "validate_only_should_not_persist"

    # Never actually written to disk, and never appears among saved pipelines.
    assert not (pipeline_store.PIPELINES_DIR / "validate_only_should_not_persist.yaml").exists()
    assert not any(p["slug"] == "validate_only_should_not_persist" for p in client.get("/api/pipelines").json())


def test_validate_rejects_a_reference_to_a_nonexistent_module():
    res = client.post(
        "/api/pipelines/validate",
        json={"name": "Bad Validate", "steps": [{"tier": "automation", "name": "does_not_exist"}]},
    )
    assert res.status_code == 400
    assert not (pipeline_store.PIPELINES_DIR / "bad_validate.yaml").exists()


def test_validate_rejects_a_mapping_to_an_undeclared_output():
    res = client.post(
        "/api/pipelines/validate",
        json={
            "name": "Bad Mapping Validate",
            "steps": [
                {"tier": "automation", "name": "fetch_raw_metrics", "inputs": {}},
                {
                    "tier": "workflow",
                    "name": "analyze_metrics",
                    "inputs": {},
                    "mappings": {"risk_threshold": {"step": 0, "output": "not_a_real_output"}},
                },
            ],
        },
    )
    assert res.status_code == 400


def test_validate_does_not_launch_a_real_run():
    """Unlike POST /api/pipelines (even with launch: false, which still
    saves), /api/pipelines/validate must never touch run history at all."""
    before = len(client.get("/api/runs?limit=10000").json())
    client.post(
        "/api/pipelines/validate",
        json={"name": "No Run From Validate", "steps": [{"tier": "automation", "name": "fetch_raw_metrics"}]},
    )
    after = len(client.get("/api/runs?limit=10000").json())
    assert after == before


# ---- Pipeline tagging + tag filter ----

def test_set_and_read_pipeline_tags():
    client.post(
        "/api/pipelines",
        json={"name": "Tag Target", "steps": [{"tier": "automation", "name": "fetch_raw_metrics"}]},
    )
    res = client.put("/api/pipelines/tag_target/tags", json={"tags": ["nightly", "Billing"]})
    assert res.status_code == 200
    assert res.json() == {"slug": "tag_target", "tags": ["Billing", "nightly"]}

    listed = client.get("/api/pipelines").json()
    entry = next(p for p in listed if p["slug"] == "tag_target")
    assert entry["tags"] == ["Billing", "nightly"]


def test_tagging_an_unknown_pipeline_404s():
    res = client.put("/api/pipelines/does_not_exist/tags", json={"tags": ["x"]})
    assert res.status_code == 404


def test_blank_tags_are_stripped_and_deduplicated_case_sensitively():
    client.post(
        "/api/pipelines",
        json={"name": "Tag Cleanup", "steps": [{"tier": "automation", "name": "fetch_raw_metrics"}]},
    )
    res = client.put("/api/pipelines/tag_cleanup/tags", json={"tags": ["  ops  ", "", "ops", "  "]})
    assert res.status_code == 200
    assert res.json()["tags"] == ["ops"]


def test_filter_saved_pipelines_by_tag():
    client.post(
        "/api/pipelines",
        json={"name": "Tag Filter A", "steps": [{"tier": "automation", "name": "fetch_raw_metrics"}]},
    )
    client.post(
        "/api/pipelines",
        json={"name": "Tag Filter B", "steps": [{"tier": "automation", "name": "fetch_raw_metrics"}]},
    )
    client.put("/api/pipelines/tag_filter_a/tags", json={"tags": ["prod"]})

    res = client.get("/api/pipelines?tag=prod")
    assert res.status_code == 200
    slugs = [p["slug"] for p in res.json()]
    assert "tag_filter_a" in slugs
    assert "tag_filter_b" not in slugs

    # case-insensitive match
    res_case = client.get("/api/pipelines?tag=PROD")
    assert "tag_filter_a" in [p["slug"] for p in res_case.json()]


def test_deleting_a_pipeline_clears_its_tags():
    client.post(
        "/api/pipelines",
        json={"name": "Tag Then Delete", "steps": [{"tier": "automation", "name": "fetch_raw_metrics"}]},
    )
    client.put("/api/pipelines/tag_then_delete/tags", json={"tags": ["temp"]})
    client.delete("/api/pipelines/tag_then_delete")

    # Recreate under the same name -- it should come back with no tags.
    client.post(
        "/api/pipelines",
        json={"name": "Tag Then Delete", "steps": [{"tier": "automation", "name": "fetch_raw_metrics"}]},
    )
    listed = client.get("/api/pipelines").json()
    entry = next(p for p in listed if p["slug"] == "tag_then_delete")
    assert entry["tags"] == []


# ---- Full-text search across saved pipeline definitions ----

def test_search_pipelines_matches_a_keyword_inside_step_inputs():
    client.post(
        "/api/pipelines",
        json={
            "name": "Search Target",
            "steps": [
                {
                    "tier": "automation",
                    "name": "fetch_raw_metrics",
                    "inputs": {"signups": 1, "churn": 1, "revenue": 1},
                }
            ],
        },
    )
    res = client.get("/api/pipelines/search?q=fetch_raw_metrics")
    assert res.status_code == 200
    body = res.json()
    assert body["query"] == "fetch_raw_metrics"
    assert any(r["slug"] == "search_target" for r in body["results"])
    match = next(r for r in body["results"] if r["slug"] == "search_target")
    assert "snippet" in match


def test_search_pipelines_returns_empty_for_a_blank_query():
    res = client.get("/api/pipelines/search?q=")
    assert res.status_code == 200
    assert res.json()["results"] == []


def test_search_pipelines_finds_nothing_for_an_unmatched_keyword():
    client.post(
        "/api/pipelines",
        json={"name": "Search Miss", "steps": [{"tier": "automation", "name": "fetch_raw_metrics"}]},
    )
    res = client.get("/api/pipelines/search?q=zzz_nonexistent_keyword_zzz")
    assert res.json()["results"] == []


# ---- "Used by" reverse lookup for modules ----

def test_module_listing_reports_which_saved_pipelines_use_it():
    client.post(
        "/api/pipelines",
        json={"name": "Uses Fetch Metrics", "steps": [{"tier": "automation", "name": "fetch_raw_metrics"}]},
    )
    modules = client.get("/api/modules").json()
    fetch_metrics = next(m for m in modules["automation"] if m["name"] == "fetch_raw_metrics")
    assert "uses_fetch_metrics" in fetch_metrics["used_by"]


def test_module_listing_used_by_includes_parallel_group_branches():
    client.post(
        "/api/pipelines",
        json={
            "name": "Parallel Uses Fetch",
            "steps": [
                {
                    "type": "parallel",
                    "name": "",
                    "branches": [
                        {"tier": "automation", "name": "fetch_raw_metrics"},
                        {"tier": "automation", "name": "http_request", "inputs": {"url": "https://example.com", "method": "GET"}},
                    ],
                }
            ],
        },
    )
    modules = client.get("/api/modules").json()
    fetch_metrics = next(m for m in modules["automation"] if m["name"] == "fetch_raw_metrics")
    assert "parallel_uses_fetch" in fetch_metrics["used_by"]
    http_request = next(m for m in modules["automation"] if m["name"] == "http_request")
    assert "parallel_uses_fetch" in http_request["used_by"]


def test_module_used_by_is_empty_when_no_pipeline_references_it():
    modules = client.get("/api/modules").json()
    escalation_agent = next(m for m in modules["agent"] if m["name"] == "escalation_agent")
    assert escalation_agent["used_by"] == []


def test_module_used_by_csv_export_has_a_header_and_a_row_listing_the_using_pipeline():
    client.post(
        "/api/pipelines",
        json={"name": "Csv Uses Fetch Metrics", "steps": [{"tier": "automation", "name": "fetch_raw_metrics"}]},
    )
    res = client.get("/api/modules/used-by.csv")
    assert res.status_code == 200
    assert res.headers["content-type"].startswith("text/csv")
    assert "attachment; filename=modules_used_by.csv" in res.headers["content-disposition"]

    lines = res.text.strip().splitlines()
    assert lines[0] == "tier,name,used_by"
    fetch_metrics_line = next(line for line in lines[1:] if line.startswith("automation,fetch_raw_metrics,"))
    assert "csv_uses_fetch_metrics" in fetch_metrics_line


def test_module_used_by_csv_export_leaves_used_by_blank_for_an_unused_module():
    res = client.get("/api/modules/used-by.csv")
    lines = res.text.strip().splitlines()
    escalation_line = next(line for line in lines[1:] if line.startswith("agent,escalation_agent,"))
    assert escalation_line == "agent,escalation_agent,"


def test_module_directory_csv_export_has_a_header_and_a_row_per_enabled_module():
    import csv as csv_module
    import io as io_module

    res = client.get("/api/modules/directory.csv")
    assert res.status_code == 200
    assert res.headers["content-type"].startswith("text/csv")
    assert "attachment; filename=modules_directory.csv" in res.headers["content-disposition"]

    rows = list(csv_module.DictReader(io_module.StringIO(res.text)))
    assert set(rows[0].keys()) == {"tier", "name", "description", "enabled", "status", "breaker_tripped"}
    escalation_row = next(r for r in rows if r["tier"] == "agent" and r["name"] == "escalation_agent")
    assert escalation_row["description"]
    assert escalation_row["enabled"] in ("True", "False")
    assert escalation_row["status"] in ("ready", "error")
    assert escalation_row["breaker_tripped"] in ("True", "False")
