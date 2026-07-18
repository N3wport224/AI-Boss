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
