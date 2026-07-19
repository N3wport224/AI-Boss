"""Batch 8: agent-to-agent delegation between churn_response_agent and
escalation_agent — genuine multi-agent collaboration (one agent deciding
another agent should take over), not just the usual tier-1->2->3 handoff.
"""
import json

from fastapi.testclient import TestClient

from agents.escalation_agent import EscalationAgent
from agents.example_agent import ChurnResponseAgent
from engine.context import ExecutionContext
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


# ---- Unit-level: each module in isolation ----

def test_churn_response_agent_requests_handoff_on_high_risk():
    context = ExecutionContext({"insight": {"risk_level": "high", "churn_rate": 0.42}})
    result = ChurnResponseAgent().run(context)

    handoff = result["handoff"]
    assert handoff["requested"] is True
    assert handoff["priority"] == "high"
    assert "42.00%" in handoff["reason"]


def test_churn_response_agent_does_not_request_handoff_on_low_risk():
    context = ExecutionContext({"insight": {"risk_level": "low", "churn_rate": 0.02}})
    result = ChurnResponseAgent().run(context)

    handoff = result["handoff"]
    assert handoff["requested"] is False
    assert handoff["reason"] == ""


def test_churn_response_agent_handoff_can_be_disabled_even_on_high_risk():
    context = ExecutionContext({"insight": {"risk_level": "high", "churn_rate": 0.9}, "enable_handoff": False})
    result = ChurnResponseAgent().run(context)
    assert result["handoff"]["requested"] is False


def test_escalation_agent_declines_when_no_handoff_is_present():
    context = ExecutionContext({})
    result = EscalationAgent().run(context)
    assert result["escalation_result"] == {"status": "skipped", "reason": "no handoff requested"}


def test_escalation_agent_declines_when_handoff_not_requested():
    context = ExecutionContext({"handoff": {"requested": False}})
    result = EscalationAgent().run(context)
    assert result["escalation_result"]["status"] == "skipped"


def test_escalation_agent_acts_on_a_genuine_handoff():
    context = ExecutionContext(
        {
            "handoff": {
                "requested": True,
                "reason": "churn rate 55.00% is in the high-risk band",
                "priority": "high",
                "risk_level": "high",
                "churn_rate": 0.55,
            }
        }
    )
    result = EscalationAgent().run(context)

    escalation = result["escalation_result"]
    assert escalation["status"] == "escalated"
    assert escalation["assigned_to"] == "senior_retention_specialist"
    assert "55.00%" in escalation["plan"]


def test_escalation_agent_assigns_a_regular_specialist_for_normal_priority():
    context = ExecutionContext({"handoff": {"requested": True, "priority": "normal", "reason": "x"}})
    result = EscalationAgent().run(context)
    assert result["escalation_result"]["assigned_to"] == "retention_specialist"


# ---- API-level: real chained pipeline through the webapp ----

def _handoff_pipeline_payload(name, signups, churn, enable_handoff=True):
    return {
        "name": name,
        "steps": [
            {"tier": "automation", "name": "fetch_raw_metrics", "inputs": {"signups": signups, "churn": churn, "revenue": 5}},
            {"tier": "workflow", "name": "analyze_metrics", "inputs": {"risk_threshold": 0.1}},
            {
                "tier": "agent",
                "name": "churn_response_agent",
                "inputs": {"notify_slack": False, "enable_handoff": enable_handoff},
            },
            {
                "tier": "agent",
                "name": "escalation_agent",
                "inputs": {},
                "mappings": {"handoff": {"step": 2, "output": "handoff"}},
            },
        ],
    }


def test_high_risk_pipeline_hands_off_and_escalation_agent_acts():
    payload = _handoff_pipeline_payload("Handoff High API", signups=10, churn=9)
    res = client.post("/api/pipelines", json=payload)
    assert res.status_code == 200

    events = _collect_stream(res.json()["stream_id"])
    final = events[-1]["context"]
    assert final["handoff"]["requested"] is True
    assert final["escalation_result"]["status"] == "escalated"


def test_low_risk_pipeline_declines_handoff_and_escalation_agent_skips():
    payload = _handoff_pipeline_payload("Handoff Low API", signups=1000, churn=1)
    res = client.post("/api/pipelines", json=payload)
    assert res.status_code == 200

    events = _collect_stream(res.json()["stream_id"])
    final = events[-1]["context"]
    assert final["handoff"]["requested"] is False
    assert final["escalation_result"]["status"] == "skipped"


def test_handoff_disabled_toggle_suppresses_escalation_even_at_high_risk():
    payload = _handoff_pipeline_payload("Handoff Disabled API", signups=10, churn=9, enable_handoff=False)
    res = client.post("/api/pipelines", json=payload)
    assert res.status_code == 200

    events = _collect_stream(res.json()["stream_id"])
    final = events[-1]["context"]
    assert final["handoff"]["requested"] is False
    assert final["escalation_result"]["status"] == "skipped"


def test_escalation_agent_is_listed_but_excluded_from_the_fixed_full_pipeline():
    listed = client.get("/api/modules").json()
    agent_names = {m["name"] for m in listed["agent"]}
    assert "escalation_agent" in agent_names

    res = client.post("/api/pipeline/run", json={"inputs": {}})
    events = _collect_stream(res.json()["stream_id"])
    assert "escalation_result" not in events[-1]["context"]
