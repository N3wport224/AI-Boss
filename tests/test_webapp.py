from fastapi.testclient import TestClient

from webapp.main import app

client = TestClient(app)


def test_list_modules_groups_by_tier_and_includes_input_schema():
    res = client.get("/api/modules")
    assert res.status_code == 200

    body = res.json()
    assert set(body.keys()) == {"automation", "workflow", "agent"}

    automation = body["automation"][0]
    assert automation["name"] == "fetch_raw_metrics"
    assert {f["name"] for f in automation["inputs"]} == {"signups", "churn", "revenue"}


def test_run_single_module_with_overridden_inputs():
    res = client.post(
        "/api/modules/automation/fetch_raw_metrics/run",
        json={"inputs": {"signups": 200, "churn": 5, "revenue": 999}},
    )
    assert res.status_code == 200

    body = res.json()
    assert body["success"] is True
    assert body["output"]["raw_metrics"] == {"signups": 200.0, "churn": 5.0, "revenue": 999.0}


def test_run_unknown_module_returns_404():
    res = client.post("/api/modules/automation/does_not_exist/run", json={"inputs": {}})
    assert res.status_code == 404


def test_run_full_pipeline_threads_context_through_all_tiers():
    res = client.post("/api/pipeline/run", json={"inputs": {}})
    assert res.status_code == 200

    body = res.json()
    assert [s["name"] for s in body["steps"]] == [
        "fetch_raw_metrics",
        "analyze_metrics",
        "churn_response_agent",
    ]
    assert all(s["success"] for s in body["steps"])
    assert "agent_decision" in body["context"]
