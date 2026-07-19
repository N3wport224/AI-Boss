"""Batch 10: one-click module self-test -- actually runs every enabled
module once with its own manifest's safe default inputs (not just checking
that manifests parse and entrypoints instantiate, which /api/health already
does), and reports a pass/fail per module with error detail and timing.
"""
import pytest
from fastapi.testclient import TestClient

from webapp.main import app, store

client = TestClient(app)

ALL_MODULE_NAMES = {
    "fetch_raw_metrics", "http_request", "analyze_metrics",
    "escalation_agent", "churn_response_agent",
}


@pytest.fixture(autouse=True)
def restore_module_state():
    """Runtime module-enabled overrides live in the shared app-level
    StateStore; leaving one disabled would leak into other test files."""
    yield
    for (tier, name) in list(store.all_module_overrides()):
        store.set_module_enabled(tier, name, True)


def test_self_test_covers_every_bundled_module():
    res = client.post("/api/self-test")
    assert res.status_code == 200
    body = res.json()

    names = {r["name"] for r in body["results"]}
    assert names == ALL_MODULE_NAMES
    assert body["passed"] + body["failed"] + body["skipped"] == len(body["results"])


def test_self_test_reports_a_definite_status_with_timing_for_every_module():
    res = client.post("/api/self-test").json()
    for r in res["results"]:
        assert r["status"] in ("pass", "fail", "skipped")
        assert isinstance(r["duration_seconds"], (int, float))
        assert r["duration_seconds"] >= 0
        if r["status"] == "fail":
            assert r["detail"]  # a failure always carries error detail


def test_self_test_actually_executes_run_not_just_import():
    """fetch_raw_metrics has no external dependency and safe manifest
    defaults -- it should genuinely execute and pass, proving this is a real
    run() call, not merely confirming the class imports and instantiates."""
    res = client.post("/api/self-test").json()
    entry = next(r for r in res["results"] if r["name"] == "fetch_raw_metrics")
    assert entry["status"] == "pass"
    assert entry["detail"] is None


def test_self_test_skips_a_runtime_disabled_module():
    store.set_module_enabled("automation", "fetch_raw_metrics", False)

    res = client.post("/api/self-test").json()
    entry = next(r for r in res["results"] if r["name"] == "fetch_raw_metrics")
    assert entry["status"] == "skipped"
    assert entry["duration_seconds"] == 0.0


def test_self_test_never_touches_real_run_history():
    """A dry-run diagnostic -- unlike a real module run, self-test should
    leave the run history untouched (no new run/step rows recorded)."""
    before = len(client.get("/api/runs?limit=10000").json())
    client.post("/api/self-test")
    after = len(client.get("/api/runs?limit=10000").json())
    assert after == before


def test_self_test_never_touches_persistent_agent_memory():
    """churn_response_agent writes context.memory['last_risk_level'] on a
    real run -- self-test's disposable ExecutionContext has no state_store
    attached, so that key must remain whatever it was before (or absent)."""
    before = store.get_memory("last_risk_level", "__absent__")
    client.post("/api/self-test")
    after = store.get_memory("last_risk_level", "__absent__")
    assert after == before


def test_self_test_is_not_recorded_in_the_audit_log():
    """Self-test is a read-only diagnostic, not a destructive/administrative
    action -- it must never appear in the Recent Actions audit trail."""
    client.post("/api/self-test")
    events = client.get("/api/audit-log").json()
    assert not any("self_test" in e["action"] for e in events)
