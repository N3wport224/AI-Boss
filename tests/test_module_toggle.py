"""Batch 10: runtime module enable/disable toggle from the dashboard --
independent of a module's manifest `enabled` flag, persisted in the state
store, and blocking every launch path exactly like a tripped circuit
breaker does.
"""
import pytest
from fastapi.testclient import TestClient

from webapp.main import app, store

client = TestClient(app)


@pytest.fixture(autouse=True)
def _restore_fetch_raw_metrics_enabled():
    """fetch_raw_metrics is used by dozens of other tests sharing this same
    process-wide store — always leave it enabled again afterward, success or
    failure, so a toggle test can never leak a disabled module into an
    unrelated test running later in the same session."""
    yield
    store.set_module_enabled("automation", "fetch_raw_metrics", True)
    store.set_module_enabled("automation", "http_request", True)


def test_module_is_enabled_by_default():
    # A module's override may or may not exist yet depending on test order
    # within this shared, process-wide store, but it must never default to
    # disabled — that's the one invariant every other test here relies on.
    listed = client.get("/api/modules").json()
    module = next(m for m in listed["automation"] if m["name"] == "fetch_raw_metrics")
    assert module["runtime_enabled"] is True


def test_disabling_a_module_reflects_in_the_listing():
    res = client.patch("/api/modules/automation/fetch_raw_metrics", json={"enabled": False})
    assert res.status_code == 200
    assert res.json() == {"tier": "automation", "name": "fetch_raw_metrics", "runtime_enabled": False}

    listed = client.get("/api/modules").json()
    module = next(m for m in listed["automation"] if m["name"] == "fetch_raw_metrics")
    assert module["runtime_enabled"] is False


def test_disabled_module_blocks_a_standalone_run():
    client.patch("/api/modules/automation/fetch_raw_metrics", json={"enabled": False})

    res = client.post(
        "/api/modules/automation/fetch_raw_metrics/run",
        json={"inputs": {"signups": 1, "churn": 1, "revenue": 1}},
    )
    assert res.status_code == 409
    assert "disabled" in res.json()["detail"].lower()
    assert "fetch_raw_metrics" in res.json()["detail"]


def test_disabled_module_blocks_a_saved_pipeline_using_it():
    client.patch("/api/modules/automation/fetch_raw_metrics", json={"enabled": False})

    payload = {
        "name": "Uses Disabled Module",
        "steps": [{"tier": "automation", "name": "fetch_raw_metrics", "inputs": {"signups": 1, "churn": 1, "revenue": 1}}],
    }
    res = client.post("/api/pipelines", json=payload)
    assert res.status_code == 409
    assert "fetch_raw_metrics" in res.json()["detail"]


def test_disabled_module_blocks_the_fixed_full_pipeline():
    client.patch("/api/modules/automation/fetch_raw_metrics", json={"enabled": False})

    res = client.post("/api/pipeline/run", json={"inputs": {}})
    assert res.status_code == 409


def test_re_enabling_lifts_the_block():
    client.patch("/api/modules/automation/fetch_raw_metrics", json={"enabled": False})
    blocked = client.post(
        "/api/modules/automation/fetch_raw_metrics/run",
        json={"inputs": {"signups": 1, "churn": 1, "revenue": 1}},
    )
    assert blocked.status_code == 409

    res = client.patch("/api/modules/automation/fetch_raw_metrics", json={"enabled": True})
    assert res.status_code == 200
    assert res.json()["runtime_enabled"] is True

    accepted = client.post(
        "/api/modules/automation/fetch_raw_metrics/run",
        json={"inputs": {"signups": 1, "churn": 1, "revenue": 1}, "force_refresh": True},
    )
    assert accepted.status_code == 200


def test_toggle_for_an_unknown_module_is_a_404():
    res = client.patch("/api/modules/automation/does_not_exist", json={"enabled": False})
    assert res.status_code == 404


def test_state_store_override_precedes_manifest_default(tmp_path):
    from engine.state_store import StateStore

    isolated_store = StateStore(str(tmp_path / "module_override_test.db"))
    assert isolated_store.get_module_enabled_override("automation", "fetch_raw_metrics") is None

    isolated_store.set_module_enabled("automation", "fetch_raw_metrics", False)
    assert isolated_store.get_module_enabled_override("automation", "fetch_raw_metrics") is False

    overrides = isolated_store.all_module_overrides()
    assert overrides[("automation", "fetch_raw_metrics")] is False

    isolated_store.set_module_enabled("automation", "fetch_raw_metrics", True)
    assert isolated_store.get_module_enabled_override("automation", "fetch_raw_metrics") is True
    isolated_store.close()


def test_bulk_set_modules_enabled_disables_and_re_enables_a_selection():
    res = client.post(
        "/api/modules/bulk-set-enabled",
        json={
            "modules": [
                {"tier": "automation", "name": "fetch_raw_metrics"},
                {"tier": "automation", "name": "http_request"},
            ],
            "enabled": False,
        },
    )
    assert res.status_code == 200
    body = res.json()
    assert body["enabled"] is False
    assert len(body["updated"]) == 2

    listed = client.get("/api/modules").json()
    metrics = next(m for m in listed["automation"] if m["name"] == "fetch_raw_metrics")
    http_req = next(m for m in listed["automation"] if m["name"] == "http_request")
    assert metrics["runtime_enabled"] is False
    assert http_req["runtime_enabled"] is False

    res2 = client.post(
        "/api/modules/bulk-set-enabled",
        json={
            "modules": [
                {"tier": "automation", "name": "fetch_raw_metrics"},
                {"tier": "automation", "name": "http_request"},
            ],
            "enabled": True,
        },
    )
    assert res2.status_code == 200
    listed2 = client.get("/api/modules").json()
    metrics2 = next(m for m in listed2["automation"] if m["name"] == "fetch_raw_metrics")
    http_req2 = next(m for m in listed2["automation"] if m["name"] == "http_request")
    assert metrics2["runtime_enabled"] is True
    assert http_req2["runtime_enabled"] is True


def test_bulk_set_modules_enabled_skips_an_unknown_module():
    res = client.post(
        "/api/modules/bulk-set-enabled",
        json={
            "modules": [
                {"tier": "automation", "name": "fetch_raw_metrics"},
                {"tier": "automation", "name": "does_not_exist"},
            ],
            "enabled": False,
        },
    )
    assert res.status_code == 200
    body = res.json()
    assert body["updated"] == [{"tier": "automation", "name": "fetch_raw_metrics"}]


def test_bulk_set_modules_enabled_is_a_no_op_on_an_empty_selection():
    res = client.post("/api/modules/bulk-set-enabled", json={"modules": [], "enabled": False})
    assert res.status_code == 200
    assert res.json() == {"updated": [], "enabled": False}


def test_clear_module_enabled_override_reverts_to_manifest_default():
    client.patch("/api/modules/automation/fetch_raw_metrics", json={"enabled": False})
    listed = client.get("/api/modules").json()
    module = next(m for m in listed["automation"] if m["name"] == "fetch_raw_metrics")
    assert module["enabled_overridden"] is True

    res = client.delete("/api/modules/automation/fetch_raw_metrics/enabled-override")
    assert res.status_code == 200
    assert res.json() == {"tier": "automation", "name": "fetch_raw_metrics", "runtime_enabled": True}

    listed2 = client.get("/api/modules").json()
    module2 = next(m for m in listed2["automation"] if m["name"] == "fetch_raw_metrics")
    assert module2["runtime_enabled"] is True
    assert module2["enabled_overridden"] is False


def test_clear_module_enabled_override_404s_for_an_unknown_module():
    res = client.delete("/api/modules/automation/does_not_exist/enabled-override")
    assert res.status_code == 404


def test_bulk_clear_modules_enabled_override_reverts_every_selected_module():
    client.post(
        "/api/modules/bulk-set-enabled",
        json={
            "modules": [
                {"tier": "automation", "name": "fetch_raw_metrics"},
                {"tier": "automation", "name": "http_request"},
            ],
            "enabled": False,
        },
    )

    res = client.post(
        "/api/modules/bulk-clear-enabled-override",
        json={
            "modules": [
                {"tier": "automation", "name": "fetch_raw_metrics"},
                {"tier": "automation", "name": "http_request"},
            ]
        },
    )
    assert res.status_code == 200
    body = res.json()
    assert len(body["updated"]) == 2

    listed = client.get("/api/modules").json()
    metrics = next(m for m in listed["automation"] if m["name"] == "fetch_raw_metrics")
    http_req = next(m for m in listed["automation"] if m["name"] == "http_request")
    assert metrics["runtime_enabled"] is True
    assert metrics["enabled_overridden"] is False
    assert http_req["runtime_enabled"] is True
    assert http_req["enabled_overridden"] is False


def test_bulk_clear_modules_enabled_override_skips_an_unknown_module():
    client.patch("/api/modules/automation/fetch_raw_metrics", json={"enabled": False})

    res = client.post(
        "/api/modules/bulk-clear-enabled-override",
        json={
            "modules": [
                {"tier": "automation", "name": "fetch_raw_metrics"},
                {"tier": "automation", "name": "does_not_exist"},
            ]
        },
    )
    assert res.status_code == 200
    body = res.json()
    assert body["updated"] == [{"tier": "automation", "name": "fetch_raw_metrics"}]


def test_bulk_clear_modules_enabled_override_is_a_no_op_on_an_empty_selection():
    res = client.post("/api/modules/bulk-clear-enabled-override", json={"modules": []})
    assert res.status_code == 200
    assert res.json() == {"updated": []}
