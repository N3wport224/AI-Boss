"""Batch 11: runtime-configurable circuit breaker threshold -- override how
many consecutive failures trip a module's breaker from the dashboard,
independent of its manifest's own circuit_breaker_threshold, no YAML edit
needed.
"""
import json
import threading
from http.server import BaseHTTPRequestHandler, HTTPServer

import pytest
from fastapi.testclient import TestClient

from engine.state_store import StateStore
from webapp.main import app, store

client = TestClient(app)

UNREACHABLE_URL = "http://127.0.0.1:9/nope"


@pytest.fixture(autouse=True)
def reset_breaker_state():
    """Breaker health and threshold overrides live in the webapp's real
    shared StateStore; leaving either behind would affect other test files
    that also exercise http_request/fetch_raw_metrics's breaker."""
    for entry in store.all_module_health():
        store.reset_breaker(entry["tier"], entry["name"])
    for tier, name in list(store.all_breaker_threshold_overrides()):
        store.clear_breaker_threshold_override(tier, name)
    yield
    for entry in store.all_module_health():
        store.reset_breaker(entry["tier"], entry["name"])
    for tier, name in list(store.all_breaker_threshold_overrides()):
        store.clear_breaker_threshold_override(tier, name)


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


def _fail_http_request_once():
    res = client.post(
        "/api/modules/automation/http_request/run",
        json={"inputs": {"url": UNREACHABLE_URL, "timeout": 2}},
    )
    assert res.status_code == 200
    events = _collect_stream(res.json()["stream_id"])
    assert any(e["kind"] == "step_failed" for e in events)


# ---- Store-level behavior ----

def test_store_threshold_override_round_trip(tmp_path):
    s = StateStore(str(tmp_path / "breaker_threshold.db"))
    assert s.get_breaker_threshold_override("automation", "m") is None

    s.set_breaker_threshold("automation", "m", 7)
    assert s.get_breaker_threshold_override("automation", "m") == 7
    assert s.all_breaker_threshold_overrides() == {("automation", "m"): 7}

    s.set_breaker_threshold("automation", "m", 2)
    assert s.get_breaker_threshold_override("automation", "m") == 2

    s.clear_breaker_threshold_override("automation", "m")
    assert s.get_breaker_threshold_override("automation", "m") is None
    assert s.all_breaker_threshold_overrides() == {}
    s.close()


# ---- API surface ----

def test_modules_listing_reports_manifest_default_when_no_override():
    listed = client.get("/api/modules").json()
    http_module = next(m for m in listed["automation"] if m["name"] == "http_request")
    assert http_module["breaker"]["threshold"] == 3
    assert http_module["breaker"]["threshold_overridden"] is False


def test_setting_threshold_is_reflected_in_modules_listing():
    res = client.patch("/api/breakers/automation/http_request/threshold", json={"threshold": 5})
    assert res.status_code == 200
    assert res.json() == {"tier": "automation", "name": "http_request", "threshold": 5}

    listed = client.get("/api/modules").json()
    http_module = next(m for m in listed["automation"] if m["name"] == "http_request")
    assert http_module["breaker"]["threshold"] == 5
    assert http_module["breaker"]["threshold_overridden"] is True


def test_clearing_threshold_reverts_to_manifest_default():
    client.patch("/api/breakers/automation/http_request/threshold", json={"threshold": 5})
    res = client.delete("/api/breakers/automation/http_request/threshold")
    assert res.status_code == 200
    assert res.json() == {"tier": "automation", "name": "http_request", "threshold": 3}

    listed = client.get("/api/modules").json()
    http_module = next(m for m in listed["automation"] if m["name"] == "http_request")
    assert http_module["breaker"]["threshold_overridden"] is False


def test_set_threshold_rejects_less_than_one():
    res = client.patch("/api/breakers/automation/http_request/threshold", json={"threshold": 0})
    assert res.status_code == 400


def test_set_threshold_for_unknown_module_404s():
    res = client.patch("/api/breakers/automation/does_not_exist/threshold", json={"threshold": 5})
    assert res.status_code == 404


def test_clear_threshold_for_unknown_module_404s():
    res = client.delete("/api/breakers/automation/does_not_exist/threshold")
    assert res.status_code == 404


def test_lowering_the_threshold_trips_the_breaker_sooner():
    """The real point of this feature: a module trips after 3 failures by
    default, but overriding the threshold to 1 trips it on the very first
    one -- proving the override actually drives record_module_failure, not
    just the reported number."""
    client.patch("/api/breakers/automation/http_request/threshold", json={"threshold": 1})

    _fail_http_request_once()

    health = store.get_module_health("automation", "http_request")
    assert health["tripped"] is True
    assert health["consecutive_failures"] == 1

    blocked = client.post(
        "/api/modules/automation/http_request/run",
        json={"inputs": {"url": UNREACHABLE_URL}},
    )
    assert blocked.status_code == 409


def test_raising_the_threshold_does_not_silently_untrip_an_open_breaker():
    """Once tripped, only an explicit reset closes it -- raising the
    threshold afterward must not quietly re-arm it."""
    client.patch("/api/breakers/automation/http_request/threshold", json={"threshold": 1})
    _fail_http_request_once()
    assert store.get_module_health("automation", "http_request")["tripped"] is True

    client.patch("/api/breakers/automation/http_request/threshold", json={"threshold": 10})
    assert store.get_module_health("automation", "http_request")["tripped"] is True

    blocked = client.post(
        "/api/modules/automation/http_request/run",
        json={"inputs": {"url": UNREACHABLE_URL}},
    )
    assert blocked.status_code == 409
