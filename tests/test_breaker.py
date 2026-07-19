import json
import threading
from http.server import BaseHTTPRequestHandler, HTTPServer

import pytest
from fastapi.testclient import TestClient

from engine.state_store import StateStore
from webapp.main import app, store

client = TestClient(app)


class _OkHandler(BaseHTTPRequestHandler):
    def do_GET(self):
        body = b'{"ok": true}'
        self.send_response(200)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def log_message(self, *args):
        pass


@pytest.fixture(scope="module")
def ok_server():
    server = HTTPServer(("127.0.0.1", 0), _OkHandler)
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    try:
        yield f"http://127.0.0.1:{server.server_port}"
    finally:
        server.shutdown()
        thread.join(timeout=5)

# Nothing listens on port 9 (discard) on loopback — connecting fails fast,
# which is exactly the kind of repeatable failure a breaker should count.
UNREACHABLE_URL = "http://127.0.0.1:9/nope"


@pytest.fixture(autouse=True)
def reset_all_breakers():
    """Breaker state lives in the webapp's real shared StateStore; leaving a
    tripped breaker behind would block http_request runs in *other* test
    files. Reset everything on both sides of every test."""
    for entry in store.all_module_health():
        store.reset_breaker(entry["tier"], entry["name"])
    yield
    for entry in store.all_module_health():
        store.reset_breaker(entry["tier"], entry["name"])


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

def test_breaker_trips_only_on_consecutive_failures(tmp_path):
    s = StateStore(str(tmp_path / "breaker.db"))
    s.record_module_failure("automation", "m", 3)
    s.record_module_failure("automation", "m", 3)
    s.record_module_success("automation", "m")  # closes the streak
    s.record_module_failure("automation", "m", 3)
    s.record_module_failure("automation", "m", 3)
    assert s.get_module_health("automation", "m")["tripped"] is False

    s.record_module_failure("automation", "m", 3)  # third consecutive
    assert s.get_module_health("automation", "m")["tripped"] is True
    s.close()


def test_success_never_closes_an_already_open_breaker(tmp_path):
    s = StateStore(str(tmp_path / "breaker.db"))
    for _ in range(3):
        s.record_module_failure("agent", "m", 3)
    s.record_module_success("agent", "m")
    assert s.get_module_health("agent", "m")["tripped"] is True

    reset = s.reset_breaker("agent", "m")
    assert reset["tripped"] is False
    assert reset["consecutive_failures"] == 0
    s.close()


# ---- End-to-end through the webapp ----

def test_three_consecutive_failures_trip_the_breaker_and_block_runs():
    for _ in range(3):
        _fail_http_request_once()

    listed = client.get("/api/modules").json()
    http_module = next(m for m in listed["automation"] if m["name"] == "http_request")
    assert http_module["breaker"]["tripped"] is True
    assert http_module["breaker"]["consecutive_failures"] == 3
    assert http_module["breaker"]["threshold"] == 3

    blocked = client.post(
        "/api/modules/automation/http_request/run",
        json={"inputs": {"url": UNREACHABLE_URL}},
    )
    assert blocked.status_code == 409
    assert "Circuit breaker open" in blocked.json()["detail"]


def test_reset_endpoint_reopens_the_module_for_runs():
    for _ in range(3):
        _fail_http_request_once()
    assert store.get_module_health("automation", "http_request")["tripped"] is True

    res = client.post("/api/breakers/automation/http_request/reset")
    assert res.status_code == 200
    assert res.json()["tripped"] is False

    # Runs are accepted again (this one will fail too, but it's not a 409).
    accepted = client.post(
        "/api/modules/automation/http_request/run",
        json={"inputs": {"url": UNREACHABLE_URL, "timeout": 2}},
    )
    assert accepted.status_code == 200
    _collect_stream(accepted.json()["stream_id"])


def test_bulk_reset_breakers_reopens_every_selected_module():
    for _ in range(3):
        _fail_http_request_once()
    store.record_module_failure("automation", "fetch_raw_metrics", 3)
    assert store.get_module_health("automation", "http_request")["tripped"] is True
    assert store.get_module_health("automation", "fetch_raw_metrics")["consecutive_failures"] == 1

    res = client.post(
        "/api/breakers/bulk-reset",
        json={
            "modules": [
                {"tier": "automation", "name": "http_request"},
                {"tier": "automation", "name": "fetch_raw_metrics"},
            ]
        },
    )
    assert res.status_code == 200
    body = res.json()
    assert len(body["reset"]) == 2

    assert store.get_module_health("automation", "http_request")["tripped"] is False
    assert store.get_module_health("automation", "fetch_raw_metrics")["consecutive_failures"] == 0


def test_bulk_reset_breakers_skips_an_unknown_module():
    res = client.post(
        "/api/breakers/bulk-reset",
        json={"modules": [{"tier": "automation", "name": "http_request"}, {"tier": "automation", "name": "does_not_exist"}]},
    )
    assert res.status_code == 200
    assert res.json()["reset"] == [{"tier": "automation", "name": "http_request"}]


def test_bulk_reset_breakers_is_a_no_op_on_an_empty_selection():
    res = client.post("/api/breakers/bulk-reset", json={"modules": []})
    assert res.status_code == 200
    assert res.json() == {"reset": []}


def test_breakers_listing_reports_failure_counts_below_threshold():
    _fail_http_request_once()

    entries = client.get("/api/breakers").json()
    entry = next(e for e in entries if e["tier"] == "automation" and e["name"] == "http_request")
    assert entry["consecutive_failures"] == 1
    assert entry["tripped"] is False


def test_saved_pipeline_containing_a_tripped_module_is_blocked():
    for _ in range(3):
        _fail_http_request_once()

    payload = {
        "name": "Uses Tripped Module",
        "steps": [{"tier": "automation", "name": "http_request", "inputs": {"url": UNREACHABLE_URL}}],
    }
    res = client.post("/api/pipelines", json=payload)
    assert res.status_code == 409
    assert "http_request" in res.json()["detail"]


def test_a_success_after_failures_resets_the_streak_through_the_real_run_path(ok_server):
    _fail_http_request_once()
    _fail_http_request_once()
    assert store.get_module_health("automation", "http_request")["consecutive_failures"] == 2

    ok = client.post(
        "/api/modules/automation/http_request/run",
        json={"inputs": {"url": ok_server}, "force_refresh": True},
    )
    events = _collect_stream(ok.json()["stream_id"])
    assert events[-1]["kind"] == "run_completed"
    assert store.get_module_health("automation", "http_request")["consecutive_failures"] == 0


def test_reset_for_an_unknown_module_is_a_404():
    res = client.post("/api/breakers/automation/does_not_exist/reset")
    assert res.status_code == 404
