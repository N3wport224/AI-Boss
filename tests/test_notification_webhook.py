"""Batch 36: forward critical notifications (breaker trips, schedule
failures, backup failures) to a configurable outbound webhook URL -- a
plain HTTP POST to a user-supplied URL, same trust model as the existing
http_request automation module and URL-ingestion feature, not a SaaS
integration.
"""
import json
import threading
from http.server import BaseHTTPRequestHandler, HTTPServer

import pytest
from fastapi.testclient import TestClient

from webapp.main import app

client = TestClient(app)

UNREACHABLE_URL = "http://127.0.0.1:9/nope"

received_requests = []


class _Handler(BaseHTTPRequestHandler):
    def do_POST(self):
        length = int(self.headers.get("Content-Length", 0))
        body = self.rfile.read(length)
        received_requests.append(json.loads(body))
        self.send_response(200)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", "2")
        self.end_headers()
        self.wfile.write(b"{}")

    def log_message(self, *args):
        pass


@pytest.fixture(scope="module")
def test_server():
    server = HTTPServer(("127.0.0.1", 0), _Handler)
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    try:
        yield f"http://127.0.0.1:{server.server_port}"
    finally:
        server.shutdown()
        thread.join(timeout=5)


@pytest.fixture(autouse=True)
def clear_received_and_reset_webhook_config():
    received_requests.clear()
    yield
    received_requests.clear()
    client.patch("/api/notifications/webhook", json={"enabled": False, "url": ""})


def test_get_notification_webhook_config_defaults_to_disabled():
    res = client.get("/api/notifications/webhook")
    assert res.status_code == 200
    assert res.json() == {"enabled": False, "url": ""}


def test_configure_notification_webhook_round_trip(test_server):
    res = client.patch("/api/notifications/webhook", json={"enabled": True, "url": f"{test_server}/hook"})
    assert res.status_code == 200
    assert res.json() == {"enabled": True, "url": f"{test_server}/hook"}

    res2 = client.get("/api/notifications/webhook")
    assert res2.json() == {"enabled": True, "url": f"{test_server}/hook"}


def test_configure_notification_webhook_rejects_enabling_without_a_url():
    res = client.patch("/api/notifications/webhook", json={"enabled": True, "url": ""})
    assert res.status_code == 400


def test_disabling_the_webhook_does_not_require_a_url():
    res = client.patch("/api/notifications/webhook", json={"enabled": False, "url": ""})
    assert res.status_code == 200
    assert res.json() == {"enabled": False, "url": ""}


def test_send_test_webhook_posts_a_synthetic_notification(test_server):
    res = client.post("/api/notifications/webhook/test", json={"url": f"{test_server}/hook"})
    assert res.status_code == 200
    assert res.json() == {"sent": True}

    assert len(received_requests) == 1
    assert received_requests[0]["kind"] == "test"
    assert "test notification" in received_requests[0]["message"].lower()


def test_send_test_webhook_reports_failure_for_an_unreachable_url():
    res = client.post("/api/notifications/webhook/test", json={"url": UNREACHABLE_URL})
    assert res.status_code == 200
    body = res.json()
    assert body["sent"] is False
    assert body["error"]


def test_send_test_webhook_rejects_a_blank_url():
    res = client.post("/api/notifications/webhook/test", json={"url": "   "})
    assert res.status_code == 400


def test_a_circuit_breaker_trip_forwards_to_the_configured_webhook(test_server):
    client.patch("/api/notifications/webhook", json={"enabled": True, "url": f"{test_server}/hook"})
    client.patch("/api/breakers/automation/http_request/threshold", json={"threshold": 1})

    res = client.post(
        "/api/modules/automation/http_request/run",
        json={"inputs": {"url": UNREACHABLE_URL, "timeout": 2}},
    )
    stream_id = res.json()["stream_id"]
    with client.stream("GET", f"/api/stream/{stream_id}") as response:
        for line in response.iter_lines():
            if not line.startswith("data: "):
                continue
            event = json.loads(line[len("data: "):])
            if event["kind"] in ("run_completed", "run_failed"):
                break

    client.delete("/api/breakers/automation/http_request/threshold")

    assert any(r.get("kind") == "breaker_tripped" for r in received_requests), received_requests


def test_a_non_critical_notification_kind_is_not_forwarded(test_server):
    client.patch("/api/notifications/webhook", json={"enabled": True, "url": f"{test_server}/hook"})
    res = client.post("/api/notifications", json={"kind": "resource_alert", "message": "cpu high"})
    assert res.status_code == 200
    assert res.json()["created"] is True
    assert received_requests == []


def test_forwarding_is_a_no_op_when_the_webhook_is_disabled(test_server):
    client.patch("/api/notifications/webhook", json={"enabled": False, "url": f"{test_server}/hook"})
    client.patch("/api/breakers/automation/http_request/threshold", json={"threshold": 1})

    res = client.post(
        "/api/modules/automation/http_request/run",
        json={"inputs": {"url": UNREACHABLE_URL, "timeout": 2}},
    )
    stream_id = res.json()["stream_id"]
    with client.stream("GET", f"/api/stream/{stream_id}") as response:
        for line in response.iter_lines():
            if not line.startswith("data: "):
                continue
            event = json.loads(line[len("data: "):])
            if event["kind"] in ("run_completed", "run_failed"):
                break

    client.delete("/api/breakers/automation/http_request/threshold")

    assert received_requests == []
