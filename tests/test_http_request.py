import json
import threading
from http.server import BaseHTTPRequestHandler, HTTPServer

import pytest
from fastapi.testclient import TestClient

from automations.http_request import HttpRequestAutomation
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


class _EchoHandler(BaseHTTPRequestHandler):
    """Loopback test server: echoes method, headers, and body back as JSON."""

    def _handle(self):
        length = int(self.headers.get("Content-Length", 0))
        raw_body = self.rfile.read(length) if length else b""
        payload = {
            "method": self.command,
            "path": self.path,
            "received_header": self.headers.get("X-Test-Header"),
            "received_body": raw_body.decode("utf-8", errors="replace"),
        }
        if self.path == "/not-found":
            self.send_response(404)
        else:
            self.send_response(200)
        body = json.dumps(payload).encode()
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def do_GET(self):
        self._handle()

    def do_POST(self):
        self._handle()

    def log_message(self, *args):
        pass  # silence request logging during tests


@pytest.fixture(scope="module")
def echo_server():
    server = HTTPServer(("127.0.0.1", 0), _EchoHandler)
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    try:
        yield f"http://127.0.0.1:{server.server_port}"
    finally:
        server.shutdown()
        thread.join(timeout=5)


def test_http_request_get_returns_parsed_json_response(echo_server):
    module = HttpRequestAutomation()
    context = ExecutionContext({"url": f"{echo_server}/hello", "method": "GET"})

    result = module.run(context)

    response = result["http_response"]
    assert response["status_code"] == 200
    assert response["ok"] is True
    assert response["body"]["method"] == "GET"
    assert response["body"]["path"] == "/hello"
    assert response["elapsed_ms"] >= 0


def test_http_request_sends_custom_headers_and_json_body(echo_server):
    module = HttpRequestAutomation()
    context = ExecutionContext(
        {
            "url": echo_server,
            "method": "POST",
            "headers": '{"X-Test-Header": "batch6"}',
            "body": '{"greeting": "hi"}',
        }
    )

    result = module.run(context)

    response = result["http_response"]
    assert response["body"]["received_header"] == "batch6"
    assert json.loads(response["body"]["received_body"]) == {"greeting": "hi"}


def test_http_request_sends_raw_text_body_when_not_json(echo_server):
    module = HttpRequestAutomation()
    context = ExecutionContext({"url": echo_server, "method": "POST", "body": "plain text"})

    result = module.run(context)

    assert result["http_response"]["body"]["received_body"] == "plain text"


def test_http_request_reports_non_2xx_status_without_raising(echo_server):
    module = HttpRequestAutomation()
    context = ExecutionContext({"url": f"{echo_server}/not-found"})

    result = module.run(context)

    response = result["http_response"]
    assert response["status_code"] == 404
    assert response["ok"] is False


def test_http_request_requires_a_url():
    module = HttpRequestAutomation()
    context = ExecutionContext({"url": ""})

    with pytest.raises(ValueError):
        module.run(context)


def test_http_request_rejects_malformed_header_json(echo_server):
    module = HttpRequestAutomation()
    context = ExecutionContext({"url": echo_server, "headers": "{not valid json"})

    with pytest.raises(ValueError):
        module.run(context)


def test_http_request_is_listed_among_automation_modules():
    res = client.get("/api/modules")
    assert res.status_code == 200
    automation_names = {m["name"] for m in res.json()["automation"]}
    assert "http_request" in automation_names


def test_http_request_runs_standalone_through_the_module_run_endpoint(echo_server):
    res = client.post(
        "/api/modules/automation/http_request/run",
        json={"inputs": {"url": f"{echo_server}/standalone", "method": "GET"}},
    )
    assert res.status_code == 200

    events = _collect_stream(res.json()["stream_id"])
    assert events[-1]["kind"] == "run_completed"
    assert events[-1]["context"]["http_response"]["status_code"] == 200
    assert events[-1]["context"]["http_response"]["body"]["path"] == "/standalone"


def test_http_request_is_excluded_from_the_fixed_full_pipeline_demo():
    """It makes a real outbound call, so it opts out of the offline one-click
    demo pipeline via `include_in_full_pipeline: false` — it's still runnable
    standalone (see above) and includable in any hand-built pipeline."""
    res = client.post("/api/pipeline/run", json={"inputs": {}})
    events = _collect_stream(res.json()["stream_id"])
    assert "http_response" not in events[-1]["context"]
