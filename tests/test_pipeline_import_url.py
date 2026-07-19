"""Batch 12: import a saved pipeline from a URL -- the same YAML import
POST /api/pipelines/import already does for an uploaded file, applied to a
pipeline definition fetched by URL instead, reusing Batch 10's
ingestion.fetch_url_bytes streaming GET.
"""
import shutil
import threading
from http.server import BaseHTTPRequestHandler, HTTPServer

import pytest
import yaml
from fastapi.testclient import TestClient

from webapp import pipelines as pipeline_store
from webapp.main import app

client = TestClient(app)

_VALID_PIPELINE_YAML = yaml.safe_dump(
    {
        "name": "Imported From URL",
        "description": "Fetched over HTTP.",
        "steps": [{"tier": "automation", "name": "fetch_raw_metrics", "inputs": {}, "mappings": {}}],
    }
).encode()

_BAD_MODULE_YAML = yaml.safe_dump(
    {"name": "Bad Import", "steps": [{"tier": "automation", "name": "does_not_exist"}]}
).encode()

_NOT_A_DICT_YAML = b"- just\n- a\n- list\n"
_MALFORMED_YAML = b"name: [unterminated"

_ROUTES = {
    "/pipeline.yaml": (200, "application/x-yaml", _VALID_PIPELINE_YAML),
    "/bad_module.yaml": (200, "application/x-yaml", _BAD_MODULE_YAML),
    "/not_a_dict.yaml": (200, "application/x-yaml", _NOT_A_DICT_YAML),
    "/malformed.yaml": (200, "application/x-yaml", _MALFORMED_YAML),
    "/missing.yaml": (404, "text/plain", b"not found"),
    "/huge.yaml": (200, "application/x-yaml", b"name: huge\nsteps: []\n# padding\n" + b"x" * 25_000_000),
}


class _Handler(BaseHTTPRequestHandler):
    def do_GET(self):
        route = _ROUTES.get(self.path)
        if route is None:
            self.send_response(404)
            self.end_headers()
            return
        status, content_type, body = route
        self.send_response(status)
        self.send_header("Content-Type", content_type)
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

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
def clean_pipelines_dir():
    if pipeline_store.PIPELINES_DIR.exists():
        shutil.rmtree(pipeline_store.PIPELINES_DIR)
    yield
    if pipeline_store.PIPELINES_DIR.exists():
        shutil.rmtree(pipeline_store.PIPELINES_DIR)


def test_import_pipeline_from_url_saves_it(test_server):
    res = client.post("/api/pipelines/import-url", json={"url": f"{test_server}/pipeline.yaml"})
    assert res.status_code == 200
    body = res.json()
    assert body["pipeline"]["slug"] == "imported_from_url"
    assert (pipeline_store.PIPELINES_DIR / "imported_from_url.yaml").exists()

    listed = client.get("/api/pipelines").json()
    assert any(p["slug"] == "imported_from_url" for p in listed)


def test_import_from_url_rejects_reference_to_unknown_module(test_server):
    res = client.post("/api/pipelines/import-url", json={"url": f"{test_server}/bad_module.yaml"})
    assert res.status_code == 400


def test_import_from_url_rejects_a_non_mapping_document(test_server):
    res = client.post("/api/pipelines/import-url", json={"url": f"{test_server}/not_a_dict.yaml"})
    assert res.status_code == 400


def test_import_from_url_rejects_malformed_yaml(test_server):
    res = client.post("/api/pipelines/import-url", json={"url": f"{test_server}/malformed.yaml"})
    assert res.status_code == 400


def test_import_from_url_surfaces_a_404_upstream(test_server):
    res = client.post("/api/pipelines/import-url", json={"url": f"{test_server}/missing.yaml"})
    assert res.status_code == 400


def test_import_from_url_rejects_a_file_over_the_size_limit(test_server):
    res = client.post("/api/pipelines/import-url", json={"url": f"{test_server}/huge.yaml"})
    assert res.status_code == 413


def test_import_from_url_rejects_a_blank_url():
    res = client.post("/api/pipelines/import-url", json={"url": "   "})
    assert res.status_code == 400
