"""Batch 10: ingest a CSV/JSON/XLSX file by fetching it from a URL, through
the exact same parse+dedupe+cleanse pipeline a direct upload already goes
through. A plain HTTP GET of a user-supplied URL -- not a SaaS integration,
same trust model as the existing http_request automation module.
"""
import io
import shutil
import threading
from http.server import BaseHTTPRequestHandler, HTTPServer

import pytest
from fastapi.testclient import TestClient

from webapp import ingestion
from webapp.main import app

client = TestClient(app)

_CSV_BODY = b"name,revenue\nAcmeFromUrl,1200\nBetaFromUrl,3400\n"
_JSON_BODY = b'[{"name": "AcmeFromUrl", "revenue": 1200}]'


def _xlsx_body():
    from openpyxl import Workbook

    workbook = Workbook()
    sheet = workbook.active
    sheet.append(["name", "revenue"])
    sheet.append(["Acme", 1200])
    buffer = io.BytesIO()
    workbook.save(buffer)
    return buffer.getvalue()


_ROUTES = {
    "/data.csv": (200, "text/csv", _CSV_BODY),
    "/data.json": (200, "application/json", _JSON_BODY),
    "/data.xlsx": (200, "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet", None),  # filled in below
    "/missing.csv": (404, "text/plain", b"not found"),
    "/no-extension": (200, "text/csv", _CSV_BODY),
    "/dedupe-test.csv": (200, "text/csv", b"name,revenue\nUnique,555\n"),
    "/huge.csv": (200, "text/csv", b"name,value\n" + b"x,1\n" * 2_000_000),
}
_ROUTES["/data.xlsx"] = (200, _ROUTES["/data.xlsx"][1], _xlsx_body())


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
def clean_artifacts_dir():
    if ingestion.ARTIFACTS_DIR.exists():
        shutil.rmtree(ingestion.ARTIFACTS_DIR)
    yield
    if ingestion.ARTIFACTS_DIR.exists():
        shutil.rmtree(ingestion.ARTIFACTS_DIR)


def test_filename_from_url_extracts_the_last_path_segment():
    assert ingestion.filename_from_url("http://example.com/path/to/report.csv") == "report.csv"
    assert ingestion.filename_from_url("http://example.com/a%20b.csv") == "a b.csv"
    assert ingestion.filename_from_url("http://example.com/") == "downloaded_file"
    assert ingestion.filename_from_url("http://example.com") == "downloaded_file"


def test_ingest_csv_from_url(test_server):
    res = client.post("/api/ingest/url", json={"url": f"{test_server}/data.csv"})
    assert res.status_code == 200
    body = res.json()
    assert body["duplicate"] is False
    assert body["row_count"] == 2
    assert body["columns"] == ["name", "revenue"]

    saved = ingestion.list_artifacts()
    assert any(f["name"].endswith("data.csv") for f in saved)


def test_ingest_json_from_url(test_server):
    res = client.post("/api/ingest/url", json={"url": f"{test_server}/data.json"})
    assert res.status_code == 200
    body = res.json()
    assert body["row_count"] == 1
    assert body["preview"] == [{"name": "AcmeFromUrl", "revenue": 1200}]


def test_ingest_xlsx_from_url(test_server):
    res = client.post("/api/ingest/url", json={"url": f"{test_server}/data.xlsx"})
    assert res.status_code == 200
    body = res.json()
    assert body["row_count"] == 1
    assert body["columns"] == ["name", "revenue"]


def test_ingest_from_url_dedupes_by_content_hash(test_server):
    first = client.post("/api/ingest/url", json={"url": f"{test_server}/dedupe-test.csv"})
    assert first.json()["duplicate"] is False

    second = client.post("/api/ingest/url", json={"url": f"{test_server}/dedupe-test.csv"})
    assert second.json()["duplicate"] is True


def test_ingest_from_url_rejects_a_url_with_no_recognized_extension(test_server):
    res = client.post("/api/ingest/url", json={"url": f"{test_server}/no-extension"})
    assert res.status_code == 400
    assert "must end in" in res.json()["detail"]


def test_ingest_from_url_surfaces_a_fetch_error(test_server):
    res = client.post("/api/ingest/url", json={"url": f"{test_server}/missing.csv"})
    assert res.status_code == 400
    assert "Could not fetch URL" in res.json()["detail"]


def test_ingest_from_url_rejects_an_unreachable_host():
    res = client.post("/api/ingest/url", json={"url": "http://127.0.0.1:9/nope.csv"})
    assert res.status_code == 400


def test_ingest_from_url_rejects_a_blank_url():
    res = client.post("/api/ingest/url", json={"url": "   "})
    assert res.status_code == 400
    assert "url is required" in res.json()["detail"]


def test_ingest_from_url_enforces_the_size_cap(test_server, monkeypatch):
    monkeypatch.setattr(ingestion, "MAX_UPLOAD_BYTES", 1024)
    res = client.post("/api/ingest/url", json={"url": f"{test_server}/huge.csv"})
    assert res.status_code == 413
