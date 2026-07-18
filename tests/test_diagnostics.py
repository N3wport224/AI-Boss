import shutil

import pytest
from fastapi.testclient import TestClient

from webapp import pipelines as pipeline_store
from webapp.main import app

client = TestClient(app)


@pytest.fixture(autouse=True)
def clean_pipelines_dir():
    if pipeline_store.PIPELINES_DIR.exists():
        shutil.rmtree(pipeline_store.PIPELINES_DIR)
    yield
    if pipeline_store.PIPELINES_DIR.exists():
        shutil.rmtree(pipeline_store.PIPELINES_DIR)


def test_health_check_reports_ready_for_the_bundled_modules():
    res = client.get("/api/health")
    assert res.status_code == 200

    body = res.json()
    assert body["status"] == "ready"
    names = {check["name"] for check in body["checks"]}
    assert names == {"state_store", "manifests", "entrypoints", "environment"}
    assert all(check["ok"] for check in body["checks"])


def test_performance_reports_a_live_process_snapshot():
    res = client.get("/api/performance")
    assert res.status_code == 200

    body = res.json()
    assert body["pid"] > 0
    assert body["memory_rss_mb"] > 0
    assert body["thread_count"] >= 1
    assert body["active_run_threads"] >= 0
    assert body["uptime_seconds"] >= 0


def test_metrics_reflect_a_completed_run():
    before = client.get("/api/metrics").json()

    res = client.post("/api/pipeline/run", json={"inputs": {}})
    stream_id = res.json()["stream_id"]
    with client.stream("GET", f"/api/stream/{stream_id}") as response:
        for line in response.iter_lines():
            if line.startswith("data: ") and '"run_completed"' in line:
                break

    after = client.get("/api/metrics").json()
    assert after["total_runs"] == before["total_runs"] + 1
    assert after["completed"] >= 1
    assert after["success_rate"] is not None
    assert after["avg_duration_seconds"] is not None


def test_runs_csv_export_has_a_header_and_rows():
    client.post("/api/pipeline/run", json={"inputs": {}})
    res = client.get("/api/runs.csv")
    assert res.status_code == 200
    assert res.headers["content-type"].startswith("text/csv")

    lines = res.text.strip().splitlines()
    assert lines[0] == "id,started_at,finished_at,status,duration_seconds"
    assert len(lines) >= 2


def test_duplicate_pipeline_creates_a_distinct_copy():
    client.post(
        "/api/pipelines",
        json={"name": "Original", "steps": [{"tier": "automation", "name": "fetch_raw_metrics"}]},
    )

    res = client.post("/api/pipelines/original/duplicate")
    assert res.status_code == 200
    duplicated = res.json()["pipeline"]
    assert duplicated["slug"] == "original_copy"
    assert duplicated["name"] == "Original (copy)"
    assert duplicated["steps"] == [
        {"tier": "automation", "name": "fetch_raw_metrics", "inputs": {}, "mappings": {}}
    ]

    slugs = {p["slug"] for p in client.get("/api/pipelines").json()}
    assert slugs == {"original", "original_copy"}


def test_duplicate_unknown_pipeline_404s():
    res = client.post("/api/pipelines/does_not_exist/duplicate")
    assert res.status_code == 404
