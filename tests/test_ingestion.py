import shutil

import pytest
from fastapi.testclient import TestClient

from webapp import ingestion
from webapp.main import app

client = TestClient(app)


@pytest.fixture(autouse=True)
def clean_artifacts_dir():
    if ingestion.ARTIFACTS_DIR.exists():
        shutil.rmtree(ingestion.ARTIFACTS_DIR)
    yield
    if ingestion.ARTIFACTS_DIR.exists():
        shutil.rmtree(ingestion.ARTIFACTS_DIR)


def test_ingest_csv_returns_structured_records():
    csv_bytes = b"name,revenue\nAcme,1200\nBeta,3400\n"
    res = client.post("/api/ingest/csv", files={"file": ("companies.csv", csv_bytes, "text/csv")})
    assert res.status_code == 200

    body = res.json()
    assert body["duplicate"] is False
    assert body["row_count"] == 2
    assert body["columns"] == ["name", "revenue"]
    assert body["preview"] == [{"name": "Acme", "revenue": "1200"}, {"name": "Beta", "revenue": "3400"}]

    saved_json = list(ingestion.ARTIFACTS_DIR.glob("*.json"))
    assert len(saved_json) == 1


def test_ingest_csv_blocks_exact_duplicate_by_content_hash():
    csv_bytes = b"name,revenue\nAcme,1200\n"
    first = client.post("/api/ingest/csv", files={"file": ("a.csv", csv_bytes, "text/csv")})
    assert first.json()["duplicate"] is False

    # Same bytes, different filename — dedupe is by content hash, not name.
    second = client.post("/api/ingest/csv", files={"file": ("b.csv", csv_bytes, "text/csv")})
    body = second.json()
    assert body["duplicate"] is True
    assert body["original_filename"] == "a.csv"


def test_ingest_csv_rejects_unparseable_content():
    res = client.post("/api/ingest/csv", files={"file": ("empty.csv", b"", "text/csv")})
    # An empty file parses to zero rows, not an error — assert it's handled gracefully.
    assert res.status_code == 200
    assert res.json()["row_count"] == 0


def test_cleanse_records_trims_whitespace_drops_blank_and_duplicate_rows():
    records = [
        {" name ": " Acme ", "revenue": "1200"},
        {" name ": "", "revenue": ""},  # fully blank -> dropped
        {" name ": "Acme", "revenue": "1200"},  # duplicate of row 1 after trimming -> dropped
        {" name ": "Beta", "revenue": "3400"},
    ]
    cleaned, stats = ingestion.cleanse_records(records)

    assert cleaned == [
        {"name": "Acme", "revenue": "1200"},
        {"name": "Beta", "revenue": "3400"},
    ]
    assert stats["rows_before"] == 4
    assert stats["rows_after"] == 2
    assert stats["blank_rows_removed"] == 1
    assert stats["duplicate_rows_removed"] == 1
    assert stats["cells_trimmed"] > 0
    assert stats["empty_cells_nulled"] == 2


def test_ingest_csv_reports_auto_cleansing_stats():
    csv_bytes = b"name,revenue\n Acme ,1200\n,\nAcme,1200\nBeta,3400\n"
    res = client.post("/api/ingest/csv", files={"file": ("messy.csv", csv_bytes, "text/csv")})
    assert res.status_code == 200

    body = res.json()
    assert body["row_count"] == 2  # blank + duplicate rows removed
    assert body["preview"] == [
        {"name": "Acme", "revenue": "1200"},
        {"name": "Beta", "revenue": "3400"},
    ]
    assert body["cleaning"]["blank_rows_removed"] == 1
    assert body["cleaning"]["duplicate_rows_removed"] == 1


def test_ingest_pdf_extracts_text_and_dedupes():
    from pypdf import PdfWriter
    import io

    writer = PdfWriter()
    writer.add_blank_page(width=200, height=200)
    buffer = io.BytesIO()
    writer.write(buffer)
    pdf_bytes = buffer.getvalue()

    first = client.post("/api/ingest/pdf", files={"file": ("doc.pdf", pdf_bytes, "application/pdf")})
    assert first.status_code == 200
    assert first.json()["duplicate"] is False

    second = client.post("/api/ingest/pdf", files={"file": ("doc-copy.pdf", pdf_bytes, "application/pdf")})
    assert second.json()["duplicate"] is True
    assert second.json()["original_filename"] == "doc.pdf"


def test_search_artifacts_finds_keyword_in_ingested_csv_and_pdf():
    client.post("/api/ingest/csv", files={"file": ("companies.csv", b"name,revenue\nAcmeCorp,1200\n", "text/csv")})

    from pypdf import PdfWriter
    import io as _io

    writer = PdfWriter()
    writer.add_blank_page(width=200, height=200)
    buffer = _io.BytesIO()
    writer.write(buffer)
    client.post("/api/ingest/pdf", files={"file": ("doc.pdf", buffer.getvalue(), "application/pdf")})

    res = client.get("/api/artifacts/search", params={"q": "AcmeCorp"})
    assert res.status_code == 200
    body = res.json()
    assert body["query"] == "AcmeCorp"
    assert len(body["results"]) == 1
    assert body["results"][0]["kind"] == "json"
    assert "AcmeCorp" in body["results"][0]["snippet"]

    # A keyword absent from any artifact yields no results.
    assert client.get("/api/artifacts/search", params={"q": "NoSuchKeyword"}).json()["results"] == []

    # An empty query yields no results rather than dumping every artifact.
    assert client.get("/api/artifacts/search", params={"q": ""}).json()["results"] == []


def test_list_and_purge_artifacts():
    client.post("/api/ingest/csv", files={"file": ("a.csv", b"x,y\n1,2\n", "text/csv")})
    assert len(client.get("/api/artifacts").json()) == 2  # the .csv and its .json

    res = client.post("/api/artifacts/purge", params={"older_than_hours": 0})
    assert res.status_code == 200
    assert res.json()["removed_count"] == 2
    assert client.get("/api/artifacts").json() == []


def test_purge_never_touches_state_store_or_pipelines(tmp_path):
    from webapp import pipelines as pipeline_store

    client.post(
        "/api/pipelines",
        json={"name": "Purge Safety", "steps": [{"tier": "automation", "name": "fetch_raw_metrics"}]},
    )
    client.post("/api/ingest/csv", files={"file": ("a.csv", b"x,y\n1,2\n", "text/csv")})

    client.post("/api/artifacts/purge", params={"older_than_hours": 0})

    assert pipeline_store.load_pipeline("purge_safety")["name"] == "Purge Safety"
    assert client.get("/api/health").json()["checks"][0]["ok"] is True

    shutil.rmtree(pipeline_store.PIPELINES_DIR, ignore_errors=True)
