import io
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


def test_ingest_csv_rejects_a_file_over_the_size_limit(monkeypatch):
    monkeypatch.setattr(ingestion, "MAX_UPLOAD_BYTES", 10)
    csv_bytes = b"name,revenue\nAcme,1200\nBeta,3400\n"  # well over 10 bytes
    res = client.post("/api/ingest/csv", files={"file": ("big.csv", csv_bytes, "text/csv")})
    assert res.status_code == 413
    assert "exceeding" in res.json()["detail"]
    # Rejected before any artifact was ever written.
    assert ingestion.list_artifacts() == []


def test_ingest_json_array_returns_structured_records():
    json_bytes = b'[{"name": "Acme", "revenue": 1200}, {"name": "Beta", "revenue": 3400}]'
    res = client.post("/api/ingest/json", files={"file": ("companies.json", json_bytes, "application/json")})
    assert res.status_code == 200

    body = res.json()
    assert body["duplicate"] is False
    assert body["row_count"] == 2
    assert body["columns"] == ["name", "revenue"]
    assert body["preview"] == [{"name": "Acme", "revenue": 1200}, {"name": "Beta", "revenue": 3400}]

    # Unlike CSV, no separate derived artifact — the raw upload is the only file.
    saved = ingestion.list_artifacts()
    assert len(saved) == 1
    assert saved[0]["name"].endswith("companies.json")


def test_ingest_json_wraps_a_single_object_as_one_row():
    json_bytes = b'{"name": "Acme", "revenue": 1200}'
    res = client.post("/api/ingest/json", files={"file": ("one.json", json_bytes, "application/json")})
    assert res.status_code == 200
    body = res.json()
    assert body["row_count"] == 1
    assert body["preview"] == [{"name": "Acme", "revenue": 1200}]


def test_ingest_json_blocks_exact_duplicate_by_content_hash():
    json_bytes = b'[{"name": "Acme"}]'
    first = client.post("/api/ingest/json", files={"file": ("a.json", json_bytes, "application/json")})
    assert first.json()["duplicate"] is False

    second = client.post("/api/ingest/json", files={"file": ("b.json", json_bytes, "application/json")})
    body = second.json()
    assert body["duplicate"] is True
    assert body["original_filename"] == "a.json"


def test_ingest_json_rejects_malformed_content():
    res = client.post("/api/ingest/json", files={"file": ("bad.json", b"{not valid json", "application/json")})
    assert res.status_code == 400
    assert "Could not parse JSON" in res.json()["detail"]


def _build_xlsx_bytes(rows):
    import io as _io

    from openpyxl import Workbook

    workbook = Workbook()
    sheet = workbook.active
    for row in rows:
        sheet.append(row)
    buffer = _io.BytesIO()
    workbook.save(buffer)
    return buffer.getvalue()


def test_ingest_xlsx_returns_structured_records():
    xlsx_bytes = _build_xlsx_bytes([["name", "revenue"], ["Acme", 1200], ["Beta", 3400]])
    res = client.post(
        "/api/ingest/xlsx",
        files={"file": ("companies.xlsx", xlsx_bytes, "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet")},
    )
    assert res.status_code == 200

    body = res.json()
    assert body["duplicate"] is False
    assert body["row_count"] == 2
    assert body["columns"] == ["name", "revenue"]
    assert body["preview"] == [{"name": "Acme", "revenue": 1200}, {"name": "Beta", "revenue": 3400}]

    # Same dual-artifact shape as CSV: the raw upload plus its JSON conversion.
    saved_json = list(ingestion.ARTIFACTS_DIR.glob("*.json"))
    assert len(saved_json) == 1
    saved_xlsx = list(ingestion.ARTIFACTS_DIR.glob("*.xlsx"))
    assert len(saved_xlsx) == 1


def test_ingest_xlsx_skips_blank_rows():
    xlsx_bytes = _build_xlsx_bytes([["name", "revenue"], ["Acme", 1200], [None, None], ["Beta", 3400]])
    res = client.post("/api/ingest/xlsx", files={"file": ("blanks.xlsx", xlsx_bytes, "application/octet-stream")})
    assert res.status_code == 200
    assert res.json()["row_count"] == 2


def test_ingest_xlsx_blocks_exact_duplicate_by_content_hash():
    xlsx_bytes = _build_xlsx_bytes([["name"], ["Acme"]])
    first = client.post("/api/ingest/xlsx", files={"file": ("a.xlsx", xlsx_bytes, "application/octet-stream")})
    assert first.json()["duplicate"] is False

    second = client.post("/api/ingest/xlsx", files={"file": ("b.xlsx", xlsx_bytes, "application/octet-stream")})
    body = second.json()
    assert body["duplicate"] is True
    assert body["original_filename"] == "a.xlsx"


def test_ingest_xlsx_rejects_unparseable_content():
    res = client.post("/api/ingest/xlsx", files={"file": ("bad.xlsx", b"not a real xlsx file", "application/octet-stream")})
    assert res.status_code == 400
    assert "Could not parse XLSX" in res.json()["detail"]


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


def test_artifacts_list_untagged_by_default():
    client.post("/api/ingest/csv", files={"file": ("untagged.csv", b"x,y\n101,201\n", "text/csv")})
    files = client.get("/api/artifacts").json()
    assert all(f["tags"] == [] for f in files)


def test_set_and_replace_artifact_tags():
    client.post("/api/ingest/csv", files={"file": ("tagme.csv", b"x,y\n111,211\n", "text/csv")})
    csv_filename = next(f["name"] for f in client.get("/api/artifacts").json() if f["name"].endswith(".csv"))

    res = client.put(f"/api/artifacts/{csv_filename}/tags", json={"tags": ["finance", "Q3", "finance"]})
    assert res.status_code == 200
    assert res.json()["tags"] == ["Q3", "finance"]  # deduped + sorted

    files = client.get("/api/artifacts").json()
    tagged = next(f for f in files if f["name"] == csv_filename)
    assert tagged["tags"] == ["Q3", "finance"]

    # a second PUT replaces the set rather than appending
    res2 = client.put(f"/api/artifacts/{csv_filename}/tags", json={"tags": ["archived"]})
    assert res2.json()["tags"] == ["archived"]


def test_artifact_tags_reject_unknown_filename():
    res = client.put("/api/artifacts/does-not-exist.csv/tags", json={"tags": ["x"]})
    assert res.status_code == 404


def test_artifact_tags_blank_entries_are_dropped():
    client.post("/api/ingest/csv", files={"file": ("blanktags.csv", b"x,y\n121,221\n", "text/csv")})
    csv_filename = next(f["name"] for f in client.get("/api/artifacts").json() if f["name"].endswith(".csv"))

    res = client.put(f"/api/artifacts/{csv_filename}/tags", json={"tags": ["  ", "kept", ""]})
    assert res.json()["tags"] == ["kept"]


def test_filter_artifacts_by_tag():
    client.post("/api/ingest/csv", files={"file": ("filterme_a.csv", b"x,y\n131,231\n", "text/csv")})
    client.post("/api/ingest/csv", files={"file": ("filterme_b.csv", b"x,y\n141,241\n", "text/csv")})
    files = client.get("/api/artifacts").json()
    csv_files = [f["name"] for f in files if f["name"].endswith(".csv")]
    assert len(csv_files) == 2

    client.put(f"/api/artifacts/{csv_files[0]}/tags", json={"tags": ["important"]})

    filtered = client.get("/api/artifacts", params={"tag": "important"}).json()
    assert [f["name"] for f in filtered] == [csv_files[0]]

    filtered_case_insensitive = client.get("/api/artifacts", params={"tag": "IMPORTANT"}).json()
    assert [f["name"] for f in filtered_case_insensitive] == [csv_files[0]]

    assert client.get("/api/artifacts", params={"tag": "no-such-tag"}).json() == []


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


# ---- Batch 12: artifact content viewer ----

def test_view_content_of_a_csv_sidecar_json_renders_as_a_table():
    client.post("/api/ingest/csv", files={"file": ("view_me.csv", b"name,revenue\nAcme,100\nBeta,200\n", "text/csv")})
    sidecar = next(f["name"] for f in client.get("/api/artifacts").json() if f["name"].endswith(".json"))

    res = client.get(f"/api/artifacts/{sidecar}/content")
    assert res.status_code == 200
    body = res.json()
    assert body["kind"] == "table"
    assert body["truncated"] is False
    assert body["content"] == [{"name": "Acme", "revenue": "100"}, {"name": "Beta", "revenue": "200"}]


def test_view_content_of_the_raw_csv_original_also_renders_as_a_table():
    client.post("/api/ingest/csv", files={"file": ("view_raw.csv", b"a,b\n1,2\n", "text/csv")})
    raw = next(f["name"] for f in client.get("/api/artifacts").json() if f["name"].endswith(".csv"))

    res = client.get(f"/api/artifacts/{raw}/content")
    assert res.status_code == 200
    body = res.json()
    assert body["kind"] == "table"
    assert body["content"] == [{"a": "1", "b": "2"}]


def test_view_content_of_a_pdf_text_sidecar_renders_as_text():
    from pypdf import PdfWriter

    writer = PdfWriter()
    writer.add_blank_page(width=72, height=72)
    buf = io.BytesIO()
    writer.write(buf)
    client.post("/api/ingest/pdf", files={"file": ("view_pdf.pdf", buf.getvalue(), "application/pdf")})

    txt_name = next(f["name"] for f in client.get("/api/artifacts").json() if f["name"].endswith(".txt"))
    res = client.get(f"/api/artifacts/{txt_name}/content")
    assert res.status_code == 200
    body = res.json()
    assert body["kind"] == "text"
    assert body["truncated"] is False


def test_view_content_of_a_directly_uploaded_json_object_renders_as_json():
    client.post("/api/ingest/json", files={"file": ("view_obj.json", b'{"a": 1, "b": 2}', "application/json")})
    name = next(f["name"] for f in client.get("/api/artifacts").json() if f["name"].endswith(".json"))

    res = client.get(f"/api/artifacts/{name}/content")
    body = res.json()
    assert body["kind"] == "json"
    assert body["content"] == {"a": 1, "b": 2}


def test_view_content_of_a_binary_original_is_marked_unsupported():
    from openpyxl import Workbook

    workbook = Workbook()
    sheet = workbook.active
    sheet.append(["x", "y"])
    sheet.append([1, 2])
    buf = io.BytesIO()
    workbook.save(buf)
    client.post(
        "/api/ingest/xlsx",
        files={"file": ("view_bin.xlsx", buf.getvalue(), "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet")},
    )
    xlsx_name = next(f["name"] for f in client.get("/api/artifacts").json() if f["name"].endswith(".xlsx"))

    res = client.get(f"/api/artifacts/{xlsx_name}/content")
    body = res.json()
    assert body["kind"] == "unsupported"
    assert body["content"] is None
    assert "companion" in body["message"]


def test_view_content_of_unknown_artifact_404s():
    res = client.get("/api/artifacts/does_not_exist.json/content")
    assert res.status_code == 404


def test_view_content_rejects_path_traversal_in_filename():
    res = client.get("/api/artifacts/..%2F..%2Fetc%2Fpasswd/content")
    assert res.status_code == 404


# ---- Batch 12: bulk tag artifacts ----

def test_bulk_tag_adds_to_files_own_existing_tags():
    client.post("/api/ingest/csv", files={"file": ("bulk_a.csv", b"x,y\n951,952\n", "text/csv")})
    client.post("/api/ingest/csv", files={"file": ("bulk_b.csv", b"x,y\n953,954\n", "text/csv")})
    files = client.get("/api/artifacts").json()
    a_name = next(f["name"] for f in files if f["name"].endswith("bulk_a.csv"))
    b_name = next(f["name"] for f in files if f["name"].endswith("bulk_b.csv"))

    client.put(f"/api/artifacts/{a_name}/tags", json={"tags": ["preexisting"]})

    res = client.post("/api/artifacts/bulk-tags", json={"filenames": [a_name, b_name], "tag": "reviewed"})
    assert res.status_code == 200
    body = res.json()
    assert body["tag"] == "reviewed"
    assert set(body["tagged"]) == {a_name, b_name}

    updated = client.get("/api/artifacts").json()
    a_tags = next(f["tags"] for f in updated if f["name"] == a_name)
    b_tags = next(f["tags"] for f in updated if f["name"] == b_name)
    assert set(a_tags) == {"preexisting", "reviewed"}
    assert set(b_tags) == {"reviewed"}


def test_bulk_tag_skips_unknown_filenames_without_failing():
    client.post("/api/ingest/csv", files={"file": ("bulk_c.csv", b"x,y\n955,956\n", "text/csv")})
    c_name = next(f["name"] for f in client.get("/api/artifacts").json() if f["name"].endswith("bulk_c.csv"))

    res = client.post(
        "/api/artifacts/bulk-tags",
        json={"filenames": [c_name, "totally_made_up_file.csv"], "tag": "batched"},
    )
    assert res.status_code == 200
    assert res.json()["tagged"] == [c_name]


def test_bulk_tag_rejects_a_blank_tag():
    res = client.post("/api/artifacts/bulk-tags", json={"filenames": [], "tag": "   "})
    assert res.status_code == 400


# ---- Batch 13: column schema summary on CSV/XLSX ingest ----

def test_infer_schema_reports_column_names_types_and_row_count():
    records = [
        {"id": "1", "name": "Acme", "active": "true", "revenue": "100"},
        {"id": "2", "name": "Beta", "active": "false", "revenue": "200.5"},
    ]
    schema = ingestion.infer_schema(records)
    assert schema["row_count"] == 2
    by_name = {c["name"]: c["type"] for c in schema["columns"]}
    assert by_name == {"id": "int", "name": "text", "active": "bool", "revenue": "float"}


def test_infer_schema_detects_date_like_columns():
    records = [{"joined": "2024-01-01"}, {"joined": "2024-06-15"}]
    schema = ingestion.infer_schema(records)
    assert schema["columns"] == [{"name": "joined", "type": "date"}]


def test_infer_schema_reports_mixed_when_no_type_has_a_clear_majority():
    records = [{"col": "1"}, {"col": "not_a_number"}]
    schema = ingestion.infer_schema(records)
    assert schema["columns"] == [{"name": "col", "type": "mixed"}]


def test_infer_schema_ignores_null_cells_when_choosing_a_type():
    records = [{"col": "1"}, {"col": None}, {"col": ""}]
    schema = ingestion.infer_schema(records)
    assert schema["columns"] == [{"name": "col", "type": "int"}]


def test_infer_schema_of_empty_records_has_no_columns():
    assert ingestion.infer_schema([]) == {"columns": [], "row_count": 0}


def test_csv_artifact_content_includes_a_schema_summary():
    client.post(
        "/api/ingest/csv",
        files={"file": ("schema_view.csv", b"id,revenue\n1,100\n2,200\n", "text/csv")},
    )
    sidecar = next(f["name"] for f in client.get("/api/artifacts").json() if f["name"] == "schema_view.csv" or f["name"].endswith("_schema_view.csv"))
    res = client.get(f"/api/artifacts/{sidecar}/content")
    body = res.json()
    assert body["schema"]["row_count"] == 2
    by_name = {c["name"]: c["type"] for c in body["schema"]["columns"]}
    assert by_name == {"id": "int", "revenue": "int"}


def test_json_sidecar_artifact_content_includes_a_schema_summary():
    client.post(
        "/api/ingest/csv",
        files={"file": ("schema_sidecar.csv", b"a,b\n951000,952000\n", "text/csv")},
    )
    json_sidecar = next(
        f["name"] for f in client.get("/api/artifacts").json() if f["name"].endswith(".json") and "schema_sidecar" in f["name"]
    )
    res = client.get(f"/api/artifacts/{json_sidecar}/content")
    body = res.json()
    assert body["kind"] == "table"
    assert body["schema"]["row_count"] == 1
    by_name = {c["name"]: c["type"] for c in body["schema"]["columns"]}
    assert by_name == {"a": "int", "b": "int"}


# ---- Batch 14: artifact tag directory with counts ----

def test_tags_summary_counts_tagged_artifacts():
    client.post("/api/ingest/csv", files={"file": ("tagdir_a.csv", b"x,y\n961,962\n", "text/csv")})
    client.post("/api/ingest/csv", files={"file": ("tagdir_b.csv", b"x,y\n963,964\n", "text/csv")})
    a_name = next(f["name"] for f in client.get("/api/artifacts").json() if f["name"].endswith("tagdir_a.csv"))
    b_name = next(f["name"] for f in client.get("/api/artifacts").json() if f["name"].endswith("tagdir_b.csv"))

    client.put(f"/api/artifacts/{a_name}/tags", json={"tags": ["reviewed", "q1"]})
    client.put(f"/api/artifacts/{b_name}/tags", json={"tags": ["reviewed"]})

    summary = client.get("/api/artifacts/tags-summary").json()
    by_tag = {entry["tag"]: entry["count"] for entry in summary}
    assert by_tag["reviewed"] == 2
    assert by_tag["q1"] == 1


def test_tags_summary_is_sorted_by_count_descending():
    client.post("/api/ingest/csv", files={"file": ("tagdir_c.csv", b"x,y\n965,966\n", "text/csv")})
    client.post("/api/ingest/csv", files={"file": ("tagdir_d.csv", b"x,y\n967,968\n", "text/csv")})
    c_name = next(f["name"] for f in client.get("/api/artifacts").json() if f["name"].endswith("tagdir_c.csv"))
    d_name = next(f["name"] for f in client.get("/api/artifacts").json() if f["name"].endswith("tagdir_d.csv"))

    client.put(f"/api/artifacts/{c_name}/tags", json={"tags": ["common", "rare"]})
    client.put(f"/api/artifacts/{d_name}/tags", json={"tags": ["common"]})

    summary = client.get("/api/artifacts/tags-summary").json()
    tags_in_order = [entry["tag"] for entry in summary]
    assert tags_in_order.index("common") < tags_in_order.index("rare")


def test_tags_summary_excludes_tags_on_purged_artifacts():
    client.post("/api/ingest/csv", files={"file": ("tagdir_e.csv", b"x,y\n969,970\n", "text/csv")})
    e_name = next(f["name"] for f in client.get("/api/artifacts").json() if f["name"].endswith("tagdir_e.csv"))
    client.put(f"/api/artifacts/{e_name}/tags", json={"tags": ["soon_purged"]})

    client.post("/api/artifacts/purge", params={"older_than_hours": 0})

    summary = client.get("/api/artifacts/tags-summary").json()
    assert not any(entry["tag"] == "soon_purged" for entry in summary)


def test_tags_summary_is_empty_when_nothing_is_tagged():
    # No assumption that the artifacts dir is empty (other tests may have
    # left untagged files) -- just confirm the shape and that an untagged
    # fresh upload contributes nothing.
    client.post("/api/ingest/csv", files={"file": ("tagdir_untagged.csv", b"x,y\n971,972\n", "text/csv")})
    summary = client.get("/api/artifacts/tags-summary").json()
    assert isinstance(summary, list)
    assert not any(entry["count"] == 0 for entry in summary)


# ---- Batch 15: bulk remove a tag from selected artifacts ----

def test_bulk_untag_removes_the_tag_but_keeps_other_tags():
    client.post("/api/ingest/csv", files={"file": ("untag_a.csv", b"x,y\n991,992\n", "text/csv")})
    client.post("/api/ingest/csv", files={"file": ("untag_b.csv", b"x,y\n993,994\n", "text/csv")})
    files = client.get("/api/artifacts").json()
    a_name = next(f["name"] for f in files if f["name"].endswith("untag_a.csv"))
    b_name = next(f["name"] for f in files if f["name"].endswith("untag_b.csv"))

    client.put(f"/api/artifacts/{a_name}/tags", json={"tags": ["keep_me", "remove_me"]})
    client.put(f"/api/artifacts/{b_name}/tags", json={"tags": ["remove_me"]})

    res = client.post("/api/artifacts/bulk-untag", json={"filenames": [a_name, b_name], "tag": "remove_me"})
    assert res.status_code == 200
    body = res.json()
    assert body["tag"] == "remove_me"
    assert set(body["untagged"]) == {a_name, b_name}

    updated = client.get("/api/artifacts").json()
    a_tags = next(f["tags"] for f in updated if f["name"] == a_name)
    b_tags = next(f["tags"] for f in updated if f["name"] == b_name)
    assert a_tags == ["keep_me"]
    assert b_tags == []


def test_bulk_untag_is_a_no_op_for_a_file_that_never_had_the_tag():
    client.post("/api/ingest/csv", files={"file": ("untag_c.csv", b"x,y\n995,996\n", "text/csv")})
    c_name = next(f["name"] for f in client.get("/api/artifacts").json() if f["name"].endswith("untag_c.csv"))
    client.put(f"/api/artifacts/{c_name}/tags", json={"tags": ["something_else"]})

    res = client.post("/api/artifacts/bulk-untag", json={"filenames": [c_name], "tag": "never_had_this"})
    assert res.status_code == 200
    assert res.json()["untagged"] == [c_name]

    updated = client.get("/api/artifacts").json()
    c_tags = next(f["tags"] for f in updated if f["name"] == c_name)
    assert c_tags == ["something_else"]


def test_bulk_untag_skips_unknown_filenames_without_failing():
    client.post("/api/ingest/csv", files={"file": ("untag_d.csv", b"x,y\n997,998\n", "text/csv")})
    d_name = next(f["name"] for f in client.get("/api/artifacts").json() if f["name"].endswith("untag_d.csv"))

    res = client.post(
        "/api/artifacts/bulk-untag",
        json={"filenames": [d_name, "totally_made_up_file.csv"], "tag": "whatever"},
    )
    assert res.status_code == 200
    assert res.json()["untagged"] == [d_name]


def test_bulk_untag_rejects_a_blank_tag():
    res = client.post("/api/artifacts/bulk-untag", json={"filenames": [], "tag": "   "})
    assert res.status_code == 400


# ---- Batch 16: rename a tag across all artifacts at once ----

def test_rename_tag_renames_across_every_affected_file():
    client.post("/api/ingest/csv", files={"file": ("rename_a.csv", b"x,y\n1011,1012\n", "text/csv")})
    client.post("/api/ingest/csv", files={"file": ("rename_b.csv", b"x,y\n1013,1014\n", "text/csv")})
    files = client.get("/api/artifacts").json()
    a_name = next(f["name"] for f in files if f["name"].endswith("rename_a.csv"))
    b_name = next(f["name"] for f in files if f["name"].endswith("rename_b.csv"))

    client.put(f"/api/artifacts/{a_name}/tags", json={"tags": ["reviewd", "keep_me"]})
    client.put(f"/api/artifacts/{b_name}/tags", json={"tags": ["reviewd"]})

    res = client.post("/api/artifacts/rename-tag", json={"old_tag": "reviewd", "new_tag": "reviewed"})
    assert res.status_code == 200
    body = res.json()
    assert body["old_tag"] == "reviewd"
    assert body["new_tag"] == "reviewed"
    assert set(body["renamed"]) == {a_name, b_name}

    updated = client.get("/api/artifacts").json()
    a_tags = next(f["tags"] for f in updated if f["name"] == a_name)
    b_tags = next(f["tags"] for f in updated if f["name"] == b_name)
    assert set(a_tags) == {"reviewed", "keep_me"}
    assert b_tags == ["reviewed"]


def test_rename_tag_merges_without_duplicating_if_new_tag_already_present():
    client.post("/api/ingest/csv", files={"file": ("rename_c.csv", b"x,y\n1015,1016\n", "text/csv")})
    c_name = next(f["name"] for f in client.get("/api/artifacts").json() if f["name"].endswith("rename_c.csv"))
    client.put(f"/api/artifacts/{c_name}/tags", json={"tags": ["old_name", "already_here"]})

    res = client.post("/api/artifacts/rename-tag", json={"old_tag": "old_name", "new_tag": "already_here"})
    assert res.status_code == 200

    updated = client.get("/api/artifacts").json()
    c_tags = next(f["tags"] for f in updated if f["name"] == c_name)
    assert c_tags == ["already_here"]


def test_rename_tag_is_a_no_op_when_nothing_has_the_tag():
    res = client.post("/api/artifacts/rename-tag", json={"old_tag": "zzz_never_used_zzz", "new_tag": "whatever"})
    assert res.status_code == 200
    assert res.json()["renamed"] == []


def test_rename_tag_rejects_blank_names():
    res = client.post("/api/artifacts/rename-tag", json={"old_tag": "  ", "new_tag": "whatever"})
    assert res.status_code == 400
    res2 = client.post("/api/artifacts/rename-tag", json={"old_tag": "whatever", "new_tag": "   "})
    assert res2.status_code == 400


def test_rename_tag_rejects_identical_old_and_new():
    res = client.post("/api/artifacts/rename-tag", json={"old_tag": "same", "new_tag": "same"})
    assert res.status_code == 400


# ---- Batch 16: compare two artifacts' schemas ----

def test_compare_schema_reports_matching_columns_when_schemas_agree():
    client.post("/api/ingest/csv", files={"file": ("cmp_a.csv", b"name,revenue\nRow2001,2002\nRow2003,2004\n", "text/csv")})
    client.post("/api/ingest/csv", files={"file": ("cmp_b.csv", b"name,revenue\nRow2005,2006\n", "text/csv")})
    files = client.get("/api/artifacts").json()
    a_name = next(f["name"] for f in files if f["name"].endswith("cmp_a.csv"))
    b_name = next(f["name"] for f in files if f["name"].endswith("cmp_b.csv"))

    res = client.get("/api/artifacts/compare-schema", params={"a": a_name, "b": b_name})
    assert res.status_code == 200
    body = res.json()
    assert body["a"]["filename"] == a_name
    assert body["b"]["filename"] == b_name
    assert body["only_in_a"] == []
    assert body["only_in_b"] == []
    assert set(body["matching"]) == {"name", "revenue"}
    assert body["type_mismatches"] == []


def test_compare_schema_reports_columns_unique_to_each_side():
    client.post("/api/ingest/csv", files={"file": ("cmp_c.csv", b"name,revenue\nRow2007,2008\n", "text/csv")})
    client.post("/api/ingest/csv", files={"file": ("cmp_d.csv", b"name,region\nRow2009,West2010\n", "text/csv")})
    files = client.get("/api/artifacts").json()
    c_name = next(f["name"] for f in files if f["name"].endswith("cmp_c.csv"))
    d_name = next(f["name"] for f in files if f["name"].endswith("cmp_d.csv"))

    res = client.get("/api/artifacts/compare-schema", params={"a": c_name, "b": d_name})
    body = res.json()
    assert body["only_in_a"] == ["revenue"]
    assert body["only_in_b"] == ["region"]
    assert body["matching"] == ["name"]


def test_compare_schema_flags_a_type_mismatch_on_a_shared_column():
    client.post("/api/ingest/csv", files={"file": ("cmp_e.csv", b"id,amount\n2011,2012\n2013,2014\n", "text/csv")})
    client.post("/api/ingest/csv", files={"file": ("cmp_f.csv", b"id,amount\nrowx2015,cash2016\nrowy2017,card2018\n", "text/csv")})
    files = client.get("/api/artifacts").json()
    e_name = next(f["name"] for f in files if f["name"].endswith("cmp_e.csv"))
    f_name = next(f["name"] for f in files if f["name"].endswith("cmp_f.csv"))

    res = client.get("/api/artifacts/compare-schema", params={"a": e_name, "b": f_name})
    body = res.json()
    mismatch_cols = {m["column"] for m in body["type_mismatches"]}
    assert "amount" in mismatch_cols
    assert "id" in mismatch_cols  # int vs text


def test_compare_schema_404s_on_unknown_filename():
    client.post("/api/ingest/csv", files={"file": ("cmp_g.csv", b"x,y\n2019,2020\n", "text/csv")})
    g_name = next(f["name"] for f in client.get("/api/artifacts").json() if f["name"].endswith("cmp_g.csv"))

    res = client.get("/api/artifacts/compare-schema", params={"a": g_name, "b": "does-not-exist.csv"})
    assert res.status_code == 404


def test_compare_schema_rejects_a_non_table_artifact():
    from pypdf import PdfWriter

    writer = PdfWriter()
    writer.add_blank_page(width=93, height=93)
    buf = io.BytesIO()
    writer.write(buf)
    client.post("/api/ingest/pdf", files={"file": ("cmp_doc.pdf", buf.getvalue(), "application/pdf")})
    client.post("/api/ingest/csv", files={"file": ("cmp_h.csv", b"x,y\n2021,2022\n", "text/csv")})
    files = client.get("/api/artifacts").json()
    pdf_name = next(f["name"] for f in files if f["name"].endswith(".pdf"))
    h_name = next(f["name"] for f in files if f["name"].endswith("cmp_h.csv"))

    res = client.get("/api/artifacts/compare-schema", params={"a": pdf_name, "b": h_name})
    assert res.status_code == 400


# ---- Batch 17: export artifacts list metadata to CSV ----

def test_artifacts_csv_export_contains_header_and_uploaded_files():
    client.post("/api/ingest/csv", files={"file": ("csvexport_a.csv", b"x,y\n4041,4042\n", "text/csv")})
    a_name = next(f["name"] for f in client.get("/api/artifacts").json() if f["name"].endswith("csvexport_a.csv"))
    client.put(f"/api/artifacts/{a_name}/tags", json={"tags": ["alpha", "beta"]})

    res = client.get("/api/artifacts.csv")
    assert res.status_code == 200
    assert res.headers["content-type"].startswith("text/csv")
    assert "attachment; filename=artifacts.csv" in res.headers["content-disposition"]

    lines = res.text.strip().splitlines()
    assert lines[0] == "filename,size_bytes,tags,modified_at"
    row = next(line for line in lines[1:] if line.startswith(a_name))
    assert "alpha;beta" in row


def test_artifacts_csv_export_is_empty_but_valid_with_no_artifacts():
    res = client.get("/api/artifacts.csv")
    assert res.status_code == 200
    lines = res.text.strip().splitlines()
    assert lines == ["filename,size_bytes,tags,modified_at"]


# ---- Batch 18: download an artifact's raw file ----

def test_download_artifact_returns_the_original_bytes():
    csv_bytes = b"name,revenue\nRowDL1,6061\n"
    client.post("/api/ingest/csv", files={"file": ("download_me.csv", csv_bytes, "text/csv")})
    name = next(f["name"] for f in client.get("/api/artifacts").json() if f["name"].endswith("download_me.csv"))

    res = client.get(f"/api/artifacts/{name}/download")
    assert res.status_code == 200
    assert res.content == csv_bytes
    assert name in res.headers["content-disposition"]


def test_download_unknown_artifact_404s():
    res = client.get("/api/artifacts/does-not-exist.csv/download")
    assert res.status_code == 404


def test_download_artifact_rejects_path_traversal_in_filename():
    res = client.get("/api/artifacts/..%2F..%2Fetc%2Fpasswd/download")
    assert res.status_code == 404


# ---- Batch 21: bulk delete selected artifacts ----

def test_bulk_delete_artifacts_removes_every_selected_file():
    client.post("/api/ingest/csv", files={"file": ("bulkdel_a.csv", b"x,y\n9101,9102\n", "text/csv")})
    client.post("/api/ingest/csv", files={"file": ("bulkdel_b.csv", b"x,y\n9103,9104\n", "text/csv")})
    client.post("/api/ingest/csv", files={"file": ("bulkdel_keep.csv", b"x,y\n9105,9106\n", "text/csv")})
    files = client.get("/api/artifacts").json()
    a_name = next(f["name"] for f in files if f["name"].endswith("bulkdel_a.csv"))
    b_name = next(f["name"] for f in files if f["name"].endswith("bulkdel_b.csv"))
    keep_name = next(f["name"] for f in files if f["name"].endswith("bulkdel_keep.csv"))

    res = client.post("/api/artifacts/bulk-delete", json={"filenames": [a_name, b_name]})
    assert res.status_code == 200
    assert set(res.json()["deleted"]) == {a_name, b_name}

    remaining = {f["name"] for f in client.get("/api/artifacts").json()}
    assert a_name not in remaining
    assert b_name not in remaining
    assert keep_name in remaining


def test_bulk_delete_artifacts_skips_unknown_filenames():
    client.post("/api/ingest/csv", files={"file": ("bulkdel_solo.csv", b"x,y\n9107,9108\n", "text/csv")})
    solo_name = next(f["name"] for f in client.get("/api/artifacts").json() if f["name"].endswith("bulkdel_solo.csv"))

    res = client.post("/api/artifacts/bulk-delete", json={"filenames": [solo_name, "does-not-exist.csv"]})
    assert res.status_code == 200
    assert res.json()["deleted"] == [solo_name]


def test_bulk_delete_artifacts_is_a_no_op_on_an_empty_selection():
    res = client.post("/api/artifacts/bulk-delete", json={"filenames": []})
    assert res.status_code == 200
    assert res.json()["deleted"] == []


def test_bulk_delete_artifacts_rejects_path_traversal_in_filenames():
    res = client.post("/api/artifacts/bulk-delete", json={"filenames": ["../../etc/passwd"]})
    assert res.status_code == 200
    assert res.json()["deleted"] == []
