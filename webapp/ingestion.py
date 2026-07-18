"""Shared file-ingestion infrastructure.

Uploads land in artifacts/ (gitignored — this is runtime data, not project
code, same as orchestrator.db), get SHA-256 hashed for dedupe tracking in the
state store, and are converted into structured data other modules can consume.
"""
import csv
import hashlib
import io
import json
import time
from pathlib import Path

from pypdf import PdfReader

ARTIFACTS_DIR = Path(__file__).resolve().parent.parent / "artifacts"


def ensure_artifacts_dir() -> Path:
    ARTIFACTS_DIR.mkdir(exist_ok=True)
    return ARTIFACTS_DIR


def hash_bytes(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


def save_artifact(filename: str, data: bytes, suffix: str = "") -> Path:
    """Save raw uploaded bytes under a timestamp-prefixed name so repeated
    uploads of same-named files never collide."""
    ensure_artifacts_dir()
    safe_name = f"{int(time.time() * 1000)}_{Path(filename).name}{suffix}"
    path = ARTIFACTS_DIR / safe_name
    path.write_bytes(data)
    return path


def csv_bytes_to_records(data: bytes) -> list[dict]:
    text = data.decode("utf-8-sig")
    reader = csv.DictReader(io.StringIO(text))
    return [dict(row) for row in reader]


def cleanse_records(records: list[dict]) -> tuple[list[dict], dict]:
    """Best-effort cleanup applied to every ingested CSV: trims whitespace from
    keys/values, turns empty-string cells into None, drops fully-blank rows,
    and drops exact-duplicate rows. Deliberately does not guess at column
    types or rewrite real values beyond trimming — no schema inference."""
    stats = {
        "rows_before": len(records),
        "blank_rows_removed": 0,
        "duplicate_rows_removed": 0,
        "cells_trimmed": 0,
        "empty_cells_nulled": 0,
    }
    cleaned = []
    seen = set()

    for row in records:
        new_row = {}
        all_blank = True
        for key, value in row.items():
            clean_key = key.strip() if isinstance(key, str) else key
            if isinstance(value, str):
                trimmed = value.strip()
                if trimmed != value:
                    stats["cells_trimmed"] += 1
                if trimmed == "":
                    new_row[clean_key] = None
                    stats["empty_cells_nulled"] += 1
                else:
                    new_row[clean_key] = trimmed
                    all_blank = False
            else:
                new_row[clean_key] = value
                if value is not None:
                    all_blank = False

        if all_blank:
            stats["blank_rows_removed"] += 1
            continue

        dedupe_key = json.dumps(new_row, sort_keys=True, default=str)
        if dedupe_key in seen:
            stats["duplicate_rows_removed"] += 1
            continue
        seen.add(dedupe_key)
        cleaned.append(new_row)

    stats["rows_after"] = len(cleaned)
    return cleaned, stats


def pdf_bytes_to_text(data: bytes) -> str:
    reader = PdfReader(io.BytesIO(data))
    return "\n\n".join((page.extract_text() or "") for page in reader.pages)


def ingest_csv_bytes(filename: str, data: bytes, store) -> dict:
    """Shared by the upload endpoint and the filesystem watcher: hash-dedupe,
    parse, save the artifact + its JSON conversion, record the hash."""
    file_hash = hash_bytes(data)
    existing = store.find_ingested_file(file_hash)
    if existing:
        return {"duplicate": True, "original_filename": existing["filename"], "ingested_at": existing["ingested_at"]}

    records = csv_bytes_to_records(data)
    records, cleaning_stats = cleanse_records(records)
    saved_path = save_artifact(filename, data)
    saved_path.with_suffix(".json").write_text(json.dumps(records, indent=2))
    store.record_ingested_file(file_hash, filename, "csv")

    return {
        "duplicate": False,
        "filename": filename,
        "row_count": len(records),
        "columns": list(records[0].keys()) if records else [],
        "preview": records[:50],
        "truncated": len(records) > 50,
        "cleaning": cleaning_stats,
    }


def ingest_pdf_bytes(filename: str, data: bytes, store) -> dict:
    file_hash = hash_bytes(data)
    existing = store.find_ingested_file(file_hash)
    if existing:
        return {"duplicate": True, "original_filename": existing["filename"], "ingested_at": existing["ingested_at"]}

    text = pdf_bytes_to_text(data)
    saved_path = save_artifact(filename, data)
    saved_path.with_suffix(".txt").write_text(text)
    store.record_ingested_file(file_hash, filename, "pdf")

    return {
        "duplicate": False,
        "filename": filename,
        "char_count": len(text),
        "preview": text[:2000],
        "truncated": len(text) > 2000,
    }


def list_artifacts() -> list[dict]:
    ensure_artifacts_dir()
    files = []
    for path in sorted(ARTIFACTS_DIR.iterdir(), key=lambda p: p.stat().st_mtime, reverse=True):
        if path.is_file():
            stat = path.stat()
            files.append({"name": path.name, "size_bytes": stat.st_size, "modified_at": stat.st_mtime})
    return files


def search_artifacts(query: str, max_results: int = 20) -> list[dict]:
    """Case-insensitive keyword search across every ingested artifact's
    extracted content (.json records for CSVs, .txt text for PDFs) — not the
    raw uploaded bytes. Returns one entry per matching file with a short
    snippet showing where the match was found."""
    ensure_artifacts_dir()
    query_lower = query.lower().strip()
    if not query_lower:
        return []

    results = []
    for path in sorted(ARTIFACTS_DIR.iterdir(), key=lambda p: p.stat().st_mtime, reverse=True):
        if path.suffix not in (".json", ".txt"):
            continue
        try:
            text = path.read_text()
        except (UnicodeDecodeError, OSError):
            continue

        idx = text.lower().find(query_lower)
        if idx == -1:
            continue

        start = max(0, idx - 60)
        end = min(len(text), idx + len(query_lower) + 60)
        snippet = " ".join(text[start:end].split())
        results.append({"artifact": path.name, "kind": path.suffix.lstrip("."), "snippet": snippet})

        if len(results) >= max_results:
            break

    return results


def purge_old_artifacts(older_than_hours: float) -> list[str]:
    """Delete files under artifacts/ older than the given age.

    Scoped strictly to ARTIFACTS_DIR — never touches orchestrator.db,
    pipelines/, or any source file, no matter what `older_than_hours` is.
    """
    if not ARTIFACTS_DIR.exists():
        return []
    cutoff = time.time() - older_than_hours * 3600
    removed = []
    for path in ARTIFACTS_DIR.iterdir():
        if path.is_file() and path.stat().st_mtime < cutoff:
            path.unlink()
            removed.append(path.name)
    return removed
