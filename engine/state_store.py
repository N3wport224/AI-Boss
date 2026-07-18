import json
import sqlite3
import threading
from datetime import datetime, timezone
from typing import Optional

from .context import StepRecord

SCHEMA = """
CREATE TABLE IF NOT EXISTS runs (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    started_at TEXT NOT NULL,
    finished_at TEXT,
    status TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS steps (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    run_id INTEGER NOT NULL REFERENCES runs(id),
    name TEXT NOT NULL,
    tier TEXT NOT NULL,
    success INTEGER NOT NULL,
    output TEXT NOT NULL,
    error TEXT,
    started_at TEXT NOT NULL,
    finished_at TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS ingested_files (
    hash TEXT PRIMARY KEY,
    filename TEXT NOT NULL,
    kind TEXT NOT NULL,
    ingested_at TEXT NOT NULL
);
"""


class StateStore:
    """SQLite-backed execution log for orchestrator runs and their steps.

    A single connection is shared across requests. FastAPI runs sync route
    handlers in a threadpool, so `check_same_thread=False` plus a lock around
    every statement keeps this safe under concurrent requests from the dashboard,
    not just the single-threaded CLI.
    """

    def __init__(self, db_path: str = "orchestrator.db"):
        self.db_path = db_path
        self._conn = sqlite3.connect(self.db_path, check_same_thread=False)
        self._lock = threading.Lock()
        with self._lock:
            self._conn.executescript(SCHEMA)
            self._conn.commit()

    def start_run(self) -> int:
        with self._lock:
            cur = self._conn.execute(
                "INSERT INTO runs (started_at, status) VALUES (?, ?)",
                (datetime.now(timezone.utc).isoformat(), "running"),
            )
            self._conn.commit()
            return cur.lastrowid

    def finish_run(self, run_id: int, status: str) -> None:
        with self._lock:
            self._conn.execute(
                "UPDATE runs SET finished_at = ?, status = ? WHERE id = ?",
                (datetime.now(timezone.utc).isoformat(), status, run_id),
            )
            self._conn.commit()

    def log_step(self, run_id: int, step: StepRecord) -> None:
        with self._lock:
            self._conn.execute(
                "INSERT INTO steps (run_id, name, tier, success, output, error, started_at, finished_at) "
                "VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
                (
                    run_id,
                    step.name,
                    step.tier,
                    int(step.success),
                    json.dumps(step.output),
                    step.error,
                    step.started_at.isoformat(),
                    step.finished_at.isoformat(),
                ),
            )
            self._conn.commit()

    def recent_runs(self, limit: int = 10) -> list[dict]:
        with self._lock:
            cur = self._conn.execute(
                "SELECT id, started_at, finished_at, status FROM runs ORDER BY id DESC LIMIT ?",
                (limit,),
            )
            rows = cur.fetchall()
        cols = ("id", "started_at", "finished_at", "status")
        return [dict(zip(cols, row)) for row in rows]

    def latest_step_status(self, name: str) -> Optional[bool]:
        """Success flag of the most recent run of a module by name, or None if never run."""
        with self._lock:
            cur = self._conn.execute(
                "SELECT success FROM steps WHERE name = ? ORDER BY id DESC LIMIT 1",
                (name,),
            )
            row = cur.fetchone()
        return bool(row[0]) if row is not None else None

    def steps_for_run(self, run_id: int) -> list[dict]:
        with self._lock:
            cur = self._conn.execute(
                "SELECT name, tier, success, output, error, started_at, finished_at "
                "FROM steps WHERE run_id = ? ORDER BY id",
                (run_id,),
            )
            rows = cur.fetchall()
        cols = ("name", "tier", "success", "output", "error", "started_at", "finished_at")
        return [dict(zip(cols, row)) for row in rows]

    def metrics_summary(self) -> dict:
        """Aggregate telemetry across every finished run: counts, success rate,
        and average wall-clock duration — enough for a header ticker without
        a client having to fetch and reduce the full run history itself."""
        with self._lock:
            cur = self._conn.execute(
                "SELECT started_at, finished_at, status FROM runs WHERE finished_at IS NOT NULL"
            )
            rows = cur.fetchall()

        total = len(rows)
        completed = sum(1 for _, _, status in rows if status == "completed")
        failed = sum(1 for _, _, status in rows if status == "failed")

        durations = []
        for started_at, finished_at, _ in rows:
            try:
                durations.append(
                    (datetime.fromisoformat(finished_at) - datetime.fromisoformat(started_at)).total_seconds()
                )
            except ValueError:
                continue

        return {
            "total_runs": total,
            "completed": completed,
            "failed": failed,
            "success_rate": round(completed / total, 4) if total else None,
            "avg_duration_seconds": round(sum(durations) / len(durations), 3) if durations else None,
        }

    def record_ingested_file(self, file_hash: str, filename: str, kind: str) -> None:
        """Record a successfully-ingested file's hash for future dedupe checks.
        INSERT OR IGNORE: if this exact hash was already recorded, keep the
        original filename/timestamp rather than overwriting them."""
        with self._lock:
            self._conn.execute(
                "INSERT OR IGNORE INTO ingested_files (hash, filename, kind, ingested_at) VALUES (?, ?, ?, ?)",
                (file_hash, filename, kind, datetime.now(timezone.utc).isoformat()),
            )
            self._conn.commit()

    def find_ingested_file(self, file_hash: str) -> Optional[dict]:
        with self._lock:
            cur = self._conn.execute(
                "SELECT hash, filename, kind, ingested_at FROM ingested_files WHERE hash = ?",
                (file_hash,),
            )
            row = cur.fetchone()
        if row is None:
            return None
        cols = ("hash", "filename", "kind", "ingested_at")
        return dict(zip(cols, row))

    def close(self) -> None:
        with self._lock:
            self._conn.close()
