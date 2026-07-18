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

CREATE TABLE IF NOT EXISTS result_cache (
    cache_key TEXT PRIMARY KEY,
    tier TEXT NOT NULL,
    name TEXT NOT NULL,
    output TEXT NOT NULL,
    cached_at TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS schedules (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    kind TEXT NOT NULL,
    tier TEXT,
    name TEXT NOT NULL,
    inputs TEXT NOT NULL,
    interval_seconds REAL NOT NULL,
    enabled INTEGER NOT NULL DEFAULT 1,
    next_run_at TEXT NOT NULL,
    last_run_at TEXT,
    last_status TEXT,
    created_at TEXT NOT NULL
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

    def set_cached_result(self, cache_key: str, tier: str, name: str, output: dict) -> None:
        with self._lock:
            self._conn.execute(
                "INSERT OR REPLACE INTO result_cache (cache_key, tier, name, output, cached_at) VALUES (?, ?, ?, ?, ?)",
                (cache_key, tier, name, json.dumps(output), datetime.now(timezone.utc).isoformat()),
            )
            self._conn.commit()

    def get_cached_result(self, cache_key: str, max_age_seconds: float = 3600) -> Optional[dict]:
        """Return a cached module output if one exists and isn't older than
        `max_age_seconds` (default 1 hour) — no eviction job needed, a stale
        entry is just treated as a miss and gets overwritten on the next run."""
        with self._lock:
            cur = self._conn.execute(
                "SELECT output, cached_at FROM result_cache WHERE cache_key = ?",
                (cache_key,),
            )
            row = cur.fetchone()
        if row is None:
            return None
        output, cached_at = row
        age = (datetime.now(timezone.utc) - datetime.fromisoformat(cached_at)).total_seconds()
        if age > max_age_seconds:
            return None
        return json.loads(output)

    def clear_cache(self) -> None:
        with self._lock:
            self._conn.execute("DELETE FROM result_cache")
            self._conn.commit()

    _SCHEDULE_COLUMNS = (
        "id", "kind", "tier", "name", "inputs", "interval_seconds",
        "enabled", "next_run_at", "last_run_at", "last_status", "created_at",
    )

    def _schedule_row_to_dict(self, row) -> dict:
        record = dict(zip(self._SCHEDULE_COLUMNS, row))
        record["enabled"] = bool(record["enabled"])
        record["inputs"] = json.loads(record["inputs"])
        return record

    def create_schedule(
        self, kind: str, name: str, interval_seconds: float, next_run_at: str,
        tier: Optional[str] = None, inputs: Optional[dict] = None,
    ) -> dict:
        with self._lock:
            cur = self._conn.execute(
                "INSERT INTO schedules (kind, tier, name, inputs, interval_seconds, enabled, "
                "next_run_at, created_at) VALUES (?, ?, ?, ?, ?, 1, ?, ?)",
                (
                    kind, tier, name, json.dumps(inputs or {}), interval_seconds,
                    next_run_at, datetime.now(timezone.utc).isoformat(),
                ),
            )
            self._conn.commit()
            schedule_id = cur.lastrowid
            row = self._conn.execute(
                f"SELECT {', '.join(self._SCHEDULE_COLUMNS)} FROM schedules WHERE id = ?", (schedule_id,)
            ).fetchone()
        return self._schedule_row_to_dict(row)

    def list_schedules(self) -> list[dict]:
        with self._lock:
            rows = self._conn.execute(
                f"SELECT {', '.join(self._SCHEDULE_COLUMNS)} FROM schedules ORDER BY id"
            ).fetchall()
        return [self._schedule_row_to_dict(row) for row in rows]

    def due_schedules(self, now_iso: str) -> list[dict]:
        with self._lock:
            rows = self._conn.execute(
                f"SELECT {', '.join(self._SCHEDULE_COLUMNS)} FROM schedules "
                "WHERE enabled = 1 AND next_run_at <= ?",
                (now_iso,),
            ).fetchall()
        return [self._schedule_row_to_dict(row) for row in rows]

    def set_schedule_enabled(self, schedule_id: int, enabled: bool) -> Optional[dict]:
        with self._lock:
            self._conn.execute(
                "UPDATE schedules SET enabled = ? WHERE id = ?", (int(enabled), schedule_id)
            )
            self._conn.commit()
            row = self._conn.execute(
                f"SELECT {', '.join(self._SCHEDULE_COLUMNS)} FROM schedules WHERE id = ?", (schedule_id,)
            ).fetchone()
        return self._schedule_row_to_dict(row) if row else None

    def record_schedule_run(self, schedule_id: int, next_run_at: str, status: str) -> None:
        with self._lock:
            self._conn.execute(
                "UPDATE schedules SET next_run_at = ?, last_run_at = ?, last_status = ? WHERE id = ?",
                (next_run_at, datetime.now(timezone.utc).isoformat(), status, schedule_id),
            )
            self._conn.commit()

    def delete_schedule(self, schedule_id: int) -> None:
        with self._lock:
            self._conn.execute("DELETE FROM schedules WHERE id = ?", (schedule_id,))
            self._conn.commit()

    def close(self) -> None:
        with self._lock:
            self._conn.close()
