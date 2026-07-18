import json
import sqlite3
from datetime import datetime, timezone

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
"""


class StateStore:
    """SQLite-backed execution log for orchestrator runs and their steps."""

    def __init__(self, db_path: str = "orchestrator.db"):
        self.db_path = db_path
        self._conn = sqlite3.connect(self.db_path)
        self._conn.executescript(SCHEMA)
        self._conn.commit()

    def start_run(self) -> int:
        cur = self._conn.execute(
            "INSERT INTO runs (started_at, status) VALUES (?, ?)",
            (datetime.now(timezone.utc).isoformat(), "running"),
        )
        self._conn.commit()
        return cur.lastrowid

    def finish_run(self, run_id: int, status: str) -> None:
        self._conn.execute(
            "UPDATE runs SET finished_at = ?, status = ? WHERE id = ?",
            (datetime.now(timezone.utc).isoformat(), status, run_id),
        )
        self._conn.commit()

    def log_step(self, run_id: int, step: StepRecord) -> None:
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
        cur = self._conn.execute(
            "SELECT id, started_at, finished_at, status FROM runs ORDER BY id DESC LIMIT ?",
            (limit,),
        )
        cols = ("id", "started_at", "finished_at", "status")
        return [dict(zip(cols, row)) for row in cur.fetchall()]

    def steps_for_run(self, run_id: int) -> list[dict]:
        cur = self._conn.execute(
            "SELECT name, tier, success, output, error, started_at, finished_at "
            "FROM steps WHERE run_id = ? ORDER BY id",
            (run_id,),
        )
        cols = ("name", "tier", "success", "output", "error", "started_at", "finished_at")
        return [dict(zip(cols, row)) for row in cur.fetchall()]

    def close(self) -> None:
        self._conn.close()
