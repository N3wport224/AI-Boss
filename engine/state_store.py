import json
import sqlite3
import threading
from datetime import datetime, timedelta, timezone
from typing import Optional

from .context import StepRecord

SCHEMA = """
CREATE TABLE IF NOT EXISTS runs (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    started_at TEXT NOT NULL,
    finished_at TEXT,
    status TEXT NOT NULL,
    blackboard TEXT NOT NULL DEFAULT '[]'
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
    created_at TEXT NOT NULL,
    schedule_type TEXT NOT NULL DEFAULT 'interval',
    daily_time TEXT,
    day_of_week INTEGER
);

CREATE TABLE IF NOT EXISTS memory (
    key TEXT PRIMARY KEY,
    value TEXT NOT NULL,
    updated_at TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS module_health (
    tier TEXT NOT NULL,
    name TEXT NOT NULL,
    consecutive_failures INTEGER NOT NULL DEFAULT 0,
    tripped INTEGER NOT NULL DEFAULT 0,
    updated_at TEXT NOT NULL,
    PRIMARY KEY (tier, name)
);

CREATE TABLE IF NOT EXISTS artifact_tags (
    filename TEXT PRIMARY KEY,
    tags TEXT NOT NULL,
    updated_at TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS artifact_notes (
    filename TEXT PRIMARY KEY,
    note TEXT NOT NULL,
    updated_at TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS module_overrides (
    tier TEXT NOT NULL,
    name TEXT NOT NULL,
    enabled INTEGER NOT NULL,
    updated_at TEXT NOT NULL,
    PRIMARY KEY (tier, name)
);

CREATE TABLE IF NOT EXISTS audit_log (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    action TEXT NOT NULL,
    detail TEXT NOT NULL,
    created_at TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS breaker_overrides (
    tier TEXT NOT NULL,
    name TEXT NOT NULL,
    threshold INTEGER NOT NULL,
    updated_at TEXT NOT NULL,
    PRIMARY KEY (tier, name)
);

CREATE TABLE IF NOT EXISTS rate_limit_override (
    id INTEGER PRIMARY KEY CHECK (id = 1),
    max_requests INTEGER NOT NULL,
    window_seconds REAL NOT NULL,
    updated_at TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS input_presets (
    tier TEXT NOT NULL,
    name TEXT NOT NULL,
    preset_name TEXT NOT NULL,
    inputs TEXT NOT NULL,
    created_at TEXT NOT NULL,
    PRIMARY KEY (tier, name, preset_name)
);

CREATE TABLE IF NOT EXISTS pipeline_tags (
    slug TEXT PRIMARY KEY,
    tags TEXT NOT NULL,
    updated_at TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS run_notes (
    run_id INTEGER PRIMARY KEY REFERENCES runs(id),
    note TEXT NOT NULL,
    updated_at TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS notifications (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    kind TEXT NOT NULL,
    message TEXT NOT NULL,
    created_at TEXT NOT NULL,
    read INTEGER NOT NULL DEFAULT 0
);

CREATE TABLE IF NOT EXISTS notification_mutes (
    kind TEXT PRIMARY KEY,
    muted INTEGER NOT NULL,
    updated_at TEXT NOT NULL
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
            self._migrate_locked()
            self._conn.commit()

    def _migrate_locked(self) -> None:
        """Lightweight schema migrations for columns added after a table
        already existed via CREATE TABLE IF NOT EXISTS — must be called
        with `self._lock` already held. Each migration is a no-op once
        applied, so this is safe to run on every startup."""
        columns = {row[1] for row in self._conn.execute("PRAGMA table_info(steps)").fetchall()}
        if "inputs" not in columns:
            self._conn.execute("ALTER TABLE steps ADD COLUMN inputs TEXT NOT NULL DEFAULT '{}'")

        schedule_columns = {row[1] for row in self._conn.execute("PRAGMA table_info(schedules)").fetchall()}
        if "schedule_type" not in schedule_columns:
            self._conn.execute("ALTER TABLE schedules ADD COLUMN schedule_type TEXT NOT NULL DEFAULT 'interval'")
        if "daily_time" not in schedule_columns:
            self._conn.execute("ALTER TABLE schedules ADD COLUMN daily_time TEXT")
        if "day_of_week" not in schedule_columns:
            self._conn.execute("ALTER TABLE schedules ADD COLUMN day_of_week INTEGER")
        if "label" not in schedule_columns:
            self._conn.execute("ALTER TABLE schedules ADD COLUMN label TEXT NOT NULL DEFAULT ''")

        run_columns = {row[1] for row in self._conn.execute("PRAGMA table_info(runs)").fetchall()}
        if "blackboard" not in run_columns:
            self._conn.execute("ALTER TABLE runs ADD COLUMN blackboard TEXT NOT NULL DEFAULT '[]'")

    def start_run(self) -> int:
        with self._lock:
            cur = self._conn.execute(
                "INSERT INTO runs (started_at, status) VALUES (?, ?)",
                (datetime.now(timezone.utc).isoformat(), "running"),
            )
            self._conn.commit()
            return cur.lastrowid

    def finish_run(self, run_id: int, status: str, blackboard: Optional[list] = None) -> None:
        with self._lock:
            self._conn.execute(
                "UPDATE runs SET finished_at = ?, status = ?, blackboard = ? WHERE id = ?",
                (datetime.now(timezone.utc).isoformat(), status, json.dumps(blackboard or []), run_id),
            )
            self._conn.commit()

    def get_run_blackboard(self, run_id: int) -> list:
        with self._lock:
            row = self._conn.execute("SELECT blackboard FROM runs WHERE id = ?", (run_id,)).fetchone()
        if row is None or not row[0]:
            return []
        return json.loads(row[0])

    def log_step(self, run_id: int, step: StepRecord) -> None:
        with self._lock:
            self._conn.execute(
                "INSERT INTO steps (run_id, name, tier, success, output, error, started_at, finished_at, inputs) "
                "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)",
                (
                    run_id,
                    step.name,
                    step.tier,
                    int(step.success),
                    json.dumps(step.output),
                    step.error,
                    step.started_at.isoformat(),
                    step.finished_at.isoformat(),
                    json.dumps(step.inputs),
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

    def latest_step_inputs(self, tier: str, name: str) -> Optional[dict]:
        """The resolved input values a module was run with most recently
        (whichever run that was in, whether a lone card run or one step of a
        pipeline), or None if it's never been run. A secret-shaped field is
        already redacted at this point -- the same redaction every other
        history view (run detail, rerun) already lives with, not something
        this method does specially."""
        with self._lock:
            cur = self._conn.execute(
                "SELECT inputs FROM steps WHERE tier = ? AND name = ? ORDER BY id DESC LIMIT 1",
                (tier, name),
            )
            row = cur.fetchone()
        return json.loads(row[0]) if row is not None else None

    def module_stats(self) -> list[dict]:
        """Per-module run statistics — total runs, success rate, and average
        duration — broken out by (tier, name). The per-module counterpart to
        `metrics_summary()`'s single global aggregate."""
        with self._lock:
            rows = self._conn.execute(
                "SELECT tier, name, success, started_at, finished_at FROM steps"
            ).fetchall()

        stats: dict[tuple[str, str], dict] = {}
        for tier, name, success, started_at, finished_at in rows:
            entry = stats.setdefault(
                (tier, name), {"tier": tier, "name": name, "total": 0, "succeeded": 0, "durations": []}
            )
            entry["total"] += 1
            if success:
                entry["succeeded"] += 1
            try:
                entry["durations"].append(
                    (datetime.fromisoformat(finished_at) - datetime.fromisoformat(started_at)).total_seconds()
                )
            except (TypeError, ValueError):
                continue

        results = []
        for entry in stats.values():
            durations = entry["durations"]
            results.append(
                {
                    "tier": entry["tier"],
                    "name": entry["name"],
                    "total_runs": entry["total"],
                    "success_count": entry["succeeded"],
                    "success_rate": round(entry["succeeded"] / entry["total"], 4) if entry["total"] else None,
                    "avg_duration_seconds": round(sum(durations) / len(durations), 4) if durations else None,
                }
            )
        results.sort(key=lambda r: (r["tier"], r["name"]))
        return results

    def steps_for_run(self, run_id: int) -> list[dict]:
        with self._lock:
            cur = self._conn.execute(
                "SELECT name, tier, success, output, error, started_at, finished_at, inputs "
                "FROM steps WHERE run_id = ? ORDER BY id",
                (run_id,),
            )
            rows = cur.fetchall()
        cols = ("name", "tier", "success", "output", "error", "started_at", "finished_at", "inputs")
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

    def prune_runs(self, older_than_hours: float) -> int:
        """Delete finished runs (and their steps) older than `older_than_hours`,
        mirroring artifact purge — scoped strictly to `runs`/`steps`, never
        touching schedules, memory, or the result cache. Returns how many
        runs were removed. A run still in progress (`finished_at IS NULL`)
        is never a candidate, no matter how old `started_at` is."""
        cutoff = (datetime.now(timezone.utc) - timedelta(hours=older_than_hours)).isoformat()
        with self._lock:
            rows = self._conn.execute(
                "SELECT id FROM runs WHERE finished_at IS NOT NULL AND finished_at < ?", (cutoff,)
            ).fetchall()
            run_ids = [row[0] for row in rows]
            if run_ids:
                placeholders = ",".join("?" * len(run_ids))
                self._conn.execute(f"DELETE FROM steps WHERE run_id IN ({placeholders})", run_ids)
                self._conn.execute(f"DELETE FROM run_notes WHERE run_id IN ({placeholders})", run_ids)
                self._conn.execute(f"DELETE FROM runs WHERE id IN ({placeholders})", run_ids)
                self._conn.commit()
        return len(run_ids)

    def delete_runs(self, run_ids: list[int]) -> int:
        """Delete a caller-chosen set of runs (and their steps) by id — the
        fine-grained counterpart to `prune_runs`'s age-based sweep. Silently
        ignores ids that don't exist; returns how many runs were actually
        removed. A run still in progress is deleted too if explicitly asked
        for by id (unlike `prune_runs`, which never touches one)."""
        if not run_ids:
            return 0
        with self._lock:
            placeholders = ",".join("?" * len(run_ids))
            existing = self._conn.execute(
                f"SELECT id FROM runs WHERE id IN ({placeholders})", run_ids
            ).fetchall()
            existing_ids = [row[0] for row in existing]
            if existing_ids:
                existing_placeholders = ",".join("?" * len(existing_ids))
                self._conn.execute(f"DELETE FROM steps WHERE run_id IN ({existing_placeholders})", existing_ids)
                self._conn.execute(f"DELETE FROM run_notes WHERE run_id IN ({existing_placeholders})", existing_ids)
                self._conn.execute(f"DELETE FROM runs WHERE id IN ({existing_placeholders})", existing_ids)
                self._conn.commit()
        return len(existing_ids)

    def search_steps(self, query: str, limit: int = 20) -> list[dict]:
        """Case-insensitive keyword search across every recorded step's output
        JSON and error text — the run-history counterpart to artifact content
        search. Returns newest matches first, each with a short snippet of
        wherever the match was found."""
        needle = query.lower().strip()
        if not needle:
            return []

        with self._lock:
            rows = self._conn.execute(
                "SELECT run_id, name, tier, success, output, error, finished_at FROM steps "
                "WHERE lower(output) LIKE ? OR lower(coalesce(error, '')) LIKE ? "
                "ORDER BY id DESC LIMIT ?",
                (f"%{needle}%", f"%{needle}%", limit),
            ).fetchall()

        results = []
        for run_id, name, tier, success, output, error, finished_at in rows:
            haystack, matched_in = (error, "error") if error and needle in error.lower() else (output, "output")
            idx = haystack.lower().find(needle)
            start = max(0, idx - 60)
            end = min(len(haystack), idx + len(needle) + 60)
            snippet = " ".join(haystack[start:end].split())
            results.append(
                {
                    "run_id": run_id,
                    "step": name,
                    "tier": tier,
                    "success": bool(success),
                    "matched_in": matched_in,
                    "snippet": snippet,
                    "finished_at": finished_at,
                }
            )
        return results

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

    def get_memory(self, key: str, default=None):
        """Backs `context.memory.get()` — a persistent key/value store any
        module can read, surviving across separate runs (unlike
        `context.variables`, which only lives for one run)."""
        with self._lock:
            row = self._conn.execute("SELECT value FROM memory WHERE key = ?", (key,)).fetchone()
        if row is None:
            return default
        return json.loads(row[0])

    def set_memory(self, key: str, value) -> None:
        with self._lock:
            self._conn.execute(
                "INSERT INTO memory (key, value, updated_at) VALUES (?, ?, ?) "
                "ON CONFLICT(key) DO UPDATE SET value = excluded.value, updated_at = excluded.updated_at",
                (key, json.dumps(value), datetime.now(timezone.utc).isoformat()),
            )
            self._conn.commit()

    def delete_memory(self, key: str) -> None:
        with self._lock:
            self._conn.execute("DELETE FROM memory WHERE key = ?", (key,))
            self._conn.commit()

    def all_memory(self) -> list[dict]:
        with self._lock:
            rows = self._conn.execute("SELECT key, value, updated_at FROM memory ORDER BY key").fetchall()
        return [{"key": key, "value": json.loads(value), "updated_at": updated_at} for key, value, updated_at in rows]

    def search_memory(self, query: str, limit: int = 20) -> list[dict]:
        """Case-insensitive keyword search across every memory entry's own
        key and its JSON-encoded value text -- mirrors search_run_notes()'s
        content-search pattern, applied to the cross-run memory store
        instead of run annotations. SQL LIKE with escaped wildcards keeps
        this a literal substring search, not a glob."""
        query_stripped = query.strip()
        if not query_stripped:
            return []
        like_pattern = "%" + query_stripped.replace("%", r"\%").replace("_", r"\_") + "%"
        with self._lock:
            rows = self._conn.execute(
                "SELECT key, value, updated_at FROM memory "
                "WHERE key LIKE ? ESCAPE '\\' OR value LIKE ? ESCAPE '\\' "
                "ORDER BY key LIMIT ?",
                (like_pattern, like_pattern, limit),
            ).fetchall()
        return [{"key": key, "value": json.loads(value), "updated_at": updated_at} for key, value, updated_at in rows]

    def clear_memory(self) -> None:
        with self._lock:
            self._conn.execute("DELETE FROM memory")
            self._conn.commit()

    # ---- Circuit breaker (module health) ----
    # Deliberately *not* part of export_snapshot/restore_snapshot: breaker
    # state is transient health data about this installation's recent runs,
    # not durable work product worth carrying into a restored copy.

    def _module_health_row(self, tier: str, name: str) -> dict:
        row = self._conn.execute(
            "SELECT consecutive_failures, tripped, updated_at FROM module_health WHERE tier = ? AND name = ?",
            (tier, name),
        ).fetchone()
        if row is None:
            return {"tier": tier, "name": name, "consecutive_failures": 0, "tripped": False, "updated_at": None}
        return {
            "tier": tier,
            "name": name,
            "consecutive_failures": row[0],
            "tripped": bool(row[1]),
            "updated_at": row[2],
        }

    def record_module_failure(self, tier: str, name: str, trip_threshold: int) -> dict:
        """Bump a module's consecutive-failure count, tripping its circuit
        breaker once the count reaches `trip_threshold`. Once tripped, the
        breaker stays open (and the count keeps climbing if anything still
        manages to run it) until `reset_breaker` is called."""
        now = datetime.now(timezone.utc).isoformat()
        with self._lock:
            self._conn.execute(
                "INSERT INTO module_health (tier, name, consecutive_failures, tripped, updated_at) "
                "VALUES (:tier, :name, 1, :initial_tripped, :now) "
                "ON CONFLICT(tier, name) DO UPDATE SET "
                "consecutive_failures = consecutive_failures + 1, "
                "tripped = CASE WHEN consecutive_failures + 1 >= :threshold THEN 1 ELSE tripped END, "
                "updated_at = :now",
                {
                    "tier": tier,
                    "name": name,
                    "initial_tripped": int(1 >= trip_threshold),
                    "threshold": trip_threshold,
                    "now": now,
                },
            )
            self._conn.commit()
            return self._module_health_row(tier, name)

    def record_module_success(self, tier: str, name: str) -> None:
        """A success closes the 'consecutive' failure streak — but does NOT
        silently close an already-tripped breaker: once open, only an explicit
        reset closes it, so a flaky module can't quietly re-arm itself."""
        with self._lock:
            self._conn.execute(
                "UPDATE module_health SET consecutive_failures = 0, updated_at = ? "
                "WHERE tier = ? AND name = ? AND tripped = 0",
                (datetime.now(timezone.utc).isoformat(), tier, name),
            )
            self._conn.commit()

    def get_module_health(self, tier: str, name: str) -> dict:
        with self._lock:
            return self._module_health_row(tier, name)

    def all_module_health(self) -> list[dict]:
        with self._lock:
            rows = self._conn.execute(
                "SELECT tier, name FROM module_health ORDER BY tier, name"
            ).fetchall()
            return [self._module_health_row(tier, name) for tier, name in rows]

    def reset_breaker(self, tier: str, name: str) -> dict:
        with self._lock:
            self._conn.execute(
                "UPDATE module_health SET consecutive_failures = 0, tripped = 0, updated_at = ? "
                "WHERE tier = ? AND name = ?",
                (datetime.now(timezone.utc).isoformat(), tier, name),
            )
            self._conn.commit()
            return self._module_health_row(tier, name)

    def set_module_enabled(self, tier: str, name: str, enabled: bool) -> None:
        """A runtime on/off override for a module, independent of its
        manifest's own `enabled` flag — toggled from the dashboard, not by
        editing a YAML file, and persisted so it survives a restart."""
        with self._lock:
            self._conn.execute(
                "INSERT INTO module_overrides (tier, name, enabled, updated_at) VALUES (?, ?, ?, ?) "
                "ON CONFLICT(tier, name) DO UPDATE SET enabled = excluded.enabled, updated_at = excluded.updated_at",
                (tier, name, int(enabled), datetime.now(timezone.utc).isoformat()),
            )
            self._conn.commit()

    def get_module_enabled_override(self, tier: str, name: str) -> Optional[bool]:
        """None means no override has ever been set — the caller should fall
        back to the manifest's own `enabled` flag (default True)."""
        with self._lock:
            row = self._conn.execute(
                "SELECT enabled FROM module_overrides WHERE tier = ? AND name = ?", (tier, name)
            ).fetchone()
        return bool(row[0]) if row is not None else None

    def all_module_overrides(self) -> dict[tuple[str, str], bool]:
        with self._lock:
            rows = self._conn.execute("SELECT tier, name, enabled FROM module_overrides").fetchall()
        return {(tier, name): bool(enabled) for tier, name, enabled in rows}

    def set_breaker_threshold(self, tier: str, name: str, threshold: int) -> None:
        """A runtime override for how many consecutive failures trip a
        module's circuit breaker, independent of its manifest's own
        `circuit_breaker_threshold` — no YAML edit needed, and it persists
        across a restart. A separate table from `module_overrides` since
        the two toggles are independent (a module can have one, both, or
        neither overridden)."""
        with self._lock:
            self._conn.execute(
                "INSERT INTO breaker_overrides (tier, name, threshold, updated_at) VALUES (?, ?, ?, ?) "
                "ON CONFLICT(tier, name) DO UPDATE SET threshold = excluded.threshold, updated_at = excluded.updated_at",
                (tier, name, threshold, datetime.now(timezone.utc).isoformat()),
            )
            self._conn.commit()

    def get_breaker_threshold_override(self, tier: str, name: str) -> Optional[int]:
        """None means no override has ever been set — the caller should
        fall back to the manifest's own `circuit_breaker_threshold` (or the
        engine-wide default)."""
        with self._lock:
            row = self._conn.execute(
                "SELECT threshold FROM breaker_overrides WHERE tier = ? AND name = ?", (tier, name)
            ).fetchone()
        return row[0] if row is not None else None

    def clear_breaker_threshold_override(self, tier: str, name: str) -> None:
        with self._lock:
            self._conn.execute("DELETE FROM breaker_overrides WHERE tier = ? AND name = ?", (tier, name))
            self._conn.commit()

    def all_breaker_threshold_overrides(self) -> dict[tuple[str, str], int]:
        with self._lock:
            rows = self._conn.execute("SELECT tier, name, threshold FROM breaker_overrides").fetchall()
        return {(tier, name): threshold for tier, name, threshold in rows}

    def set_rate_limit_override(self, max_requests: int, window_seconds: float) -> None:
        """A runtime override for the run-triggering rate limiter's
        max_requests/window_seconds, independent of the hardcoded default
        set at process startup -- no restart needed, and it persists
        across one. A single-row table (id always 1) since there's only
        ever one rate limiter to configure, unlike the per-module
        breaker_overrides table."""
        with self._lock:
            self._conn.execute(
                "INSERT INTO rate_limit_override (id, max_requests, window_seconds, updated_at) "
                "VALUES (1, ?, ?, ?) ON CONFLICT(id) DO UPDATE SET "
                "max_requests = excluded.max_requests, window_seconds = excluded.window_seconds, "
                "updated_at = excluded.updated_at",
                (max_requests, window_seconds, datetime.now(timezone.utc).isoformat()),
            )
            self._conn.commit()

    def get_rate_limit_override(self) -> Optional[tuple[int, float]]:
        """None means no override has ever been set -- the caller should
        fall back to the hardcoded startup default."""
        with self._lock:
            row = self._conn.execute(
                "SELECT max_requests, window_seconds FROM rate_limit_override WHERE id = 1"
            ).fetchone()
        return (row[0], row[1]) if row is not None else None

    def clear_rate_limit_override(self) -> None:
        with self._lock:
            self._conn.execute("DELETE FROM rate_limit_override WHERE id = 1")
            self._conn.commit()

    def save_input_preset(self, tier: str, name: str, preset_name: str, inputs: dict) -> dict:
        """A named set of input values for a module's own card, so a user
        can reapply a combination they use often instead of retyping it —
        saving under a name that already exists for this module overwrites
        it, same convention as re-saving a pipeline in the builder."""
        created_at = datetime.now(timezone.utc).isoformat()
        with self._lock:
            self._conn.execute(
                "INSERT INTO input_presets (tier, name, preset_name, inputs, created_at) VALUES (?, ?, ?, ?, ?) "
                "ON CONFLICT(tier, name, preset_name) DO UPDATE SET inputs = excluded.inputs, created_at = excluded.created_at",
                (tier, name, preset_name, json.dumps(inputs), created_at),
            )
            self._conn.commit()
        return {"tier": tier, "name": name, "preset_name": preset_name, "inputs": inputs, "created_at": created_at}

    def list_input_presets(self, tier: str, name: str) -> list[dict]:
        with self._lock:
            rows = self._conn.execute(
                "SELECT preset_name, inputs, created_at FROM input_presets WHERE tier = ? AND name = ? ORDER BY preset_name",
                (tier, name),
            ).fetchall()
        return [{"preset_name": r[0], "inputs": json.loads(r[1]), "created_at": r[2]} for r in rows]

    def delete_input_preset(self, tier: str, name: str, preset_name: str) -> bool:
        with self._lock:
            cursor = self._conn.execute(
                "DELETE FROM input_presets WHERE tier = ? AND name = ? AND preset_name = ?",
                (tier, name, preset_name),
            )
            self._conn.commit()
            return cursor.rowcount > 0

    def record_audit_event(self, action: str, detail: str) -> dict:
        """Append-only log of destructive/administrative actions (run purge,
        bulk-delete, artifact purge, backup restore, circuit breaker reset,
        schedule pause/resume, pipeline version restore) so a user can see
        what happened and when without guessing. Never edited or deduped —
        each call is its own row."""
        created_at = datetime.now(timezone.utc).isoformat()
        with self._lock:
            cursor = self._conn.execute(
                "INSERT INTO audit_log (action, detail, created_at) VALUES (?, ?, ?)",
                (action, detail, created_at),
            )
            self._conn.commit()
            return {"id": cursor.lastrowid, "action": action, "detail": detail, "created_at": created_at}

    def list_audit_events(self, limit: int = 50) -> list[dict]:
        with self._lock:
            rows = self._conn.execute(
                "SELECT id, action, detail, created_at FROM audit_log ORDER BY id DESC LIMIT ?",
                (limit,),
            ).fetchall()
        return [{"id": r[0], "action": r[1], "detail": r[2], "created_at": r[3]} for r in rows]

    def search_audit_events(self, query: str, limit: int = 20) -> list[dict]:
        """Case-insensitive keyword search across every audit event's own
        action and detail text -- mirrors search_run_notes()'s content-search
        pattern, applied to the audit trail instead of run annotations. SQL
        LIKE with escaped wildcards keeps this a literal substring search."""
        query_stripped = query.strip()
        if not query_stripped:
            return []
        like_pattern = "%" + query_stripped.replace("%", r"\%").replace("_", r"\_") + "%"
        with self._lock:
            rows = self._conn.execute(
                "SELECT id, action, detail, created_at FROM audit_log "
                "WHERE action LIKE ? ESCAPE '\\' OR detail LIKE ? ESCAPE '\\' "
                "ORDER BY id DESC LIMIT ?",
                (like_pattern, like_pattern, limit),
            ).fetchall()
        return [{"id": r[0], "action": r[1], "detail": r[2], "created_at": r[3]} for r in rows]

    def clear_audit_log(self) -> int:
        """Delete every audit log entry unconditionally -- a manual full
        reset, mirroring clear_read_notifications()/clear_favorites()
        elsewhere in this store. See purge_audit_log() for the
        age-based, partial-trim counterpart. Returns the number of rows
        deleted."""
        with self._lock:
            cur = self._conn.execute("DELETE FROM audit_log")
            self._conn.commit()
        return cur.rowcount

    def purge_audit_log(self, older_than_hours: float) -> int:
        """Delete audit log entries older than `older_than_hours` -- the
        age-based, partial-trim counterpart to clear_audit_log()'s
        unconditional wipe, mirroring prune_runs()/purge_old_artifacts()'s
        age-based sweep pattern elsewhere in this app. Returns the number
        of rows deleted."""
        cutoff = (datetime.now(timezone.utc) - timedelta(hours=older_than_hours)).isoformat()
        with self._lock:
            cur = self._conn.execute("DELETE FROM audit_log WHERE created_at < ?", (cutoff,))
            self._conn.commit()
        return cur.rowcount

    _SCHEDULE_COLUMNS = (
        "id", "kind", "tier", "name", "inputs", "interval_seconds",
        "enabled", "next_run_at", "last_run_at", "last_status", "created_at",
        "schedule_type", "daily_time", "day_of_week", "label",
    )

    def _schedule_row_to_dict(self, row) -> dict:
        record = dict(zip(self._SCHEDULE_COLUMNS, row))
        record["enabled"] = bool(record["enabled"])
        record["inputs"] = json.loads(record["inputs"])
        return record

    def create_schedule(
        self, kind: str, name: str, interval_seconds: Optional[float], next_run_at: str,
        tier: Optional[str] = None, inputs: Optional[dict] = None,
        schedule_type: str = "interval", daily_time: Optional[str] = None,
        day_of_week: Optional[int] = None, label: str = "",
    ) -> dict:
        with self._lock:
            cur = self._conn.execute(
                "INSERT INTO schedules (kind, tier, name, inputs, interval_seconds, enabled, "
                "next_run_at, created_at, schedule_type, daily_time, day_of_week, label) "
                "VALUES (?, ?, ?, ?, ?, 1, ?, ?, ?, ?, ?, ?)",
                (
                    kind, tier, name, json.dumps(inputs or {}), interval_seconds or 0.0,
                    next_run_at, datetime.now(timezone.utc).isoformat(), schedule_type, daily_time, day_of_week, label,
                ),
            )
            self._conn.commit()
            schedule_id = cur.lastrowid
            row = self._conn.execute(
                f"SELECT {', '.join(self._SCHEDULE_COLUMNS)} FROM schedules WHERE id = ?", (schedule_id,)
            ).fetchone()
        return self._schedule_row_to_dict(row)

    def set_schedule_label(self, schedule_id: int, label: str) -> Optional[dict]:
        """A free-text label a user can set per schedule, independent of its
        target -- so several schedules against the same module/pipeline
        (different cadences, different purposes) can be told apart at a
        glance instead of only by cadence text. Blank clears it back to
        unlabeled."""
        with self._lock:
            self._conn.execute("UPDATE schedules SET label = ? WHERE id = ?", (label, schedule_id))
            self._conn.commit()
            row = self._conn.execute(
                f"SELECT {', '.join(self._SCHEDULE_COLUMNS)} FROM schedules WHERE id = ?", (schedule_id,)
            ).fetchone()
        return self._schedule_row_to_dict(row) if row else None

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

    def repoint_pipeline_schedules(self, old_slug: str, new_slug: str) -> int:
        """Update every pipeline-kind schedule's target from old_slug to
        new_slug -- a saved pipeline's slug is its identity for scheduling
        purposes (the scheduler loads it by `schedule["name"]`), so renaming
        a pipeline would otherwise silently orphan any schedule pointed at
        its old slug. Returns the number of schedules repointed."""
        with self._lock:
            cur = self._conn.execute(
                "UPDATE schedules SET name = ? WHERE kind = 'pipeline' AND name = ?",
                (new_slug, old_slug),
            )
            self._conn.commit()
        return cur.rowcount

    def set_artifact_tags(self, filename: str, tags: list[str]) -> list[str]:
        """Replace the full tag set for an artifact (keyed by its on-disk
        filename, which is unique per upload thanks to the timestamp prefix
        `save_artifact` gives it). An empty list clears tagging entirely."""
        with self._lock:
            self._conn.execute(
                "INSERT INTO artifact_tags (filename, tags, updated_at) VALUES (?, ?, ?) "
                "ON CONFLICT(filename) DO UPDATE SET tags = excluded.tags, updated_at = excluded.updated_at",
                (filename, json.dumps(tags), datetime.now(timezone.utc).isoformat()),
            )
            self._conn.commit()
        return tags

    def get_artifact_tags(self, filename: str) -> list[str]:
        with self._lock:
            row = self._conn.execute(
                "SELECT tags FROM artifact_tags WHERE filename = ?", (filename,)
            ).fetchone()
        return json.loads(row[0]) if row else []

    def all_artifact_tags(self) -> dict[str, list[str]]:
        """Every tagged artifact's tags in one query, for list endpoints that
        join tags onto every file without a query per row."""
        with self._lock:
            rows = self._conn.execute("SELECT filename, tags FROM artifact_tags").fetchall()
        return {filename: json.loads(tags) for filename, tags in rows}

    def rename_artifact_tags(self, old_filename: str, new_filename: str) -> None:
        """Move an artifact's tag row to follow it when the underlying file
        is renamed -- tags are keyed by filename, so without this a rename
        would silently orphan them under the old name. A no-op if the old
        filename was never tagged."""
        with self._lock:
            self._conn.execute(
                "UPDATE artifact_tags SET filename = ? WHERE filename = ?", (new_filename, old_filename)
            )
            self._conn.commit()

    def set_artifact_note(self, filename: str, note: str) -> str:
        """A single free-text note per artifact (keyed by filename, same as
        tags) -- for a longer human comment tags aren't suited to, mirroring
        the existing per-schedule label. An empty string clears the note."""
        with self._lock:
            self._conn.execute(
                "INSERT INTO artifact_notes (filename, note, updated_at) VALUES (?, ?, ?) "
                "ON CONFLICT(filename) DO UPDATE SET note = excluded.note, updated_at = excluded.updated_at",
                (filename, note, datetime.now(timezone.utc).isoformat()),
            )
            self._conn.commit()
        return note

    def get_artifact_note(self, filename: str) -> str:
        with self._lock:
            row = self._conn.execute(
                "SELECT note FROM artifact_notes WHERE filename = ?", (filename,)
            ).fetchone()
        return row[0] if row else ""

    def all_artifact_notes(self) -> dict[str, str]:
        """Every noted artifact's note in one query, for list endpoints that
        join notes onto every file without a query per row."""
        with self._lock:
            rows = self._conn.execute("SELECT filename, note FROM artifact_notes").fetchall()
        return dict(rows)

    def rename_artifact_note(self, old_filename: str, new_filename: str) -> None:
        """Move an artifact's note row to follow it when the underlying file
        is renamed -- notes are keyed by filename, so without this a rename
        would silently orphan the note under the old name. A no-op if the
        old filename was never noted."""
        with self._lock:
            self._conn.execute(
                "UPDATE artifact_notes SET filename = ? WHERE filename = ?", (new_filename, old_filename)
            )
            self._conn.commit()

    def set_pipeline_tags(self, slug: str, tags: list[str]) -> list[str]:
        """Replace the full tag set for a saved pipeline (keyed by its slug).
        An empty list clears tagging entirely."""
        with self._lock:
            self._conn.execute(
                "INSERT INTO pipeline_tags (slug, tags, updated_at) VALUES (?, ?, ?) "
                "ON CONFLICT(slug) DO UPDATE SET tags = excluded.tags, updated_at = excluded.updated_at",
                (slug, json.dumps(tags), datetime.now(timezone.utc).isoformat()),
            )
            self._conn.commit()
        return tags

    def get_pipeline_tags(self, slug: str) -> list[str]:
        with self._lock:
            row = self._conn.execute(
                "SELECT tags FROM pipeline_tags WHERE slug = ?", (slug,)
            ).fetchone()
        return json.loads(row[0]) if row else []

    def all_pipeline_tags(self) -> dict[str, list[str]]:
        """Every tagged pipeline's tags in one query, for list endpoints that
        join tags onto every pipeline without a query per row."""
        with self._lock:
            rows = self._conn.execute("SELECT slug, tags FROM pipeline_tags").fetchall()
        return {slug: json.loads(tags) for slug, tags in rows}

    def delete_pipeline_tags(self, slug: str) -> None:
        """Drop any tag record for a pipeline slug, e.g. when the pipeline itself is deleted."""
        with self._lock:
            self._conn.execute("DELETE FROM pipeline_tags WHERE slug = ?", (slug,))
            self._conn.commit()

    def run_exists(self, run_id: int) -> bool:
        with self._lock:
            row = self._conn.execute("SELECT 1 FROM runs WHERE id = ?", (run_id,)).fetchone()
        return row is not None

    def set_run_note(self, run_id: int, note: str) -> str:
        """Save (or overwrite) a free-text note on a past run, for the user's
        own future reference. An empty string clears the note entirely."""
        with self._lock:
            if not note:
                self._conn.execute("DELETE FROM run_notes WHERE run_id = ?", (run_id,))
            else:
                self._conn.execute(
                    "INSERT INTO run_notes (run_id, note, updated_at) VALUES (?, ?, ?) "
                    "ON CONFLICT(run_id) DO UPDATE SET note = excluded.note, updated_at = excluded.updated_at",
                    (run_id, note, datetime.now(timezone.utc).isoformat()),
                )
            self._conn.commit()
        return note

    def get_run_note(self, run_id: int) -> Optional[str]:
        with self._lock:
            row = self._conn.execute(
                "SELECT note FROM run_notes WHERE run_id = ?", (run_id,)
            ).fetchone()
        return row[0] if row else None

    def all_run_notes(self) -> dict[int, str]:
        """Every annotated run's note in one query, for list endpoints that
        join notes onto every run without a query per row."""
        with self._lock:
            rows = self._conn.execute("SELECT run_id, note FROM run_notes").fetchall()
        return {run_id: note for run_id, note in rows}

    def search_run_notes(self, query: str, limit: int = 20) -> list[dict]:
        """Case-insensitive keyword search across every run's own note --
        mirrors webapp.pipelines.search_pipelines()'s content-search pattern,
        applied to run annotations instead of pipeline YAML text. A run
        without a note is never a match; SQL LIKE with escaped wildcards
        keeps this a literal substring search, not a glob."""
        query_stripped = query.strip()
        if not query_stripped:
            return []
        like_pattern = "%" + query_stripped.replace("%", r"\%").replace("_", r"\_") + "%"
        with self._lock:
            rows = self._conn.execute(
                "SELECT run_id, note FROM run_notes WHERE note LIKE ? ESCAPE '\\' "
                "ORDER BY run_id DESC LIMIT ?",
                (like_pattern, limit),
            ).fetchall()
        return [{"run_id": run_id, "note": note} for run_id, note in rows]

    def add_notification(self, kind: str, message: str) -> Optional[dict]:
        """Record a durable notification -- unlike a toast (which vanishes on
        reload) or the audit log (a record of user-initiated actions), this
        is for events the app itself decides are worth surfacing: a circuit
        breaker tripping, a schedule failing, a resource-usage alert firing.
        Returns None (and writes nothing) if this kind is currently muted."""
        if self.is_notification_kind_muted(kind):
            return None
        now = datetime.now(timezone.utc).isoformat()
        with self._lock:
            cur = self._conn.execute(
                "INSERT INTO notifications (kind, message, created_at, read) VALUES (?, ?, ?, 0)",
                (kind, message, now),
            )
            self._conn.commit()
            notification_id = cur.lastrowid
        return {"id": notification_id, "kind": kind, "message": message, "created_at": now, "read": False}

    def set_notification_kind_muted(self, kind: str, muted: bool) -> None:
        with self._lock:
            self._conn.execute(
                "INSERT INTO notification_mutes (kind, muted, updated_at) VALUES (?, ?, ?) "
                "ON CONFLICT(kind) DO UPDATE SET muted = excluded.muted, updated_at = excluded.updated_at",
                (kind, int(muted), datetime.now(timezone.utc).isoformat()),
            )
            self._conn.commit()

    def is_notification_kind_muted(self, kind: str) -> bool:
        with self._lock:
            row = self._conn.execute(
                "SELECT muted FROM notification_mutes WHERE kind = ?", (kind,)
            ).fetchone()
        return bool(row[0]) if row else False

    def all_notification_mute_state(self) -> dict[str, bool]:
        with self._lock:
            rows = self._conn.execute("SELECT kind, muted FROM notification_mutes").fetchall()
        return {kind: bool(muted) for kind, muted in rows}

    def list_notifications(self, unread_only: bool = False, limit: int = 200) -> list[dict]:
        query = "SELECT id, kind, message, created_at, read FROM notifications"
        if unread_only:
            query += " WHERE read = 0"
        query += " ORDER BY id DESC LIMIT ?"
        with self._lock:
            rows = self._conn.execute(query, (limit,)).fetchall()
        return [
            {"id": r[0], "kind": r[1], "message": r[2], "created_at": r[3], "read": bool(r[4])}
            for r in rows
        ]

    def search_notifications(self, query: str, limit: int = 20) -> list[dict]:
        """Case-insensitive keyword search across every notification's own
        message -- mirrors search_run_notes()'s content-search pattern,
        applied to the alert feed instead of run annotations. SQL LIKE with
        escaped wildcards keeps this a literal substring search, not a glob."""
        query_stripped = query.strip()
        if not query_stripped:
            return []
        like_pattern = "%" + query_stripped.replace("%", r"\%").replace("_", r"\_") + "%"
        with self._lock:
            rows = self._conn.execute(
                "SELECT id, kind, message, created_at, read FROM notifications "
                "WHERE message LIKE ? ESCAPE '\\' ORDER BY id DESC LIMIT ?",
                (like_pattern, limit),
            ).fetchall()
        return [
            {"id": r[0], "kind": r[1], "message": r[2], "created_at": r[3], "read": bool(r[4])}
            for r in rows
        ]

    def unread_notification_count(self) -> int:
        with self._lock:
            row = self._conn.execute("SELECT COUNT(*) FROM notifications WHERE read = 0").fetchone()
        return row[0] if row else 0

    def mark_notification_read(self, notification_id: int) -> bool:
        with self._lock:
            cur = self._conn.execute(
                "UPDATE notifications SET read = 1 WHERE id = ?", (notification_id,)
            )
            self._conn.commit()
        return cur.rowcount > 0

    def mark_all_notifications_read(self) -> int:
        with self._lock:
            cur = self._conn.execute("UPDATE notifications SET read = 1 WHERE read = 0")
            self._conn.commit()
        return cur.rowcount

    def clear_read_notifications(self) -> int:
        """Delete every already-read notification -- keeps the Alerts list
        from growing unbounded, distinct from mark_all_notifications_read()
        which only flips the read flag and leaves every row in place."""
        with self._lock:
            cur = self._conn.execute("DELETE FROM notifications WHERE read = 1")
            self._conn.commit()
        return cur.rowcount

    def mark_notifications_read(self, notification_ids: list[int]) -> int:
        """Mark a user-picked set of notifications read at once -- the
        finer-grained counterpart to mark_all_notifications_read(), for a
        checkbox multi-select in the Alerts list. An unknown id is simply
        a no-op UPDATE, same as mark_notification_read()'s single-id form."""
        if not notification_ids:
            return 0
        placeholders = ",".join("?" * len(notification_ids))
        with self._lock:
            cur = self._conn.execute(
                f"UPDATE notifications SET read = 1 WHERE id IN ({placeholders}) AND read = 0",
                notification_ids,
            )
            self._conn.commit()
        return cur.rowcount

    def delete_notifications(self, notification_ids: list[int]) -> int:
        """Delete a user-picked set of notifications at once -- the
        finer-grained counterpart to clear_read_notifications() (which only
        ever deletes already-read ones), for a checkbox multi-select that
        may include unread alerts too."""
        if not notification_ids:
            return 0
        placeholders = ",".join("?" * len(notification_ids))
        with self._lock:
            cur = self._conn.execute(
                f"DELETE FROM notifications WHERE id IN ({placeholders})",
                notification_ids,
            )
            self._conn.commit()
        return cur.rowcount

    def export_snapshot(self) -> dict:
        """A full, human-readable JSON snapshot of everything this store has
        recorded — every run, every step (with `output` parsed back into a
        dict), every schedule, and every ingested-file hash record. Meant as
        a portable backup/inspection format, not a byte-for-byte copy of the
        database (see also: downloading the raw .db file for that)."""
        with self._lock:
            run_rows = self._conn.execute(
                "SELECT id, started_at, finished_at, status FROM runs ORDER BY id"
            ).fetchall()
            step_rows = self._conn.execute(
                "SELECT id, run_id, name, tier, success, output, error, started_at, finished_at, inputs "
                "FROM steps ORDER BY id"
            ).fetchall()
            schedule_rows = self._conn.execute(
                f"SELECT {', '.join(self._SCHEDULE_COLUMNS)} FROM schedules ORDER BY id"
            ).fetchall()
            ingested_rows = self._conn.execute(
                "SELECT hash, filename, kind, ingested_at FROM ingested_files ORDER BY ingested_at"
            ).fetchall()

        run_cols = ("id", "started_at", "finished_at", "status")
        step_cols = ("id", "run_id", "name", "tier", "success", "output", "error", "started_at", "finished_at", "inputs")
        ingested_cols = ("hash", "filename", "kind", "ingested_at")

        steps = []
        for row in step_rows:
            record = dict(zip(step_cols, row))
            record["success"] = bool(record["success"])
            try:
                record["output"] = json.loads(record["output"])
            except (TypeError, json.JSONDecodeError):
                pass
            try:
                record["inputs"] = json.loads(record["inputs"])
            except (TypeError, json.JSONDecodeError):
                record["inputs"] = {}
            steps.append(record)

        return {
            "exported_at": datetime.now(timezone.utc).isoformat(),
            "runs": [dict(zip(run_cols, row)) for row in run_rows],
            "steps": steps,
            "schedules": [self._schedule_row_to_dict(row) for row in schedule_rows],
            "ingested_files": [dict(zip(ingested_cols, row)) for row in ingested_rows],
            "memory": self.all_memory(),
        }

    def restore_snapshot(self, snapshot: dict) -> dict:
        """Additively restore a JSON snapshot produced by `export_snapshot()`:
        fills in any run/step/schedule/ingested-file/memory-key not already
        present (matched by primary key), never overwriting something already
        there. Not a full sync/replace — restoring the same snapshot twice,
        or into a store that already has some of this data, is always safe
        and just a no-op for whatever already exists. Returns how many rows
        of each kind were actually inserted."""
        counts = {"runs": 0, "steps": 0, "schedules": 0, "ingested_files": 0, "memory": 0}
        with self._lock:
            for run in snapshot.get("runs", []):
                cur = self._conn.execute(
                    "INSERT OR IGNORE INTO runs (id, started_at, finished_at, status) VALUES (?, ?, ?, ?)",
                    (run["id"], run["started_at"], run.get("finished_at"), run["status"]),
                )
                if cur.rowcount > 0:
                    counts["runs"] += 1

            for step in snapshot.get("steps", []):
                cur = self._conn.execute(
                    "INSERT OR IGNORE INTO steps (id, run_id, name, tier, success, output, error, started_at, finished_at, inputs) "
                    "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
                    (
                        step["id"], step["run_id"], step["name"], step["tier"], int(step["success"]),
                        json.dumps(step["output"]), step.get("error"), step["started_at"], step["finished_at"],
                        json.dumps(step.get("inputs") or {}),
                    ),
                )
                if cur.rowcount > 0:
                    counts["steps"] += 1

            for schedule in snapshot.get("schedules", []):
                cur = self._conn.execute(
                    "INSERT OR IGNORE INTO schedules (id, kind, tier, name, inputs, interval_seconds, enabled, "
                    "next_run_at, last_run_at, last_status, created_at, schedule_type, daily_time, day_of_week) "
                    "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
                    (
                        schedule["id"], schedule["kind"], schedule.get("tier"), schedule["name"],
                        json.dumps(schedule["inputs"]), schedule["interval_seconds"], int(schedule["enabled"]),
                        schedule["next_run_at"], schedule.get("last_run_at"), schedule.get("last_status"),
                        schedule["created_at"], schedule.get("schedule_type", "interval"), schedule.get("daily_time"),
                        schedule.get("day_of_week"),
                    ),
                )
                if cur.rowcount > 0:
                    counts["schedules"] += 1

            for entry in snapshot.get("ingested_files", []):
                cur = self._conn.execute(
                    "INSERT OR IGNORE INTO ingested_files (hash, filename, kind, ingested_at) VALUES (?, ?, ?, ?)",
                    (entry["hash"], entry["filename"], entry["kind"], entry["ingested_at"]),
                )
                if cur.rowcount > 0:
                    counts["ingested_files"] += 1

            for entry in snapshot.get("memory", []):
                cur = self._conn.execute(
                    "INSERT OR IGNORE INTO memory (key, value, updated_at) VALUES (?, ?, ?)",
                    (
                        entry["key"],
                        json.dumps(entry["value"]),
                        entry.get("updated_at") or datetime.now(timezone.utc).isoformat(),
                    ),
                )
                if cur.rowcount > 0:
                    counts["memory"] += 1

            self._conn.commit()
        return counts

    def close(self) -> None:
        with self._lock:
            self._conn.close()
