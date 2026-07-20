"""A background timer that periodically writes a full state-store
snapshot to disk as a timestamped JSON file, pruning old snapshots
beyond a keep-count -- a belt-and-suspenders safety net alongside the
existing manual GET /api/backup/export, for a user who forgets to
export by hand. Writes to disk only; never touches git (per this
project's standing rule that no automatic git operation is ever wired
up from the running webapp itself).
"""
import json
import threading
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Callable, Optional

BACKUP_FILENAME_FORMAT = "backup_%Y%m%dT%H%M%SZ.json"


class AutoBackup:
    def __init__(
        self,
        backups_dir: Path,
        build_snapshot: Callable[[], dict],
        poll_interval: float = 60.0,
        on_failure: Optional[Callable[[Exception], None]] = None,
        get_protected: Optional[Callable[[], set]] = None,
    ):
        self.backups_dir = backups_dir
        self.build_snapshot = build_snapshot
        self.poll_interval = poll_interval
        self.on_failure = on_failure
        self.get_protected = get_protected or (lambda: set())
        self.enabled = False
        self.interval_hours = 24.0
        self.keep_count = 7
        self.last_backup_at: Optional[str] = None
        self._stop = threading.Event()
        self._thread: Optional[threading.Thread] = None
        self._lock = threading.Lock()

    def configure(self, enabled: bool, interval_hours: float, keep_count: int) -> None:
        with self._lock:
            self.enabled = enabled
            self.interval_hours = interval_hours
            self.keep_count = keep_count

    def status(self) -> dict:
        with self._lock:
            next_backup_at = None
            if self.enabled and self.last_backup_at:
                next_dt = datetime.fromisoformat(self.last_backup_at) + timedelta(hours=self.interval_hours)
                next_backup_at = next_dt.isoformat()
            return {
                "enabled": self.enabled,
                "interval_hours": self.interval_hours,
                "keep_count": self.keep_count,
                "last_backup_at": self.last_backup_at,
                "next_backup_at": next_backup_at,
            }

    def start(self) -> None:
        if self._thread is not None:
            return
        self._thread = threading.Thread(target=self._loop, daemon=True)
        self._thread.start()

    def stop(self) -> None:
        self._stop.set()
        # Join so teardown can't close the StateStore while a snapshot
        # write is still in progress.
        if self._thread is not None:
            self._thread.join(timeout=5.0)

    def run_now(self) -> str:
        """Write a snapshot immediately, outside the timer's own interval
        check -- used by the manual "back up now" trigger and by tests."""
        now = datetime.now(timezone.utc)
        self._write_snapshot(now)
        with self._lock:
            keep_count = self.keep_count
        self._prune(keep_count)
        with self._lock:
            self.last_backup_at = now.isoformat()
        return self.last_backup_at

    def _loop(self) -> None:
        while not self._stop.is_set():
            self._tick()
            self._stop.wait(self.poll_interval)

    def _tick(self) -> None:
        with self._lock:
            enabled = self.enabled
            interval_hours = self.interval_hours
            last_backup_at = self.last_backup_at
        if not enabled:
            return
        now = datetime.now(timezone.utc)
        if last_backup_at is not None:
            last_dt = datetime.fromisoformat(last_backup_at)
            if (now - last_dt).total_seconds() < interval_hours * 3600:
                return
        try:
            self.run_now()
        except Exception as exc:
            # A write failure here (disk full, permissions, etc.) must not
            # kill the background thread's loop -- swallow it, but surface
            # it via on_failure so the caller can raise a notification, and
            # leave last_backup_at untouched so the next tick retries.
            if self.on_failure is not None:
                self.on_failure(exc)

    def _write_snapshot(self, now: datetime) -> None:
        self.backups_dir.mkdir(exist_ok=True)
        filename = now.strftime(BACKUP_FILENAME_FORMAT)
        path = self.backups_dir / filename
        snapshot = self.build_snapshot()
        path.write_text(json.dumps(snapshot, indent=2, default=str))

    def _prune(self, keep_count: int) -> None:
        if not self.backups_dir.exists():
            return
        protected = self.get_protected()
        files = sorted(self.backups_dir.glob("backup_*.json"), key=lambda p: p.name, reverse=True)
        for stale in files[keep_count:]:
            if stale.name in protected:
                continue
            stale.unlink()

    def purge_older_than(self, hours: float) -> int:
        """Delete snapshot files older than `hours`, by the timestamp
        encoded in their own filename -- an age-based counterpart to the
        keep_count-based _prune() the background timer already does after
        every write, for a manual "clear out anything past this age"
        rather than "always keep exactly N". A snapshot pinned via
        get_protected() survives this regardless of age. Returns how many
        files were deleted."""
        if not self.backups_dir.exists():
            return 0
        protected = self.get_protected()
        cutoff = datetime.now(timezone.utc) - timedelta(hours=hours)
        deleted = 0
        for path in self.backups_dir.glob("backup_*.json"):
            if path.name in protected:
                continue
            try:
                timestamp = datetime.strptime(path.stem, "backup_%Y%m%dT%H%M%SZ").replace(tzinfo=timezone.utc)
            except ValueError:
                continue
            if timestamp < cutoff:
                path.unlink()
                deleted += 1
        return deleted
