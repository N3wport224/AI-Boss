"""Lightweight, dependency-free filesystem watcher.

Polls `watched_input/` (gitignored, same as artifacts/) every few seconds
instead of pulling in the `watchdog` package — good enough for "drop a file
in, it gets processed automatically" without adding a new dependency for
something a simple loop already does.
"""
import os
import threading
from pathlib import Path
from typing import Callable

from engine.logging_config import get_logger

logger = get_logger(__name__)

# Overridable so a test session (or a second deployment) can watch its own
# directory instead of sharing the repo-root one with a live dev server.
WATCH_DIR = Path(os.environ.get("AIBOSS_WATCH_DIR", Path(__file__).resolve().parent.parent / "watched_input"))


def ensure_watch_dir() -> Path:
    WATCH_DIR.mkdir(exist_ok=True)
    return WATCH_DIR


class FilesystemWatcher:
    """Calls `on_new_file(path)` once for each file that appears in WATCH_DIR
    after the watcher starts. Files already present at startup are treated as
    already-seen, not replayed as "new" on every restart."""

    def __init__(self, on_new_file: Callable[[Path], None], interval: float = 2.0):
        self.on_new_file = on_new_file
        self.interval = interval
        self._seen: set[str] = set()
        self._thread: threading.Thread | None = None
        self._stop = threading.Event()

    def _scan_once(self) -> None:
        ensure_watch_dir()
        for path in sorted(WATCH_DIR.iterdir()):
            if path.is_file() and path.name not in self._seen:
                self._seen.add(path.name)
                # "watched_filename", not "filename" -- the latter is a
                # reserved LogRecord attribute (the source file of the log
                # call itself) that extra={} can't override.
                logger.info("watcher detected file", extra={"watched_filename": path.name})
                try:
                    self.on_new_file(path)
                except Exception as exc:
                    logger.warning(
                        "watcher ingest handler failed",
                        extra={"watched_filename": path.name, "error": str(exc)},
                    )

    def _loop(self) -> None:
        while not self._stop.is_set():
            self._scan_once()
            self._stop.wait(self.interval)

    def start(self) -> None:
        if self._thread is not None:
            return
        ensure_watch_dir()
        self._seen = {p.name for p in WATCH_DIR.iterdir() if p.is_file()}
        self._thread = threading.Thread(target=self._loop, daemon=True)
        self._thread.start()

    def stop(self) -> None:
        self._stop.set()
        # Join so teardown can't close shared resources under a scan
        # that's still running.
        if self._thread is not None:
            self._thread.join(timeout=5.0)

    def is_alive(self) -> bool:
        """Whether the polling thread is up and running -- used by the
        liveness health check."""
        return self._thread is not None and self._thread.is_alive()
