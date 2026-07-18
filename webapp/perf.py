"""Live process performance snapshot for the header ticker.

Reports the AI-Boss process's own resource usage, not the host machine's —
this is a single-process local tool, so "how is this app doing" means "how
is this one process doing," not a system-wide monitor.
"""
import os
import time

import psutil

_process = psutil.Process(os.getpid())
_start_time = time.time()

# psutil's first cpu_percent() call always returns 0.0 (it needs a baseline
# measurement) -- call it once now so the first real request already gets a
# meaningful number instead of a misleading zero.
_process.cpu_percent(interval=None)


def snapshot(active_run_threads: int = 0) -> dict:
    mem = _process.memory_info()
    return {
        "pid": _process.pid,
        "cpu_percent": _process.cpu_percent(interval=None),
        "memory_rss_mb": round(mem.rss / (1024 * 1024), 1),
        "thread_count": _process.num_threads(),
        "active_run_threads": active_run_threads,
        "uptime_seconds": round(time.time() - _start_time, 1),
    }
