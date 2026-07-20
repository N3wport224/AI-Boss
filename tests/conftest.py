import os
import tempfile

import pytest

# Point the app at a fresh, throwaway database for this pytest session,
# BEFORE any test module imports webapp.main (conftest is always imported
# first). Two problems disappear at once: the suite can no longer read or
# destroy a developer's real orchestrator.db, and reruns are deterministic --
# previously a second run within the cache TTL hit stale result_cache rows
# from the first run and dozens of cache/count-sensitive tests failed.
# setdefault, not assignment, so a caller can still point the suite at a
# specific store deliberately.
_TEST_DB_DIR = tempfile.mkdtemp(prefix="aiboss-test-store-")
os.environ.setdefault("AIBOSS_DB_PATH", os.path.join(_TEST_DB_DIR, "orchestrator.db"))


@pytest.fixture(autouse=True)
def _reset_run_rate_limiter():
    """The run-trigger rate limiter is a module-level singleton shared by
    every test that imports `webapp.main` (Python only loads the module
    once), so without a reset the whole test suite's cumulative request
    count could trip it well before any single test actually means to
    exercise that behavior. Clearing it before each test keeps tests
    isolated from each other regardless of run order or how fast they fire.
    Also restores max_requests/window_seconds to the hardcoded default and
    clears any persisted override afterward, so a test that exercises the
    runtime-configurable rate limit can never leak a changed limit into
    whatever runs next."""
    from webapp.main import (
        DEFAULT_RATE_LIMIT_MAX_REQUESTS,
        DEFAULT_RATE_LIMIT_WINDOW_SECONDS,
        _run_rate_limiter,
        store,
    )

    _run_rate_limiter._hits.clear()
    yield
    store.clear_rate_limit_override()
    _run_rate_limiter.max_requests = DEFAULT_RATE_LIMIT_MAX_REQUESTS
    _run_rate_limiter.window_seconds = DEFAULT_RATE_LIMIT_WINDOW_SECONDS
    _run_rate_limiter._hits.clear()


@pytest.fixture(autouse=True)
def _clear_leftover_schedules():
    """The scheduler thread starts at webapp.main import time and stays live
    for the whole test session, polling the shared on-disk StateStore. Any
    test that creates an enabled interval schedule and doesn't delete it
    leaves a live timer behind: 30-second-interval schedules from earlier
    tests (or from a previous pytest process, since the store is persistent)
    fire mid-suite and pollute run counts, notifications, metrics, and the
    audit log for unrelated tests. Deleting every schedule before each test
    closes both windows -- a schedule can only ever exist for the duration
    of the single test that created it, which is shorter than the minimum
    allowed interval, so it can never fire on its own timer."""
    from webapp.main import store

    for schedule in store.list_schedules():
        store.delete_schedule(schedule["id"])
    yield


@pytest.fixture(autouse=True)
def _reset_all_circuit_breakers():
    """Circuit breaker state lives in the shared, process-wide StateStore
    every test file's TestClient(app) points at. A module's breaker is
    recorded from the same background worker thread that publishes its SSE
    events, one statement after `bus.publish()` -- so a test that awaits
    "run_failed"/"step_failed" over the stream and then immediately resets
    or asserts on breaker state has no guarantee that recording has actually
    happened yet (no happens-before relationship is enforced between that
    worker thread and whichever thread the client's stream-read resumes on).
    A test that intentionally trips a breaker can still race its own
    best-effort reset and leave it tripped for whatever runs next. Resetting
    before every test in the whole suite closes that window globally,
    rather than chasing it file by file."""
    from webapp.main import store

    for entry in store.all_module_health():
        store.reset_breaker(entry["tier"], entry["name"])
    yield
