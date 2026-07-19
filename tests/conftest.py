import pytest


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
