import pytest


@pytest.fixture(autouse=True)
def _reset_run_rate_limiter():
    """The run-trigger rate limiter is a module-level singleton shared by
    every test that imports `webapp.main` (Python only loads the module
    once), so without a reset the whole test suite's cumulative request
    count could trip it well before any single test actually means to
    exercise that behavior. Clearing it before each test keeps tests
    isolated from each other regardless of run order or how fast they fire."""
    from webapp.main import _run_rate_limiter

    _run_rate_limiter._hits.clear()
    yield
