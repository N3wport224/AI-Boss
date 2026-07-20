"""Startup / runtime diagnostics.

Validates that every manifest parses, every entrypoint actually imports and
instantiates, and the state store is reachable — so a broken manifest or a
typo'd entrypoint shows up as a clear "Degraded" beacon with a specific reason,
instead of surfacing later as a confusing 404 or 500 when someone clicks Run.
"""
import os
from pathlib import Path

import yaml

from engine.redaction import _is_secret_key
from engine.registry import instantiate, load_manifests

OPTIONAL_ENV_KEYS = ["ANTHROPIC_API_KEY"]


def environment_report(env_example_path: Path) -> list[dict]:
    """One entry per environment variable the project declares (the keys in
    .env.example plus anything the health check watches): whether it's set,
    and a safe preview. Secret-shaped names (per the same key heuristic the
    redactor uses) only ever reveal their length — everything else shows its
    actual value, since LOG_LEVEL=INFO isn't worth hiding."""
    declared: list[str] = []
    if env_example_path.exists():
        for line in env_example_path.read_text().splitlines():
            line = line.strip()
            if not line or line.startswith("#") or "=" not in line:
                continue
            declared.append(line.split("=", 1)[0].strip())
    for key in OPTIONAL_ENV_KEYS:
        if key not in declared:
            declared.append(key)

    entries = []
    for key in declared:
        value = os.environ.get(key)
        is_secret = _is_secret_key(key)
        if not value:
            preview = None
        elif is_secret:
            preview = f"set ({len(value)} characters, hidden)"
        else:
            preview = value
        entries.append({"name": key, "set": bool(value), "secret": is_secret, "preview": preview})
    return entries


def _check_manifests(tier_dirs: dict) -> tuple[bool, str]:
    problems = []
    total = 0
    for tier, directory in tier_dirs.items():
        directory = Path(directory)
        if not directory.exists():
            problems.append(f"{tier}: directory '{directory}' does not exist")
            continue
        for manifest_path in sorted(directory.glob("*.yaml")):
            total += 1
            try:
                manifest = yaml.safe_load(manifest_path.read_text()) or {}
            except yaml.YAMLError as exc:
                problems.append(f"{manifest_path.name}: invalid YAML ({exc})")
                continue
            if not manifest.get("name") or not manifest.get("entrypoint"):
                problems.append(f"{manifest_path.name}: missing required 'name' or 'entrypoint'")

    ok = not problems
    detail = f"{total} manifest(s) parsed across {len(tier_dirs)} tier(s)." if ok else "; ".join(problems)
    return ok, detail


def _check_entrypoints(tier_dirs: dict) -> tuple[bool, str]:
    problems = []
    total = 0
    for tier, directory in tier_dirs.items():
        for manifest in load_manifests(directory):
            if not manifest.get("enabled", True):
                continue
            total += 1
            try:
                instantiate(manifest["entrypoint"])
            except Exception as exc:
                problems.append(f"{manifest.get('name', '?')}: {exc}")

    ok = not problems
    detail = f"{total} module(s) import and instantiate cleanly." if ok else "; ".join(problems)
    return ok, detail


def _check_environment() -> tuple[bool, str]:
    missing = [key for key in OPTIONAL_ENV_KEYS if not os.environ.get(key)]
    if not missing:
        return True, "All optional environment variables are set."
    # Missing is not an error today — nothing bundled requires these yet — so this
    # never marks the app degraded, only surfaces as an informational note.
    return True, f"Not set (optional, no bundled module needs it yet): {', '.join(missing)}"


def _check_state_store(store) -> tuple[bool, str]:
    try:
        store.recent_runs(1)
        return True, f"SQLite state store reachable at {store.db_path}."
    except Exception as exc:
        return False, str(exc)


def run_health_checks(tier_dirs: dict, store) -> dict:
    checks = [
        {"name": "state_store", **dict(zip(("ok", "detail"), _check_state_store(store)))},
        {"name": "manifests", **dict(zip(("ok", "detail"), _check_manifests(tier_dirs)))},
        {"name": "entrypoints", **dict(zip(("ok", "detail"), _check_entrypoints(tier_dirs)))},
        {"name": "environment", **dict(zip(("ok", "detail"), _check_environment()))},
    ]
    overall_ok = all(check["ok"] for check in checks)
    return {"status": "ready" if overall_ok else "degraded", "checks": checks}


def run_liveness_check(store, background_threads: dict) -> dict:
    """A cheap liveness probe, distinct from run_health_checks() above: no
    manifest parsing or entrypoint instantiation (that's readiness-grade
    diagnostic work, not something a monitoring system should pay for on
    every poll). Just the two things that mean the process is actually
    still doing its job -- the database answers a trivial query, and every
    named background thread (scheduler/watcher/auto-backup) is still
    running -- so an external prober (a container orchestrator, a cron
    job) can catch "the process is up but its scheduler thread silently
    died" without touching the module registry at all.

    `background_threads` maps a name to an object with an `is_alive()`
    method (Scheduler/FilesystemWatcher/AutoBackup all have one)."""
    checks = {"database": dict(zip(("ok", "detail"), _check_state_store(store)))}
    for name, component in background_threads.items():
        alive = component.is_alive()
        checks[name] = {"ok": alive, "detail": "running" if alive else "not running"}
    overall_ok = all(check["ok"] for check in checks.values())
    return {"status": "ok" if overall_ok else "degraded", "checks": checks}
