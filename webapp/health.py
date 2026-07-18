"""Startup / runtime diagnostics.

Validates that every manifest parses, every entrypoint actually imports and
instantiates, and the state store is reachable — so a broken manifest or a
typo'd entrypoint shows up as a clear "Degraded" beacon with a specific reason,
instead of surfacing later as a confusing 404 or 500 when someone clicks Run.
"""
import os
from pathlib import Path

import yaml

from engine.registry import instantiate, load_manifests

OPTIONAL_ENV_KEYS = ["ANTHROPIC_API_KEY"]


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
