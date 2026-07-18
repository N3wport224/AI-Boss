"""Read-only source + lint view for a module.

The file path is always resolved server-side from a manifest's own
`entrypoint` (via `importlib.util.find_spec`, the same mechanism Python itself
uses to import it) — a client can never hand this module an arbitrary path.
"""
import importlib.util
import json
import subprocess
from pathlib import Path


class SourceNotFoundError(Exception):
    pass


def resolve_source_path(entrypoint: str) -> Path:
    module_path, _, _ = entrypoint.partition(":")
    spec = importlib.util.find_spec(module_path)
    if spec is None or spec.origin is None:
        raise SourceNotFoundError(f"Could not resolve source file for '{module_path}'.")
    return Path(spec.origin)


def lint_source(path: Path) -> list[dict]:
    """Run `ruff check` against exactly this one file. `ruff` exits 1 when it
    finds issues (not an error condition) and >1 on a genuine tool failure —
    either way we only care about parsing whatever JSON it printed."""
    try:
        result = subprocess.run(
            ["ruff", "check", "--output-format=json", "--no-cache", str(path)],
            capture_output=True,
            text=True,
            timeout=10,
        )
    except (FileNotFoundError, subprocess.TimeoutExpired):
        return []

    if not result.stdout.strip():
        return []
    try:
        issues = json.loads(result.stdout)
    except json.JSONDecodeError:
        return []

    return [
        {
            "line": issue["location"]["row"],
            "column": issue["location"]["column"],
            "code": issue["code"],
            "message": issue["message"],
        }
        for issue in issues
    ]


def read_module_source(entrypoint: str) -> dict:
    path = resolve_source_path(entrypoint)
    return {
        "path": str(path),
        "source": path.read_text(),
        "issues": lint_source(path),
    }
