import importlib
from pathlib import Path

import yaml

from .base import BaseModule


def instantiate(entrypoint: str) -> BaseModule:
    """Instantiate a module from an `module.path:ClassName` entrypoint string."""
    module_path, class_name = entrypoint.split(":")
    module = importlib.import_module(module_path)
    cls = getattr(module, class_name)
    return cls()


def load_manifests(directory: Path) -> list[dict]:
    """Return every `*.yaml` manifest dict in a tier directory (enabled or not).

    Used by the dashboard/API to read module metadata (name, description, input
    schema) without instantiating anything.
    """
    manifests = []
    for manifest_path in sorted(Path(directory).glob("*.yaml")):
        manifest = yaml.safe_load(manifest_path.read_text()) or {}
        manifest.setdefault("inputs", [])
        manifests.append(manifest)
    return manifests


def discover(directory: Path) -> list[BaseModule]:
    """Load every enabled `*.yaml` manifest in a tier directory and instantiate its module.

    Dropping a new `<name>.py` + `<name>.yaml` pair into `automations/`, `workflows/`,
    or `agents/` is enough to add it to the pipeline — no core engine code changes needed.
    """
    modules = []
    for manifest in load_manifests(directory):
        if not manifest.get("enabled", True):
            continue
        modules.append(instantiate(manifest["entrypoint"]))
    return modules
