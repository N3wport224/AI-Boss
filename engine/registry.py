import importlib
from pathlib import Path

import yaml

from .base import BaseModule


def _instantiate(entrypoint: str) -> BaseModule:
    module_path, class_name = entrypoint.split(":")
    module = importlib.import_module(module_path)
    cls = getattr(module, class_name)
    return cls()


def discover(directory: Path) -> list[BaseModule]:
    """Load every enabled `*.yaml` manifest in a tier directory and instantiate its module.

    Dropping a new `<name>.py` + `<name>.yaml` pair into `automations/`, `workflows/`,
    or `agents/` is enough to add it to the pipeline — no core engine code changes needed.
    """
    modules = []
    for manifest_path in sorted(Path(directory).glob("*.yaml")):
        manifest = yaml.safe_load(manifest_path.read_text()) or {}
        if not manifest.get("enabled", True):
            continue
        modules.append(_instantiate(manifest["entrypoint"]))
    return modules
