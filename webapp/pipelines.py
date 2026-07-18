"""Storage and validation for user-built, no-code pipelines.

A pipeline is just a named sequence of existing automation/workflow/agent
modules — each with its own static input values and, optionally, field
mappings pulling a value from an earlier step's declared output. Persistence
mirrors the same YAML-manifest convention already used by automations/,
workflows/, and agents/: `pipelines/<slug>.yaml`.
"""
import re
from pathlib import Path

import yaml

from engine.conditions import OPERATORS as CONDITION_OPERATORS
from engine.registry import load_manifests

PIPELINES_DIR = Path(__file__).resolve().parent.parent / "pipelines"


class PipelineValidationError(ValueError):
    pass


def _slugify(name: str) -> str:
    slug = re.sub(r"[^a-z0-9]+", "_", name.strip().lower()).strip("_")
    if not slug:
        raise PipelineValidationError("Pipeline name must contain at least one letter or number.")
    return slug


def _manifest_lookup(tier_dirs: dict) -> dict:
    lookup = {}
    for tier, directory in tier_dirs.items():
        for manifest in load_manifests(directory):
            lookup[(tier, manifest["name"])] = manifest
    return lookup


def is_parallel_step(step: dict) -> bool:
    return step.get("type") == "parallel"


def _declared_outputs(step: dict, lookup: dict) -> set:
    """Output names a top-level step contributes to context: its own manifest's
    outputs for a module step, or the union of every branch's for a group."""
    if is_parallel_step(step):
        names = set()
        for branch in step.get("branches") or []:
            manifest = lookup.get((branch.get("tier"), branch.get("name")), {})
            names |= {o["name"] for o in manifest.get("outputs", [])}
        return names
    manifest = lookup.get((step.get("tier"), step.get("name")), {})
    return {o["name"] for o in manifest.get("outputs", [])}


def _validate_module_step(step: dict, index: int, steps: list, lookup: dict, label: str) -> None:
    tier = step.get("tier")
    step_name = step.get("name")
    if (tier, step_name) not in lookup:
        raise PipelineValidationError(f"{label}: no {tier} module named '{step_name}'.")

    for field, mapping in (step.get("mappings") or {}).items():
        source_index = mapping.get("step")
        output_name = mapping.get("output")
        # A mapping may only reference a strictly earlier *top-level* step —
        # for a branch inside a group, "earlier" means before the whole group,
        # never a sibling branch (branches run concurrently, with no ordering).
        if not isinstance(source_index, int) or not (0 <= source_index < index):
            raise PipelineValidationError(
                f"{label}: mapping for '{field}' must reference an earlier step."
            )
        output_names = _declared_outputs(steps[source_index], lookup)
        # A dotted path (e.g. "insight.risk_level") reaches into a nested
        # key of the declared output — only the base name is checked here,
        # since the manifest doesn't describe a nested output's shape.
        base_output = (output_name or "").split(".")[0]
        if base_output not in output_names:
            raise PipelineValidationError(
                f"{label}: Step {source_index + 1} has no declared output '{base_output}'."
            )

    condition = step.get("condition")
    if condition:
        if not (condition.get("source") or "").strip():
            raise PipelineValidationError(f"{label}: condition needs a context key to check.")
        if condition.get("operator") not in CONDITION_OPERATORS:
            raise PipelineValidationError(
                f"{label}: unknown condition operator '{condition.get('operator')}'."
            )


def _normalize_module_step(step: dict) -> dict:
    normalized = {
        "tier": step.get("tier"),
        "name": step.get("name"),
        "inputs": step.get("inputs") or {},
        "mappings": step.get("mappings") or {},
    }
    if step.get("condition"):
        normalized["condition"] = step["condition"]
    return normalized


def _normalize_step(step: dict) -> dict:
    """Strip Pydantic's `condition: null` / `type: null` / unused-field noise
    so the saved YAML only carries what each step kind actually uses."""
    if is_parallel_step(step):
        normalized = {"type": "parallel", "branches": [_normalize_module_step(b) for b in step.get("branches") or []]}
        if (step.get("name") or "").strip():
            normalized["name"] = step["name"].strip()
        return normalized
    return _normalize_module_step(step)


def validate_pipeline(definition: dict, tier_dirs: dict) -> dict:
    """Validate a pipeline definition against the modules actually on disk.

    A top-level step is either a module step ({tier, name, ...}) or a parallel
    group ({type: "parallel", branches: [module steps...]}) whose branches run
    concurrently in that slot. Returns a normalized definition (with a `slug`).
    Raises PipelineValidationError with a human-readable message on any problem.
    """
    name = (definition.get("name") or "").strip()
    if not name:
        raise PipelineValidationError("Pipeline name is required.")
    slug = _slugify(name)

    steps = definition.get("steps") or []
    if not steps:
        raise PipelineValidationError("A pipeline needs at least one step.")

    lookup = _manifest_lookup(tier_dirs)
    for index, step in enumerate(steps):
        if is_parallel_step(step):
            branches = step.get("branches") or []
            if len(branches) < 2:
                raise PipelineValidationError(
                    f"Step {index + 1}: a parallel group needs at least two branches."
                )
            for branch_index, branch in enumerate(branches):
                _validate_module_step(
                    branch, index, steps, lookup,
                    label=f"Step {index + 1}, branch {branch_index + 1}",
                )
        else:
            _validate_module_step(step, index, steps, lookup, label=f"Step {index + 1}")

    steps = [_normalize_step(step) for step in steps]

    return {
        "name": name,
        "slug": slug,
        "description": definition.get("description") or "",
        "steps": steps,
    }


def save_pipeline(definition: dict, tier_dirs: dict) -> dict:
    validated = validate_pipeline(definition, tier_dirs)
    PIPELINES_DIR.mkdir(exist_ok=True)
    path = PIPELINES_DIR / f"{validated['slug']}.yaml"
    path.write_text(yaml.safe_dump(validated, sort_keys=False))
    return validated


def list_pipelines() -> list[dict]:
    if not PIPELINES_DIR.exists():
        return []
    return [yaml.safe_load(path.read_text()) or {} for path in sorted(PIPELINES_DIR.glob("*.yaml"))]


def load_pipeline(slug: str) -> dict:
    path = PIPELINES_DIR / f"{slug}.yaml"
    if not path.exists():
        raise FileNotFoundError(slug)
    return yaml.safe_load(path.read_text()) or {}


def duplicate_pipeline(slug: str, tier_dirs: dict) -> dict:
    """Clone a saved pipeline under a new, non-colliding name — same steps,
    same mappings, ready to tweak independently of the original."""
    original = load_pipeline(slug)

    base_name = f"{original['name']} (copy)"
    candidate_name = base_name
    counter = 2
    while (PIPELINES_DIR / f"{_slugify(candidate_name)}.yaml").exists():
        candidate_name = f"{base_name} {counter}"
        counter += 1

    new_definition = {
        "name": candidate_name,
        "description": original.get("description", ""),
        "steps": original["steps"],
    }
    return save_pipeline(new_definition, tier_dirs)
