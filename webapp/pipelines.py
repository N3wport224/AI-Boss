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


def validate_pipeline(definition: dict, tier_dirs: dict) -> dict:
    """Validate a pipeline definition against the modules actually on disk.

    Returns a normalized definition (with a `slug`). Raises
    PipelineValidationError with a human-readable message on any problem.
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
        tier = step.get("tier")
        step_name = step.get("name")
        if (tier, step_name) not in lookup:
            raise PipelineValidationError(f"Step {index + 1}: no {tier} module named '{step_name}'.")

        for field, mapping in (step.get("mappings") or {}).items():
            source_index = mapping.get("step")
            output_name = mapping.get("output")
            if not isinstance(source_index, int) or not (0 <= source_index < index):
                raise PipelineValidationError(
                    f"Step {index + 1}: mapping for '{field}' must reference an earlier step."
                )
            source_step = steps[source_index]
            source_manifest = lookup[(source_step.get("tier"), source_step.get("name"))]
            output_names = {o["name"] for o in source_manifest.get("outputs", [])}
            if output_name not in output_names:
                raise PipelineValidationError(
                    f"Step {index + 1}: Step {source_index + 1} has no declared output '{output_name}'."
                )

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
