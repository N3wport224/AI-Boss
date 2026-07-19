"""Storage and validation for user-built, no-code pipelines.

A pipeline is just a named sequence of existing automation/workflow/agent
modules — each with its own static input values and, optionally, field
mappings pulling a value from an earlier step's declared output. Persistence
mirrors the same YAML-manifest convention already used by automations/,
workflows/, and agents/: `pipelines/<slug>.yaml`.
"""
import re
from datetime import datetime, timezone
from pathlib import Path

import yaml

from engine.conditions import OPERATORS as CONDITION_OPERATORS
from engine.registry import load_manifests

PIPELINES_DIR = Path(__file__).resolve().parent.parent / "pipelines"
_VERSIONS_DIR_NAME = "_versions"
_VERSION_ID_FORMAT = "%Y%m%dT%H%M%S%f"


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

    retry = step.get("retry")
    if retry:
        if not isinstance(retry.get("max_retries"), int) or retry["max_retries"] < 0:
            raise PipelineValidationError(f"{label}: retry max_retries must be a non-negative integer.")
        if not isinstance(retry.get("backoff_seconds"), (int, float)) or retry["backoff_seconds"] < 0:
            raise PipelineValidationError(f"{label}: retry backoff_seconds must be a non-negative number.")


def _normalize_module_step(step: dict) -> dict:
    normalized = {
        "tier": step.get("tier"),
        "name": step.get("name"),
        "inputs": step.get("inputs") or {},
        "mappings": step.get("mappings") or {},
    }
    if step.get("condition"):
        normalized["condition"] = step["condition"]
    if step.get("retry") and step["retry"].get("max_retries"):
        normalized["retry"] = step["retry"]
    return normalized


def _normalize_step(step: dict) -> dict:
    """Strip Pydantic's `condition: null` / `type: null` / unused-field noise
    so the saved YAML only carries what each step kind actually uses."""
    if is_parallel_step(step):
        normalized = {"type": "parallel", "branches": [_normalize_module_step(b) for b in step.get("branches") or []]}
        if (step.get("name") or "").strip():
            normalized["name"] = step["name"].strip()
        if (step.get("note") or "").strip():
            normalized["note"] = step["note"].strip()
        return normalized
    normalized = _normalize_module_step(step)
    if (step.get("note") or "").strip():
        normalized["note"] = step["note"].strip()
    return normalized


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


def _versions_dir(slug: str) -> Path:
    return PIPELINES_DIR / _VERSIONS_DIR_NAME / slug


def _archive_current_version(slug: str) -> None:
    """Snapshot a pipeline's current on-disk YAML into its version history
    before it gets overwritten — by a builder re-save, an import under an
    existing name, or a restore. A no-op if the pipeline doesn't exist yet
    (a brand-new pipeline has no prior version to keep)."""
    path = PIPELINES_DIR / f"{slug}.yaml"
    if not path.exists():
        return
    versions_dir = _versions_dir(slug)
    versions_dir.mkdir(parents=True, exist_ok=True)
    version_id = datetime.now(timezone.utc).strftime(_VERSION_ID_FORMAT)
    version_path = versions_dir / f"{version_id}.yaml"
    # Two saves within the same microsecond (rare, but possible on a fast
    # machine/test run) would otherwise silently overwrite one archived
    # version with another — disambiguate with a numeric suffix instead.
    suffix = 2
    while version_path.exists():
        version_path = versions_dir / f"{version_id}-{suffix}.yaml"
        suffix += 1
    version_path.write_text(path.read_text())


def save_pipeline(definition: dict, tier_dirs: dict) -> dict:
    validated = validate_pipeline(definition, tier_dirs)
    PIPELINES_DIR.mkdir(exist_ok=True)
    path = PIPELINES_DIR / f"{validated['slug']}.yaml"
    _archive_current_version(validated["slug"])
    path.write_text(yaml.safe_dump(validated, sort_keys=False))
    return validated


def list_pipeline_versions(slug: str) -> list[dict]:
    """Every archived version of a pipeline, newest first. The version id is
    itself a fixed-width UTC timestamp (plus an occasional `-N` disambiguator
    suffix, see `_archive_current_version`), so lexicographic and
    chronological order coincide on the timestamp portion — no separate sort
    key needed."""
    versions_dir = _versions_dir(slug)
    if not versions_dir.exists():
        return []
    versions = []
    for version_path in sorted(versions_dir.glob("*.yaml"), reverse=True):
        version_id = version_path.stem
        timestamp_part = version_id.split("-")[0]
        saved_at = datetime.strptime(timestamp_part, _VERSION_ID_FORMAT).replace(tzinfo=timezone.utc)
        versions.append({"version_id": version_id, "saved_at": saved_at.isoformat()})
    return versions


def load_pipeline_version(slug: str, version_id: str) -> dict:
    version_path = _versions_dir(slug) / f"{version_id}.yaml"
    if not version_path.exists():
        raise FileNotFoundError(version_id)
    return yaml.safe_load(version_path.read_text()) or {}


def restore_pipeline_version(slug: str, version_id: str, tier_dirs: dict) -> dict:
    """Restore an archived version back to current. Re-validated against
    modules on disk today — a version saved when a module existed may no
    longer validate if that module's manifest changed or was removed since.
    The current definition is archived first (via save_pipeline's own
    archiving), so a restore is itself undoable with another restore."""
    old_definition = load_pipeline_version(slug, version_id)
    return save_pipeline(old_definition, tier_dirs)


def branch_pipeline_version(slug: str, version_id: str, new_name: str, tier_dirs: dict) -> dict:
    """Restore an archived version as a brand-new saved pipeline under
    `new_name`, leaving the pipeline at `slug` (and its own current
    definition) completely untouched — the branching counterpart to
    `restore_pipeline_version()`'s in-place overwrite, for when the old
    version is worth keeping around as its own pipeline rather than
    replacing what's there now."""
    old_definition = load_pipeline_version(slug, version_id)
    new_slug = _slugify(new_name)
    if (PIPELINES_DIR / f"{new_slug}.yaml").exists():
        raise PipelineValidationError(f"A pipeline named '{new_name}' already exists.")
    new_definition = {
        "name": new_name,
        "description": old_definition.get("description", ""),
        "steps": old_definition["steps"],
    }
    return save_pipeline(new_definition, tier_dirs)


def list_pipelines() -> list[dict]:
    if not PIPELINES_DIR.exists():
        return []
    return [yaml.safe_load(path.read_text()) or {} for path in sorted(PIPELINES_DIR.glob("*.yaml"))]


def load_pipeline(slug: str) -> dict:
    path = PIPELINES_DIR / f"{slug}.yaml"
    if not path.exists():
        raise FileNotFoundError(slug)
    return yaml.safe_load(path.read_text()) or {}


def pipelines_using_module(tier: str, name: str) -> list[str]:
    """Slugs of every saved pipeline with a step (or parallel branch) that
    references the given tier+name -- the blast radius a user should see
    before disabling, deleting, or duplicating a module out from under it."""
    slugs = []
    for definition in list_pipelines():
        for step in definition.get("steps", []):
            if is_parallel_step(step):
                refs = step.get("branches") or []
            else:
                refs = [step]
            if any(ref.get("tier") == tier and ref.get("name") == name for ref in refs):
                slugs.append(definition.get("slug", ""))
                break
    return slugs


def search_pipelines(query: str, max_results: int = 20) -> list[dict]:
    """Case-insensitive keyword search across every saved pipeline's raw
    YAML text -- not just the name/description shown by the search omnibar,
    but also step-level details like input values, mapped fields, and
    conditions. Returns one entry per matching pipeline with a short
    snippet showing where the match was found, mirroring
    ingestion.search_artifacts()'s content-search pattern."""
    query_lower = query.lower().strip()
    if not query_lower or not PIPELINES_DIR.exists():
        return []

    results = []
    for path in sorted(PIPELINES_DIR.glob("*.yaml"), key=lambda p: p.stat().st_mtime, reverse=True):
        text = path.read_text()
        idx = text.lower().find(query_lower)
        if idx == -1:
            continue

        start = max(0, idx - 60)
        end = min(len(text), idx + len(query_lower) + 60)
        snippet = " ".join(text[start:end].split())
        results.append({"slug": path.stem, "snippet": snippet})
        if len(results) >= max_results:
            break
    return results


def delete_pipeline(slug: str) -> None:
    """Remove a saved pipeline's current definition. Its archived version
    history under `_versions/<slug>/` is deliberately left in place —
    `list_pipeline_versions()`/`restore_pipeline_version()` don't require the
    current file to exist, so a deleted pipeline can still be brought back
    by restoring any of its prior versions."""
    path = PIPELINES_DIR / f"{slug}.yaml"
    if not path.exists():
        raise FileNotFoundError(slug)
    path.unlink()


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


def rename_pipeline(old_slug: str, new_name: str) -> dict:
    """Rename a saved pipeline in place -- distinct from duplicate_pipeline(),
    which clones it under a new slug and leaves the original untouched. Since
    a pipeline's slug is just its name slugified, a rename that changes the
    slug means moving the YAML file (and any archived version history) to
    the new slug rather than merely rewriting a field; callers are
    responsible for migrating anything else keyed by slug (pipeline tags,
    schedules referencing this pipeline by name) since those live in the
    state store, not here."""
    definition = load_pipeline(old_slug)
    new_slug = _slugify(new_name)

    if new_slug == old_slug:
        definition["name"] = new_name
        (PIPELINES_DIR / f"{old_slug}.yaml").write_text(yaml.safe_dump(definition, sort_keys=False))
        return definition

    new_path = PIPELINES_DIR / f"{new_slug}.yaml"
    if new_path.exists():
        raise PipelineValidationError(f"A pipeline named '{new_name}' already exists.")

    definition["name"] = new_name
    definition["slug"] = new_slug
    new_path.write_text(yaml.safe_dump(definition, sort_keys=False))
    (PIPELINES_DIR / f"{old_slug}.yaml").unlink()

    old_versions_dir = _versions_dir(old_slug)
    if old_versions_dir.exists():
        old_versions_dir.rename(_versions_dir(new_slug))

    return definition


def save_pipeline_from_template(template: dict, tier_dirs: dict) -> dict:
    """Clone a built-in starter template (see webapp/templates.py) into the
    user's own saved pipelines. Uses the template's own name unless that
    name's slug is already taken (e.g. this template was cloned before),
    in which case a numeric suffix disambiguates it — same non-colliding
    naming scheme as `duplicate_pipeline`."""
    base_name = template["name"]
    candidate_name = base_name
    counter = 2
    while (PIPELINES_DIR / f"{_slugify(candidate_name)}.yaml").exists():
        candidate_name = f"{base_name} ({counter})"
        counter += 1

    new_definition = {
        "name": candidate_name,
        "description": template.get("description", ""),
        "steps": template["steps"],
    }
    return save_pipeline(new_definition, tier_dirs)
