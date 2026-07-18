"""Visual control center for the orchestration engine.

Every automation, workflow, and agent becomes a card with a one-click Run button
and, if it declares `inputs` in its manifest, a small form. No terminal required —
this is a thin HTTP layer over the exact same engine the CLI uses, so both share
one run history in the same SQLite state store.
"""
from pathlib import Path
from typing import Any

from fastapi import FastAPI, HTTPException
from fastapi.responses import FileResponse
from fastapi.staticfiles import StaticFiles
from pydantic import BaseModel

from engine import Orchestrator, StateStore, discover
from engine.registry import instantiate, load_manifests

ROOT = Path(__file__).resolve().parent.parent
STATIC_DIR = Path(__file__).parent / "static"
TIER_DIRS = {
    "automation": ROOT / "automations",
    "workflow": ROOT / "workflows",
    "agent": ROOT / "agents",
}

app = FastAPI(title="AI-Boss Control Center")
store = StateStore(str(ROOT / "orchestrator.db"))


class RunRequest(BaseModel):
    inputs: dict[str, Any] = {}


def _manifest_by_name(tier: str, name: str) -> dict:
    if tier not in TIER_DIRS:
        raise HTTPException(status_code=404, detail=f"Unknown tier '{tier}'")
    for manifest in load_manifests(TIER_DIRS[tier]):
        if manifest.get("name") == name:
            return manifest
    raise HTTPException(status_code=404, detail=f"No {tier} module named '{name}'")


def _coerce_inputs(manifest: dict, raw_inputs: dict) -> dict:
    """Apply each input field's declared type and default, ignoring unknown keys."""
    coerced = {}
    for field in manifest.get("inputs", []):
        key = field["name"]
        value = raw_inputs.get(key, field.get("default"))
        field_type = field.get("type", "text")
        if field_type == "number" and value not in (None, ""):
            value = float(value)
        elif field_type == "toggle":
            value = bool(value)
        coerced[key] = value
    return coerced


def _build_pipeline():
    pipeline = []
    for tier in ("automation", "workflow", "agent"):
        pipeline.extend(discover(TIER_DIRS[tier]))
    return pipeline


@app.get("/api/modules")
def list_modules():
    result = {}
    for tier, directory in TIER_DIRS.items():
        modules = []
        for manifest in load_manifests(directory):
            if not manifest.get("enabled", True):
                continue
            last_success = store.latest_step_status(manifest["name"])
            modules.append(
                {
                    "name": manifest["name"],
                    "tier": tier,
                    "description": manifest.get("description", ""),
                    "inputs": manifest.get("inputs", []),
                    "status": "error" if last_success is False else "ready",
                }
            )
        result[tier] = modules
    return result


@app.post("/api/modules/{tier}/{name}/run")
def run_module(tier: str, name: str, request: RunRequest):
    manifest = _manifest_by_name(tier, name)
    module = instantiate(manifest["entrypoint"])
    inputs = _coerce_inputs(manifest, request.inputs)

    orchestrator = Orchestrator([module], state_store=store, stop_on_error=False)
    context = orchestrator.run(inputs)
    step = context.history[-1]

    return {
        "success": step.success,
        "error": step.error,
        "output": step.output,
        "context": context.variables,
    }


@app.post("/api/pipeline/run")
def run_pipeline(request: RunRequest):
    orchestrator = Orchestrator(_build_pipeline(), state_store=store, stop_on_error=False)
    context = orchestrator.run(request.inputs)

    return {
        "context": context.variables,
        "steps": [
            {"name": s.name, "tier": s.tier, "success": s.success, "error": s.error}
            for s in context.history
        ],
    }


@app.get("/api/runs")
def recent_runs(limit: int = 10):
    return store.recent_runs(limit)


app.mount("/static", StaticFiles(directory=STATIC_DIR), name="static")


@app.get("/")
def index():
    return FileResponse(STATIC_DIR / "index.html")
