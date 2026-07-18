"""Visual control center for the orchestration engine.

Every automation, workflow, and agent becomes a card with a one-click Run button
and, if it declares `inputs` in its manifest, a small form. No terminal required —
this is a thin HTTP layer over the exact same engine the CLI uses, so both share
one run history in the same SQLite state store.

Runs execute in a background thread and stream their progress back over SSE
(`/api/stream/{stream_id}`), so the dashboard can show a live step tracker and,
for Tier 3 agents, a running "thought" log — not just a final result once the
whole thing is done.
"""
import csv
import io
import json
import threading
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from fastapi import FastAPI, File, HTTPException, UploadFile
from fastapi.responses import FileResponse, StreamingResponse
from fastapi.staticfiles import StaticFiles
from pydantic import BaseModel

from engine import Orchestrator, StateStore, StepSpec, interpolate_template_fields
from engine.registry import instantiate, load_manifests

from . import health, ingestion, linting, pipelines as pipeline_store
from .events import RunEventBus
from .watcher import FilesystemWatcher, WATCH_DIR, ensure_watch_dir

ROOT = Path(__file__).resolve().parent.parent
STATIC_DIR = Path(__file__).parent / "static"
TIER_DIRS = {
    "automation": ROOT / "automations",
    "workflow": ROOT / "workflows",
    "agent": ROOT / "agents",
}

app = FastAPI(title="AI-Boss Control Center")
store = StateStore(str(ROOT / "orchestrator.db"))
bus = RunEventBus()

ingestion.ensure_artifacts_dir()
ingestion.purge_old_artifacts(older_than_hours=24 * 7)  # sweep anything left over a week ago

_watcher_log: list[dict] = []


def _on_watched_file(path: Path) -> None:
    """Auto-ingest a file the moment it appears in watched_input/ — same CSV/PDF
    handling as a manual upload, just triggered by the filesystem instead of a click."""
    suffix = path.suffix.lower()
    entry = {"filename": path.name, "at": datetime.now(timezone.utc).isoformat()}
    try:
        data = path.read_bytes()
        if suffix == ".csv":
            result = ingestion.ingest_csv_bytes(path.name, data, store)
        elif suffix == ".pdf":
            result = ingestion.ingest_pdf_bytes(path.name, data, store)
        else:
            entry["error"] = f"Unsupported file type '{suffix}' — only .csv and .pdf are auto-ingested."
            _watcher_log.append(entry)
            return
        entry["duplicate"] = result.get("duplicate", False)
    except Exception as exc:
        entry["error"] = str(exc)
    _watcher_log.append(entry)


ensure_watch_dir()
_watcher = FilesystemWatcher(on_new_file=_on_watched_file, interval=2.0)
_watcher.start()


class RunRequest(BaseModel):
    inputs: dict[str, Any] = {}


class MappingSpec(BaseModel):
    step: int
    output: str


class PipelineStepSpec(BaseModel):
    tier: str
    name: str
    inputs: dict[str, Any] = {}
    mappings: dict[str, MappingSpec] = {}


class PipelineDefinition(BaseModel):
    name: str
    description: str = ""
    steps: list[PipelineStepSpec]


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


def _build_pipeline() -> list[StepSpec]:
    """The fixed tier-1 -> tier-2 -> tier-3 pipeline, run with each module's own
    manifest defaults. Wrapped in StepSpecs (not bare modules) so any `template`
    field's default text — e.g. "{signups} signups, {churn} churn events" — still
    gets interpolated against whatever's in context by the time that step runs,
    the same as a hand-built pipeline's own template fields."""
    steps = []
    for tier in ("automation", "workflow", "agent"):
        for manifest in load_manifests(TIER_DIRS[tier]):
            if not manifest.get("enabled", True):
                continue
            module = instantiate(manifest["entrypoint"])
            defaults = _coerce_inputs(manifest, {})

            def seed(ctx, manifest=manifest, defaults=defaults):
                return interpolate_template_fields(manifest, defaults, ctx.get)

            steps.append(StepSpec(module=module, seed=seed))
    return steps


def _run_in_background(orchestrator: Orchestrator, inputs: dict, stream_id: int) -> None:
    def worker() -> None:
        try:
            orchestrator.run(inputs, on_event=lambda event: bus.publish(stream_id, event))
        finally:
            bus.close(stream_id)

    threading.Thread(target=worker, daemon=True).start()


def _build_steps_from_definition(definition: dict) -> list[StepSpec]:
    """Turn a saved pipeline definition into StepSpecs, wiring each field mapping
    to read from whatever context key the source step's output actually landed
    under — resolved at run time, once that earlier step has actually executed.
    """
    steps = []
    for step in definition["steps"]:
        manifest = _manifest_by_name(step["tier"], step["name"])
        module = instantiate(manifest["entrypoint"])
        static_inputs = _coerce_inputs(manifest, step.get("inputs") or {})
        mappings = step.get("mappings") or {}

        def seed(ctx, static=static_inputs, maps=mappings, manifest=manifest):
            resolved = dict(static)
            for field, mapping in maps.items():
                resolved[field] = ctx.get(mapping["output"])
            return interpolate_template_fields(manifest, resolved, ctx.get)

        steps.append(StepSpec(module=module, seed=seed))
    return steps


def _launch_steps(steps: list[StepSpec]) -> int:
    orchestrator = Orchestrator(steps, state_store=store, stop_on_error=False)
    stream_id = bus.create()
    _run_in_background(orchestrator, {}, stream_id)
    return stream_id


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
                    "outputs": manifest.get("outputs", []),
                    "status": "error" if last_success is False else "ready",
                }
            )
        result[tier] = modules
    return result


@app.get("/api/modules/{tier}/{name}/source")
def module_source(tier: str, name: str):
    manifest = _manifest_by_name(tier, name)
    try:
        return linting.read_module_source(manifest["entrypoint"])
    except linting.SourceNotFoundError as exc:
        raise HTTPException(status_code=404, detail=str(exc))


@app.post("/api/modules/{tier}/{name}/run")
def run_module(tier: str, name: str, request: RunRequest):
    manifest = _manifest_by_name(tier, name)
    module = instantiate(manifest["entrypoint"])
    inputs = _coerce_inputs(manifest, request.inputs)
    # A template field can reference {a_sibling_field} on this same card; there's
    # no earlier pipeline step here, so the lookup is just the inputs dict itself.
    inputs = interpolate_template_fields(manifest, inputs, inputs.get)

    orchestrator = Orchestrator([module], state_store=store, stop_on_error=False)
    stream_id = bus.create()
    _run_in_background(orchestrator, inputs, stream_id)
    return {"stream_id": stream_id}


@app.post("/api/pipeline/run")
def run_pipeline(request: RunRequest):
    orchestrator = Orchestrator(_build_pipeline(), state_store=store, stop_on_error=False)
    stream_id = bus.create()
    _run_in_background(orchestrator, request.inputs, stream_id)
    return {"stream_id": stream_id}


@app.get("/api/pipelines")
def list_saved_pipelines():
    return pipeline_store.list_pipelines()


@app.post("/api/pipelines")
def save_and_launch_pipeline(definition: PipelineDefinition):
    """One-click save-and-launch for a pipeline built in the visual sequencer:
    persists it to pipelines/<slug>.yaml (the same convention as every other
    module folder) and immediately runs it through the same background-thread
    + SSE mechanism as any other run."""
    try:
        saved = pipeline_store.save_pipeline(definition.model_dump(), TIER_DIRS)
    except pipeline_store.PipelineValidationError as exc:
        raise HTTPException(status_code=400, detail=str(exc))

    steps = _build_steps_from_definition(saved)
    stream_id = _launch_steps(steps)
    return {"pipeline": saved, "stream_id": stream_id}


@app.post("/api/pipelines/{slug}/run")
def run_saved_pipeline(slug: str):
    try:
        definition = pipeline_store.load_pipeline(slug)
    except FileNotFoundError:
        raise HTTPException(status_code=404, detail=f"No saved pipeline named '{slug}'.")

    steps = _build_steps_from_definition(definition)
    stream_id = _launch_steps(steps)
    return {"stream_id": stream_id}


@app.post("/api/pipelines/{slug}/duplicate")
def duplicate_pipeline(slug: str):
    try:
        duplicated = pipeline_store.duplicate_pipeline(slug, TIER_DIRS)
    except FileNotFoundError:
        raise HTTPException(status_code=404, detail=f"No saved pipeline named '{slug}'.")
    return {"pipeline": duplicated}


@app.get("/api/stream/{stream_id}")
def stream_events(stream_id: int):
    def event_source():
        for event in bus.stream(stream_id):
            yield f"data: {json.dumps(event)}\n\n"

    return StreamingResponse(event_source(), media_type="text/event-stream", headers={"Cache-Control": "no-cache"})


@app.get("/api/runs")
def recent_runs(limit: int = 10):
    return store.recent_runs(limit)


@app.get("/api/runs.csv")
def recent_runs_csv(limit: int = 100):
    buffer = io.StringIO()
    writer = csv.DictWriter(buffer, fieldnames=["id", "started_at", "finished_at", "status", "duration_seconds"])
    writer.writeheader()
    for run in store.recent_runs(limit):
        duration = None
        if run["started_at"] and run["finished_at"]:
            duration = round(
                (datetime.fromisoformat(run["finished_at"]) - datetime.fromisoformat(run["started_at"])).total_seconds(),
                3,
            )
        writer.writerow({**run, "duration_seconds": duration})

    return StreamingResponse(
        iter([buffer.getvalue()]),
        media_type="text/csv",
        headers={"Content-Disposition": "attachment; filename=run_history.csv"},
    )


@app.get("/api/metrics")
def metrics():
    return store.metrics_summary()


@app.get("/api/health")
def health_check():
    return health.run_health_checks(TIER_DIRS, store)


@app.post("/api/ingest/csv")
async def ingest_csv(file: UploadFile = File(...)):
    """Upload a CSV, get back structured JSON records. Identical files (by
    content hash, not filename) are reported as duplicates instead of being
    reprocessed. Shared with the filesystem watcher (webapp/watcher.py)."""
    data = await file.read()
    try:
        return ingestion.ingest_csv_bytes(file.filename, data, store)
    except Exception as exc:
        raise HTTPException(status_code=400, detail=f"Could not parse CSV: {exc}")


@app.post("/api/ingest/pdf")
async def ingest_pdf(file: UploadFile = File(...)):
    """Upload a PDF, get back its extracted text. Same content-hash dedupe as CSV,
    same shared implementation as the filesystem watcher."""
    data = await file.read()
    try:
        return ingestion.ingest_pdf_bytes(file.filename, data, store)
    except Exception as exc:
        raise HTTPException(status_code=400, detail=f"Could not read PDF: {exc}")


@app.get("/api/artifacts")
def list_artifacts():
    return ingestion.list_artifacts()


@app.post("/api/artifacts/purge")
def purge_artifacts(older_than_hours: float = 24):
    removed = ingestion.purge_old_artifacts(older_than_hours)
    return {"removed_count": len(removed), "removed": removed}


@app.get("/api/watcher/status")
def watcher_status():
    return {"watch_dir": str(WATCH_DIR), "processed": _watcher_log[-20:]}


app.mount("/static", StaticFiles(directory=STATIC_DIR), name="static")


@app.get("/")
def index():
    return FileResponse(STATIC_DIR / "index.html")
