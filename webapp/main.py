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
import contextlib
import csv
import io
import json
import threading
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Callable, Optional

from fastapi import FastAPI, File, HTTPException, Request, UploadFile
from fastapi.responses import FileResponse, StreamingResponse
from fastapi.staticfiles import StaticFiles
from pydantic import BaseModel

from engine import (
    Orchestrator,
    ParallelGroup,
    StateStore,
    StepSpec,
    evaluate_condition,
    interpolate_template_fields,
    redact_secrets,
    resolve_path,
)
from engine.registry import instantiate, load_manifests

from . import cache, graph, health, ingestion, linting, perf, pipelines as pipeline_store
from .events import RunEventBus
from .ratelimit import RateLimiter
from .scheduler import Scheduler
from .watcher import FilesystemWatcher, WATCH_DIR, ensure_watch_dir

ROOT = Path(__file__).resolve().parent.parent
STATIC_DIR = Path(__file__).parent / "static"
TIER_DIRS = {
    "automation": ROOT / "automations",
    "workflow": ROOT / "workflows",
    "agent": ROOT / "agents",
}

# Applied to every StepSpec built here so one hung module can't wedge a run
# forever; a card or pipeline step that genuinely needs longer can still be
# reached by raising this, but nothing today runs anywhere close to it.
DEFAULT_STEP_TIMEOUT_SECONDS = 60.0

store = StateStore(str(ROOT / "orchestrator.db"))
bus = RunEventBus()

# Guards every run-triggering endpoint against an accidental request storm —
# generous enough for normal interactive use, tight enough to catch a stuck
# retry loop or a misconfigured schedule hammering the thread pool.
_run_rate_limiter = RateLimiter(max_requests=30, window_seconds=10.0)


def _enforce_run_rate_limit(request: Request) -> None:
    key = request.client.host if request.client else "unknown"
    if not _run_rate_limiter.allow(key):
        raise HTTPException(
            status_code=429,
            detail="Too many run requests in a short window — slow down and try again shortly.",
        )

# Background run threads currently in flight, so shutdown can give them a
# chance to finish (or at least stop waiting deliberately) instead of the
# process disappearing out from under them mid-run.
_active_run_threads: set[threading.Thread] = set()
_active_run_threads_lock = threading.Lock()


@contextlib.asynccontextmanager
async def lifespan(app: FastAPI):
    yield
    with _active_run_threads_lock:
        threads = list(_active_run_threads)
    for thread in threads:
        thread.join(timeout=5.0)
    _watcher.stop()
    _scheduler.stop()
    store.close()


app = FastAPI(title="AI-Boss Control Center", lifespan=lifespan)

ingestion.ensure_artifacts_dir()
ingestion.purge_old_artifacts(older_than_hours=24 * 7)  # sweep anything left over a week ago

_watcher_log: list[dict] = []


def _on_watched_file(path: Path) -> None:
    """Auto-ingest a file the moment it appears in watched_input/ — same
    CSV/PDF/JSON handling as a manual upload, just triggered by the
    filesystem instead of a click."""
    suffix = path.suffix.lower()
    entry = {"filename": path.name, "at": datetime.now(timezone.utc).isoformat()}
    try:
        size = path.stat().st_size
        if size > ingestion.MAX_UPLOAD_BYTES:
            entry["error"] = (
                f"File is {size} bytes, exceeding the "
                f"{ingestion.MAX_UPLOAD_BYTES // (1024 * 1024)} MB auto-ingest limit — skipped."
            )
            _watcher_log.append(entry)
            return

        data = path.read_bytes()
        if suffix == ".csv":
            result = ingestion.ingest_csv_bytes(path.name, data, store)
        elif suffix == ".pdf":
            result = ingestion.ingest_pdf_bytes(path.name, data, store)
        elif suffix == ".json":
            result = ingestion.ingest_json_bytes(path.name, data, store)
        else:
            entry["error"] = f"Unsupported file type '{suffix}' — only .csv, .pdf, and .json are auto-ingested."
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
    force_refresh: bool = False


class MappingSpec(BaseModel):
    step: int
    output: str


class ConditionSpec(BaseModel):
    source: str  # context key, may be a dotted path (e.g. "insight.risk_level")
    operator: str  # one of engine.conditions.OPERATORS
    value: Any = None  # literal to compare against (unused for truthy/falsy)


class PipelineStepSpec(BaseModel):
    # A module step ({tier, name, ...}) or, when type == "parallel", a group
    # of branches run concurrently in this slot ({type, branches, name?}).
    tier: str = ""
    name: str = ""
    inputs: dict[str, Any] = {}
    mappings: dict[str, MappingSpec] = {}
    condition: Optional[ConditionSpec] = None
    type: Optional[str] = None
    branches: Optional[list["PipelineStepSpec"]] = None


PipelineStepSpec.model_rebuild()


class PipelineDefinition(BaseModel):
    name: str
    description: str = ""
    steps: list[PipelineStepSpec]


MIN_SCHEDULE_INTERVAL_SECONDS = 10.0


class ScheduleCreate(BaseModel):
    kind: str  # "module" or "pipeline"
    tier: Optional[str] = None  # required when kind == "module"
    name: str  # module name, or saved pipeline slug
    inputs: dict[str, Any] = {}  # only used when kind == "module"
    interval_seconds: float


class ScheduleUpdate(BaseModel):
    enabled: bool


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
            # Modules that make a real outbound call (e.g. http_request) opt
            # out of the fixed one-click demo pipeline so it stays fast,
            # deterministic, and offline; they're still runnable standalone
            # and includable in any hand-built pipeline via the builder.
            if not manifest.get("include_in_full_pipeline", True):
                continue
            module = instantiate(manifest["entrypoint"])
            defaults = _coerce_inputs(manifest, {})

            def seed(ctx, manifest=manifest, defaults=defaults):
                return interpolate_template_fields(manifest, defaults, ctx.get)

            steps.append(StepSpec(module=module, seed=seed, timeout_seconds=DEFAULT_STEP_TIMEOUT_SECONDS))
    return steps


# ---- Circuit breaker ----
# After N *consecutive* failures (per module, threshold overridable with a
# `circuit_breaker_threshold` manifest key) the breaker trips and every
# launch path — module card, full pipeline, saved pipelines, schedules —
# refuses to run that module until it's explicitly reset. A success closes
# a failure streak but never closes an already-open breaker.

DEFAULT_BREAKER_THRESHOLD = 3


def _breaker_threshold(tier: str, name: str) -> int:
    try:
        manifest = _manifest_by_name(tier, name)
    except HTTPException:
        return DEFAULT_BREAKER_THRESHOLD
    return int(manifest.get("circuit_breaker_threshold", DEFAULT_BREAKER_THRESHOLD))


def _record_breaker_event(event: dict) -> None:
    kind = event.get("kind")
    if kind == "step_completed":
        store.record_module_success(event["tier"], event["name"])
    elif kind == "step_failed":
        tier, name = event["tier"], event["name"]
        health = store.record_module_failure(tier, name, _breaker_threshold(tier, name))
        if health["tripped"] and health["consecutive_failures"] == _breaker_threshold(tier, name):
            print(f"[breaker] circuit opened for {tier}/{name} after {health['consecutive_failures']} consecutive failures")


def _module_refs_from_steps(steps: list[dict]) -> list[tuple[str, str]]:
    """(tier, name) of every module a definition's steps would run — branches
    of a parallel group included."""
    refs = []
    for step in steps:
        if pipeline_store.is_parallel_step(step):
            refs.extend((b["tier"], b["name"]) for b in step.get("branches") or [])
        else:
            refs.append((step["tier"], step["name"]))
    return refs


def _ensure_breakers_closed(module_refs: list[tuple[str, str]]) -> None:
    tripped = []
    for tier, name in dict.fromkeys(module_refs):  # de-dupe, keep order
        health = store.get_module_health(tier, name)
        if health["tripped"]:
            tripped.append(f"{tier}/{name} ({health['consecutive_failures']} consecutive failures)")
    if tripped:
        raise HTTPException(
            status_code=409,
            detail=(
                f"Circuit breaker open for {', '.join(tripped)}. "
                "Reset it from the module card to allow runs again."
            ),
        )


def _run_in_background(
    orchestrator: Orchestrator,
    inputs: dict,
    stream_id: int,
    on_step_completed: Optional[Callable[[dict], None]] = None,
) -> None:
    def handle_event(event: dict) -> None:
        bus.publish(stream_id, event)
        _record_breaker_event(event)
        if on_step_completed is not None and event.get("kind") == "step_completed":
            on_step_completed(event["output"])

    def worker() -> None:
        thread = threading.current_thread()
        try:
            orchestrator.run(inputs, on_event=handle_event)
        finally:
            bus.close(stream_id)
            with _active_run_threads_lock:
                _active_run_threads.discard(thread)

    thread = threading.Thread(target=worker, daemon=True)
    with _active_run_threads_lock:
        _active_run_threads.add(thread)
    thread.start()


def _replay_cached_result(tier: str, name: str, output: dict) -> dict:
    """Synthesize the same step_started/step_completed/run_completed event
    shape a real run would emit, so the frontend's existing SSE handling
    doesn't need a special case for a cache hit — just a `cached: true` flag."""
    stream_id = bus.create()
    bus.publish(stream_id, {"kind": "step_started", "index": 0, "tier": tier, "name": name})
    bus.publish(
        stream_id,
        {
            "kind": "step_completed",
            "index": 0,
            "tier": tier,
            "name": name,
            "output": output,
            "duration_ms": 0.0,
            "cached": True,
        },
    )
    bus.publish(stream_id, {"kind": "run_completed", "context": output, "cached": True})
    bus.close(stream_id)
    return {"stream_id": stream_id, "cached": True}


def _module_step_to_spec(step: dict) -> StepSpec:
    """One module step's StepSpec: static inputs, run-time-resolved mappings
    (a mapping may be a dotted path into a nested output key), template-field
    interpolation, and an optional skip-unless condition."""
    manifest = _manifest_by_name(step["tier"], step["name"])
    module = instantiate(manifest["entrypoint"])
    static_inputs = _coerce_inputs(manifest, step.get("inputs") or {})
    mappings = step.get("mappings") or {}

    def seed(ctx, static=static_inputs, maps=mappings, manifest=manifest):
        resolved = dict(static)
        for field, mapping in maps.items():
            resolved[field] = resolve_path(mapping["output"], ctx.get)
        return interpolate_template_fields(manifest, resolved, ctx.get)

    condition_fn, condition_label = None, ""
    raw_condition = step.get("condition") or None
    if raw_condition:
        source = raw_condition["source"]
        operator = raw_condition["operator"]
        value = raw_condition.get("value")

        def condition_fn(ctx, source=source, operator=operator, value=value):
            return evaluate_condition(resolve_path(source, ctx.get), operator, value)

        condition_label = f"{source} {operator}" + ("" if operator in ("truthy", "falsy") else f" {value}")

    return StepSpec(
        module=module,
        seed=seed,
        timeout_seconds=DEFAULT_STEP_TIMEOUT_SECONDS,
        condition=condition_fn,
        condition_label=condition_label,
    )


def _build_steps_from_definition(definition: dict) -> list:
    """Turn a saved pipeline definition into the orchestrator's step list —
    StepSpecs for module steps, ParallelGroups (of branch StepSpecs) for
    `type: "parallel"` steps."""
    steps = []
    for step in definition["steps"]:
        if pipeline_store.is_parallel_step(step):
            branches = [_module_step_to_spec(branch) for branch in step.get("branches") or []]
            steps.append(ParallelGroup(steps=branches, name=step.get("name") or "parallel_group"))
        else:
            steps.append(_module_step_to_spec(step))
    return steps


def _launch_steps(steps: list[StepSpec]) -> int:
    orchestrator = Orchestrator(steps, state_store=store, stop_on_error=False)
    stream_id = bus.create()
    _run_in_background(orchestrator, {}, stream_id)
    return stream_id


def _run_scheduled(steps: list[StepSpec], inputs: dict) -> None:
    """Run steps in the background the same way any other launch does, but
    without an SSE stream — nobody is watching a scheduled run live, and a
    bus queue nobody ever drains would just leak. The run still lands in
    the same StateStore, so it shows up in Recent Runs like any other."""
    orchestrator = Orchestrator(steps, state_store=store, stop_on_error=False)

    def worker() -> None:
        thread = threading.current_thread()
        try:
            orchestrator.run(inputs, on_event=_record_breaker_event)
        finally:
            with _active_run_threads_lock:
                _active_run_threads.discard(thread)

    thread = threading.Thread(target=worker, daemon=True)
    with _active_run_threads_lock:
        _active_run_threads.add(thread)
    thread.start()


def _trigger_schedule(schedule: dict) -> None:
    # Raising here is deliberate: the Scheduler records the exception text as
    # the schedule's last_status, so a schedule blocked by an open breaker
    # says so in the Schedules panel instead of silently not running.
    if schedule["kind"] == "pipeline":
        definition = pipeline_store.load_pipeline(schedule["name"])
        _ensure_breakers_closed(_module_refs_from_steps(definition["steps"]))
        steps = _build_steps_from_definition(definition)
        _run_scheduled(steps, {})
    else:
        _ensure_breakers_closed([(schedule["tier"], schedule["name"])])
        manifest = _manifest_by_name(schedule["tier"], schedule["name"])
        module = instantiate(manifest["entrypoint"])
        inputs = _coerce_inputs(manifest, schedule["inputs"])
        inputs = interpolate_template_fields(manifest, inputs, inputs.get)
        step = StepSpec(module=module, timeout_seconds=DEFAULT_STEP_TIMEOUT_SECONDS)
        _run_scheduled([step], inputs)


_scheduler = Scheduler(store, _trigger_schedule)
_scheduler.start()


@app.get("/api/modules")
def list_modules():
    result = {}
    for tier, directory in TIER_DIRS.items():
        modules = []
        for manifest in load_manifests(directory):
            if not manifest.get("enabled", True):
                continue
            last_success = store.latest_step_status(manifest["name"])
            health = store.get_module_health(tier, manifest["name"])
            modules.append(
                {
                    "name": manifest["name"],
                    "tier": tier,
                    "description": manifest.get("description", ""),
                    "inputs": manifest.get("inputs", []),
                    "outputs": manifest.get("outputs", []),
                    "status": "error" if last_success is False else "ready",
                    "breaker": {
                        "tripped": health["tripped"],
                        "consecutive_failures": health["consecutive_failures"],
                        "threshold": int(manifest.get("circuit_breaker_threshold", DEFAULT_BREAKER_THRESHOLD)),
                    },
                }
            )
        result[tier] = modules
    return result


@app.get("/api/modules/{tier}/{name}/source")
async def module_source(tier: str, name: str):
    manifest = _manifest_by_name(tier, name)
    try:
        return await linting.read_module_source_async(manifest["entrypoint"])
    except linting.SourceNotFoundError as exc:
        raise HTTPException(status_code=404, detail=str(exc))


@app.post("/api/modules/{tier}/{name}/run")
def run_module(tier: str, name: str, payload: RunRequest, http_request: Request):
    _enforce_run_rate_limit(http_request)
    manifest = _manifest_by_name(tier, name)
    _ensure_breakers_closed([(tier, name)])
    module = instantiate(manifest["entrypoint"])
    inputs = _coerce_inputs(manifest, payload.inputs)
    # A template field can reference {a_sibling_field} on this same card; there's
    # no earlier pipeline step here, so the lookup is just the inputs dict itself.
    inputs = interpolate_template_fields(manifest, inputs, inputs.get)

    cache_key = cache.make_cache_key(tier, name, inputs)
    if not payload.force_refresh:
        cached = store.get_cached_result(cache_key)
        if cached is not None:
            return _replay_cached_result(tier, name, cached)

    step = StepSpec(module=module, timeout_seconds=DEFAULT_STEP_TIMEOUT_SECONDS)
    orchestrator = Orchestrator([step], state_store=store, stop_on_error=False)
    stream_id = bus.create()

    def cache_on_success(output: dict) -> None:
        store.set_cached_result(cache_key, tier, name, output)

    _run_in_background(orchestrator, inputs, stream_id, on_step_completed=cache_on_success)
    return {"stream_id": stream_id}


@app.post("/api/pipeline/run")
def run_pipeline(payload: RunRequest, http_request: Request):
    _enforce_run_rate_limit(http_request)
    steps = _build_pipeline()
    _ensure_breakers_closed([(s.module.tier.value, s.module.name) for s in steps])
    orchestrator = Orchestrator(steps, state_store=store, stop_on_error=False)
    stream_id = bus.create()
    _run_in_background(orchestrator, payload.inputs, stream_id)
    return {"stream_id": stream_id}


@app.get("/api/breakers")
def list_breakers():
    """Every module the breaker has ever seen fail (or succeed after failing) —
    modules with no history simply aren't listed, which reads as 'closed'."""
    return store.all_module_health()


@app.post("/api/breakers/{tier}/{name}/reset")
def reset_module_breaker(tier: str, name: str):
    _manifest_by_name(tier, name)  # 404 for a module that doesn't exist
    return store.reset_breaker(tier, name)


@app.get("/api/pipelines")
def list_saved_pipelines():
    return pipeline_store.list_pipelines()


@app.post("/api/pipelines")
def save_and_launch_pipeline(definition: PipelineDefinition, http_request: Request):
    """One-click save-and-launch for a pipeline built in the visual sequencer:
    persists it to pipelines/<slug>.yaml (the same convention as every other
    module folder) and immediately runs it through the same background-thread
    + SSE mechanism as any other run."""
    _enforce_run_rate_limit(http_request)
    _ensure_breakers_closed(_module_refs_from_steps(definition.model_dump()["steps"]))
    try:
        saved = pipeline_store.save_pipeline(definition.model_dump(), TIER_DIRS)
    except pipeline_store.PipelineValidationError as exc:
        raise HTTPException(status_code=400, detail=str(exc))

    steps = _build_steps_from_definition(saved)
    stream_id = _launch_steps(steps)
    return {"pipeline": saved, "stream_id": stream_id}


@app.post("/api/pipelines/{slug}/run")
def run_saved_pipeline(slug: str, http_request: Request):
    _enforce_run_rate_limit(http_request)
    try:
        definition = pipeline_store.load_pipeline(slug)
    except FileNotFoundError:
        raise HTTPException(status_code=404, detail=f"No saved pipeline named '{slug}'.")

    _ensure_breakers_closed(_module_refs_from_steps(definition["steps"]))
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


@app.get("/api/pipelines/{slug}/graph")
def pipeline_graph(slug: str):
    try:
        definition = pipeline_store.load_pipeline(slug)
    except FileNotFoundError:
        raise HTTPException(status_code=404, detail=f"No saved pipeline named '{slug}'.")
    return graph.build_pipeline_graph(definition)


@app.get("/api/schedules")
def list_schedules():
    return store.list_schedules()


@app.post("/api/schedules")
def create_schedule(payload: ScheduleCreate):
    if payload.kind not in ("module", "pipeline"):
        raise HTTPException(status_code=400, detail="kind must be 'module' or 'pipeline'")
    if payload.interval_seconds < MIN_SCHEDULE_INTERVAL_SECONDS:
        raise HTTPException(
            status_code=400,
            detail=f"interval_seconds must be at least {MIN_SCHEDULE_INTERVAL_SECONDS}",
        )

    if payload.kind == "module":
        if not payload.tier:
            raise HTTPException(status_code=400, detail="tier is required when kind is 'module'")
        _manifest_by_name(payload.tier, payload.name)  # 404s if it doesn't exist
    else:
        try:
            pipeline_store.load_pipeline(payload.name)
        except FileNotFoundError:
            raise HTTPException(status_code=404, detail=f"No saved pipeline named '{payload.name}'.")

    next_run_at = datetime.now(timezone.utc).isoformat()
    schedule = store.create_schedule(
        kind=payload.kind,
        name=payload.name,
        interval_seconds=payload.interval_seconds,
        next_run_at=next_run_at,
        tier=payload.tier,
        inputs=payload.inputs,
    )
    return schedule


@app.patch("/api/schedules/{schedule_id}")
def update_schedule(schedule_id: int, payload: ScheduleUpdate):
    schedule = store.set_schedule_enabled(schedule_id, payload.enabled)
    if schedule is None:
        raise HTTPException(status_code=404, detail=f"No schedule with id {schedule_id}.")
    return schedule


@app.delete("/api/schedules/{schedule_id}")
def delete_schedule(schedule_id: int):
    store.delete_schedule(schedule_id)
    return {"deleted": schedule_id}


@app.get("/api/stream/{stream_id}")
def stream_events(stream_id: int, request: Request):
    # A reconnecting EventSource sends back whatever `id:` it last saw, so a
    # dropped connection (network blip, browser reload) resumes exactly where
    # it left off instead of replaying from scratch or losing events.
    last_event_id = request.headers.get("last-event-id")
    from_index = int(last_event_id) + 1 if last_event_id is not None else 0

    def event_source():
        for index, event in bus.stream(stream_id, from_index=from_index):
            yield f"id: {index}\ndata: {json.dumps(event)}\n\n"

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


@app.get("/api/runs.xlsx")
def recent_runs_xlsx(limit: int = 100):
    """Same run history as the CSV export, in a spreadsheet with a second
    'Steps' sheet holding per-step detail — the one thing a flat CSV can't
    carry. openpyxl is imported lazily so simply serving the dashboard never
    pays for the dependency."""
    from openpyxl import Workbook

    workbook = Workbook()

    runs_sheet = workbook.active
    runs_sheet.title = "Runs"
    runs_sheet.append(["id", "started_at", "finished_at", "status", "duration_seconds"])
    runs = store.recent_runs(limit)
    for run in runs:
        duration = None
        if run["started_at"] and run["finished_at"]:
            duration = round(
                (datetime.fromisoformat(run["finished_at"]) - datetime.fromisoformat(run["started_at"])).total_seconds(),
                3,
            )
        runs_sheet.append([run["id"], run["started_at"], run["finished_at"], run["status"], duration])

    steps_sheet = workbook.create_sheet("Steps")
    steps_sheet.append(["run_id", "step", "tier", "success", "error", "started_at", "finished_at"])
    for run in runs:
        for step in store.steps_for_run(run["id"]):
            steps_sheet.append(
                [
                    run["id"],
                    step["name"],
                    step["tier"],
                    bool(step["success"]),
                    step["error"] or "",
                    step["started_at"],
                    step["finished_at"],
                ]
            )

    buffer = io.BytesIO()
    workbook.save(buffer)
    buffer.seek(0)
    return StreamingResponse(
        buffer,
        media_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        headers={"Content-Disposition": "attachment; filename=run_history.xlsx"},
    )


@app.get("/api/runs/search")
def search_run_history(q: str = "", limit: int = 20):
    """Full-text keyword search across past run step outputs and errors —
    the run-history counterpart to /api/artifacts/search. Step outputs are
    redacted before they're ever logged, so snippets are already safe."""
    return {"query": q, "results": store.search_steps(q, limit)}


@app.post("/api/runs/purge")
def purge_runs(older_than_hours: float = 24 * 30):
    """Delete finished runs (and their steps) older than `older_than_hours`
    — mirrors artifact purge, but for run history instead of uploaded files.
    Defaults to 30 days; never touches a run that's still in progress."""
    removed = store.prune_runs(older_than_hours)
    return {"removed_count": removed}


def _parsed_steps_for_run(run_id: int) -> list[dict]:
    steps = store.steps_for_run(run_id)
    for step in steps:
        step["output"] = json.loads(step["output"]) if step["output"] else {}
        step["success"] = bool(step["success"])
    return steps


def _diff_dicts(a: dict, b: dict) -> dict:
    """Key-level diff: every key present in either dict whose value differs
    (or is missing from one side), each reported as {"a": ..., "b": ...}."""
    keys = set(a) | set(b)
    return {key: {"a": a.get(key), "b": b.get(key)} for key in sorted(keys) if a.get(key) != b.get(key)}


@app.get("/api/runs/compare")
def compare_runs(a: int, b: int):
    steps_a = _parsed_steps_for_run(a)
    steps_b = _parsed_steps_for_run(b)
    if not steps_a or not steps_b:
        raise HTTPException(status_code=404, detail="One or both runs have no recorded steps.")

    comparisons = []
    for index in range(max(len(steps_a), len(steps_b))):
        step_a = steps_a[index] if index < len(steps_a) else None
        step_b = steps_b[index] if index < len(steps_b) else None
        comparisons.append(
            {
                "index": index,
                "a": step_a,
                "b": step_b,
                "output_diff": _diff_dicts(
                    (step_a or {}).get("output", {}),
                    (step_b or {}).get("output", {}),
                ),
            }
        )

    return {"run_a": a, "run_b": b, "steps": comparisons}


@app.get("/api/runs/{run_id}")
def run_detail(run_id: int):
    steps = _parsed_steps_for_run(run_id)
    if not steps:
        raise HTTPException(status_code=404, detail=f"No recorded steps for run {run_id}.")
    return {"run_id": run_id, "steps": steps}


@app.get("/api/metrics")
def metrics():
    return store.metrics_summary()


@app.get("/api/performance")
def performance():
    with _active_run_threads_lock:
        active = len(_active_run_threads)
    return perf.snapshot(active_run_threads=active)


@app.get("/api/health")
def health_check():
    return health.run_health_checks(TIER_DIRS, store)


@app.get("/api/environment")
def environment_view():
    """Browsable environment + runtime-config view: which declared env vars
    are set (secret-shaped values only reveal their length), plus the live
    values of the app's operational knobs. Extends the health check's one-line
    environment note into something you can actually read."""
    from webapp.events import _RETENTION_SECONDS

    settings = [
        {"name": "Step timeout", "value": f"{DEFAULT_STEP_TIMEOUT_SECONDS:g} s", "detail": "Max wall-clock time one module step may run before it fails."},
        {"name": "Circuit breaker threshold", "value": str(DEFAULT_BREAKER_THRESHOLD), "detail": "Consecutive failures before a module's breaker trips (per-module override: circuit_breaker_threshold in its manifest)."},
        {"name": "Run rate limit", "value": f"{_run_rate_limiter.max_requests} requests / {_run_rate_limiter.window_seconds:g} s", "detail": "Cap on run-triggering API calls, per client."},
        {"name": "Upload / ingest size limit", "value": f"{ingestion.MAX_UPLOAD_BYTES // (1024 * 1024)} MB", "detail": "Largest file the upload endpoint or folder watcher will ingest."},
        {"name": "Result cache max age", "value": "3600 s", "detail": "Cached module results older than this are treated as a miss."},
        {"name": "Folder watcher interval", "value": f"{_watcher.interval:g} s", "detail": "How often watched_input/ is polled for new files."},
        {"name": "Scheduler poll interval", "value": f"{_scheduler.poll_interval:g} s", "detail": "How often due schedules are checked."},
        {"name": "SSE stream retention", "value": f"{_RETENTION_SECONDS:g} s", "detail": "How long a finished run's event stream stays replayable for reconnects."},
        {"name": "State store", "value": store.db_path, "detail": "SQLite database holding runs, steps, schedules, memory, and breaker state."},
    ]
    return {
        "environment": health.environment_report(ROOT / ".env.example"),
        "settings": settings,
    }


@app.get("/api/backup/export")
def export_backup():
    """A portable JSON snapshot of every run, step, schedule, ingested-file
    record, and saved pipeline — for basic diagnostics/maintenance backup,
    not a byte-for-byte database copy (see /api/backup/db for that)."""
    snapshot = store.export_snapshot()
    snapshot["pipelines"] = pipeline_store.list_pipelines()
    return StreamingResponse(
        iter([json.dumps(snapshot, indent=2, default=str)]),
        media_type="application/json",
        headers={"Content-Disposition": "attachment; filename=ai-boss-backup.json"},
    )


@app.get("/api/backup/db")
def download_backup_db():
    """The raw SQLite file itself — an exact copy, not just what
    export_snapshot() knows how to describe (e.g. the result_cache table)."""
    return FileResponse(store.db_path, filename="orchestrator.db", media_type="application/octet-stream")


@app.post("/api/backup/restore")
async def restore_backup(file: UploadFile = File(...)):
    """Restore a JSON snapshot from GET /api/backup/export. Additive only —
    fills in whatever isn't already present (by id/key/slug), never
    overwrites existing data. A pipeline referencing a module that no longer
    exists here is skipped rather than failing the whole restore."""
    try:
        data = await ingestion.read_upload_with_limit(file)
    except ingestion.UploadTooLargeError as exc:
        raise HTTPException(status_code=413, detail=str(exc))
    try:
        snapshot = json.loads(data)
    except json.JSONDecodeError as exc:
        raise HTTPException(status_code=400, detail=f"Could not parse backup file: {exc}")

    counts = store.restore_snapshot(snapshot)

    restored_pipelines = 0
    for pipeline in snapshot.get("pipelines", []):
        slug = pipeline.get("slug")
        if slug and (pipeline_store.PIPELINES_DIR / f"{slug}.yaml").exists():
            continue  # never clobber an existing pipeline of the same name
        try:
            pipeline_store.save_pipeline(pipeline, TIER_DIRS)
            restored_pipelines += 1
        except pipeline_store.PipelineValidationError:
            continue  # references a module that doesn't exist in this install

    counts["pipelines"] = restored_pipelines
    return counts


@app.get("/api/memory")
def list_memory():
    """Every key currently in the persistent cross-run memory store
    (`context.memory` inside a module), redacted the same way run history
    is — this is an inspection/debugging view, not the mechanism agents
    actually use to read/write it."""
    entries = store.all_memory()
    # redact_secrets checks a *key* against its own value, so treating the
    # whole memory table as one flat {key: value} dict catches a secret-shaped
    # memory key even when its value is a bare string, not just a nested dict.
    redacted = redact_secrets({entry["key"]: entry["value"] for entry in entries})
    return [{**entry, "value": redacted[entry["key"]]} for entry in entries]


@app.delete("/api/memory/{key}")
def delete_memory_key(key: str):
    store.delete_memory(key)
    return {"deleted": key}


@app.delete("/api/memory")
def clear_memory():
    store.clear_memory()
    return {"cleared": True}


@app.post("/api/ingest/csv")
async def ingest_csv(file: UploadFile = File(...)):
    """Upload a CSV, get back structured JSON records. Identical files (by
    content hash, not filename) are reported as duplicates instead of being
    reprocessed. Shared with the filesystem watcher (webapp/watcher.py)."""
    try:
        data = await ingestion.read_upload_with_limit(file)
    except ingestion.UploadTooLargeError as exc:
        raise HTTPException(status_code=413, detail=str(exc))
    try:
        return ingestion.ingest_csv_bytes(file.filename, data, store)
    except Exception as exc:
        raise HTTPException(status_code=400, detail=f"Could not parse CSV: {exc}")


@app.post("/api/ingest/pdf")
async def ingest_pdf(file: UploadFile = File(...)):
    """Upload a PDF, get back its extracted text. Same content-hash dedupe as CSV,
    same shared implementation as the filesystem watcher."""
    try:
        data = await ingestion.read_upload_with_limit(file)
    except ingestion.UploadTooLargeError as exc:
        raise HTTPException(status_code=413, detail=str(exc))
    try:
        return ingestion.ingest_pdf_bytes(file.filename, data, store)
    except Exception as exc:
        raise HTTPException(status_code=400, detail=f"Could not read PDF: {exc}")


@app.post("/api/ingest/json")
async def ingest_json(file: UploadFile = File(...)):
    """Upload a JSON file (an array of records, or a single object), get back
    the same row_count/columns/preview shape as a CSV upload. Same content-hash
    dedupe, same shared implementation as the filesystem watcher."""
    try:
        data = await ingestion.read_upload_with_limit(file)
    except ingestion.UploadTooLargeError as exc:
        raise HTTPException(status_code=413, detail=str(exc))
    try:
        return ingestion.ingest_json_bytes(file.filename, data, store)
    except Exception as exc:
        raise HTTPException(status_code=400, detail=f"Could not parse JSON: {exc}")


@app.get("/api/artifacts")
def list_artifacts():
    return ingestion.list_artifacts()


@app.post("/api/artifacts/purge")
def purge_artifacts(older_than_hours: float = 24):
    removed = ingestion.purge_old_artifacts(older_than_hours)
    return {"removed_count": len(removed), "removed": removed}


@app.get("/api/artifacts/search")
def search_artifacts(q: str = ""):
    return {"query": q, "results": ingestion.search_artifacts(q)}


@app.get("/api/watcher/status")
def watcher_status():
    return {"watch_dir": str(WATCH_DIR), "processed": _watcher_log[-20:]}


app.mount("/static", StaticFiles(directory=STATIC_DIR), name="static")


@app.get("/")
def index():
    return FileResponse(STATIC_DIR / "index.html")
