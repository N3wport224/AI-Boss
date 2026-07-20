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
import re
import threading
import time
import zipfile

import httpx
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Callable, Optional

import yaml
from fastapi import FastAPI, File, HTTPException, Request, UploadFile
from fastapi.responses import FileResponse, StreamingResponse
from fastapi.staticfiles import StaticFiles
from pydantic import BaseModel

from engine import (
    ExecutionContext,
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

from . import cache, graph, health, ingestion, linting, perf, pipelines as pipeline_store, scaffold
from .templates import PIPELINE_TEMPLATES
from .events import RunEventBus
from .ratelimit import RateLimiter
from .auto_backup import AutoBackup
from .scheduler import Scheduler, next_daily_run_at, next_n_daily_run_ats, next_n_weekly_run_ats, next_weekly_run_at
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
DEFAULT_RATE_LIMIT_MAX_REQUESTS = 30
DEFAULT_RATE_LIMIT_WINDOW_SECONDS = 10.0
_run_rate_limiter = RateLimiter(
    max_requests=DEFAULT_RATE_LIMIT_MAX_REQUESTS, window_seconds=DEFAULT_RATE_LIMIT_WINDOW_SECONDS
)
_persisted_rate_limit = store.get_rate_limit_override()
if _persisted_rate_limit is not None:
    _run_rate_limiter.max_requests, _run_rate_limiter.window_seconds = _persisted_rate_limit


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
    _auto_backup.stop()
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
        elif suffix == ".xlsx":
            result = ingestion.ingest_xlsx_bytes(path.name, data, store)
        else:
            entry["error"] = (
                f"Unsupported file type '{suffix}' — only .csv, .pdf, .json, and .xlsx are auto-ingested."
            )
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


class RetrySpec(BaseModel):
    max_retries: int = 0
    backoff_seconds: float = 1.0


class PipelineStepSpec(BaseModel):
    # A module step ({tier, name, ...}) or, when type == "parallel", a group
    # of branches run concurrently in this slot ({type, branches, name?}).
    tier: str = ""
    name: str = ""
    inputs: dict[str, Any] = {}
    mappings: dict[str, MappingSpec] = {}
    condition: Optional[ConditionSpec] = None
    retry: Optional[RetrySpec] = None
    type: Optional[str] = None
    branches: Optional[list["PipelineStepSpec"]] = None
    note: str = ""  # optional author-time documentation, purely descriptive -- never used at runtime


PipelineStepSpec.model_rebuild()


class PipelineDefinition(BaseModel):
    name: str
    description: str = ""
    steps: list[PipelineStepSpec]
    launch: bool = True  # False saves a draft without immediately running it


MIN_SCHEDULE_INTERVAL_SECONDS = 10.0
DAILY_TIME_RE = re.compile(r"^([01]\d|2[0-3]):([0-5]\d)$")


class ScheduleCreate(BaseModel):
    kind: str  # "module" or "pipeline"
    tier: Optional[str] = None  # required when kind == "module"
    name: str  # module name, or saved pipeline slug
    inputs: dict[str, Any] = {}  # only used when kind == "module"
    schedule_type: str = "interval"  # "interval", "daily", "weekly", or "once"
    interval_seconds: Optional[float] = None  # required when schedule_type == "interval"
    daily_time: Optional[str] = None  # "HH:MM" (local time), required when schedule_type in ("daily", "weekly")
    day_of_week: Optional[int] = None  # 0=Monday..6=Sunday, required when schedule_type == "weekly"
    run_at: Optional[str] = None  # ISO 8601 datetime (local, with offset or naive-local), required when schedule_type == "once"


class ScheduleUpdate(BaseModel):
    enabled: Optional[bool] = None
    label: Optional[str] = None


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
    """A runtime override (set from the dashboard, persisted in
    `breaker_overrides`) wins if one's ever been set; otherwise falls back
    to the module's own manifest `circuit_breaker_threshold`, or the
    engine-wide default."""
    override = store.get_breaker_threshold_override(tier, name)
    if override is not None:
        return override
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
            _notify(
                "breaker_tripped",
                f"Circuit breaker opened for [{tier}] {name} after {health['consecutive_failures']} consecutive failures.",
            )


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


def _is_module_effectively_enabled(tier: str, name: str) -> bool:
    """A runtime toggle override (from the dashboard) wins if one has ever
    been set; otherwise falls back to the manifest's own `enabled` flag."""
    override = store.get_module_enabled_override(tier, name)
    if override is not None:
        return override
    try:
        manifest = _manifest_by_name(tier, name)
    except HTTPException:
        return True  # unknown module — not this check's concern
    return manifest.get("enabled", True)


def _ensure_modules_runnable(module_refs: list[tuple[str, str]]) -> None:
    """Blocks a launch outright if any module it touches has a tripped
    breaker or is runtime-disabled — same blast radius either way: every
    launch path (module card, full pipeline, saved pipelines, schedules,
    webhooks, rerun) refuses to run until the module is reset/re-enabled."""
    tripped = []
    disabled = []
    for tier, name in dict.fromkeys(module_refs):  # de-dupe, keep order
        health = store.get_module_health(tier, name)
        if health["tripped"]:
            tripped.append(f"{tier}/{name} ({health['consecutive_failures']} consecutive failures)")
        if not _is_module_effectively_enabled(tier, name):
            disabled.append(f"{tier}/{name}")
    if tripped:
        raise HTTPException(
            status_code=409,
            detail=(
                f"Circuit breaker open for {', '.join(tripped)}. "
                "Reset it from the module card to allow runs again."
            ),
        )
    if disabled:
        raise HTTPException(
            status_code=409,
            detail=(
                f"Module disabled: {', '.join(disabled)}. "
                "Re-enable it from the module card to allow runs again."
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

    raw_retry = step.get("retry") or {}

    return StepSpec(
        module=module,
        seed=seed,
        timeout_seconds=DEFAULT_STEP_TIMEOUT_SECONDS,
        condition=condition_fn,
        condition_label=condition_label,
        max_retries=int(raw_retry.get("max_retries", 0)),
        retry_backoff_seconds=float(raw_retry.get("backoff_seconds", 1.0)),
    )


def _apply_entry_overrides(step: dict, overrides: dict) -> dict:
    """Merge `overrides` into a top-level step's own input dict (every
    branch's, for a parallel group) — used by the webhook trigger to let an
    inbound call's JSON body feed pipeline step 1 directly. Unknown keys are
    harmless: `_coerce_inputs` already ignores anything that isn't one of
    the target module's declared input fields."""
    if pipeline_store.is_parallel_step(step):
        return {
            **step,
            "branches": [
                {**branch, "inputs": {**(branch.get("inputs") or {}), **overrides}}
                for branch in step.get("branches") or []
            ],
        }
    return {**step, "inputs": {**(step.get("inputs") or {}), **overrides}}


def _build_steps_from_definition(definition: dict, entry_overrides: Optional[dict] = None) -> list:
    """Turn a saved pipeline definition into the orchestrator's step list —
    StepSpecs for module steps, ParallelGroups (of branch StepSpecs) for
    `type: "parallel"` steps. `entry_overrides`, if given, is merged into
    the *first* step's own inputs (see `_apply_entry_overrides`) — how the
    webhook trigger passes an inbound call's JSON body into the pipeline."""
    raw_steps = definition["steps"]
    if entry_overrides and raw_steps:
        raw_steps = [_apply_entry_overrides(raw_steps[0], entry_overrides), *raw_steps[1:]]

    steps = []
    for step in raw_steps:
        if pipeline_store.is_parallel_step(step):
            branches = [_module_step_to_spec(branch) for branch in step.get("branches") or []]
            steps.append(ParallelGroup(steps=branches, name=step.get("name") or "parallel_group"))
        else:
            steps.append(_module_step_to_spec(step))
    return steps


def _launch_steps(steps: list[StepSpec], inputs: Optional[dict] = None) -> int:
    orchestrator = Orchestrator(steps, state_store=store, stop_on_error=False)
    stream_id = bus.create()
    _run_in_background(orchestrator, inputs or {}, stream_id)
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
        _ensure_modules_runnable(_module_refs_from_steps(definition["steps"]))
        steps = _build_steps_from_definition(definition)
        _run_scheduled(steps, {})
    else:
        _ensure_modules_runnable([(schedule["tier"], schedule["name"])])
        manifest = _manifest_by_name(schedule["tier"], schedule["name"])
        module = instantiate(manifest["entrypoint"])
        inputs = _coerce_inputs(manifest, schedule["inputs"])
        inputs = interpolate_template_fields(manifest, inputs, inputs.get)
        step = StepSpec(module=module, timeout_seconds=DEFAULT_STEP_TIMEOUT_SECONDS)
        _run_scheduled([step], inputs)


def _notify_schedule_error(schedule: dict, error_message: str) -> None:
    label = schedule["name"] if schedule.get("kind") == "pipeline" else f"[{schedule['tier']}] {schedule['name']}"
    _notify("schedule_failed", f"Scheduled run of {label} failed: {error_message}")


def _notify_once_schedule_fired(schedule: dict) -> None:
    label = schedule["name"] if schedule.get("kind") == "pipeline" else f"[{schedule['tier']}] {schedule['name']}"
    _notify("schedule_once_fired", f"One-time scheduled run of {label} fired successfully.")


_scheduler = Scheduler(store, _trigger_schedule, on_error=_notify_schedule_error, on_once_fired=_notify_once_schedule_fired)
_scheduler.start()


BACKUPS_DIR = ROOT / "backups"


def _build_backup_snapshot() -> dict:
    """The same snapshot shape GET /api/backup/export downloads on demand,
    reused here so the automatic timer and the manual export can never
    drift apart into two different backup formats."""
    snapshot = store.export_snapshot()
    snapshot["pipelines"] = pipeline_store.list_pipelines()
    return snapshot


def _notify_auto_backup_failure(error: Exception) -> None:
    _notify("backup_failed", f"Automatic backup snapshot failed: {error}")


_auto_backup = AutoBackup(
    BACKUPS_DIR,
    _build_backup_snapshot,
    on_failure=_notify_auto_backup_failure,
    get_protected=lambda: store.all_protected_backup_filenames(),
)
_auto_backup.start()


@app.get("/api/modules")
def list_modules():
    stats_by_key = {(s["tier"], s["name"]): s for s in store.module_stats()}
    result = {}
    for tier, directory in TIER_DIRS.items():
        modules = []
        for manifest in load_manifests(directory):
            if not manifest.get("enabled", True):
                continue
            last_success = store.latest_step_status(manifest["name"])
            health = store.get_module_health(tier, manifest["name"])
            stats = stats_by_key.get((tier, manifest["name"]))
            modules.append(
                {
                    "name": manifest["name"],
                    "tier": tier,
                    "description": manifest.get("description", ""),
                    "inputs": manifest.get("inputs", []),
                    "outputs": manifest.get("outputs", []),
                    "status": "error" if last_success is False else "ready",
                    "runtime_enabled": _is_module_effectively_enabled(tier, manifest["name"]),
                    "breaker": {
                        "tripped": health["tripped"],
                        "consecutive_failures": health["consecutive_failures"],
                        "threshold": _breaker_threshold(tier, manifest["name"]),
                        "threshold_overridden": store.get_breaker_threshold_override(tier, manifest["name"]) is not None,
                    },
                    "stats": {
                        "total_runs": stats["total_runs"] if stats else 0,
                        "success_rate": stats["success_rate"] if stats else None,
                        "avg_duration_seconds": stats["avg_duration_seconds"] if stats else None,
                    },
                    "used_by": pipeline_store.pipelines_using_module(tier, manifest["name"]),
                }
            )
        result[tier] = modules
    return result


@app.get("/api/modules/used-by.csv")
def modules_used_by_csv():
    """The module-to-pipeline reverse lookup (see the 'used_by' field on
    GET /api/modules, and the "Used by" chips shown inline on each module
    card) as a downloadable CSV -- one row per enabled module, with a
    semicolon-joined list of the saved pipeline slugs that reference it
    (a plain comma would collide with the CSV column separator).
    Mirrors every other CSV export in this app."""
    buffer = io.StringIO()
    writer = csv.DictWriter(buffer, fieldnames=["tier", "name", "used_by"])
    writer.writeheader()
    for tier, directory in TIER_DIRS.items():
        for manifest in load_manifests(directory):
            if not manifest.get("enabled", True):
                continue
            used_by = pipeline_store.pipelines_using_module(tier, manifest["name"])
            writer.writerow({"tier": tier, "name": manifest["name"], "used_by": ";".join(used_by)})

    return StreamingResponse(
        iter([buffer.getvalue()]),
        media_type="text/csv",
        headers={"Content-Disposition": "attachment; filename=modules_used_by.csv"},
    )


@app.get("/api/modules/directory.csv")
def modules_directory_csv():
    """The full module directory as a single downloadable CSV -- name,
    tier, description, whether it's currently enabled, and its live
    status/breaker state all in one row per module. Distinct from
    used-by.csv (just the pipeline reverse lookup), stats.csv (just run
    counters), and breakers.csv (just breaker state): none of those three
    carries the description or the enabled/runtime-status fields together,
    so this is the one CSV that captures "what modules exist and are they
    healthy" at a glance."""
    buffer = io.StringIO()
    writer = csv.DictWriter(
        buffer,
        fieldnames=["tier", "name", "description", "enabled", "status", "breaker_tripped"],
    )
    writer.writeheader()
    for tier, directory in TIER_DIRS.items():
        for manifest in load_manifests(directory):
            if not manifest.get("enabled", True):
                continue
            last_success = store.latest_step_status(manifest["name"])
            health = store.get_module_health(tier, manifest["name"])
            writer.writerow(
                {
                    "tier": tier,
                    "name": manifest["name"],
                    "description": manifest.get("description", ""),
                    "enabled": _is_module_effectively_enabled(tier, manifest["name"]),
                    "status": "error" if last_success is False else "ready",
                    "breaker_tripped": health["tripped"],
                }
            )

    return StreamingResponse(
        iter([buffer.getvalue()]),
        media_type="text/csv",
        headers={"Content-Disposition": "attachment; filename=modules_directory.csv"},
    )


class ModuleScaffoldRequest(BaseModel):
    tier: str
    name: str
    description: str = ""


@app.post("/api/modules/scaffold")
def scaffold_module_route(payload: ModuleScaffoldRequest):
    """Generate a starter <name>.py + <name>.yaml pair for a brand-new
    automation/workflow/agent -- the boilerplate the README's "Adding a new
    module" section otherwise asks a user to hand-write. Immediately
    discoverable: the registry re-scans each tier directory on every
    request, no server restart needed."""
    try:
        result = scaffold.scaffold_module(payload.tier, payload.name, payload.description, TIER_DIRS)
    except scaffold.ScaffoldError as exc:
        raise HTTPException(status_code=400, detail=str(exc))
    return result


class ModuleDuplicateRequest(BaseModel):
    new_name: str
    description: str = ""


@app.post("/api/modules/{tier}/{name}/duplicate")
def duplicate_module_route(tier: str, name: str, payload: ModuleDuplicateRequest):
    """Clone an existing module's manifest + source under a new name in the
    same tier -- a working starting point (keeps the original's inputs/
    outputs/run() logic intact) rather than an empty from-scratch stub."""
    try:
        result = scaffold.duplicate_module(tier, name, payload.new_name, payload.description, TIER_DIRS)
    except scaffold.ScaffoldError as exc:
        raise HTTPException(status_code=400, detail=str(exc))
    return result


@app.get("/api/modules/stats")
def module_stats():
    """Per-module run statistics — total runs, success rate, average
    duration — broken out by (tier, name). Also folded into `/api/modules`
    per card, but exposed standalone for anyone who just wants the numbers."""
    return store.module_stats()


@app.get("/api/modules/stats.csv")
def module_stats_csv():
    """Same per-module run statistics as GET /api/modules/stats, as a
    downloadable CSV -- mirrors every other CSV export in this app."""
    buffer = io.StringIO()
    writer = csv.DictWriter(
        buffer, fieldnames=["tier", "name", "total_runs", "success_count", "success_rate", "avg_duration_seconds"]
    )
    writer.writeheader()
    for row in store.module_stats():
        writer.writerow(row)

    return StreamingResponse(
        iter([buffer.getvalue()]),
        media_type="text/csv",
        headers={"Content-Disposition": "attachment; filename=module_stats.csv"},
    )


@app.post("/api/self-test")
def run_self_test():
    """Actually run every enabled module once with its own manifest's default
    inputs — unlike /api/health (which only checks that manifests parse and
    entrypoints instantiate), this exercises real run() code, catching a
    module that imports fine but breaks the moment it's actually called.

    Each module gets its own fresh, disposable ExecutionContext with no
    state_store attached — nothing here touches real run history, circuit
    breaker counts, or persistent agent memory. It's a read-only diagnostic,
    not a real run, so it's never logged to the audit trail either."""
    results = []
    for tier, directory in TIER_DIRS.items():
        for manifest in load_manifests(directory):
            name = manifest["name"]
            if not manifest.get("enabled", True) or not _is_module_effectively_enabled(tier, name):
                results.append(
                    {"tier": tier, "name": name, "status": "skipped", "detail": "Module disabled.", "duration_seconds": 0.0}
                )
                continue

            inputs = _coerce_inputs(manifest, {})
            inputs = interpolate_template_fields(manifest, inputs, inputs.get)
            context = ExecutionContext(initial=inputs)

            started = time.perf_counter()
            try:
                module = instantiate(manifest["entrypoint"])
                module.run(context)
                status, detail = "pass", None
            except Exception as exc:
                status, detail = "fail", str(exc)
            duration = time.perf_counter() - started

            results.append(
                {"tier": tier, "name": name, "status": status, "detail": detail, "duration_seconds": round(duration, 4)}
            )

    return {
        "results": results,
        "passed": sum(1 for r in results if r["status"] == "pass"),
        "failed": sum(1 for r in results if r["status"] == "fail"),
        "skipped": sum(1 for r in results if r["status"] == "skipped"),
    }


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
    _ensure_modules_runnable([(tier, name)])
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
    _ensure_modules_runnable([(s.module.tier.value, s.module.name) for s in steps])
    orchestrator = Orchestrator(steps, state_store=store, stop_on_error=False)
    stream_id = bus.create()
    _run_in_background(orchestrator, payload.inputs, stream_id)
    return {"stream_id": stream_id}


class ModuleRef(BaseModel):
    tier: str
    name: str


@app.get("/api/breakers")
def list_breakers():
    """Every module the breaker has ever seen fail (or succeed after failing) —
    modules with no history simply aren't listed, which reads as 'closed'."""
    return store.all_module_health()


@app.get("/api/breakers.csv")
def breakers_csv():
    """Same breaker-state list as GET /api/breakers, as a downloadable CSV —
    mirrors every other CSV export in this app. A literal path, not a
    suffix on a dynamic segment, so there's no route-ordering conflict
    with /api/breakers/{tier}/{name}/reset (also a different HTTP method)."""
    buffer = io.StringIO()
    writer = csv.DictWriter(
        buffer, fieldnames=["tier", "name", "consecutive_failures", "tripped", "updated_at"]
    )
    writer.writeheader()
    for entry in store.all_module_health():
        writer.writerow(entry)
    return StreamingResponse(
        iter([buffer.getvalue()]),
        media_type="text/csv",
        headers={"Content-Disposition": "attachment; filename=breakers.csv"},
    )


@app.post("/api/breakers/{tier}/{name}/reset")
def reset_module_breaker(tier: str, name: str):
    _manifest_by_name(tier, name)  # 404 for a module that doesn't exist
    result = store.reset_breaker(tier, name)
    store.record_audit_event("circuit_breaker_reset", f"Reset circuit breaker for {tier}/{name}.")
    return result


class BulkResetBreakers(BaseModel):
    modules: list[ModuleRef]


@app.post("/api/breakers/bulk-reset")
def bulk_reset_breakers(payload: BulkResetBreakers):
    """Reset the circuit breaker for a user-picked set of modules in one
    action -- the finer-grained counterpart to resetting
    POST /api/breakers/{tier}/{name}/reset one at a time, mirroring the
    existing bulk pattern already built for schedules/artifacts/modules.
    A ref naming a tier/name that doesn't exist is skipped rather than
    failing the whole batch."""
    reset = []
    for ref in payload.modules:
        try:
            _manifest_by_name(ref.tier, ref.name)
        except HTTPException:
            continue
        store.reset_breaker(ref.tier, ref.name)
        reset.append({"tier": ref.tier, "name": ref.name})
    store.record_audit_event(
        "circuit_breaker_bulk_reset", f"Reset circuit breaker for {len(reset)} selected module(s): {reset}."
    )
    return {"reset": reset}


class BreakerThresholdUpdate(BaseModel):
    threshold: int


@app.patch("/api/breakers/{tier}/{name}/threshold")
def set_breaker_threshold(tier: str, name: str, payload: BreakerThresholdUpdate):
    """Override how many consecutive failures trip this module's breaker,
    independent of its manifest's own `circuit_breaker_threshold` — no YAML
    edit needed, and it persists across a restart. Doesn't touch the
    breaker's current failure count or tripped state, only the threshold
    the *next* failure is measured against."""
    _manifest_by_name(tier, name)  # 404 for a module that doesn't exist
    if payload.threshold < 1:
        raise HTTPException(status_code=400, detail="threshold must be at least 1.")
    store.set_breaker_threshold(tier, name, payload.threshold)
    return {"tier": tier, "name": name, "threshold": payload.threshold}


@app.delete("/api/breakers/{tier}/{name}/threshold")
def clear_breaker_threshold(tier: str, name: str):
    """Revert to the module's manifest-declared threshold (or the
    engine-wide default) instead of a specific overridden number."""
    _manifest_by_name(tier, name)  # 404 for a module that doesn't exist
    store.clear_breaker_threshold_override(tier, name)
    return {"tier": tier, "name": name, "threshold": _breaker_threshold(tier, name)}


class RateLimitUpdate(BaseModel):
    max_requests: int
    window_seconds: float


@app.get("/api/ratelimit")
def get_rate_limit():
    """The run-triggering rate limiter's live configuration -- whatever is
    currently enforced, whether that's the hardcoded startup default or a
    persisted override."""
    return {
        "max_requests": _run_rate_limiter.max_requests,
        "window_seconds": _run_rate_limiter.window_seconds,
        "overridden": store.get_rate_limit_override() is not None,
    }


@app.patch("/api/ratelimit")
def set_rate_limit(payload: RateLimitUpdate):
    """Override the run-triggering rate limiter's max_requests/window_seconds
    at runtime, independent of the hardcoded startup default -- no restart
    needed, and it persists across one, mirroring the existing
    runtime-configurable circuit breaker threshold. Takes effect
    immediately for every subsequent request; in-flight sliding-window
    hits already recorded aren't retroactively rescored."""
    if payload.max_requests < 1:
        raise HTTPException(status_code=400, detail="max_requests must be at least 1.")
    if payload.window_seconds <= 0:
        raise HTTPException(status_code=400, detail="window_seconds must be greater than 0.")
    store.set_rate_limit_override(payload.max_requests, payload.window_seconds)
    _run_rate_limiter.max_requests = payload.max_requests
    _run_rate_limiter.window_seconds = payload.window_seconds
    return {"max_requests": payload.max_requests, "window_seconds": payload.window_seconds}


@app.delete("/api/ratelimit")
def clear_rate_limit():
    """Revert the run-triggering rate limiter to its hardcoded startup
    default instead of a persisted override."""
    store.clear_rate_limit_override()
    _run_rate_limiter.max_requests = DEFAULT_RATE_LIMIT_MAX_REQUESTS
    _run_rate_limiter.window_seconds = DEFAULT_RATE_LIMIT_WINDOW_SECONDS
    return {"max_requests": _run_rate_limiter.max_requests, "window_seconds": _run_rate_limiter.window_seconds}


class BulkClearBreakerThresholds(BaseModel):
    modules: list[ModuleRef]


@app.post("/api/breakers/bulk-clear-threshold")
def bulk_clear_breaker_thresholds(payload: BulkClearBreakerThresholds):
    """Revert a user-picked set of modules back to their manifest-declared
    (or engine-default) breaker threshold in one action -- the bulk
    counterpart to DELETE /api/breakers/{tier}/{name}/threshold, distinct
    from POST /api/breakers/bulk-reset which clears the trip *state*
    (consecutive failures/tripped flag) rather than the threshold
    *configuration*. An unknown tier/name is skipped rather than failing
    the whole batch."""
    cleared = []
    for ref in payload.modules:
        try:
            _manifest_by_name(ref.tier, ref.name)
        except HTTPException:
            continue
        store.clear_breaker_threshold_override(ref.tier, ref.name)
        cleared.append({"tier": ref.tier, "name": ref.name})
    store.record_audit_event(
        "breaker_threshold_bulk_clear",
        f"Cleared breaker threshold override for {len(cleared)} selected module(s): {cleared}.",
    )
    return {"cleared": cleared}


class ModuleEnabledUpdate(BaseModel):
    enabled: bool


@app.patch("/api/modules/{tier}/{name}")
def set_module_enabled(tier: str, name: str, payload: ModuleEnabledUpdate):
    """Runtime on/off toggle for a module, independent of its manifest's own
    `enabled` flag — no YAML edit needed, and it persists across a restart.
    A disabled module is blocked from every launch path exactly like a
    tripped circuit breaker (see `_ensure_modules_runnable`)."""
    _manifest_by_name(tier, name)  # 404 for a module that doesn't exist
    store.set_module_enabled(tier, name, payload.enabled)
    return {"tier": tier, "name": name, "runtime_enabled": payload.enabled}


class BulkSetModulesEnabled(BaseModel):
    modules: list[ModuleRef]
    enabled: bool


@app.post("/api/modules/bulk-set-enabled")
def bulk_set_modules_enabled(payload: BulkSetModulesEnabled):
    """Enable or disable a user-picked set of modules in one action --
    the finer-grained counterpart to toggling PATCH /api/modules/{tier}/{name}
    one at a time, mirroring the existing bulk pause/resume pattern already
    built for Schedules. A ref naming a tier/name that doesn't exist is
    skipped rather than failing the whole batch, same as bulk-tagging."""
    updated = []
    for ref in payload.modules:
        try:
            _manifest_by_name(ref.tier, ref.name)
        except HTTPException:
            continue
        store.set_module_enabled(ref.tier, ref.name, payload.enabled)
        updated.append({"tier": ref.tier, "name": ref.name})
    store.record_audit_event(
        "module_bulk_set_enabled",
        f"{'Enabled' if payload.enabled else 'Disabled'} {len(updated)} selected module(s): {updated}.",
    )
    return {"updated": updated, "enabled": payload.enabled}


class InputPresetSave(BaseModel):
    preset_name: str
    inputs: dict[str, Any] = {}


@app.get("/api/modules/{tier}/{name}/last-run-inputs")
def last_run_inputs(tier: str, name: str):
    """The input values this module was actually run with most recently --
    a one-click quick-fill distinct from a named preset (Batch 12): no
    saving required, just "whatever I ran last time." 404s if this module
    has never been run at all (not merely unknown -- an unknown module
    reaching this far isn't this endpoint's job to validate)."""
    inputs = store.latest_step_inputs(tier, name)
    if inputs is None:
        raise HTTPException(status_code=404, detail=f"{tier}/{name} has never been run.")
    return {"inputs": inputs}


@app.get("/api/modules/{tier}/{name}/presets")
def list_input_presets(tier: str, name: str):
    _manifest_by_name(tier, name)  # 404 for a module that doesn't exist
    return store.list_input_presets(tier, name)


@app.post("/api/modules/{tier}/{name}/presets")
def save_input_preset(tier: str, name: str, payload: InputPresetSave):
    """Save the current values in a module card's input form under a name,
    so a user can reapply that exact combination later with one click
    instead of retyping it every time."""
    _manifest_by_name(tier, name)  # 404 for a module that doesn't exist
    if not payload.preset_name.strip():
        raise HTTPException(status_code=400, detail="Preset name is required.")
    return store.save_input_preset(tier, name, payload.preset_name.strip(), payload.inputs)


@app.delete("/api/modules/{tier}/{name}/presets/{preset_name}")
def delete_input_preset(tier: str, name: str, preset_name: str):
    _manifest_by_name(tier, name)  # 404 for a module that doesn't exist
    deleted = store.delete_input_preset(tier, name, preset_name)
    if not deleted:
        raise HTTPException(status_code=404, detail=f"No preset named '{preset_name}' for {tier}/{name}.")
    return {"deleted": preset_name}


@app.get("/api/pipelines")
def list_saved_pipelines(tag: Optional[str] = None):
    pipelines = pipeline_store.list_pipelines()
    tags_by_slug = store.all_pipeline_tags()
    for p in pipelines:
        p["tags"] = tags_by_slug.get(p.get("slug", ""), [])
    if tag and tag.strip():
        tag_lower = tag.strip().lower()
        pipelines = [p for p in pipelines if tag_lower in (t.lower() for t in p["tags"])]
    return pipelines


@app.get("/api/pipelines/search")
def search_saved_pipelines(q: str = ""):
    return {"query": q, "results": pipeline_store.search_pipelines(q)}


@app.post("/api/pipelines/validate")
def validate_pipeline_route(definition: PipelineDefinition):
    """Dry-run validation for the pipeline builder -- checks a definition
    against validate_pipeline() (module references exist, mappings point to
    a declared output on a strictly earlier step, condition/retry shapes are
    sane) without writing anything to disk or launching a run. Lets a user
    catch a mistake mid-edit without either saving a draft or triggering a
    real run just to find out."""
    try:
        validated = pipeline_store.validate_pipeline(definition.model_dump(), TIER_DIRS)
    except pipeline_store.PipelineValidationError as exc:
        raise HTTPException(status_code=400, detail=str(exc))
    return {"valid": True, "pipeline": validated}


@app.post("/api/pipelines")
def save_pipeline_route(definition: PipelineDefinition, http_request: Request):
    """Save a pipeline built in the visual sequencer to pipelines/<slug>.yaml
    (the same convention as every other module folder). By default (`launch`
    omitted or true) it also immediately runs it through the same
    background-thread + SSE mechanism as any other run — the original
    one-click "Save & Launch" behavior. Passing `launch: false` saves a draft
    without running it, for a pipeline that isn't ready to fire yet (missing
    real input values, still being sketched out) or one that's only ever
    meant to be triggered later by a schedule or webhook."""
    payload = definition.model_dump()
    launch = payload.pop("launch")
    if launch:
        _enforce_run_rate_limit(http_request)
        _ensure_modules_runnable(_module_refs_from_steps(payload["steps"]))
    try:
        saved = pipeline_store.save_pipeline(payload, TIER_DIRS)
    except pipeline_store.PipelineValidationError as exc:
        raise HTTPException(status_code=400, detail=str(exc))

    if not launch:
        return {"pipeline": saved}

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

    _ensure_modules_runnable(_module_refs_from_steps(definition["steps"]))
    steps = _build_steps_from_definition(definition)
    stream_id = _launch_steps(steps)
    return {"stream_id": stream_id}


@app.post("/api/pipelines/{slug}/webhook")
async def trigger_pipeline_webhook(slug: str, http_request: Request):
    """Launch a saved pipeline from an inbound HTTP call. The POST body (a
    JSON object, or empty) overrides step 1's own input fields — any key
    that isn't one of its declared inputs is silently ignored, same as any
    other input dict in this app. No auth: this app is single-user/local
    like every other endpoint, but it still respects the run rate limit and
    a tripped circuit breaker exactly like every other launch path."""
    _enforce_run_rate_limit(http_request)
    try:
        definition = pipeline_store.load_pipeline(slug)
    except FileNotFoundError:
        raise HTTPException(status_code=404, detail=f"No saved pipeline named '{slug}'.")

    raw_body = await http_request.body()
    if not raw_body:
        overrides = {}
    else:
        try:
            overrides = json.loads(raw_body)
        except json.JSONDecodeError:
            raise HTTPException(status_code=400, detail="Webhook body must be valid JSON.")
    if not isinstance(overrides, dict):
        raise HTTPException(status_code=400, detail="Webhook body must be a JSON object.")

    _ensure_modules_runnable(_module_refs_from_steps(definition["steps"]))
    steps = _build_steps_from_definition(definition, entry_overrides=overrides)
    stream_id = _launch_steps(steps)
    return {"stream_id": stream_id}


@app.get("/api/pipelines/export-all")
def export_all_pipelines():
    """Every saved pipeline's own YAML file bundled into a single zip --
    distinct from exporting one pipeline (GET /api/pipelines/{slug}/export)
    and from the full state-store backup snapshot (GET /api/backup/export,
    which covers runs/schedules/memory/ingested-file records too, not just
    pipeline definitions). Useful for backing up or sharing the whole
    pipeline library in one download."""
    pipelines = pipeline_store.list_pipelines()
    if not pipelines:
        raise HTTPException(status_code=404, detail="No saved pipelines to export.")

    buffer = io.BytesIO()
    with zipfile.ZipFile(buffer, "w", zipfile.ZIP_DEFLATED) as zf:
        for definition in pipelines:
            slug = definition.get("slug", "")
            path = pipeline_store.PIPELINES_DIR / f"{slug}.yaml"
            if path.exists():
                zf.write(path, arcname=f"{slug}.yaml")
    buffer.seek(0)

    return StreamingResponse(
        buffer,
        media_type="application/zip",
        headers={"Content-Disposition": "attachment; filename=pipelines_export.zip"},
    )


@app.get("/api/pipelines/{slug}/export")
def export_pipeline(slug: str):
    """The pipeline's own saved YAML file, as a standalone download — for
    sharing or backing up one pipeline outside the full state-store
    snapshot (see /api/backup/export for that)."""
    path = pipeline_store.PIPELINES_DIR / f"{slug}.yaml"
    if not path.exists():
        raise HTTPException(status_code=404, detail=f"No saved pipeline named '{slug}'.")
    return FileResponse(path, filename=f"{slug}.yaml", media_type="application/x-yaml")


@app.get("/api/pipelines/{slug}/export.json")
def export_pipeline_json(slug: str):
    """The same pipeline definition as GET /api/pipelines/{slug}/export,
    just serialized as JSON instead of YAML -- for a user or tool that
    prefers JSON. Not re-importable via POST /api/pipelines/import, which
    only reads YAML; this is a read-only alternate format."""
    try:
        definition = pipeline_store.load_pipeline(slug)
    except FileNotFoundError:
        raise HTTPException(status_code=404, detail=f"No saved pipeline named '{slug}'.")
    buffer = json.dumps(definition, indent=2)
    return StreamingResponse(
        iter([buffer]),
        media_type="application/json",
        headers={"Content-Disposition": f"attachment; filename={slug}.json"},
    )


@app.post("/api/pipelines/import")
async def import_pipeline(file: UploadFile = File(...)):
    """Import a pipeline YAML file (as produced by /api/pipelines/{slug}/export)
    into this install's pipelines/ folder. Goes through the same validation
    as a builder save — an unknown module, a bad mapping, or an out-of-range
    condition/retry is rejected with the same error a hand-built pipeline
    would get. Saving under a name that already exists overwrites it, same
    as re-saving an edited pipeline in the builder."""
    try:
        data = await ingestion.read_upload_with_limit(file)
    except ingestion.UploadTooLargeError as exc:
        raise HTTPException(status_code=413, detail=str(exc))
    try:
        definition = yaml.safe_load(data)
    except yaml.YAMLError as exc:
        raise HTTPException(status_code=400, detail=f"Could not parse pipeline YAML: {exc}")
    if not isinstance(definition, dict):
        raise HTTPException(status_code=400, detail="Pipeline YAML must describe a single object.")

    try:
        saved = pipeline_store.save_pipeline(definition, TIER_DIRS)
    except pipeline_store.PipelineValidationError as exc:
        raise HTTPException(status_code=400, detail=str(exc))
    return {"pipeline": saved}


@app.post("/api/pipelines/import-zip")
async def import_pipelines_zip(file: UploadFile = File(...)):
    """Import every pipeline YAML file inside a zip bundle at once -- the
    counterpart to GET /api/pipelines/export-all, for restoring or sharing
    a whole pipeline library rather than one file at a time. Each `.yaml`
    member goes through the exact same save_pipeline() validation as a
    single import; one bad file doesn't block the rest of the batch --
    its name and error are reported alongside the ones that succeeded."""
    try:
        data = await ingestion.read_upload_with_limit(file)
    except ingestion.UploadTooLargeError as exc:
        raise HTTPException(status_code=413, detail=str(exc))

    try:
        zf = zipfile.ZipFile(io.BytesIO(data))
    except zipfile.BadZipFile:
        raise HTTPException(status_code=400, detail="Could not read this file as a zip archive.")

    imported = []
    failed = []
    for name in zf.namelist():
        if not name.endswith(".yaml"):
            continue
        try:
            definition = yaml.safe_load(zf.read(name))
        except yaml.YAMLError as exc:
            failed.append({"name": name, "error": f"Could not parse YAML: {exc}"})
            continue
        if not isinstance(definition, dict):
            failed.append({"name": name, "error": "Pipeline YAML must describe a single object."})
            continue
        try:
            saved = pipeline_store.save_pipeline(definition, TIER_DIRS)
            imported.append(saved["slug"])
        except pipeline_store.PipelineValidationError as exc:
            failed.append({"name": name, "error": str(exc)})

    if not imported and not failed:
        raise HTTPException(status_code=400, detail="No .yaml pipeline files found in this zip.")
    return {"imported": imported, "failed": failed}


class ImportPipelineUrlRequest(BaseModel):
    url: str


@app.post("/api/pipelines/import-url")
def import_pipeline_from_url(payload: ImportPipelineUrlRequest):
    """Same import as POST /api/pipelines/import (a YAML file produced by
    /api/pipelines/{slug}/export), but fetched from a URL instead of
    uploaded from disk -- the same streaming, size-capped GET Batch 10's
    file-from-URL ingestion already uses."""
    if not payload.url.strip():
        raise HTTPException(status_code=400, detail="A URL is required.")
    try:
        data = ingestion.fetch_url_bytes(payload.url)
    except ingestion.UploadTooLargeError as exc:
        raise HTTPException(status_code=413, detail=str(exc))
    except Exception as exc:
        raise HTTPException(status_code=400, detail=f"Could not fetch that URL: {exc}")

    try:
        definition = yaml.safe_load(data)
    except yaml.YAMLError as exc:
        raise HTTPException(status_code=400, detail=f"Could not parse pipeline YAML: {exc}")
    if not isinstance(definition, dict):
        raise HTTPException(status_code=400, detail="Pipeline YAML must describe a single object.")

    try:
        saved = pipeline_store.save_pipeline(definition, TIER_DIRS)
    except pipeline_store.PipelineValidationError as exc:
        raise HTTPException(status_code=400, detail=str(exc))
    return {"pipeline": saved}


@app.post("/api/pipelines/{slug}/duplicate")
def duplicate_pipeline(slug: str):
    try:
        duplicated = pipeline_store.duplicate_pipeline(slug, TIER_DIRS)
    except FileNotFoundError:
        raise HTTPException(status_code=404, detail=f"No saved pipeline named '{slug}'.")
    return {"pipeline": duplicated}


class BulkDuplicatePipelines(BaseModel):
    slugs: list[str]


@app.post("/api/pipelines/bulk-duplicate")
def bulk_duplicate_pipelines(payload: BulkDuplicatePipelines):
    """Clone a user-picked set of saved pipelines in one action -- the
    finer-grained counterpart to duplicating one at a time, mirroring the
    existing bulk-delete/bulk-tag patterns for pipelines. Each duplicate
    gets the same non-colliding '(copy)' naming scheme as the single
    duplicate action. An unknown slug is skipped rather than failing the
    whole batch."""
    duplicated = []
    for slug in payload.slugs:
        try:
            duplicated.append(pipeline_store.duplicate_pipeline(slug, TIER_DIRS))
        except FileNotFoundError:
            continue
    store.record_audit_event(
        "pipeline_bulk_duplicate",
        f"Duplicated {len(duplicated)} selected pipeline(s): {[p['slug'] for p in duplicated]}.",
    )
    return {"duplicated": duplicated}


class PipelineRename(BaseModel):
    name: str


@app.post("/api/pipelines/{slug}/rename")
def rename_saved_pipeline(slug: str, payload: PipelineRename):
    """Rename a saved pipeline in place -- distinct from
    POST /api/pipelines/{slug}/duplicate, which clones it under a new slug
    and leaves the original untouched. Migrates the pipeline's tags and
    repoints any schedule targeting it by slug, since both are keyed by
    slug in the state store rather than living inside the pipeline's own
    YAML file."""
    old_slug = slug
    try:
        renamed = pipeline_store.rename_pipeline(old_slug, payload.name)
    except FileNotFoundError:
        raise HTTPException(status_code=404, detail=f"No saved pipeline named '{old_slug}'.")
    except pipeline_store.PipelineValidationError as exc:
        raise HTTPException(status_code=400, detail=str(exc))

    new_slug = renamed["slug"]
    if new_slug != old_slug:
        tags = store.get_pipeline_tags(old_slug)
        if tags:
            store.set_pipeline_tags(new_slug, tags)
            store.delete_pipeline_tags(old_slug)
        repointed = store.repoint_pipeline_schedules(old_slug, new_slug)
        if repointed:
            store.record_audit_event(
                "pipeline_rename",
                f"Renamed pipeline '{old_slug}' to '{new_slug}' and repointed {repointed} schedule(s).",
            )
        else:
            store.record_audit_event("pipeline_rename", f"Renamed pipeline '{old_slug}' to '{new_slug}'.")
    return {"pipeline": renamed}


@app.delete("/api/pipelines/{slug}")
def delete_saved_pipeline(slug: str):
    try:
        pipeline_store.delete_pipeline(slug)
    except FileNotFoundError:
        raise HTTPException(status_code=404, detail=f"No saved pipeline named '{slug}'.")
    store.delete_pipeline_tags(slug)
    return {"deleted": slug}


class BulkDeletePipelines(BaseModel):
    slugs: list[str]


@app.post("/api/pipelines/bulk-delete")
def bulk_delete_pipelines(payload: BulkDeletePipelines):
    """Delete a user-picked set of saved pipelines in one action -- the
    finer-grained counterpart to deleting one at a time, mirroring the
    existing bulk-delete pattern for Recent Runs. An unknown slug is
    skipped rather than failing the whole batch, same as bulk-tagging an
    unknown artifact filename."""
    deleted = []
    for slug in payload.slugs:
        try:
            pipeline_store.delete_pipeline(slug)
        except FileNotFoundError:
            continue
        store.delete_pipeline_tags(slug)
        deleted.append(slug)
    store.record_audit_event("pipeline_bulk_delete", f"Deleted {len(deleted)} selected pipeline(s): {deleted}.")
    return {"deleted": deleted}


class PipelineTagsUpdate(BaseModel):
    tags: list[str]


@app.put("/api/pipelines/{slug}/tags")
def set_pipeline_tags(slug: str, payload: PipelineTagsUpdate):
    try:
        pipeline_store.load_pipeline(slug)
    except FileNotFoundError:
        raise HTTPException(status_code=404, detail=f"No saved pipeline named '{slug}'.")
    cleaned = sorted({t.strip() for t in payload.tags if t.strip()})
    return {"slug": slug, "tags": store.set_pipeline_tags(slug, cleaned)}


class PipelineBulkTag(BaseModel):
    slugs: list[str]
    tag: str


@app.post("/api/pipelines/bulk-tags")
def bulk_tag_pipelines(payload: PipelineBulkTag):
    """Add one tag to every selected saved pipeline at once -- the pipeline
    counterpart to POST /api/artifacts/bulk-tags: adds to whatever tags a
    pipeline already has, same as typing into its own '+ tag' field, just
    applied to a whole selection instead of one pipeline at a time. An
    unknown slug is skipped rather than failing the whole batch."""
    tag = payload.tag.strip()
    if not tag:
        raise HTTPException(status_code=400, detail="A tag is required.")

    existing_tags = store.all_pipeline_tags()
    tagged = []
    for slug in payload.slugs:
        try:
            pipeline_store.load_pipeline(slug)
        except FileNotFoundError:
            continue
        current = set(existing_tags.get(slug, []))
        current.add(tag)
        store.set_pipeline_tags(slug, sorted(current))
        tagged.append(slug)
    store.record_audit_event(
        "pipeline_bulk_tag", f"Added tag '{tag}' to {len(tagged)} selected pipeline(s): {tagged}."
    )
    return {"tag": tag, "tagged": tagged}


class PipelineBulkUntag(BaseModel):
    slugs: list[str]
    tag: str


@app.post("/api/pipelines/bulk-untag")
def bulk_untag_pipelines(payload: PipelineBulkUntag):
    """Remove one tag from every selected saved pipeline's existing tag set
    in one action, leaving any other tags alone -- the pipeline counterpart
    to POST /api/artifacts/bulk-untag. An unknown slug is skipped rather
    than failing the whole batch; a pipeline that never had this tag is a
    no-op, not skipped, since it's still a real, existing pipeline."""
    tag = payload.tag.strip()
    if not tag:
        raise HTTPException(status_code=400, detail="A tag is required.")

    existing_tags = store.all_pipeline_tags()
    untagged = []
    for slug in payload.slugs:
        try:
            pipeline_store.load_pipeline(slug)
        except FileNotFoundError:
            continue
        current = set(existing_tags.get(slug, []))
        current.discard(tag)
        store.set_pipeline_tags(slug, sorted(current))
        untagged.append(slug)
    store.record_audit_event(
        "pipeline_bulk_untag", f"Removed tag '{tag}' from {len(untagged)} selected pipeline(s): {untagged}."
    )
    return {"tag": tag, "untagged": untagged}


class PipelineTagRename(BaseModel):
    old_tag: str
    new_tag: str


@app.post("/api/pipelines/rename-tag")
def rename_pipeline_tag(payload: PipelineTagRename):
    """Rename a tag across every saved pipeline that carries it in one
    action -- the pipeline counterpart to POST /api/artifacts/rename-tag,
    applied to pipeline_store's own independent tag set instead of the
    artifact one. If a pipeline already carries new_tag too, the rename
    just merges into that (no duplicate, same as any other tag set, which
    is stored as a de-duplicated, sorted list)."""
    old_tag = payload.old_tag.strip()
    new_tag = payload.new_tag.strip()
    if not old_tag or not new_tag:
        raise HTTPException(status_code=400, detail="Both old_tag and new_tag are required.")
    if old_tag == new_tag:
        raise HTTPException(status_code=400, detail="new_tag must be different from old_tag.")

    renamed = []
    for slug, tags in store.all_pipeline_tags().items():
        if old_tag not in tags:
            continue
        current = set(tags)
        current.discard(old_tag)
        current.add(new_tag)
        store.set_pipeline_tags(slug, sorted(current))
        renamed.append(slug)
    return {"old_tag": old_tag, "new_tag": new_tag, "renamed": renamed}


@app.get("/api/pipelines/compare")
def compare_pipelines(a: str, b: str):
    """Side-by-side key-level diff between two saved pipelines' current
    definitions -- the same _diff_dicts() shallow diff already used for
    /api/runs/compare and a pipeline's own version history, applied to two
    different pipelines instead of two runs or two versions of one."""
    try:
        definition_a = pipeline_store.load_pipeline(a)
    except FileNotFoundError:
        raise HTTPException(status_code=404, detail=f"No saved pipeline named '{a}'.")
    try:
        definition_b = pipeline_store.load_pipeline(b)
    except FileNotFoundError:
        raise HTTPException(status_code=404, detail=f"No saved pipeline named '{b}'.")
    return {"a": definition_a, "b": definition_b, "diff": _diff_dicts(definition_a, definition_b)}


@app.get("/api/pipelines/{slug}/graph")
def pipeline_graph(slug: str):
    try:
        definition = pipeline_store.load_pipeline(slug)
    except FileNotFoundError:
        raise HTTPException(status_code=404, detail=f"No saved pipeline named '{slug}'.")
    return graph.build_pipeline_graph(definition)


@app.get("/api/pipeline-templates")
def list_pipeline_templates():
    """Built-in starter templates — static, curated content, not user data."""
    return PIPELINE_TEMPLATES


@app.post("/api/pipeline-templates/{template_id}/clone")
def clone_pipeline_template(template_id: str):
    template = next((t for t in PIPELINE_TEMPLATES if t["id"] == template_id), None)
    if template is None:
        raise HTTPException(status_code=404, detail=f"No template named '{template_id}'.")
    saved = pipeline_store.save_pipeline_from_template(template, TIER_DIRS)
    return {"pipeline": saved}


@app.get("/api/pipelines/{slug}/versions")
def list_pipeline_versions(slug: str):
    """404s only when there's truly nothing under this slug — neither a
    current pipeline nor any archived version. `DELETE /api/pipelines/{slug}`
    only removes the current definition, not its version history, so a
    deleted pipeline's versions stay listable (and restorable via
    `POST .../restore`, which never required the current file to exist
    either) without needing to recreate it under the same name first."""
    versions = pipeline_store.list_pipeline_versions(slug)
    if not versions:
        try:
            pipeline_store.load_pipeline(slug)
        except FileNotFoundError:
            raise HTTPException(status_code=404, detail=f"No saved pipeline named '{slug}'.")
    return versions


@app.post("/api/pipelines/{slug}/versions/prune")
def prune_pipeline_versions(slug: str, keep: int = 5):
    """Delete a pipeline's oldest archived versions, keeping only the most
    recent `keep`. A POST route, so it doesn't collide with the GET
    .../versions/{version_id} route at the same path depth -- FastAPI
    dispatches by method first, so a literal-vs-dynamic segment only
    needs ordering care within the *same* HTTP method (see the
    run-detail .json route elsewhere in this file for that case)."""
    if keep < 0:
        raise HTTPException(status_code=400, detail="keep must be zero or greater.")
    try:
        pipeline_store.load_pipeline(slug)
    except FileNotFoundError:
        if not pipeline_store.list_pipeline_versions(slug):
            raise HTTPException(status_code=404, detail=f"No saved pipeline named '{slug}'.")
    deleted = pipeline_store.prune_pipeline_versions(slug, keep)
    store.record_audit_event(
        "pipeline_version_prune", f"Pruned {deleted} old version(s) of pipeline '{slug}', keeping the latest {keep}."
    )
    return {"deleted": deleted}


@app.get("/api/pipelines/{slug}/versions/{version_id}")
def get_pipeline_version(slug: str, version_id: str):
    try:
        current = pipeline_store.load_pipeline(slug)
    except FileNotFoundError:
        raise HTTPException(status_code=404, detail=f"No saved pipeline named '{slug}'.")
    try:
        version = pipeline_store.load_pipeline_version(slug, version_id)
    except FileNotFoundError:
        raise HTTPException(status_code=404, detail=f"No version '{version_id}' for pipeline '{slug}'.")
    return {"version": version, "diff": _diff_dicts(version, current)}


@app.get("/api/pipelines/{slug}/versions/{version_id}/compare/{other_version_id}")
def compare_pipeline_versions(slug: str, version_id: str, other_version_id: str):
    """Diff any two archived versions of the same pipeline against each
    other -- distinct from GET .../versions/{version_id}, which only ever
    diffs one archived version against the pipeline's *current*
    definition. Reuses the same _diff_dicts() shallow diff."""
    try:
        version_a = pipeline_store.load_pipeline_version(slug, version_id)
    except FileNotFoundError:
        raise HTTPException(status_code=404, detail=f"No version '{version_id}' for pipeline '{slug}'.")
    try:
        version_b = pipeline_store.load_pipeline_version(slug, other_version_id)
    except FileNotFoundError:
        raise HTTPException(status_code=404, detail=f"No version '{other_version_id}' for pipeline '{slug}'.")
    return {"a": version_a, "b": version_b, "diff": _diff_dicts(version_a, version_b)}


@app.post("/api/pipelines/{slug}/versions/{version_id}/restore")
def restore_pipeline_version(slug: str, version_id: str):
    try:
        restored = pipeline_store.restore_pipeline_version(slug, version_id, TIER_DIRS)
    except FileNotFoundError:
        raise HTTPException(status_code=404, detail=f"No version '{version_id}' for pipeline '{slug}'.")
    except pipeline_store.PipelineValidationError as exc:
        raise HTTPException(status_code=400, detail=str(exc))
    store.record_audit_event("pipeline_version_restore", f"Restored '{slug}' to version {version_id}.")
    return {"pipeline": restored}


class PipelineVersionBranch(BaseModel):
    new_name: str


@app.post("/api/pipelines/{slug}/versions/{version_id}/branch")
def branch_pipeline_version(slug: str, version_id: str, payload: PipelineVersionBranch):
    """Restore an archived version as a brand-new pipeline instead of
    overwriting `slug`'s current definition -- for when an old version is
    worth keeping as its own pipeline, not just recovering from a mistake."""
    if not payload.new_name.strip():
        raise HTTPException(status_code=400, detail="A name for the new pipeline is required.")
    try:
        branched = pipeline_store.branch_pipeline_version(slug, version_id, payload.new_name.strip(), TIER_DIRS)
    except FileNotFoundError:
        raise HTTPException(status_code=404, detail=f"No version '{version_id}' for pipeline '{slug}'.")
    except pipeline_store.PipelineValidationError as exc:
        raise HTTPException(status_code=400, detail=str(exc))
    store.record_audit_event(
        "pipeline_version_branch", f"Branched '{slug}' version {version_id} into new pipeline '{branched['slug']}'."
    )
    return {"pipeline": branched}


@app.get("/api/schedules")
def list_schedules():
    return store.list_schedules()


@app.get("/api/schedules.csv")
def schedules_csv():
    """Same schedule list as GET /api/schedules, as a downloadable CSV --
    mirrors every other CSV export in this app. A literal path, not a
    suffix on a dynamic segment, so there's no route-ordering conflict
    with /api/schedules/{schedule_id}."""
    buffer = io.StringIO()
    fieldnames = [
        "id", "kind", "tier", "name", "label", "schedule_type", "interval_seconds",
        "daily_time", "day_of_week", "enabled", "next_run_at", "last_run_at", "last_status",
    ]
    writer = csv.DictWriter(buffer, fieldnames=fieldnames, extrasaction="ignore")
    writer.writeheader()
    for schedule in store.list_schedules():
        writer.writerow(schedule)
    return StreamingResponse(
        iter([buffer.getvalue()]),
        media_type="text/csv",
        headers={"Content-Disposition": "attachment; filename=schedules.csv"},
    )


@app.post("/api/schedules")
def create_schedule(payload: ScheduleCreate):
    if payload.kind not in ("module", "pipeline"):
        raise HTTPException(status_code=400, detail="kind must be 'module' or 'pipeline'")
    if payload.schedule_type not in ("interval", "daily", "weekly", "once"):
        raise HTTPException(status_code=400, detail="schedule_type must be 'interval', 'daily', 'weekly', or 'once'")

    now = datetime.now(timezone.utc)
    if payload.schedule_type == "interval":
        if payload.interval_seconds is None or payload.interval_seconds < MIN_SCHEDULE_INTERVAL_SECONDS:
            raise HTTPException(
                status_code=400,
                detail=f"interval_seconds must be at least {MIN_SCHEDULE_INTERVAL_SECONDS}",
            )
        next_run_at = now.isoformat()
    elif payload.schedule_type == "daily":
        if not payload.daily_time or not DAILY_TIME_RE.match(payload.daily_time):
            raise HTTPException(status_code=400, detail="daily_time must be in 'HH:MM' 24-hour format")
        next_run_at = next_daily_run_at(payload.daily_time, now).isoformat()
    elif payload.schedule_type == "weekly":
        if not payload.daily_time or not DAILY_TIME_RE.match(payload.daily_time):
            raise HTTPException(status_code=400, detail="daily_time must be in 'HH:MM' 24-hour format")
        if payload.day_of_week is None or not (0 <= payload.day_of_week <= 6):
            raise HTTPException(status_code=400, detail="day_of_week must be an integer from 0 (Monday) to 6 (Sunday)")
        next_run_at = next_weekly_run_at(payload.day_of_week, payload.daily_time, now).isoformat()
    else:
        if not payload.run_at:
            raise HTTPException(status_code=400, detail="run_at is required for a one-time schedule")
        try:
            parsed = datetime.fromisoformat(payload.run_at)
        except ValueError:
            raise HTTPException(status_code=400, detail="run_at must be a valid ISO 8601 datetime")
        run_at_utc = parsed.astimezone(timezone.utc)
        if run_at_utc <= now:
            raise HTTPException(status_code=400, detail="run_at must be in the future")
        next_run_at = run_at_utc.isoformat()

    if payload.kind == "module":
        if not payload.tier:
            raise HTTPException(status_code=400, detail="tier is required when kind is 'module'")
        _manifest_by_name(payload.tier, payload.name)  # 404s if it doesn't exist
    else:
        try:
            pipeline_store.load_pipeline(payload.name)
        except FileNotFoundError:
            raise HTTPException(status_code=404, detail=f"No saved pipeline named '{payload.name}'.")

    schedule = store.create_schedule(
        kind=payload.kind,
        name=payload.name,
        interval_seconds=payload.interval_seconds,
        next_run_at=next_run_at,
        tier=payload.tier,
        inputs=payload.inputs,
        schedule_type=payload.schedule_type,
        daily_time=payload.daily_time,
        day_of_week=payload.day_of_week,
    )
    return schedule


@app.patch("/api/schedules/{schedule_id}")
def update_schedule(schedule_id: int, payload: ScheduleUpdate):
    if payload.enabled is None and payload.label is None:
        raise HTTPException(status_code=400, detail="Provide 'enabled' and/or 'label' to update.")
    schedule = None
    if payload.enabled is not None:
        schedule = store.set_schedule_enabled(schedule_id, payload.enabled)
        if schedule is None:
            raise HTTPException(status_code=404, detail=f"No schedule with id {schedule_id}.")
    if payload.label is not None:
        schedule = store.set_schedule_label(schedule_id, payload.label)
        if schedule is None:
            raise HTTPException(status_code=404, detail=f"No schedule with id {schedule_id}.")
    return schedule


@app.delete("/api/schedules/{schedule_id}")
def delete_schedule(schedule_id: int):
    store.delete_schedule(schedule_id)
    return {"deleted": schedule_id}


@app.post("/api/schedules/{schedule_id}/run-now")
def run_schedule_now(schedule_id: int):
    """Fire a schedule's target immediately, once, without touching its
    next_run_at or cadence -- for "I don't want to wait for the next
    tick" rather than editing the schedule itself. Reuses the exact same
    _trigger_schedule() dispatch the scheduler's own timer calls, so a
    manual run goes through the same breaker/enabled checks and lands in
    Recent Runs identically to a real scheduled fire; the only thing that
    doesn't happen is the schedule row's own bookkeeping (next_run_at,
    last_run_at, last_status), which is exactly the point."""
    schedule = next((s for s in store.list_schedules() if s["id"] == schedule_id), None)
    if schedule is None:
        raise HTTPException(status_code=404, detail=f"No schedule with id {schedule_id}.")
    try:
        _trigger_schedule(schedule)
    except Exception as exc:
        raise HTTPException(status_code=400, detail=str(exc))
    store.record_audit_event(
        "schedule_run_now", f"Manually ran schedule {schedule_id} ({schedule['kind']}/{schedule['name']}) now."
    )
    return {"triggered": True}


@app.get("/api/schedules/{schedule_id}/next-occurrences")
def schedule_next_occurrences(schedule_id: int, count: int = 5):
    """The next `count` fire times for a daily or weekly schedule -- unlike
    an interval schedule (whose next occurrences are just a trivial
    now + n*interval) or a one-time schedule (which only ever fires once),
    daily/weekly cadences are the only kind where "what are my next few
    fire times" is genuinely useful to preview before it happens."""
    schedule = next((s for s in store.list_schedules() if s["id"] == schedule_id), None)
    if schedule is None:
        raise HTTPException(status_code=404, detail=f"No schedule with id {schedule_id}.")
    if schedule["schedule_type"] not in ("daily", "weekly"):
        raise HTTPException(
            status_code=400,
            detail="next-occurrences is only meaningful for 'daily' or 'weekly' schedules.",
        )
    if count < 1:
        raise HTTPException(status_code=400, detail="count must be at least 1.")

    now = datetime.now(timezone.utc)
    if schedule["schedule_type"] == "daily":
        occurrences = next_n_daily_run_ats(schedule["daily_time"], now, count)
    else:
        occurrences = next_n_weekly_run_ats(schedule["day_of_week"], schedule["daily_time"], now, count)
    return {"occurrences": [dt.isoformat() for dt in occurrences]}


class BulkScheduleIds(BaseModel):
    schedule_ids: list[int]


@app.post("/api/schedules/bulk-delete")
def bulk_delete_schedules(payload: BulkScheduleIds):
    """Delete a user-picked set of schedules in one action, mirroring the
    existing bulk-delete pattern for runs and saved pipelines. An unknown
    id is simply a no-op delete (delete_schedule() already tolerates
    that), so every id in the request is reported as deleted."""
    for schedule_id in payload.schedule_ids:
        store.delete_schedule(schedule_id)
    return {"deleted": payload.schedule_ids}


class BulkScheduleSetEnabled(BaseModel):
    schedule_ids: list[int]
    enabled: bool


@app.post("/api/schedules/bulk-set-enabled")
def bulk_set_schedules_enabled(payload: BulkScheduleSetEnabled):
    """Pause or resume a user-picked set of schedules at once -- the same
    per-schedule enabled flag PATCH /api/schedules/{id} already flips,
    just applied to a whole selection. An unknown id is skipped rather
    than failing the whole batch."""
    updated = []
    for schedule_id in payload.schedule_ids:
        schedule = store.set_schedule_enabled(schedule_id, payload.enabled)
        if schedule is not None:
            updated.append(schedule_id)
    return {"updated": updated, "enabled": payload.enabled}


@app.post("/api/schedules/bulk-run-now")
def bulk_run_schedules_now(payload: BulkScheduleIds):
    """The same immediate, cadence-preserving fire as
    POST /api/schedules/{id}/run-now, applied to a whole checkbox
    selection at once. Best-effort: one schedule blocked by an open
    breaker (or any other trigger failure) doesn't stop the rest of the
    selection from running -- each id succeeds or fails independently,
    same as every other bulk action in this app."""
    schedules_by_id = {s["id"]: s for s in store.list_schedules()}
    triggered = []
    failed = {}
    for schedule_id in payload.schedule_ids:
        schedule = schedules_by_id.get(schedule_id)
        if schedule is None:
            failed[schedule_id] = "No schedule with this id."
            continue
        try:
            _trigger_schedule(schedule)
            triggered.append(schedule_id)
        except Exception as exc:
            failed[schedule_id] = str(exc)
    store.record_audit_event(
        "schedule_bulk_run_now", f"Manually ran {len(triggered)} selected schedule(s) now."
    )
    return {"triggered": triggered, "failed": failed}


@app.post("/api/schedules/bulk-duplicate")
def bulk_duplicate_schedules(payload: BulkScheduleIds):
    """Actually clones each selected schedule into a brand new schedule
    row with the same kind/target/inputs/cadence -- distinct from the
    existing per-row "Duplicate" button, which only pre-fills the
    create-schedule form for the user to review and submit by hand. An
    interval/daily/weekly cadence is recomputed fresh from now (a cloned
    interval schedule starts its own countdown; a cloned daily/weekly
    schedule targets its own next occurrence), while a one-time schedule's
    next_run_at is carried over as-is. An unknown id is skipped rather
    than failing the whole batch."""
    schedules_by_id = {s["id"]: s for s in store.list_schedules()}
    now = datetime.now(timezone.utc)
    duplicated = []
    for schedule_id in payload.schedule_ids:
        schedule = schedules_by_id.get(schedule_id)
        if schedule is None:
            continue
        schedule_type = schedule["schedule_type"]
        if schedule_type == "interval":
            next_run_at = now.isoformat()
        elif schedule_type == "daily":
            next_run_at = next_daily_run_at(schedule["daily_time"], now).isoformat()
        elif schedule_type == "weekly":
            next_run_at = next_weekly_run_at(schedule["day_of_week"], schedule["daily_time"], now).isoformat()
        else:
            next_run_at = schedule["next_run_at"]
        duplicated.append(
            store.create_schedule(
                kind=schedule["kind"],
                name=schedule["name"],
                interval_seconds=schedule["interval_seconds"],
                next_run_at=next_run_at,
                tier=schedule["tier"],
                inputs=schedule["inputs"],
                schedule_type=schedule_type,
                daily_time=schedule["daily_time"],
                day_of_week=schedule["day_of_week"],
            )
        )
    store.record_audit_event(
        "schedule_bulk_duplicate",
        f"Duplicated {len(duplicated)} selected schedule(s): {[s['id'] for s in duplicated]}.",
    )
    return {"duplicated": duplicated}


@app.post("/api/schedules/bulk-export")
def bulk_export_schedules(payload: BulkScheduleIds):
    """Download a user-picked set of schedules as a JSON file -- the
    finer-grained counterpart to the full-list CSV export
    (GET /api/schedules.csv), for backing up or sharing just a handful of
    schedules rather than the whole list. An unknown id is silently
    skipped rather than failing the whole request."""
    all_schedules = {s["id"]: s for s in store.list_schedules()}
    selected = [all_schedules[sid] for sid in payload.schedule_ids if sid in all_schedules]
    buffer = json.dumps(selected, indent=2)
    return StreamingResponse(
        iter([buffer]),
        media_type="application/json",
        headers={"Content-Disposition": "attachment; filename=schedules_selected.json"},
    )


@app.post("/api/schedules/bulk-clear-label")
def bulk_clear_schedule_labels(payload: BulkScheduleIds):
    """Blank out the label on a user-picked set of schedules at once,
    mirroring the existing bulk-remove-tag pattern for artifacts/pipelines.
    An unknown id is skipped rather than failing the whole batch."""
    updated = []
    for schedule_id in payload.schedule_ids:
        schedule = store.set_schedule_label(schedule_id, "")
        if schedule is not None:
            updated.append(schedule_id)
    return {"updated": updated}


@app.get("/api/scheduler/status")
def scheduler_status():
    return {"paused": _scheduler.paused}


@app.post("/api/scheduler/pause")
def pause_scheduler():
    """Master pause for the whole scheduler — a maintenance-window switch,
    distinct from pausing any individual schedule. Every schedule's own
    `enabled` flag is left untouched; nothing fires until resumed."""
    _scheduler.pause()
    store.record_audit_event("scheduler_pause", "Paused the scheduler — no schedule will fire until resumed.")
    return {"paused": True}


@app.post("/api/scheduler/resume")
def resume_scheduler():
    _scheduler.resume()
    store.record_audit_event("scheduler_resume", "Resumed the scheduler.")
    return {"paused": False}


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
    runs = store.recent_runs(limit)
    notes_by_run = store.all_run_notes()
    for run in runs:
        run["note"] = notes_by_run.get(run["id"], "")
    return runs


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


@app.get("/api/runs.json")
def recent_runs_json(limit: int = 100):
    """Same summary run-history list as GET /api/runs.csv and
    /api/runs.xlsx, as a downloadable JSON file -- distinct from
    GET /api/runs/{run_id}.json, which downloads one run's full nested
    detail (steps/inputs/outputs/blackboard), not the whole list."""
    runs = store.recent_runs(limit)
    notes_by_run = store.all_run_notes()
    for run in runs:
        run["note"] = notes_by_run.get(run["id"], "")
    return StreamingResponse(
        iter([json.dumps(runs, indent=2)]),
        media_type="application/json",
        headers={"Content-Disposition": "attachment; filename=run_history.json"},
    )


@app.get("/api/runs/search")
def search_run_history(q: str = "", limit: int = 20):
    """Full-text keyword search across past run step outputs and errors —
    the run-history counterpart to /api/artifacts/search. Step outputs are
    redacted before they're ever logged, so snippets are already safe."""
    return {"query": q, "results": store.search_steps(q, limit)}


@app.get("/api/runs/search-notes")
def search_run_notes(q: str = "", limit: int = 20):
    """Keyword search across the free-text notes attached to past runs
    (Batch 13) -- distinct from /api/runs/search, which searches step
    outputs/errors, not the user's own annotations."""
    return {"query": q, "results": store.search_run_notes(q, limit)}


@app.post("/api/runs/purge")
def purge_runs(older_than_hours: float = 24 * 30):
    """Delete finished runs (and their steps) older than `older_than_hours`
    — mirrors artifact purge, but for run history instead of uploaded files.
    Defaults to 30 days; never touches a run that's still in progress."""
    removed = store.prune_runs(older_than_hours)
    store.record_audit_event("run_purge", f"Purged {removed} run(s) older than {older_than_hours}h.")
    return {"removed_count": removed}


class BulkDeleteRuns(BaseModel):
    run_ids: list[int]


@app.post("/api/runs/bulk-delete")
def bulk_delete_runs(payload: BulkDeleteRuns):
    """Delete a user-picked set of runs — the finer-grained counterpart to
    /api/runs/purge's age-based sweep, for a checkbox multi-select in the UI."""
    removed = store.delete_runs(payload.run_ids)
    store.record_audit_event("run_bulk_delete", f"Deleted {removed} selected run(s): {payload.run_ids}.")
    return {"removed_count": removed}


def _parsed_steps_for_run(run_id: int) -> list[dict]:
    steps = store.steps_for_run(run_id)
    for step in steps:
        step["output"] = json.loads(step["output"]) if step["output"] else {}
        step["inputs"] = json.loads(step["inputs"]) if step.get("inputs") else {}
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


def _run_detail(run_id: int) -> dict:
    steps = _parsed_steps_for_run(run_id)
    if not steps:
        raise HTTPException(status_code=404, detail=f"No recorded steps for run {run_id}.")
    return {
        "run_id": run_id,
        "steps": steps,
        "blackboard": store.get_run_blackboard(run_id),
        "note": store.get_run_note(run_id) or "",
    }


@app.get("/api/runs/{run_id}.json")
def download_run_detail(run_id: int):
    """The same full run detail as GET /api/runs/{run_id} (every step's
    inputs/outputs/timing, the shared blackboard, and any note), as a
    downloadable JSON file -- distinct from GET /api/runs.csv and
    /api/runs.xlsx, which only ever cover the summary run-history list,
    never one run's full nested detail. Registered *before*
    GET /api/runs/{run_id} -- same route-ordering requirement as every
    other literal-suffixed dynamic path in this file (e.g. /api/pipelines
    export routes): otherwise Starlette's plain {run_id} matches
    "N.json" too and 422s on the int conversion before this route is
    ever tried."""
    detail = _run_detail(run_id)
    return StreamingResponse(
        iter([json.dumps(detail, indent=2)]),
        media_type="application/json",
        headers={"Content-Disposition": f"attachment; filename=run_{run_id}.json"},
    )


@app.get("/api/runs/{run_id}")
def run_detail(run_id: int):
    return _run_detail(run_id)


class RunNoteUpdate(BaseModel):
    note: str = ""


@app.put("/api/runs/{run_id}/note")
def set_run_note(run_id: int, payload: RunNoteUpdate):
    """Attach (or overwrite) a free-text note on a past run -- purely for the
    user's own future reference (e.g. "this failure was expected, ignore").
    An empty string clears it. Unlike most run endpoints this doesn't require
    the run to have any recorded steps -- notes are keyed by run_id alone,
    so this 404s only when the run itself never existed."""
    if not store.run_exists(run_id):
        raise HTTPException(status_code=404, detail=f"No run with id {run_id}.")
    return {"run_id": run_id, "note": store.set_run_note(run_id, payload.note.strip())}


@app.post("/api/runs/{run_id}/rerun")
def rerun_run(run_id: int, http_request: Request):
    """Replay a past run: re-instantiate each recorded step's module and
    seed it with the exact resolved input values that step actually ran
    with, executing them in the same order as a brand-new run. This
    replays each step's own module + inputs faithfully; it does NOT
    reconstruct whatever pipeline (mappings, conditions, parallel groups)
    originally produced that run — that topology isn't part of run
    history, only each step's module and its resolved inputs are. A
    secret-shaped input is redacted before it's ever logged (same as
    everywhere else), so a re-run can't recover or resend the original
    secret value — only whatever masked placeholder history holds."""
    _enforce_run_rate_limit(http_request)
    steps = _parsed_steps_for_run(run_id)
    if not steps:
        raise HTTPException(status_code=404, detail=f"No recorded steps for run {run_id}.")

    _ensure_modules_runnable([(s["tier"], s["name"]) for s in steps])

    specs = []
    for s in steps:
        manifest = _manifest_by_name(s["tier"], s["name"])
        module = instantiate(manifest["entrypoint"])
        recorded_inputs = dict(s["inputs"])
        specs.append(
            StepSpec(
                module=module,
                seed=lambda ctx, inputs=recorded_inputs: dict(inputs),
                timeout_seconds=DEFAULT_STEP_TIMEOUT_SECONDS,
            )
        )

    stream_id = _launch_steps(specs)
    return {"stream_id": stream_id, "replayed_steps": len(specs)}


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
    snapshot = _build_backup_snapshot()
    return StreamingResponse(
        iter([json.dumps(snapshot, indent=2, default=str)]),
        media_type="application/json",
        headers={"Content-Disposition": "attachment; filename=ai-boss-backup.json"},
    )


@app.get("/api/backup/auto/status")
def auto_backup_status():
    """Whether the automatic periodic backup timer is on, its interval and
    retention, and when it last (and will next) run."""
    return _auto_backup.status()


class AutoBackupConfig(BaseModel):
    enabled: bool
    interval_hours: float = 24.0
    keep_count: int = 7


@app.patch("/api/backup/auto")
def configure_auto_backup(payload: AutoBackupConfig):
    if payload.interval_hours <= 0:
        raise HTTPException(status_code=400, detail="interval_hours must be greater than 0.")
    if payload.keep_count < 1:
        raise HTTPException(status_code=400, detail="keep_count must be at least 1.")
    _auto_backup.configure(payload.enabled, payload.interval_hours, payload.keep_count)
    store.record_audit_event(
        "auto_backup_configured",
        f"Automatic backup {'enabled' if payload.enabled else 'disabled'} "
        f"(every {payload.interval_hours}h, keep last {payload.keep_count}).",
    )
    return _auto_backup.status()


@app.post("/api/backup/auto/run-now")
def run_auto_backup_now():
    """Write a snapshot immediately, outside the timer's own interval --
    for "back this up right now" without waiting or changing the
    configured cadence."""
    timestamp = _auto_backup.run_now()
    return {"backed_up_at": timestamp}


@app.get("/api/backup/db")
def download_backup_db():
    """The raw SQLite file itself — an exact copy, not just what
    export_snapshot() knows how to describe (e.g. the result_cache table)."""
    return FileResponse(store.db_path, filename="orchestrator.db", media_type="application/octet-stream")


def _apply_backup_restore(snapshot: dict) -> dict:
    """Shared by the upload-based restore and the restore-from-an-
    automatic-backup-file endpoint below. Additive only — fills in whatever
    isn't already present (by id/key/slug), never overwrites existing data.
    A pipeline referencing a module that no longer exists here is skipped
    rather than failing the whole restore."""
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


@app.post("/api/backup/restore")
async def restore_backup(file: UploadFile = File(...)):
    """Restore a JSON snapshot from GET /api/backup/export."""
    try:
        data = await ingestion.read_upload_with_limit(file)
    except ingestion.UploadTooLargeError as exc:
        raise HTTPException(status_code=413, detail=str(exc))
    try:
        snapshot = json.loads(data)
    except json.JSONDecodeError as exc:
        raise HTTPException(status_code=400, detail=f"Could not parse backup file: {exc}")

    counts = _apply_backup_restore(snapshot)
    summary = ", ".join(f"{k}: {v}" for k, v in counts.items())
    store.record_audit_event("backup_restore", f"Restored backup snapshot ({summary}).")
    return counts


def _list_auto_backups() -> list[dict]:
    if not BACKUPS_DIR.exists():
        return []
    protected = store.all_protected_backup_filenames()
    files = sorted(BACKUPS_DIR.glob("backup_*.json"), key=lambda p: p.name, reverse=True)
    return [
        {
            "filename": f.name,
            "size_bytes": f.stat().st_size,
            "modified_at": datetime.fromtimestamp(f.stat().st_mtime, timezone.utc).isoformat(),
            "protected": f.name in protected,
        }
        for f in files
    ]


@app.get("/api/backup/auto/list")
def list_auto_backups():
    """The automatic-backup snapshots currently sitting in backups/ --
    newest first -- so the dashboard can offer a one-click restore from one
    of them without the user having to find and re-upload the file by
    hand."""
    return _list_auto_backups()


@app.get("/api/backup/auto/list.csv")
def list_auto_backups_csv():
    """Same snapshot listing as GET /api/backup/auto/list, as a downloadable
    CSV -- mirroring every other list-to-CSV export in this app."""
    buffer = io.StringIO()
    writer = csv.DictWriter(buffer, fieldnames=["filename", "size_bytes", "modified_at", "protected"])
    writer.writeheader()
    writer.writerows(_list_auto_backups())
    return StreamingResponse(
        iter([buffer.getvalue()]),
        media_type="text/csv",
        headers={"Content-Disposition": "attachment; filename=auto_backup_snapshots.csv"},
    )


@app.get("/api/backup/auto/download-all")
def download_all_auto_backups():
    """Every automatic-backup snapshot currently in backups/ bundled into a
    single zip -- for offline safekeeping of the whole automatic-backup
    history at once, mirroring how POST /api/artifacts/bulk-download zips a
    selection of artifacts. Unlike that endpoint this one is a plain GET
    with no selection to make (there's exactly one thing to download: all
    of it), so no request body or query string is needed. 404s if
    backups/ is empty rather than returning an empty zip."""
    snapshots = _list_auto_backups()
    if not snapshots:
        raise HTTPException(status_code=404, detail="No automatic backup snapshots to download yet.")

    buffer = io.BytesIO()
    with zipfile.ZipFile(buffer, "w", zipfile.ZIP_DEFLATED) as zf:
        for snapshot in snapshots:
            zf.write(BACKUPS_DIR / snapshot["filename"], arcname=snapshot["filename"])

    buffer.seek(0)
    return StreamingResponse(
        buffer,
        media_type="application/zip",
        headers={"Content-Disposition": "attachment; filename=auto_backup_snapshots.zip"},
    )


@app.post("/api/backup/auto/purge")
def purge_auto_backups(older_than_hours: float = 24 * 7):
    """Delete automatic backup snapshots older than `older_than_hours` --
    the age-based counterpart to the configured keep_count's "always keep
    exactly N most recent" pruning, mirroring the existing age-based
    audit-log purge (POST /api/audit-log/purge) and run-history retention
    controls elsewhere in this app."""
    if older_than_hours <= 0:
        raise HTTPException(status_code=400, detail="older_than_hours must be greater than 0.")
    deleted = _auto_backup.purge_older_than(older_than_hours)
    if deleted:
        store.record_audit_event(
            "auto_backup_purge", f"Purged {deleted} automatic backup snapshot(s) older than {older_than_hours}h."
        )
    return {"deleted": deleted}


def _resolve_auto_backup_path(filename: str) -> Path:
    """Shared filename validation for every endpoint below that reads or
    deletes a specific file out of backups/ -- rejects path traversal
    (any '/' or '\\') and anything that isn't shaped like a snapshot this
    timer itself would have written, before ever touching the filesystem."""
    if "/" in filename or "\\" in filename or not filename.startswith("backup_") or not filename.endswith(".json"):
        raise HTTPException(status_code=404, detail=f"No automatic backup file named '{filename}'.")
    path = BACKUPS_DIR / filename
    if not path.is_file():
        raise HTTPException(status_code=404, detail=f"No automatic backup file named '{filename}'.")
    return path


@app.post("/api/backup/auto/restore/{filename}")
def restore_auto_backup(filename: str):
    """Restore directly from one of the timestamped snapshots in backups/
    written by the automatic backup timer (see /api/backup/auto/list) --
    the same additive merge as POST /api/backup/restore, just sourced from
    disk instead of a fresh upload."""
    path = _resolve_auto_backup_path(filename)
    try:
        snapshot = json.loads(path.read_text())
    except json.JSONDecodeError as exc:
        raise HTTPException(status_code=400, detail=f"Could not parse backup file: {exc}")

    counts = _apply_backup_restore(snapshot)
    summary = ", ".join(f"{k}: {v}" for k, v in counts.items())
    store.record_audit_event("backup_restore", f"Restored automatic backup snapshot '{filename}' ({summary}).")
    return counts


@app.get("/api/backup/auto/snapshot/{filename}")
def download_auto_backup_snapshot(filename: str):
    """Download one automatic backup snapshot's raw JSON file as-is --
    distinct from POST /api/backup/auto/restore/{filename}, which merges
    its contents into the live store instead of just handing you the
    file. Same GET/DELETE method-based dispatch as every other pair of
    routes sharing this path in this file (FastAPI dispatches by HTTP
    method first, so there's no ordering conflict with the DELETE handler
    below)."""
    path = _resolve_auto_backup_path(filename)
    return FileResponse(path, filename=filename, media_type="application/json")


@app.delete("/api/backup/auto/snapshot/{filename}")
def delete_auto_backup_snapshot(filename: str):
    """Remove a single automatic backup snapshot early, without waiting for
    the configured keep_count to prune it -- useful for freeing disk space
    or discarding a snapshot taken right before some bad state you don't
    want to keep around as a restore option."""
    path = _resolve_auto_backup_path(filename)
    path.unlink()
    store.clear_backup_protection(filename)
    store.record_audit_event("backup_snapshot_deleted", f"Deleted automatic backup snapshot '{filename}'.")
    return {"deleted": filename}


class BackupProtectUpdate(BaseModel):
    protected: bool


@app.put("/api/backup/auto/snapshot/{filename}/protect")
def set_auto_backup_protected(filename: str, payload: BackupProtectUpdate):
    """Pin (or unpin) a snapshot so it survives both the keep_count-based
    background prune and POST /api/backup/auto/purge's age-based cutoff --
    for a snapshot worth keeping around indefinitely regardless of how old
    it gets or how many newer ones pile up. Deleting the file directly via
    DELETE /api/backup/auto/snapshot/{filename} still works regardless of
    this flag; protection only exempts a snapshot from the two automatic/
    bulk removal paths, not from an explicit single-file delete."""
    _resolve_auto_backup_path(filename)
    protected = store.set_backup_protected(filename, payload.protected)
    return {"filename": filename, "protected": protected}


class BackupBulkProtect(BaseModel):
    filenames: list[str]
    protected: bool


@app.post("/api/backup/auto/bulk-protect")
def bulk_protect_auto_backups(payload: BackupBulkProtect):
    """Flip the protected flag for a whole checkbox selection of snapshots
    at once -- the bulk counterpart to PUT
    /api/backup/auto/snapshot/{filename}/protect, reusing the same
    selection checkboxes the Compare feature already puts on each
    snapshot row. An unknown or unsafe filename in the selection is
    skipped rather than failing the whole batch, matching every other
    bulk action in this app."""
    updated = []
    for filename in payload.filenames:
        if "/" in filename or "\\" in filename or not filename.startswith("backup_") or not filename.endswith(".json"):
            continue
        if not (BACKUPS_DIR / filename).is_file():
            continue
        store.set_backup_protected(filename, payload.protected)
        updated.append(filename)
    return {"protected": payload.protected, "updated": updated}


def _snapshot_counts(snapshot: dict) -> dict:
    counts = {key: len(snapshot.get(key) or []) for key in ("runs", "steps", "schedules", "ingested_files", "pipelines")}
    counts["memory"] = len(snapshot.get("memory") or {})
    return counts


@app.get("/api/backup/auto/compare")
def compare_auto_backups(a: str, b: str):
    """Diff two automatic backup snapshots' top-level record counts --
    mirroring the existing pipeline-version diff and artifact-schema
    compare features, but for backup snapshots: not a full deep diff of
    every field (these can hold thousands of run/step rows), just "how
    many runs/steps/schedules/etc. changed between these two points in
    time," which is the useful question when deciding which snapshot to
    restore from."""
    path_a = _resolve_auto_backup_path(a)
    path_b = _resolve_auto_backup_path(b)
    try:
        snapshot_a = json.loads(path_a.read_text())
        snapshot_b = json.loads(path_b.read_text())
    except json.JSONDecodeError as exc:
        raise HTTPException(status_code=400, detail=f"Could not parse a snapshot file: {exc}")

    counts_a = _snapshot_counts(snapshot_a)
    counts_b = _snapshot_counts(snapshot_b)
    delta = {key: counts_b[key] - counts_a[key] for key in counts_a}
    return {
        "a": {"filename": a, "exported_at": snapshot_a.get("exported_at"), "counts": counts_a},
        "b": {"filename": b, "exported_at": snapshot_b.get("exported_at"), "counts": counts_b},
        "delta": delta,
    }


@app.get("/api/audit-log")
def list_audit_log(limit: int = 50):
    """Newest-first log of destructive/administrative actions taken through
    this dashboard — what happened and when, without guessing. Populated only
    by the actions above; read-only, nothing here is user-editable."""
    return store.list_audit_events(limit)


@app.get("/api/audit-log/search")
def search_audit_log(q: str = ""):
    return {"query": q, "results": store.search_audit_events(q)}


@app.post("/api/audit-log/clear")
def clear_audit_log():
    """Delete every audit log entry unconditionally -- a manual full reset
    for when the trail isn't worth keeping at all, mirroring the existing
    'clear all read notifications'/'clear all favorites' controls. See
    POST /api/audit-log/purge for the age-based, partial-trim
    counterpart."""
    deleted = store.clear_audit_log()
    return {"deleted": deleted}


@app.post("/api/audit-log/purge")
def purge_audit_log(older_than_hours: float = 24):
    """Delete audit log entries older than `older_than_hours` -- the
    age-based, partial-trim counterpart to POST /api/audit-log/clear's
    unconditional wipe, mirroring the existing artifact-purge and
    run-history-retention controls elsewhere in this app."""
    deleted = store.purge_audit_log(older_than_hours)
    return {"deleted": deleted}


@app.get("/api/audit-log.csv")
def audit_log_csv(limit: int = 1000):
    """Same audit trail as the dashboard panel, as a downloadable CSV --
    mirrors the run-history CSV export pattern (GET /api/runs.csv)."""
    buffer = io.StringIO()
    writer = csv.DictWriter(buffer, fieldnames=["id", "action", "detail", "created_at"])
    writer.writeheader()
    for event in store.list_audit_events(limit):
        writer.writerow(event)

    return StreamingResponse(
        iter([buffer.getvalue()]),
        media_type="text/csv",
        headers={"Content-Disposition": "attachment; filename=audit_log.csv"},
    )


@app.get("/api/notifications")
def list_notifications(unread_only: bool = False, limit: int = 200):
    """Durable notifications -- unlike a toast (gone on reload), these
    survive: circuit breaker trips, schedule failures, and (from the
    frontend, when the resource ticker's own thresholds are crossed)
    resource-usage alerts."""
    return store.list_notifications(unread_only=unread_only, limit=limit)


@app.get("/api/notifications/search")
def search_notifications(q: str = ""):
    return {"query": q, "results": store.search_notifications(q)}


@app.get("/api/notifications.csv")
def notifications_csv(limit: int = 1000):
    """Same alert/notification history as the bell-icon dropdown, as a
    downloadable CSV -- mirrors the audit-log CSV export pattern
    (GET /api/audit-log.csv)."""
    buffer = io.StringIO()
    writer = csv.DictWriter(buffer, fieldnames=["id", "kind", "message", "created_at", "read"])
    writer.writeheader()
    for notification in store.list_notifications(limit=limit):
        writer.writerow(notification)

    return StreamingResponse(
        iter([buffer.getvalue()]),
        media_type="text/csv",
        headers={"Content-Disposition": "attachment; filename=notifications.csv"},
    )


@app.get("/api/notifications.json")
def notifications_json(limit: int = 1000):
    """Same alert/notification history as GET /api/notifications.csv, as a
    downloadable JSON file instead -- mirrors the existing runs.json and
    memory.json exports, which offer the same data both plain (for the UI
    to fetch) and as a Content-Disposition attachment (for saving to
    disk)."""
    buffer = json.dumps(store.list_notifications(limit=limit), indent=2)
    return StreamingResponse(
        iter([buffer]),
        media_type="application/json",
        headers={"Content-Disposition": "attachment; filename=notifications.json"},
    )


@app.get("/api/notifications/unread-count")
def notifications_unread_count():
    return {"count": store.unread_notification_count()}


class NotificationCreate(BaseModel):
    kind: str
    message: str


@app.post("/api/notifications")
def create_notification(payload: NotificationCreate):
    """Lets the frontend record a notification for something only it can see
    happening in real time -- specifically the resource-usage alert banner's
    own CPU/memory thresholds, computed client-side from the perf ticker.
    Silently accepted-but-not-stored (created: false) if this kind is muted,
    same as every other write path into notifications."""
    if not payload.message.strip():
        raise HTTPException(status_code=400, detail="A message is required.")
    kind = payload.kind.strip() or "info"
    notification = _notify(kind, payload.message.strip())
    if notification is None:
        return {"created": False, "kind": kind, "muted": True}
    return {"created": True, **notification}


@app.post("/api/notifications/{notification_id}/read")
def mark_notification_read(notification_id: int):
    marked = store.mark_notification_read(notification_id)
    if not marked:
        raise HTTPException(status_code=404, detail=f"No notification with id {notification_id}.")
    return {"id": notification_id, "read": True}


@app.post("/api/notifications/mark-all-read")
def mark_all_notifications_read():
    return {"marked": store.mark_all_notifications_read()}


@app.post("/api/notifications/clear-read")
def clear_read_notifications():
    """Delete every already-read notification from the Alerts list --
    distinct from mark-all-read, which only flips the read flag and keeps
    every row around forever."""
    return {"cleared": store.clear_read_notifications()}


class BulkNotificationIds(BaseModel):
    notification_ids: list[int]


@app.post("/api/notifications/bulk-mark-read")
def bulk_mark_notifications_read(payload: BulkNotificationIds):
    """Mark a user-picked set of notifications read at once -- the
    finer-grained counterpart to mark-all-read, for a checkbox
    multi-select in the Alerts list."""
    return {"marked": store.mark_notifications_read(payload.notification_ids)}


@app.post("/api/notifications/bulk-delete")
def bulk_delete_notifications(payload: BulkNotificationIds):
    """Delete a user-picked set of notifications at once, whether read or
    unread -- the finer-grained counterpart to clear-read, which only ever
    deletes already-read ones."""
    return {"deleted": store.delete_notifications(payload.notification_ids)}


@app.post("/api/notifications/bulk-export")
def bulk_export_notifications(payload: BulkNotificationIds):
    """Download a user-picked set of notifications as a CSV -- the
    finer-grained counterpart to the full-list GET /api/notifications.csv
    export, for a checkbox multi-select in the Alerts list."""
    wanted = set(payload.notification_ids)
    buffer = io.StringIO()
    writer = csv.DictWriter(buffer, fieldnames=["id", "kind", "message", "created_at", "read"])
    writer.writeheader()
    for notification in store.list_notifications(limit=100000):
        if notification["id"] in wanted:
            writer.writerow(notification)
    return StreamingResponse(
        iter([buffer.getvalue()]),
        media_type="text/csv",
        headers={"Content-Disposition": "attachment; filename=notifications_selected.csv"},
    )


NOTIFICATION_KINDS = ("breaker_tripped", "schedule_failed", "resource_alert", "schedule_once_fired", "backup_failed")

CRITICAL_WEBHOOK_NOTIFICATION_KINDS = {"breaker_tripped", "schedule_failed", "backup_failed"}


class NotificationWebhookState:
    def __init__(self):
        self.enabled = False
        self.url = ""


_notification_webhook = NotificationWebhookState()


def _forward_notification_to_webhook(notification: dict) -> None:
    """POST the notification's own JSON shape to the configured webhook
    URL -- a plain HTTP POST to a user-supplied URL, the same trust model
    as the existing http_request automation module and URL-ingestion
    feature, not a SaaS integration. Failures are recorded to the audit
    log rather than raised (a broken webhook must never break the calling
    code path, whether that's a request handler or the scheduler's own
    background thread) and are deliberately NOT routed back through
    _notify(), to avoid a "webhook failed" notification trying to
    re-forward itself in a loop."""
    try:
        httpx.post(_notification_webhook.url, json=notification, timeout=5.0)
    except Exception as exc:
        store.record_audit_event(
            "notification_webhook_failed",
            f"Failed to forward a '{notification['kind']}' notification to the configured webhook: {exc}",
        )


def _notify(kind: str, message: str) -> Optional[dict]:
    """The single choke point every notification-creation call site in
    this file goes through instead of calling store.add_notification()
    directly -- creates the durable notification, then forwards it to the
    configured outbound webhook if one is enabled and the kind is one of
    the critical ones already surfaced as a desktop notification (Batch 31)."""
    notification = store.add_notification(kind, message)
    if notification is not None and _notification_webhook.enabled and kind in CRITICAL_WEBHOOK_NOTIFICATION_KINDS:
        _forward_notification_to_webhook(notification)
    return notification


class NotificationWebhookConfig(BaseModel):
    enabled: bool
    url: str = ""


@app.get("/api/notifications/webhook")
def get_notification_webhook_config():
    return {"enabled": _notification_webhook.enabled, "url": _notification_webhook.url}


@app.patch("/api/notifications/webhook")
def configure_notification_webhook(payload: NotificationWebhookConfig):
    if payload.enabled and not payload.url.strip():
        raise HTTPException(status_code=400, detail="url is required when enabling the webhook.")
    _notification_webhook.enabled = payload.enabled
    _notification_webhook.url = payload.url.strip()
    store.record_audit_event(
        "notification_webhook_configured",
        f"Notification webhook {'enabled' if payload.enabled else 'disabled'}"
        + (f" ({_notification_webhook.url})" if payload.enabled else "")
        + ".",
    )
    return {"enabled": _notification_webhook.enabled, "url": _notification_webhook.url}


class NotificationWebhookTestRequest(BaseModel):
    url: str


@app.post("/api/notifications/webhook/test")
def test_notification_webhook(payload: NotificationWebhookTestRequest):
    """Send a synthetic sample notification to the given URL immediately --
    the notification-webhook counterpart to the existing "send a test
    webhook from a saved pipeline card" feature (which tests this app's own
    *inbound* webhook receiver, not an outbound one). Lets a user verify a
    URL actually works before waiting for a real breaker trip or schedule
    failure to fire it. Does not require the webhook to be enabled or
    saved first -- tests whatever URL is currently typed."""
    if not payload.url.strip():
        raise HTTPException(status_code=400, detail="url is required.")
    sample = {
        "id": 0,
        "kind": "test",
        "message": "This is a test notification from AI-Boss.",
        "created_at": datetime.now(timezone.utc).isoformat(),
        "read": False,
    }
    try:
        httpx.post(payload.url.strip(), json=sample, timeout=5.0)
    except Exception as exc:
        return {"sent": False, "error": str(exc)}
    return {"sent": True}


@app.get("/api/notifications/preferences")
def get_notification_preferences():
    """Mute state for every notification kind this app ever writes -- a kind
    absent from the dict (never toggled) reads as unmuted."""
    mute_state = store.all_notification_mute_state()
    return {kind: mute_state.get(kind, False) for kind in NOTIFICATION_KINDS}


class NotificationPreferenceUpdate(BaseModel):
    muted: bool


@app.put("/api/notifications/preferences/{kind}")
def set_notification_preference(kind: str, payload: NotificationPreferenceUpdate):
    if kind not in NOTIFICATION_KINDS:
        raise HTTPException(status_code=400, detail=f"Unknown notification kind '{kind}'.")
    store.set_notification_kind_muted(kind, payload.muted)
    return {"kind": kind, "muted": payload.muted}


@app.put("/api/notifications/preferences")
def set_all_notification_preferences(payload: NotificationPreferenceUpdate):
    """Mute or unmute every notification kind in one action -- the same
    per-kind PUT .../preferences/{kind} already does, just applied to the
    whole fixed set of kinds at once instead of one at a time."""
    for kind in NOTIFICATION_KINDS:
        store.set_notification_kind_muted(kind, payload.muted)
    return {kind: payload.muted for kind in NOTIFICATION_KINDS}


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


@app.get("/api/memory/search")
def search_memory(q: str = ""):
    """Case-insensitive keyword search across every memory entry's key and
    value, redacted the same way GET /api/memory is -- a secret-shaped key
    never leaks its value into a search result either."""
    entries = store.search_memory(q)
    redacted = redact_secrets({entry["key"]: entry["value"] for entry in entries})
    results = [{**entry, "value": redacted[entry["key"]]} for entry in entries]
    return {"query": q, "results": results}


@app.get("/api/memory.csv")
def memory_csv():
    """The persistent cross-run memory store as a downloadable CSV --
    mirrors the audit-log/notifications/artifacts CSV export pattern. Goes
    through the same redact_secrets() pass as GET /api/memory so a
    secret-shaped key doesn't leak its value into the download either.
    Each value is JSON-encoded into its own cell since a memory value can
    be any JSON type (dict, list, string, number), not just a scalar."""
    entries = store.all_memory()
    redacted = redact_secrets({entry["key"]: entry["value"] for entry in entries})

    buffer = io.StringIO()
    writer = csv.DictWriter(buffer, fieldnames=["key", "value", "updated_at"])
    writer.writeheader()
    for entry in entries:
        writer.writerow({
            "key": entry["key"],
            "value": json.dumps(redacted[entry["key"]]),
            "updated_at": entry["updated_at"],
        })

    return StreamingResponse(
        iter([buffer.getvalue()]),
        media_type="text/csv",
        headers={"Content-Disposition": "attachment; filename=memory.csv"},
    )


@app.get("/api/memory.json")
def memory_json():
    """The persistent cross-run memory store as a downloadable JSON file of
    `{key: value}` pairs -- shaped to be re-uploaded straight back through
    POST /api/memory/import, unlike the CSV export (whose per-row
    key/value/updated_at shape isn't importable as-is). Goes through the
    same redact_secrets() pass as GET /api/memory and the CSV export."""
    entries = store.all_memory()
    redacted = redact_secrets({entry["key"]: entry["value"] for entry in entries})
    buffer = json.dumps(redacted, indent=2)
    return StreamingResponse(
        iter([buffer]),
        media_type="application/json",
        headers={"Content-Disposition": "attachment; filename=memory.json"},
    )


@app.post("/api/memory/import")
async def import_memory(file: UploadFile = File(...)):
    """Restore/seed agent memory from a JSON file of `{key: value}` pairs --
    the import counterpart to GET /api/memory.csv's export, for restoring a
    previous export or hand-seeding memory before a run rather than waiting
    for a module to write it naturally. Upserts each key via the same
    `store.set_memory()` a module itself would call, so an existing key is
    overwritten and everything else in memory is left untouched -- this is
    additive, not a wholesale replace. Values are used as-is, not
    redacted, since importing IS the user deliberately writing that value
    back into their own memory store."""
    try:
        data = await ingestion.read_upload_with_limit(file)
    except ingestion.UploadTooLargeError as exc:
        raise HTTPException(status_code=413, detail=str(exc))
    try:
        entries = json.loads(data)
    except json.JSONDecodeError as exc:
        raise HTTPException(status_code=400, detail=f"Could not parse memory JSON: {exc}")
    if not isinstance(entries, dict):
        raise HTTPException(status_code=400, detail="Memory JSON must describe a single object of key/value pairs.")

    imported = []
    for key, value in entries.items():
        store.set_memory(key, value)
        imported.append(key)
    store.record_audit_event("memory_import", f"Imported {len(imported)} memory key(s): {imported}.")
    return {"imported": imported}


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


@app.post("/api/ingest/xlsx")
async def ingest_xlsx(file: UploadFile = File(...)):
    """Upload an .xlsx spreadsheet, get back the same row_count/columns/preview
    shape as a CSV upload (first sheet, row 1 as headers). Same content-hash
    dedupe, same shared implementation as the filesystem watcher."""
    try:
        data = await ingestion.read_upload_with_limit(file)
    except ingestion.UploadTooLargeError as exc:
        raise HTTPException(status_code=413, detail=str(exc))
    try:
        return ingestion.ingest_xlsx_bytes(file.filename, data, store)
    except Exception as exc:
        raise HTTPException(status_code=400, detail=f"Could not parse XLSX: {exc}")


class IngestUrlRequest(BaseModel):
    url: str


_URL_INGEST_DISPATCH = {
    ".csv": ingestion.ingest_csv_bytes,
    ".json": ingestion.ingest_json_bytes,
    ".xlsx": ingestion.ingest_xlsx_bytes,
}


@app.post("/api/ingest/url")
def ingest_from_url(payload: IngestUrlRequest):
    """Fetch a CSV/JSON/XLSX file by URL and ingest it through the exact same
    parse+dedupe+cleanse pipeline as a direct upload. A plain HTTP GET of a
    user-supplied URL, not a SaaS integration — same trust model as the
    http_request automation module. The file type is inferred from the
    URL's own extension, same as the folder watcher's dispatch."""
    if not payload.url.strip():
        raise HTTPException(status_code=400, detail="url is required")

    filename = ingestion.filename_from_url(payload.url)
    suffix = Path(filename).suffix.lower()
    ingest_fn = _URL_INGEST_DISPATCH.get(suffix)
    if ingest_fn is None:
        raise HTTPException(
            status_code=400,
            detail=f"Unrecognized file type '{suffix or '(none)'}' — the URL must end in .csv, .json, or .xlsx.",
        )

    try:
        data = ingestion.fetch_url_bytes(payload.url)
    except ingestion.UploadTooLargeError as exc:
        raise HTTPException(status_code=413, detail=str(exc))
    except Exception as exc:
        raise HTTPException(status_code=400, detail=f"Could not fetch URL: {exc}")

    try:
        return ingest_fn(filename, data, store)
    except Exception as exc:
        raise HTTPException(status_code=400, detail=f"Could not parse fetched file: {exc}")


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
def list_artifacts(tag: Optional[str] = None):
    files = ingestion.list_artifacts()
    tags_by_file = store.all_artifact_tags()
    notes_by_file = store.all_artifact_notes()
    for f in files:
        f["tags"] = tags_by_file.get(f["name"], [])
        f["note"] = notes_by_file.get(f["name"], "")
    if tag and tag.strip():
        tag_lower = tag.strip().lower()
        files = [f for f in files if tag_lower in (t.lower() for t in f["tags"])]
    return files


@app.get("/api/artifacts.csv")
def artifacts_csv():
    """Every ingested artifact's metadata (not its content) as a downloadable
    CSV -- mirrors the audit-log/notifications CSV export pattern. Tags are
    joined with ';' since a CSV cell can't hold a list, and modified_at (a
    raw Unix timestamp, the same value the API returns) is rendered as an
    ISO 8601 UTC string for readability."""
    files = ingestion.list_artifacts()
    tags_by_file = store.all_artifact_tags()

    buffer = io.StringIO()
    writer = csv.DictWriter(buffer, fieldnames=["filename", "size_bytes", "tags", "modified_at"])
    writer.writeheader()
    for f in files:
        writer.writerow({
            "filename": f["name"],
            "size_bytes": f["size_bytes"],
            "tags": ";".join(tags_by_file.get(f["name"], [])),
            "modified_at": datetime.fromtimestamp(f["modified_at"], tz=timezone.utc).isoformat(),
        })

    return StreamingResponse(
        iter([buffer.getvalue()]),
        media_type="text/csv",
        headers={"Content-Disposition": "attachment; filename=artifacts.csv"},
    )


@app.get("/api/artifacts/tags-summary")
def artifact_tags_summary():
    """Every tag currently in use across ingested artifacts and how many
    files carry it -- a directory view for the tag filter box, letting a
    user browse by tag rather than having to already know one to type in.
    Only counts files that still exist on disk (a purged artifact's stale
    tag row, if any, doesn't inflate the count)."""
    existing_names = {f["name"] for f in ingestion.list_artifacts()}
    counts: dict[str, int] = {}
    for filename, tags in store.all_artifact_tags().items():
        if filename not in existing_names:
            continue
        for tag in tags:
            counts[tag] = counts.get(tag, 0) + 1
    summary = [{"tag": tag, "count": count} for tag, count in counts.items()]
    summary.sort(key=lambda entry: (-entry["count"], entry["tag"].lower()))
    return summary


@app.get("/api/artifacts/tags-summary.csv")
def artifact_tags_summary_csv():
    """Same tag/count directory as GET /api/artifacts/tags-summary, as a
    downloadable CSV -- mirrors every other CSV export in this app. A
    literal path, not a suffix on a dynamic segment, so there's no
    route-ordering conflict with any /api/artifacts/{name}-style route."""
    buffer = io.StringIO()
    writer = csv.DictWriter(buffer, fieldnames=["tag", "count"])
    writer.writeheader()
    for entry in artifact_tags_summary():
        writer.writerow(entry)
    return StreamingResponse(
        iter([buffer.getvalue()]),
        media_type="text/csv",
        headers={"Content-Disposition": "attachment; filename=artifact_tags.csv"},
    )


@app.post("/api/artifacts/purge")
def purge_artifacts(older_than_hours: float = 24):
    removed = ingestion.purge_old_artifacts(older_than_hours)
    store.record_audit_event("artifact_purge", f"Purged {len(removed)} artifact(s) older than {older_than_hours}h.")
    return {"removed_count": len(removed), "removed": removed}


class BulkDeleteArtifacts(BaseModel):
    filenames: list[str]


@app.post("/api/artifacts/bulk-delete")
def bulk_delete_artifacts(payload: BulkDeleteArtifacts):
    """Delete a user-picked set of specific artifacts regardless of age --
    the finer-grained counterpart to /api/artifacts/purge's age-based
    sweep, for a checkbox multi-select in the UI. An unknown filename is
    skipped rather than failing the whole batch, same as bulk-tagging."""
    deleted = []
    for filename in payload.filenames:
        safe_name = Path(filename).name
        if ingestion.delete_artifact(safe_name):
            deleted.append(safe_name)
    store.record_audit_event("artifact_bulk_delete", f"Deleted {len(deleted)} selected artifact(s): {deleted}.")
    return {"deleted": deleted}


class BulkDownloadArtifacts(BaseModel):
    filenames: list[str]


@app.post("/api/artifacts/bulk-download")
def bulk_download_artifacts(payload: BulkDownloadArtifacts):
    """A user-picked set of artifacts bundled into a single zip -- the
    finer-grained counterpart to a hypothetical 'download everything',
    mirroring how GET /api/pipelines/export-all bundles every saved
    pipeline; here the selection is a checkbox multi-select rather than
    'all', so it has to be POST (a filename list doesn't fit cleanly in a
    GET's query string) with the zip built from disk on demand. An
    unknown filename is skipped rather than failing the whole batch,
    same as bulk-delete; 404s only if none of the requested files exist."""
    included = []
    buffer = io.BytesIO()
    with zipfile.ZipFile(buffer, "w", zipfile.ZIP_DEFLATED) as zf:
        for filename in payload.filenames:
            safe_name = Path(filename).name
            path = ingestion.ARTIFACTS_DIR / safe_name
            if path.is_file():
                zf.write(path, arcname=safe_name)
                included.append(safe_name)

    if not included:
        raise HTTPException(status_code=404, detail="None of the requested artifacts exist.")

    buffer.seek(0)
    return StreamingResponse(
        buffer,
        media_type="application/zip",
        headers={"Content-Disposition": "attachment; filename=artifacts_export.zip"},
    )


@app.post("/api/artifacts/bulk-export-csv")
def bulk_export_artifacts_csv(payload: BulkDownloadArtifacts):
    """A user-picked set of artifacts' metadata (not their content) as a
    downloadable CSV -- the finer-grained counterpart to the full-list
    GET /api/artifacts.csv export, complementing bulk-download's zip of
    file *content* with just the rows for a checkbox multi-select. An
    unknown filename is skipped rather than failing the whole batch."""
    wanted = {Path(name).name for name in payload.filenames}
    tags_by_file = store.all_artifact_tags()

    buffer = io.StringIO()
    writer = csv.DictWriter(buffer, fieldnames=["filename", "size_bytes", "tags", "modified_at"])
    writer.writeheader()
    for f in ingestion.list_artifacts():
        if f["name"] not in wanted:
            continue
        writer.writerow({
            "filename": f["name"],
            "size_bytes": f["size_bytes"],
            "tags": ";".join(tags_by_file.get(f["name"], [])),
            "modified_at": datetime.fromtimestamp(f["modified_at"], tz=timezone.utc).isoformat(),
        })

    return StreamingResponse(
        iter([buffer.getvalue()]),
        media_type="text/csv",
        headers={"Content-Disposition": "attachment; filename=artifacts_selected.csv"},
    )


_ZIP_INGEST_DISPATCH = {**_URL_INGEST_DISPATCH, ".pdf": ingestion.ingest_pdf_bytes}


@app.post("/api/artifacts/import-zip")
async def import_artifacts_zip(file: UploadFile = File(...)):
    """Ingest every CSV/JSON/XLSX/PDF file inside a zip bundle at once --
    the artifact-side counterpart to POST /api/pipelines/import-zip, for
    bulk-loading a folder's worth of files without dragging them in one at
    a time. Each member is dispatched by its own extension through the
    exact same per-type ingest_*_bytes() function (and therefore the same
    content-hash dedupe) as a direct single-file upload; one bad or
    duplicate file doesn't block the rest of the batch. Any member whose
    extension isn't csv/json/xlsx/pdf is silently skipped rather than
    reported as a failure -- a zip full of a user's other files shouldn't
    read as broken."""
    try:
        data = await ingestion.read_upload_with_limit(file)
    except ingestion.UploadTooLargeError as exc:
        raise HTTPException(status_code=413, detail=str(exc))

    try:
        zf = zipfile.ZipFile(io.BytesIO(data))
    except zipfile.BadZipFile:
        raise HTTPException(status_code=400, detail="Could not read this file as a zip archive.")

    imported = []
    duplicates = []
    failed = []
    for name in zf.namelist():
        if name.endswith("/"):
            continue  # directory entry
        suffix = Path(name).suffix.lower()
        ingest_fn = _ZIP_INGEST_DISPATCH.get(suffix)
        if ingest_fn is None:
            continue
        member_name = Path(name).name  # strip any folder path inside the zip
        try:
            result = ingest_fn(member_name, zf.read(name), store)
        except Exception as exc:
            failed.append({"name": name, "error": str(exc)})
            continue
        if result.get("duplicate"):
            duplicates.append(name)
        else:
            imported.append(name)

    if not imported and not duplicates and not failed:
        raise HTTPException(status_code=400, detail="No .csv, .json, .xlsx, or .pdf files found in this zip.")

    store.record_audit_event(
        "artifacts_bulk_import_zip",
        f"Imported {len(imported)} artifact(s) from a zip bundle ({len(duplicates)} duplicate(s), {len(failed)} failed).",
    )
    return {"imported": imported, "duplicates": duplicates, "failed": failed}


@app.get("/api/artifacts/search")
def search_artifacts(q: str = ""):
    return {"query": q, "results": ingestion.search_artifacts(q)}


@app.get("/api/artifacts/search-notes")
def search_artifact_notes(q: str = "", limit: int = 20):
    """Keyword search across the free-text notes attached to artifacts
    (Batch 33) -- distinct from /api/artifacts/search, which searches
    extracted .json/.txt file content, not the user's own annotations."""
    return {"query": q, "results": store.search_artifact_notes(q, limit)}


class ArtifactTagsUpdate(BaseModel):
    tags: list[str]


@app.put("/api/artifacts/{filename}/tags")
def set_artifact_tags(filename: str, payload: ArtifactTagsUpdate):
    safe_name = Path(filename).name  # strip any path components — filenames only, never a traversal target
    path = ingestion.ARTIFACTS_DIR / safe_name
    if not path.is_file():
        raise HTTPException(status_code=404, detail=f"No artifact named '{safe_name}'.")
    cleaned = sorted({t.strip() for t in payload.tags if t.strip()})
    return {"filename": safe_name, "tags": store.set_artifact_tags(safe_name, cleaned)}


class ArtifactNoteUpdate(BaseModel):
    note: str


@app.put("/api/artifacts/{filename}/note")
def set_artifact_note(filename: str, payload: ArtifactNoteUpdate):
    """A single free-text note per artifact -- distinct from tags, which
    are short structured keywords, not a place for a longer human comment.
    Mirrors the existing per-schedule label. An empty string clears it."""
    safe_name = Path(filename).name
    path = ingestion.ARTIFACTS_DIR / safe_name
    if not path.is_file():
        raise HTTPException(status_code=404, detail=f"No artifact named '{safe_name}'.")
    return {"filename": safe_name, "note": store.set_artifact_note(safe_name, payload.note.strip())}


@app.post("/api/artifacts/bulk-clear-note")
def bulk_clear_artifact_notes(payload: BulkDeleteArtifacts):
    """Blank out the note on a user-picked set of artifacts at once,
    mirroring the existing bulk-clear-schedule-labels pattern. A filename
    that never had a note is still counted as cleared (it's still a real
    existing artifact, just nothing to remove), matching every other bulk
    action in this app that treats "already in the target state" as a
    success rather than a skip."""
    cleared = []
    for filename in payload.filenames:
        safe_name = Path(filename).name
        path = ingestion.ARTIFACTS_DIR / safe_name
        if not path.is_file():
            continue
        store.set_artifact_note(safe_name, "")
        cleared.append(safe_name)
    return {"cleared": cleared}


class ArtifactRename(BaseModel):
    new_name: str


@app.post("/api/artifacts/{filename}/rename")
def rename_artifact(filename: str, payload: ArtifactRename):
    """Rename an artifact's underlying file -- distinct from
    POST /api/artifacts/rename-tag, which relabels a *tag* across every
    file that carries it. Moves the file on disk, then moves its tag-store
    row (keyed by filename) to follow it, so tagging survives the rename;
    a favorited or recently-viewed reference to the old filename simply
    stops resolving, the same way it already does when an artifact is
    deleted outright."""
    safe_old = Path(filename).name
    safe_new = Path(payload.new_name).name
    if not safe_new.strip():
        raise HTTPException(status_code=400, detail="new_name must not be empty.")
    try:
        ingestion.rename_artifact(safe_old, safe_new)
    except ingestion.ArtifactRenameError as exc:
        detail = str(exc)
        status_code = 404 if "No artifact named" in detail else 409
        raise HTTPException(status_code=status_code, detail=detail)
    store.rename_artifact_tags(safe_old, safe_new)
    store.rename_artifact_note(safe_old, safe_new)
    store.record_audit_event("artifact_rename", f"Renamed artifact '{safe_old}' to '{safe_new}'.")
    return {"old_name": safe_old, "new_name": safe_new}


class ArtifactBulkTag(BaseModel):
    filenames: list[str]
    tag: str


@app.post("/api/artifacts/bulk-tags")
def bulk_tag_artifacts(payload: ArtifactBulkTag):
    """Add one tag to every selected artifact at once -- adds to whatever
    tags a file already has, the same as typing into its own '+ tag' field,
    just applied to a whole selection instead of one file at a time.
    Unknown filenames are skipped rather than failing the whole batch."""
    tag = payload.tag.strip()
    if not tag:
        raise HTTPException(status_code=400, detail="A tag is required.")

    existing_tags = store.all_artifact_tags()
    tagged = []
    for filename in payload.filenames:
        safe_name = Path(filename).name
        if not (ingestion.ARTIFACTS_DIR / safe_name).is_file():
            continue
        current = set(existing_tags.get(safe_name, []))
        current.add(tag)
        store.set_artifact_tags(safe_name, sorted(current))
        tagged.append(safe_name)
    return {"tag": tag, "tagged": tagged}


@app.post("/api/artifacts/bulk-untag")
def bulk_untag_artifacts(payload: ArtifactBulkTag):
    """The inverse of bulk-tag -- removes one tag from every selected
    artifact's existing tag set, leaving any other tags on that file alone.
    A file that never had this tag is a no-op, not skipped from the
    response the way an unknown filename is (it's still a real, existing
    artifact -- just nothing to remove)."""
    tag = payload.tag.strip()
    if not tag:
        raise HTTPException(status_code=400, detail="A tag is required.")

    existing_tags = store.all_artifact_tags()
    untagged = []
    for filename in payload.filenames:
        safe_name = Path(filename).name
        if not (ingestion.ARTIFACTS_DIR / safe_name).is_file():
            continue
        current = set(existing_tags.get(safe_name, []))
        current.discard(tag)
        store.set_artifact_tags(safe_name, sorted(current))
        untagged.append(safe_name)
    return {"tag": tag, "untagged": untagged}


class ArtifactTagRename(BaseModel):
    old_tag: str
    new_tag: str


@app.post("/api/artifacts/rename-tag")
def rename_artifact_tag(payload: ArtifactTagRename):
    """Rename a tag everywhere it's used in one action (e.g. a typo like
    "reviewd" -> "reviewed") instead of removing it from and re-adding it to
    every file individually. If a file already carries `new_tag` too, the
    rename just merges into that (no duplicate, same as any other tag set
    which is stored as a de-duplicated, sorted list)."""
    old_tag = payload.old_tag.strip()
    new_tag = payload.new_tag.strip()
    if not old_tag or not new_tag:
        raise HTTPException(status_code=400, detail="Both old_tag and new_tag are required.")
    if old_tag == new_tag:
        raise HTTPException(status_code=400, detail="new_tag must be different from old_tag.")

    renamed = []
    for filename, tags in store.all_artifact_tags().items():
        if old_tag not in tags:
            continue
        current = set(tags)
        current.discard(old_tag)
        current.add(new_tag)
        store.set_artifact_tags(filename, sorted(current))
        renamed.append(filename)
    return {"old_tag": old_tag, "new_tag": new_tag, "renamed": renamed}


@app.get("/api/artifacts/compare-schema")
def compare_artifact_schemas(a: str, b: str):
    """Column-by-column schema diff between two table-shaped artifacts (CSV
    or a .json record list) -- which columns are unique to each side, and
    which shared columns disagree on inferred type. Mirrors the existing
    run-compare/pipeline-compare pattern, just for artifact schemas."""
    safe_a = Path(a).name
    safe_b = Path(b).name

    def _schema_for(safe_name: str) -> dict:
        try:
            content = ingestion.read_artifact_content(safe_name)
        except FileNotFoundError:
            raise HTTPException(status_code=404, detail=f"No artifact named '{safe_name}'.")
        if content.get("kind") != "table" or not content.get("schema"):
            raise HTTPException(status_code=400, detail=f"'{safe_name}' isn't a table-shaped artifact with an inferable schema.")
        return content["schema"]

    schema_a = _schema_for(safe_a)
    schema_b = _schema_for(safe_b)

    types_a = {col["name"]: col["type"] for col in schema_a["columns"]}
    types_b = {col["name"]: col["type"] for col in schema_b["columns"]}

    only_in_a = sorted(set(types_a) - set(types_b))
    only_in_b = sorted(set(types_b) - set(types_a))
    common = sorted(set(types_a) & set(types_b))
    matching = [col for col in common if types_a[col] == types_b[col]]
    type_mismatches = [
        {"column": col, "a_type": types_a[col], "b_type": types_b[col]}
        for col in common
        if types_a[col] != types_b[col]
    ]

    return {
        "a": {"filename": safe_a, "schema": schema_a},
        "b": {"filename": safe_b, "schema": schema_b},
        "only_in_a": only_in_a,
        "only_in_b": only_in_b,
        "matching": matching,
        "type_mismatches": type_mismatches,
    }


@app.get("/api/artifacts/{filename}/content")
def artifact_content(filename: str):
    """Full (capped) extracted content for one artifact -- not just a
    search-result snippet. Structured records (CSV/XLSX) render as a table,
    extracted PDF text as plain text, and a binary original that has no
    directly-viewable content (points to its .json/.txt companion instead)."""
    safe_name = Path(filename).name  # strip any path components — filenames only, never a traversal target
    try:
        return ingestion.read_artifact_content(safe_name)
    except FileNotFoundError:
        raise HTTPException(status_code=404, detail=f"No artifact named '{safe_name}'.")


@app.get("/api/artifacts/{filename}/download")
def download_artifact(filename: str):
    """The original ingested file's raw bytes as a downloadable attachment --
    distinct from GET .../content, which returns extracted/derived content
    (a CSV's parsed rows, a PDF's stripped text) rather than the original
    file a user could reopen in another program."""
    safe_name = Path(filename).name  # strip any path components — filenames only, never a traversal target
    path = ingestion.ARTIFACTS_DIR / safe_name
    if not path.is_file():
        raise HTTPException(status_code=404, detail=f"No artifact named '{safe_name}'.")
    return FileResponse(path, filename=safe_name, media_type="application/octet-stream")


@app.get("/api/watcher/status")
def watcher_status():
    return {"watch_dir": str(WATCH_DIR), "processed": _watcher_log[-20:]}


app.mount("/static", StaticFiles(directory=STATIC_DIR), name="static")


@app.get("/")
def index():
    return FileResponse(STATIC_DIR / "index.html")
