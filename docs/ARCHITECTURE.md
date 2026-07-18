# Architecture

## 1. Why a monorepo, why synchronous-first

Every workflow, automation, and agent lives in one repository so there is exactly
one place to look for "how does the system behave end to end." A single
`ExecutionContext` object is threaded through every module in a pipeline run —
that's the "shared context window" requirement: no databases-as-message-buses, no
serializing state between separately-deployed services, just a dict and a list
that every module reads and appends to.

Synchronous-first means: given a pipeline `[A, B, C]`, `B` never starts until `A`
has returned, and `B`'s output is already merged into the context `C` sees. This
is what makes "tier-1 output feeds tier-3 input" trivial — there is no queue to
wire up, just a return dict.

## 2. Data flow: automation → workflow → agent

```mermaid
flowchart LR
    subgraph Tier1["Tier 1 — Automation (deterministic)"]
        A[DataFetchAutomation<br/>fetch_raw_metrics]
    end

    subgraph Tier2["Tier 2 — AI Workflow (data-driven)"]
        B[MetricsAnalysisWorkflow<br/>analyze_metrics]
    end

    subgraph Tier3["Tier 3 — AI Agent (autonomous, NLP)"]
        C[ChurnResponseAgent<br/>churn_response_agent]
    end

    Ctx[(ExecutionContext<br/>shared variables + history)]

    A -- "raw_metrics" --> Ctx
    Ctx -- "raw_metrics" --> B
    B -- "insight" --> Ctx
    Ctx -- "insight" --> C
    C -- "agent_decision" --> Ctx
```

Concretely, in `cli.py`:

1. `discover()` scans `automations/`, `workflows/`, `agents/` for `*.yaml`
   manifests (in that tier order) and instantiates the module each one points to.
2. `Orchestrator.run()` creates one `ExecutionContext` and calls `module.run(context)`
   for each module in sequence.
3. Each module's return dict is merged into `context.variables` via
   `context.update(...)` **before** the next module runs.
4. Every step's inputs/outputs/timing/success are appended to `context.history`
   and, if a `StateStore` is attached, persisted to SQLite (`runs` + `steps` tables).

This is exactly the "output from a tier-1 script feeds a tier-3 agent" requirement:
`raw_metrics` (tier 1) → `insight` (tier 2) → `agent_decision` (tier 3), each key
just sitting in the same dict.

## 3. Core engine components

| File | Responsibility |
|---|---|
| `engine/base.py` | `Tier` enum + `BaseModule` abstract contract every module implements |
| `engine/context.py` | `ExecutionContext` (shared state) + `StepRecord` (one step's audit trail) |
| `engine/orchestrator.py` | Sequential runner; merges outputs, records history, optionally persists to `StateStore`, controls stop-vs-continue on error, emits live progress events via `on_event`. Also defines `StepSpec`, for steps that need per-step seeded/mapped inputs rather than just running a bare module. |
| `engine/registry.py` | Reads `*.yaml` manifests in a tier folder (`load_manifests`), dynamically imports and instantiates the referenced class (`instantiate`/`discover`) |
| `engine/state_store.py` | SQLite persistence of run/step history for the `status` CLI command |

`Orchestrator.stop_on_error` controls failure behavior: `True` (default) re-raises
after recording the failed step, so a broken pipeline fails loudly; `False` lets
independent later steps still run (useful once pipelines have parallel-ish
branches, or a non-critical logging agent at the tail).

`Orchestrator.run(initial_context, on_event)` also accepts an optional
`on_event` callback. If given, it's called with a stream of events as the run
happens — `step_started` / `step_completed` / `step_failed` for each module,
plus `run_completed` / `run_failed` at the end (each carries the final
`context`). Per-step events carry an `index` (the step's position in the
pipeline) alongside `tier`/`name`, specifically so a pipeline that uses the
same module twice — entirely possible once pipelines are built by picking
modules freely rather than one-per-tier — doesn't have two tracker rows
fighting over the same identity. A module can call `context.emit(kind,
message)` from inside its own `run()` to add its own events (e.g. `"thought"`,
`"tool_call"`) into that same stream, automatically tagged with which
tier/module produced them (`ExecutionContext.active_module`, set by the
orchestrator before each `module.run()` call). The CLI ignores this entirely
(`on_event=None`, a no-op); the dashboard uses it to drive the live tracker
described below.

Each pipeline "step" the orchestrator runs is a `StepSpec(module, seed)`, not
just a bare module — `seed` is a function of the *current* `ExecutionContext`,
called right before that step's `module.run()`. For the default case (bare
`BaseModule` instances, e.g. `Orchestrator([Double(), AddOne()])`) it's a
no-op wrapped in automatically by `_as_step()`. For a pipeline assembled by the
visual builder, `seed` is where a field mapping actually happens: `seed = lambda
ctx: {"notify_slack": ctx.get("insight")}` copies whatever's currently sitting
under the `insight` key (written by an earlier step) into `notify_slack` right
before this step reads it — the mapping is resolved at run time, once the
source step has actually executed, not baked in ahead of time.

## 4. Unified control layer

There are two front doors onto the same engine and the same `orchestrator.db`:

**`webapp/` — the visual control center (primary, day-to-day use).** A FastAPI
app (`webapp/main.py`) exposes:

- `GET /api/modules` — every enabled module per tier, with its manifest
  `description` and `inputs` schema, plus a `status` (`ready`/`error`) derived
  from `StateStore.latest_step_status()`.
- `POST /api/modules/{tier}/{name}/run` — builds a single-module pipeline
  (`Orchestrator([module])`), starts it in a **background thread**, and
  returns immediately with `{"stream_id": N}` — it does not wait for the run
  to finish.
- `POST /api/pipeline/run` — same idea, for the full tier-1→2→3 pipeline.
- `GET /api/stream/{stream_id}` — a Server-Sent Events stream of that run's
  events, as they happen, as `data: {...}\n\n` lines.
- `GET /api/runs` — recent, persisted run history from `StateStore`, for
  anything that wants to poll it instead.

`stream_id` is deliberately a separate, ephemeral ID space from `StateStore`'s
persisted `run_id` (`webapp/events.py::RunEventBus`) — it only exists to give a
live SSE channel something to key on, and is discarded once that stream ends.
Execution genuinely happens in a background thread (`threading.Thread`, not an
`asyncio` task), so a slow module never blocks FastAPI's event loop; a
`queue.Queue` per stream buffers events between the worker thread and whichever
request is currently reading `/api/stream/{id}`, so a client that connects a
moment late still gets everything from the start.

The static frontend (`webapp/static/`) is plain HTML/CSS/vanilla JS — no
bundler, no framework. Clicking a card's Run button (or "Run Full Pipeline")
POSTs to kick off the run, then opens `new EventSource('/api/stream/{id}')` and
renders three things live as events arrive (`app.js`):

- **A step tracker** — one row per module in that run (just one, for a single
  card; all three tiers, for the full pipeline), each showing ⚪ pending → 🟡
  running → 🟢 done / 🔴 failed as `step_started`/`step_completed`/
  `step_failed` events land.
- **An agent thought accordion** — for any tracker row whose tier is `agent`,
  `thought` and `tool_call` events append live lines to an expandable panel
  under that row (💭 for a thought, 🔧 for a tool call), so you can watch an
  agent's reasoning as it happens rather than only see its final message.
- **A toast notification** — on the terminal `run_completed`/`run_failed`
  event, a corner toast reports success or failure, the card's status pill
  updates (ready/error), and the client explicitly closes the `EventSource`
  (so a normal completion doesn't trigger the browser's automatic SSE
  reconnect).

Automations get a sky-blue accent, workflows violet, agents emerald, so the
tier boundary is visible at a glance independent of the tracker's own
pending/running/done coloring.

**`cli.py` — the scriptable / CI-friendly control layer:**

- `python cli.py list` — enumerate every discovered module per tier (health check
  that manifests + entrypoints resolve).
- `python cli.py run` — execute the pipeline once, print the final context and
  per-step status.
- `python cli.py status` — read `orchestrator.db` and print recent runs and their
  steps.

Both front doors call the exact same `Orchestrator`/`StateStore`/`registry`
code — the web layer adds zero orchestration logic of its own, only HTTP
plumbing and input coercion (`webapp/main.py::_coerce_inputs`, which applies
each field's declared `type` and `default` from the manifest).

## 5. The no-code visual pipeline builder

Everything through §4 assumes one fixed pipeline (automation → workflow →
agent, in that order). The builder lets a user compose their *own* pipeline —
any modules, any order, any number of steps — entirely from the dashboard.

**Manifests declare `outputs` too, not just `inputs`.** `outputs: [{name,
label}]` names the keys a module's `run()` meaningfully writes into context
(e.g. `fetch_raw_metrics` declares `raw_metrics`). This is purely descriptive —
nothing enforces that a module's return dict matches its declared outputs — it
exists so the builder's "map from an earlier step" dropdown has something to
offer. `GET /api/modules` includes `outputs` alongside `inputs` for exactly
this reason.

**A pipeline definition** (what the builder POSTs, and what ends up on disk)
is a name/description plus an ordered list of steps, each `{tier, name,
inputs, mappings}`:

```yaml
name: Custom Churn Chain
slug: custom_churn_chain
description: Built visually
steps:
  - tier: automation
    name: fetch_raw_metrics
    inputs: {signups: 128, churn: 14, revenue: 4210.5}
    mappings: {}
  - tier: workflow
    name: analyze_metrics
    inputs: {risk_threshold: 0.1}
    mappings: {}
  - tier: agent
    name: churn_response_agent
    inputs: {}
    mappings: {notify_slack: {step: 1, output: insight}}
```

A field is either in `inputs` (a literal, same as a module card's form) or in
`mappings` (sourced from an earlier step's declared output) — never both.
`webapp/main.py::_build_steps_from_definition` turns this into a list of
`StepSpec`s: each step's `seed` merges its static `inputs` with, for every
mapped field, `ctx.get(mapping["output"])` at the moment that step runs.

**Storage and validation** (`webapp/pipelines.py`) follow the same convention
as every other tier: `pipelines/<slug>.yaml`, one file per pipeline, plain
enough to read, diff, and hand-edit. `validate_pipeline()` runs before
anything is saved: every step's `(tier, name)` must resolve to a real,
discovered module, and every mapping must reference an *earlier* step index
whose module actually declares that output name — a typo or a forward
reference comes back as a `400` with a specific message, not a pipeline that
silently does the wrong thing (or crashes) when launched.

**One-click save-and-launch.** `POST /api/pipelines` validates, writes the
YAML file, and immediately launches the pipeline through the exact same
background-thread-plus-SSE mechanism as a single-module or full-pipeline run
(`_launch_steps`, shared with the other run endpoints) — the builder's live
tracker, agent thought accordion, and completion toast are the *same* frontend
code as everywhere else, just pointed at a differently-assembled step list.
Once saved, a pipeline gets its own card in the dashboard's **Saved Pipelines**
section (`GET /api/pipelines` to list, `POST /api/pipelines/{slug}/run` to
relaunch it later without rebuilding it).

Step reordering, arbitrary-step removal, and nested-field (dotted-path)
mapping were all originally deferred here as "addressable later without
changing the storage format" — Batches 4 and 5 addressed all three; see
their build-history entries in section 8 for how.

## 6. Diagnostics, telemetry, and everyday UI

A batch of additions that don't change how runs execute, only how visible their
state is:

- **`webapp/health.py`** runs four checks on demand (`GET /api/health`): the
  `StateStore` is reachable, every `*.yaml` manifest parses and has a `name`/
  `entrypoint`, every enabled entrypoint actually imports and instantiates, and
  which optional environment variables (currently just `ANTHROPIC_API_KEY`)
  aren't set. The last one never marks the app "degraded" — it's informational,
  since nothing bundled requires it yet. The other three do, because they'd
  otherwise surface later as a confusing error the first time someone clicks
  Run on a module with a broken manifest.
- **`StateStore.metrics_summary()`** aggregates every finished run into total
  count, success rate, and average duration — read by the header ticker.
  `GET /api/runs.csv` streams the same history as CSV via the stdlib `csv`
  module, no new dependency.
- **Per-step `duration_ms`** was added to the orchestrator's `step_completed`/
  `step_failed` events (`engine/orchestrator.py`) so the dashboard's log tabs
  can show real timing, not just pass/fail.
- **`pipeline_store.duplicate_pipeline()`** clones a saved pipeline's `steps`
  under an auto-incremented "(copy)" name, reusing the same `validate_pipeline`
  + `save_pipeline` path a fresh save takes — no separate write path to keep
  in sync.
- **Favorites are shortcuts, not copies.** An earlier design would have
  rendered a favorited card a second time inside a "Favorites" shelf — but two
  DOM elements sharing one `id` (`card__automation__fetch_raw_metrics`) breaks
  every `getElementById` lookup the tracker/status code relies on, silently
  updating only whichever element the browser finds first. Favorites are
  instead thin chips that call the exact same `runModule`/`runSavedPipeline`
  functions, which scroll to and animate the one real card elsewhere on the
  page — no duplicate IDs, ever.
- **Collapsible sections and the search bar** operate purely on CSS classes
  (`.collapsed`, `.search-hidden`, `.search-no-match`) applied to existing DOM,
  re-applied after every re-render since `innerHTML` replacement wipes any
  classes added by a previous pass.

## 7. Data ingestion, templating, the folder watcher, and source viewing

**Ingestion is one shared implementation, two triggers.**
`webapp/ingestion.py::ingest_csv_bytes()`/`ingest_pdf_bytes()` do the actual
work — hash the bytes, check `StateStore` for that hash, parse/extract, save
the raw upload plus its converted form into `artifacts/`, record the hash.
Both `POST /api/ingest/csv`/`pdf` (a browser upload) and
`webapp/watcher.py`'s background thread (a file appearing in
`watched_input/`) call the exact same functions — there is no separate
"auto-ingest" code path to drift out of sync with the manual one. Dedupe is
by SHA-256 of the file's bytes, not filename, so renaming a file doesn't
bypass it.

**The watcher is a plain polling loop**, not `watchdog`: a background
`threading.Thread` lists `watched_input/` every 2 seconds and calls a
callback for any filename it hasn't seen since it started. Files already
present at boot are marked seen immediately (not replayed as "new" on every
restart). This was a deliberate dependency trade-off — a 15-line loop against
one more third-party package for a feature that only needs to notice "a file
that wasn't here before now is."

**Template fields are generic string interpolation**, not LLM-specific
scaffolding. `engine/templating.py::render_template()` replaces `{name}` with
whatever a `lookup` callable resolves it to, leaving unknown names literal.
`interpolate_template_fields()` applies this to a manifest's `type: template`
inputs only. It's called from three places depending on what "context" means
for that run: the single-module endpoint (lookup = the request's own inputs
dict — a template field can reference a sibling field on the same card), a
custom pipeline's per-step `seed()` (lookup = live `ExecutionContext`, so a
later step can reference an earlier one's output), and the built-in
tier-1→2→3 pipeline (`_build_pipeline()` now wraps bare modules in
`StepSpec`s seeded from their own manifest defaults, specifically so a
template field's *default* value — see `churn_response_agent`'s
`custom_note` — interpolates real data even with zero user configuration).
A `BaseModule` never needs to know templating exists; by the time its `run()`
reads `context.get("custom_note")`, substitution already happened.

**Source viewing never accepts a client-supplied path.**
`webapp/linting.py::resolve_source_path()` takes a manifest's own
`entrypoint` string and resolves it via `importlib.util.find_spec()` — the
same mechanism Python itself uses to import the module — so the file lint
runs against is always exactly the file that manifest points to, never
anything a request could redirect. `ruff check --output-format=json` runs
against that one resolved path with a 10-second timeout; a non-zero exit
code (ruff's own convention for "found issues") is not treated as a failure —
only a JSON parse error or timeout falls back to an empty issue list.

## 8. Step-by-step: how this was built (and how to extend it)

1. **Define the contract.** `BaseModule` with `name`, `tier`, `description`, and a
   single `run(context) -> dict` method. Every tier implements the same shape on
   purpose — the orchestrator shouldn't need to know which tier it's calling.
2. **Define the shared state.** `ExecutionContext` — a `variables` dict for data
   and a `history` list of `StepRecord`s for audit/debugging.
3. **Build the sequential runner.** `Orchestrator.run()` iterates a list of
   `BaseModule` instances, merging each output into the context and recording a
   `StepRecord`, with a switch for stop-on-error vs. continue-on-error.
4. **Add persistence.** `StateStore` wraps SQLite with two tables (`runs`,
   `steps`) so history survives process restarts and the CLI's `status` command
   has something to read.
5. **Add extensibility.** `registry.discover(directory)` scans a folder for
   `*.yaml` manifests, each naming an `entrypoint` (`module.path:ClassName`), and
   instantiates it — this is what makes "drop a file in, don't touch the core"
   true.
6. **Write example modules per tier** (`automations/example_automation.py`,
   `workflows/example_workflow.py`, `agents/example_agent.py`) that demonstrably
   pass data forward, proving the flow in code rather than just in a diagram.
7. **Build the CLI control layer** (`cli.py`) wiring `discover()` + `Orchestrator` +
   `StateStore` into `list` / `run` / `status` subcommands.
8. **Add tests** (`tests/test_orchestrator.py`) covering context-threading and
   both error modes.
9. **Add CI** (`.github/workflows/ci.yml`) running `pytest` and a CLI smoke test
   on every push, so a broken module or a bad manifest fails fast.
10. **Add an `inputs` schema to manifests** (list of `{name, label, type, default,
    options}`) so modules can declare form fields without any engine change —
    `registry.load_manifests()` returns them as plain dicts.
11. **Build the visual control center** (`webapp/`): a FastAPI layer
    (`list_modules` / `run_module` / `run_pipeline`) over the same engine, plus a
    static, build-step-free HTML/CSS/JS dashboard that renders manifest `inputs`
    as forms and turns each module into a one-click card.
12. **Add API tests** (`tests/test_webapp.py`, via FastAPI's `TestClient`)
    covering module listing, single-module runs with overridden inputs, 404s,
    and the full-pipeline endpoint.
13. **Add live progress events.** `ExecutionContext.emit()` +
    `Orchestrator.run(..., on_event=...)` turn a run into a stream of
    step/thought/tool-call events instead of an opaque black box (also fixed a
    latent bug along the way: `finish_run("completed")` was unconditionally
    overwriting `finish_run("failed")` at the end of a continue-on-error run).
14. **Instrument the example agent** (`agents/example_agent.py`) to call
    `context.emit("thought", ...)` / `context.emit("tool_call", ...")` with
    small `time.sleep()`s between them — enough to make the live stream
    visibly stream rather than resolve instantly, which is what a real
    LLM-backed agent's latency would look like anyway.
15. **Move execution to a background thread + SSE** (`webapp/events.py`'s
    `RunEventBus`, plus `webapp/main.py`'s run endpoints returning a
    `stream_id` instead of blocking): a synchronous HTTP response can't show
    "in progress," so a live tracker requires the run to happen off the
    request thread and the client to subscribe separately.
16. **Build the tracker, thought accordion, and toasts** (`webapp/static/app.js`,
    `styles.css`) on top of that stream via `EventSource`.
17. **Add `StepSpec` to the orchestrator** (`engine/orchestrator.py`): a
    module plus a `seed(context) -> dict` function run right before it, so a
    pipeline step can pull static values *and* values mapped from an earlier
    step's output — without this, a heterogeneous, user-composed pipeline has
    no way to feed differently-named fields into each other.
18. **Tag events with a step `index`**, not just `tier`/`name` — needed the
    moment a pipeline can legitimately contain the same module twice.
19. **Add `outputs` to manifests** (mirroring `inputs`) so the builder knows
    what's available to map from at each point in a pipeline.
20. **Build pipeline storage + validation** (`webapp/pipelines.py`): the same
    `pipelines/<slug>.yaml` convention as every other tier folder, with
    `validate_pipeline()` checking every module reference and mapping before
    anything is saved or run.
21. **Wire save-and-launch + re-run endpoints** (`webapp/main.py`): `POST
    /api/pipelines` (validate, save, launch) and `POST
    /api/pipelines/{slug}/run` (relaunch a saved one), both reusing
    `_build_steps_from_definition` + the existing background-thread/SSE
    launch path — no new execution mechanism needed.
22. **Build the visual sequencer** (`webapp/static/app.js`, `index.html`,
    `styles.css`): a step-by-step builder UI where each field gets a
    "static value vs. map from Step N" dropdown, reusing the same tracker/
    thought-accordion/toast code the rest of the dashboard already had.
23. **Add pipeline tests** (`tests/test_pipelines.py`): save-and-launch end to
    end, a mapping actually changing a module's behavior (not just being
    accepted), and both validation failure modes (unknown module, bad
    mapping reference).
24. **Add startup/runtime diagnostics** (`webapp/health.py`): validate
    manifests, entrypoints, and the state store on demand via `GET
    /api/health`, surfaced as a header beacon.
25. **Add metrics + CSV export** (`StateStore.metrics_summary()`, `GET
    /api/runs.csv`, `GET /api/metrics`): aggregate run telemetry for the
    header ticker and a downloadable history.
26. **Add pipeline duplication** (`pipeline_store.duplicate_pipeline()`): clone
    a saved pipeline under an auto-incremented name via the same
    validate-and-save path a fresh save uses.
27. **Add per-step timing** (`duration_ms` on `step_completed`/`step_failed`
    events) so the dashboard's log tabs can show real durations.
28. **Build the remaining dashboard polish** (`webapp/static/`): theme toggle,
    search, favorites (as shortcuts to the one real card, not duplicates),
    collapsible sections, toast/notification history, skeleton loaders, and
    the System Info / Agent Thoughts / Raw Output log tabs.
29. **Add diagnostics tests** (`tests/test_diagnostics.py`): health check
    shape, metrics reflecting a completed run, CSV export format, and
    duplication (including the 404 case).
30. **Add shared ingestion + hash-dedupe** (`webapp/ingestion.py`,
    `ingested_files` table): CSV/PDF upload endpoints, both backed by the same
    `ingest_csv_bytes`/`ingest_pdf_bytes` functions the watcher also calls.
31. **Add the folder watcher** (`webapp/watcher.py`): a plain polling loop,
    deliberately not the `watchdog` package, wired to call the same ingestion
    functions as a manual upload.
32. **Add generic template interpolation** (`engine/templating.py`): `{name}`
    substitution against manifest `type: template` fields, applied at three
    call sites depending on what "context" means for that run (single-module
    inputs, a custom pipeline step's live context, or the built-in pipeline's
    own manifest defaults) — demonstrated via `churn_response_agent`'s
    `custom_note` field, not a fake LLM persona picker.
33. **Add read-only source + lint viewing** (`webapp/linting.py`): resolve a
    module's file via `importlib.util.find_spec()` on its own manifest
    entrypoint (never a client-supplied path), run `ruff check` against
    exactly that file.
34. **Add tests for all of the above** (`tests/test_ingestion.py`,
    `test_templating.py`, `test_watcher.py`, `test_linting.py`): dedupe by
    content hash, purge never touching `orchestrator.db`/`pipelines/`, a
    template actually changing a module's output (not just being accepted),
    the watcher ignoring pre-existing files but catching new ones, and lint
    results for both a clean file and a deliberately broken one.
35. **Extend from here:** to add a real module, drop a `<name>.py` + `<name>.yaml`
    pair into the right tier folder (see the README's "Adding a new module"
    section) — it appears in `cli.py list`, the dashboard, and the pipeline
    builder's module picker with no engine or webapp code changes. Declare
    `outputs` too if you want other steps to be able to map from it, or
    `type: template` on an input to make it interpolate against context.
36. **Add per-step execution timeout** (`engine/orchestrator.py`): `StepSpec.
    timeout_seconds` runs `module.run()` in a shared `ThreadPoolExecutor` and
    calls `future.result(timeout=...)`; a timeout is surfaced as a normal
    failed step (a `TimeoutError`), with the error message explicit that
    Python can't forcibly kill the thread — it only stops the orchestrator
    from waiting on it.
37. **Add result caching** (`webapp/cache.py`, `StateStore.set_cached_result`/
    `get_cached_result`, a new `result_cache` table): a single-module run's
    output is cached by a SHA-256 hash of `{tier, name, inputs}`; a repeat run
    with identical inputs replays instantly via a synthesized SSE event
    sequence (`_replay_cached_result` in `webapp/main.py`) instead of a second
    background thread, so the frontend's existing tracker code needs no
    special case beyond a `cached: true` flag. A **Force refresh** checkbox
    per card bypasses it. Deliberately scoped to single-module runs — a
    pipeline step's inputs can depend on live upstream context, so "same
    inputs" isn't well-defined there the way it is for one module in isolation.
38. **Add graceful shutdown** (`webapp/main.py`'s `lifespan` context manager):
    every background run thread is tracked in an `_active_run_threads` set;
    on shutdown, each gets joined with a timeout before the watcher, scheduler,
    and state store connection are torn down.
39. **Add a cron-style scheduler** (`webapp/scheduler.py`, a new `schedules`
    table in `StateStore`): a background thread polls for schedules whose
    `next_run_at` has passed and calls back into `_trigger_schedule`, which
    launches the same module/pipeline execution path as a manual run — just
    without an SSE stream, since nobody's watching a scheduled run live (a
    stream nobody drains would leak). This is a plain recurring-interval
    timer, not a real cron-expression parser — no `croniter` dependency, and
    it's honest about not supporting minute/hour/day-of-week syntax nothing
    here would use.
40. **Add CSV auto-cleansing** (`ingestion.cleanse_records`): every ingested
    CSV is trimmed, blank rows and exact-duplicate rows are dropped, and
    empty cells become `null`, applied automatically before the JSON
    conversion is saved — with the before/after counts surfaced in the
    ingestion result and a small UI banner when anything changed.
41. **Add keyword search over artifacts** (`ingestion.search_artifacts`,
    `GET /api/artifacts/search`): a case-insensitive substring search across
    every ingested file's *extracted* content (the `.json`/`.txt` conversion,
    not the raw upload bytes), returning a snippet per match.
42. **Add a process performance ticker** (`webapp/perf.py`, via `psutil`):
    this process's own CPU%, RSS memory, thread count, and uptime, polled
    into the header every few seconds — deliberately scoped to "this one
    process," not a system-wide monitor.
43. **Add run comparison** (`_parsed_steps_for_run`/`_diff_dicts` in
    `webapp/main.py`, `GET /api/runs/{id}` and `GET /api/runs/compare`): a
    step-by-step, key-level diff between any two recorded runs' outputs,
    surfaced via two dropdowns and a Compare button under Recent Runs.
44. **Add a DAG visualizer for saved pipelines** (`webapp/graph.py`,
    `GET /api/pipelines/{slug}/graph`): since a pipeline step's mapping can
    reference *any* strictly-earlier step (enforced by `validate_pipeline`),
    the step list is already a valid topological order and the mapping edges
    form a genuine DAG, not just a chain — rendered client-side as hand-built
    SVG (sequential backbone + labeled arcs for mapping edges that skip
    steps), no charting library.
45. **Add keyboard shortcuts, a regex tester, and a slowest-step highlight**:
    `/` focuses search, `Esc` closes open dropdown panels, `?` shows a
    shortcut reminder; a pure-client-side regex tester widget lives under a
    new **Tools** section; and after any multi-step run, whichever step had
    the highest `duration_ms` gets a highlighted row and a badge, read
    straight off the same SSE events the tracker already receives.
46. **Add tests for all of Batch 3** (`test_orchestrator.py`'s timeout test,
    `test_scheduler.py`, `test_webapp.py`'s cache/compare/schedule tests,
    `test_ingestion.py`'s cleansing/search tests, `test_pipelines.py`'s graph
    test, `test_diagnostics.py`'s performance test): 55 tests total, all
    exercising real behavior (a monkeypatched call counter proving a cache
    hit skips re-execution, an actual slow module proving a timeout fires,
    a real scheduler thread proving it fires on interval and honors
    pause/resume) rather than asserting shapes alone.
47. **Add pipeline step reordering** (`webapp/static/app.js`'s
    `moveBuilderStep`): swapping two step positions rewires every explicit
    mapping's step index that referenced either position, and is blocked
    outright if the step moving into the earlier slot has an explicit
    mapping referencing what's currently there (the only direction that can
    create a forward reference, since a mapping can only ever point at a
    strictly earlier step). Documented, not silently ignored: a step that
    implicitly reads a shared context key another step produces *without* a
    declared mapping can still change behavior when reordered, since every
    module's output lands in the shared context regardless of mapping — the
    builder shows this caveat inline.
48. **Add nested-path (dotted) mapping and template resolution**
    (`engine/templating.py`'s `resolve_path`, reused by both
    `interpolate_template_fields`'s `{a.b.c}` syntax and
    `_build_steps_from_definition`'s field-mapping seed): a path's base name
    is resolved via the existing flat `lookup`, then each remaining segment
    walks one level into a dict, returning `None` (leaving a template
    literal, or `None` for a mapped field) the moment a segment is missing or
    the value isn't a dict. `validate_pipeline` checks only a mapping's base
    output name against the manifest's declared outputs — it can't validate
    a nested path since the manifest doesn't describe a nested output's shape.
49. **Add an upload/ingestion size limit** (`ingestion.read_upload_with_limit`,
    `MAX_UPLOAD_BYTES` = 20 MB): uploads are read in 1 MB chunks and rejected
    (413) the moment the running total exceeds the cap, so an oversized file
    is never fully buffered into memory just to be turned away; the folder
    watcher checks `path.stat().st_size` before ever reading a dropped file.
50. **Move linting off the request thread** (`linting.read_module_source_async`,
    a dedicated `ThreadPoolExecutor`): the file read + `ruff` subprocess call
    now runs via `loop.run_in_executor`, so the async endpoint awaits it
    without blocking the event loop on a slow lint or a large file.
51. **Add state store backup/export** (`StateStore.export_snapshot()`,
    `GET /api/backup/export` for a portable JSON snapshot of every run/step/
    schedule/ingested-file record plus the saved-pipelines list, and
    `GET /api/backup/db` for the raw SQLite file itself), surfaced as two
    download links in the health panel dropdown.
52. **Add secrets redaction** (`engine/redaction.py`'s `redact_secrets`,
    wired into `Orchestrator.run()`): a dict value is masked before it's
    used to build a `StepRecord` (what `StateStore.log_step` persists) or an
    emitted SSE event — but *after* `context.update(output)`, so the live
    `ExecutionContext` a later step reads still has the real value. One
    redaction point in the engine protects every downstream consumer
    (persisted history, live SSE, the JSON backup, the result cache) at once,
    rather than requiring each to remember to redact separately.
53. **Add rate limiting to run-triggering endpoints** (`webapp/ratelimit.py`'s
    `RateLimiter`, a per-client sliding window at 30 requests/10s): guards
    module runs, pipeline runs, and save-and-launch against an accidental
    request storm. Also fixed a related frontend gap while adding this: the
    run-trigger fetch calls never checked `res.ok` before destructuring
    `stream_id`, so a 4xx/5xx response (now genuinely reachable via a 429)
    would silently try to stream from `stream_id: undefined` instead of
    surfacing the server's error — a shared `fetchRunTrigger()` helper fixes
    this at all four call sites.
54. **Add JSON file ingestion** (`ingestion.ingest_json_bytes`,
    `POST /api/ingest/json`, folder-watcher `.json` handling): a top-level
    array is treated as the record list (mirroring a CSV's rows); anything
    else is wrapped as one row. Unlike CSV/PDF, no separate converted
    artifact is written — the raw upload already ships in the format
    ingestion turns CSV/PDF into, so it's directly reused as the
    searchable/previewable artifact.
55. **Add tests for all of Batch 4, plus a shared test fixture**
    (`tests/conftest.py`'s autouse `_reset_run_rate_limiter`): the rate
    limiter is a module-level singleton shared by every test that imports
    `webapp.main` (Python loads a module once), so without a reset the whole
    suite's cumulative request count could trip it well before any single
    test meant to exercise that behavior — the fixture clears it before each
    test so results stay independent of run order or timing. 77 tests total.
56. **Add arbitrary middle-step removal to the builder**
    (`webapp/static/app.js`'s `removeBuilderStep`): the mirror image of
    Batch 4's reordering — removing a step shifts every later mapping's
    step index down by one, and is blocked outright if another step's
    explicit mapping depends on the one being removed (there's no "step N
    no longer exists" fallback value).
57. **Add parallel branch execution** (`engine.orchestrator.ParallelGroup`,
    `Orchestrator._run_parallel_group`): branches are seeded sequentially
    (a plain dict merge — no need for concurrency there) before any
    branch's `module.run()` is submitted, so no branch ever sees a
    sibling's output during setup; then every branch's `module.run()` is
    submitted directly to the shared `_EXECUTOR` and awaited with its own
    optional timeout via `future.result(timeout=...)` — deliberately not
    routed through `_run_module()`'s own timeout path, which would
    re-submit to the same pool from within a pool thread and risk
    exhausting it. Each branch still gets its own `StepRecord` and
    step_started/step_completed/step_failed events (tagged `parallel: True`
    plus `branch_index`), so nothing about a single step's own behavior
    needs to know it ran inside a group. `_EXECUTOR`'s worker count was
    bumped from 8 to 16 for headroom, since a group submits every branch at
    once. Not yet exposed in the no-code visual builder — a real, separate
    UI feature (see the roadmap).
58. **Add SSE reconnect/replay** (`webapp/events.py`'s `RunEventBus`
    rewritten from a destructive `queue.Queue` per stream to an append-only
    event list per stream, guarded by a `threading.Condition`; the
    `/api/stream/{id}` endpoint pairs this with SSE's native
    `id:`/`Last-Event-ID` mechanism): the previous queue-based bus's own
    docstring claimed "a client that connects a moment late still gets
    everything from the start," which was only true for the *first*
    connection — a second connection (or a reconnect after a drop) got
    nothing, since `queue.Queue.get()` permanently removes what it reads.
    The frontend's `subscribeToStream` also used to treat any `onerror` as
    fatal, closing the connection immediately and pre-empting the browser's
    own auto-reconnect; it now only gives up after 5 consecutive errors,
    letting a transient drop resolve itself via reconnect-and-resume.
    Finished streams' event logs are kept for 5 minutes (swept lazily on
    the next `create()` call) so a reconnect shortly after a run ends can
    still replay it.
59. **Add persistent cross-run agent memory** (`engine.context.MemoryStore`,
    exposed as `context.memory`; `StateStore.get_memory`/`set_memory`/
    `delete_memory`/`all_memory`, a new `memory` table): accepts a
    `state_store` as `Any` rather than importing `StateStore` by type, to
    avoid a `context`↔`state_store` import cycle (`state_store.py` already
    imports `StepRecord` from `context.py`). Falls back to a local
    in-memory dict when no store is attached (e.g. a bare `ExecutionContext`
    in a test), so nothing about existing code needs to change. Demonstrated
    in `churn_response_agent`, which now remembers the last run's risk
    level and calls out when a *separate, independent* later run's risk
    level differs — proving the persistence is real, not just plumbing.
    Surfaced for inspection (redacted, like everything else) via
    `GET /api/memory` and a new **Agent Memory** dashboard section.
    Fixed a latent test-isolation gap along the way:
    `test_pipeline_graph_reports_nodes_sequence_and_mapping_edges` launched
    a pipeline via `POST /api/pipelines` but never waited for it to finish,
    so its background run could leak into whichever test ran next — harmless
    before (the agent had no observable side effects), but a real source of
    flakiness the moment it gained one.
60. **Add run history retention** (`StateStore.prune_runs`,
    `POST /api/runs/purge`, a purge control next to Recent Runs): deletes
    finished runs and their steps older than a cutoff, mirroring artifact
    purge — a run still in progress is never a candidate no matter how old
    it started.
61. **Add backup restore** (`StateStore.restore_snapshot`,
    `POST /api/backup/restore`, a **Restore backup…** upload in the health
    panel): strictly additive — every table is restored via
    `INSERT OR IGNORE` keyed by its own primary key/slug, so restoring the
    same snapshot twice, or into a store that already has some of this
    data, never overwrites anything and is always safe to repeat. A
    pipeline referencing a module that doesn't exist in this install is
    skipped rather than failing the whole restore. `export_snapshot()` was
    extended to include `memory` (previously omitted) so a restore is a
    genuine round trip of everything `context.memory` can hold.
62. **Add a compact/dense view toggle** (`#density-toggle`, a
    `body.density-compact` class): shrinks card padding, grid column
    minimum width, and form-field sizing; persisted in `localStorage` via
    the exact same pattern as the light/dark theme toggle.
63. **Add tests for all of Batch 5**: `ParallelGroup` concurrency proven by
    wall-clock timing (concurrent branches finish in ~1 branch's duration,
    not the sum of all of them), `webapp/events.py`'s replay/reconnect
    behavior at both the bus level and through the real `/api/stream`
    endpoint with a genuine `Last-Event-ID` header, cross-run memory
    proven via two separate `Orchestrator.run()` calls (and, at the API
    level, two separate pipeline launches) sharing one `StateStore`, and
    restore's additive/idempotent guarantee (restoring twice inserts
    nothing new; an existing key is never clobbered). 100 tests total.

## 9. Roadmap

The current engine is intentionally a single-process, synchronous, SQLite-backed
core — enough to prove the three-tier handoff and be genuinely useful for small
pipelines. Grow it only when a real need shows up:

- **Redis** — once more than one process needs to read/write the same
  `ExecutionContext` (e.g. a long-running agent tier and a web dashboard both
  need live state), move `variables` into Redis hashes keyed by run ID instead of
  an in-memory dict.
- **Celery / RQ** — once a workflow or agent step is slow enough that it
  shouldn't block the rest of the pipeline (e.g. an LLM call with retries), wrap
  that specific module's `run()` in a task queue instead of making the whole
  orchestrator async.
- **LangGraph** — once an agent's "task" is itself a multi-step reasoning loop
  (planning, tool calls, reflection) rather than a single `run()` call, model
  that agent internally as a LangGraph graph while it still presents a single
  `BaseModule` interface to the orchestrator, calling `context.emit()` at each
  internal step so the dashboard's thought accordion keeps working unchanged.
- **Auth on the dashboard** — the current `webapp/` has no auth layer, fine for
  local/single-user use; add it before exposing the control center beyond
  localhost.
- **`ParallelGroup` isn't exposed in the no-code visual builder** — Batch 5
  added concurrent branch execution at the engine level
  (`engine.ParallelGroup`), but authoring one requires writing Python
  directly; the builder's linear step model has no UI yet for "these N
  steps run together." A real, separate UI feature if it's ever needed.
  Relatedly: two branches writing the same output key is undefined
  (whichever merges back into context last wins) — there's no detection or
  warning for this today.
- **Committing saved pipelines to git automatically** — `POST /api/pipelines`
  writes `pipelines/<slug>.yaml` to disk (so it's a normal file to `git add`
  and commit like anything else), but it deliberately does **not** run `git
  commit`/`push` itself. Auto-committing from a live web handler is a bigger
  trust boundary than a local save — add it explicitly, behind its own opt-in,
  if that's actually wanted later.
- **Watcher poll interval / event-driven alternative** — 2 seconds is fine for
  "drop a file, wait a moment"; if that's ever too slow, swap the polling loop
  for the `watchdog` package's OS-level file events without changing
  `FilesystemWatcher`'s public interface (`start()`/`stop()`/`on_new_file`).
- **Rate limiter is per-process, in-memory, IP-keyed** — fine for a single
  local process behind no reverse proxy; if this ever runs behind one that
  changes the client IP `request.client.host` sees (or if this needs to
  survive process restarts / scale to multiple processes), swap
  `webapp/ratelimit.py`'s in-memory dict for something shared (Redis) rather
  than adding process-local state that silently stops working.
- **Redaction is key-name-based, not content-based** — `engine/redaction.py`
  masks a value only if its *key* looks like a secret; a module that embeds a
  real secret inside a differently-named field, or inside a larger string
  (e.g. an error message), isn't protected. Catching every possible secret
  shape would mean scanning arbitrary string content, a much bigger and much
  less reliable problem than this solves.
- **JSON ingestion doesn't get CSV's auto-cleansing** — `ingest_json_bytes`
  parses and hash-dedupes but doesn't run `cleanse_records()` — that was
  designed for messy hand-edited/spreadsheet-exported CSVs, a failure mode
  that doesn't really apply to JSON's more rigid structure. Revisit if a real
  need for JSON-specific cleanup shows up.

Each step above is additive — none require rewriting `BaseModule`, the manifest
format, or the example modules.
