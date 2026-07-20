# AI-Boss

A single monorepo that hosts, orchestrates, and executes three tiers of automated
work — deterministic **automations**, data-driven **AI workflows**, and autonomous
**AI agents** — in one dependable, synchronous pipeline.

See [`docs/ARCHITECTURE.md`](docs/ARCHITECTURE.md) for the full architecture write-up,
data-flow diagram, and step-by-step build history.

## Core concepts

| Tier | Folder | Nature | Example |
|---|---|---|---|
| **1. Automation** | `automations/` | Deterministic, rule-based, no learning | cron jobs, API polling, data entry |
| **2. AI Workflow** | `workflows/` | Data/ML pipeline producing predictive or optimized decisions | anomaly detection, scoring, forecasting |
| **3. AI Agent** | `agents/` | Autonomous, NLP-capable, context-dependent | query resolution, dynamic task planning |

All three implement the same `BaseModule` contract (`engine/base.py`), so the
orchestrator treats them identically: read the shared context, do work, write
back to the shared context.

## Folder structure

```
AI-Boss/
├── engine/                  # Core orchestrator — the only "framework" code
│   ├── base.py              #   BaseModule contract + Tier enum
│   ├── context.py           #   ExecutionContext (run-scoped) + MemoryStore (cross-run, persistent)
│   ├── orchestrator.py      #   Sequential runner + ParallelGroup for concurrent branches
│   ├── registry.py          #   Discovers modules + reads manifests (incl. input schemas)
│   ├── templating.py        #   {variable} and {nested.path} interpolation
│   ├── redaction.py         #   Masks secret-shaped keys before they're persisted/streamed
│   └── state_store.py       #   SQLite-backed run/step history + schedules + memory + snapshot export/restore
├── webapp/                  # Visual control center (FastAPI + vanilla JS, no build step)
│   ├── main.py               #   REST API: list modules, run one, run the pipeline, history
│   ├── pipelines.py           #   Storage/validation for user-built pipelines
│   ├── events.py               #   SSE event bus with reconnect/replay for live run streaming
│   ├── health.py                #   Startup diagnostics (manifests, entrypoints, state store)
│   ├── ingestion.py              #   CSV/PDF/JSON upload, hash-dedupe, auto-cleansing, artifact search/purge
│   ├── watcher.py                 #   Polling-based folder watcher (no watchdog dependency)
│   ├── linting.py                  #   Read-only per-module source + ruff lint view (off the event loop)
│   ├── cache.py                     #   Cache-key hashing for single-module result caching
│   ├── scheduler.py                  #   In-process recurring-run scheduler ("cron-style")
│   ├── perf.py                        #   Live process resource snapshot (CPU/mem/threads)
│   ├── graph.py                        #   Builds a saved pipeline's DAG (nodes/edges) for the visualizer
│   ├── ratelimit.py                    #   In-memory rate limiter for run-triggering endpoints
│   └── static/                          #   index.html / styles.css / app.js — the dashboard itself
├── automations/              # Tier 1 — drop in a <name>.py + <name>.yaml pair
│   ├── example_automation.py
│   └── example_automation.yaml
├── workflows/                 # Tier 2 — same pattern
│   ├── example_workflow.py
│   └── example_workflow.yaml
├── agents/                     # Tier 3 — same pattern
│   ├── example_agent.py
│   └── example_agent.yaml
├── pipelines/                    # User-built pipelines saved from the visual builder
│   └── <slug>.yaml                #   created at runtime — empty until you save one
├── artifacts/                       # Uploaded/converted files — created at runtime, gitignored
├── watched_input/                     # Drop a .csv/.pdf/.json here for auto-ingestion — gitignored
├── tests/
│   ├── conftest.py
│   ├── test_orchestrator.py
│   ├── test_webapp.py
│   ├── test_pipelines.py
│   ├── test_diagnostics.py
│   ├── test_ingestion.py
│   ├── test_templating.py
│   ├── test_watcher.py
│   ├── test_linting.py
│   ├── test_scheduler.py
│   ├── test_redaction.py
│   └── test_events.py
├── docs/
│   └── ARCHITECTURE.md
├── cli.py                    # Scriptable control layer: list / run / status
├── requirements.txt
├── .env.example
└── .github/workflows/ci.yml
```

## Quickstart

```bash
pip install -r requirements.txt

# Visual control center — the primary way to use this day to day
uvicorn webapp.main:app --reload
# then open http://localhost:8000

# Scriptable / CI-friendly alternative
python cli.py list      # show every discovered automation/workflow/agent
python cli.py run       # execute the full pipeline once, synchronously
python cli.py status    # inspect recent run history from the SQLite state store
```

The dashboard renders every automation, workflow, and agent as a card, grouped
into three visually separated sections. Cards with declared `inputs` (see any
`example_*.yaml`) show a small form — text, number, dropdown, or toggle — right
in the card. A **Run Full Pipeline** button at the top runs all three tiers in
sequence, the same tier-1 → tier-2 → tier-3 handoff `cli.py run` performs. No
terminal, no commands — everything is a button, form field, or dropdown.

Clicking **Run** doesn't just wait and show a final result — it streams the run
live over Server-Sent Events:

- A **step tracker** shows each module going ⚪ pending → 🟡 running → 🟢 done
  (or 🔴 failed) as it actually happens.
- For **Tier 3 agents**, an expandable **thought stream** appends the agent's
  reasoning and tool calls live, underneath its tracker row, as they're
  emitted — not just the final message.
- A **toast notification** in the corner reports success or failure the
  moment the run finishes, and the card's status pill updates to match.

The web UI and the CLI are two views over the exact same engine and the same
`orchestrator.db` state store, so a run triggered from one shows up in the
other's history.

## Building a custom pipeline visually

The dashboard's **Visual Pipeline Builder** lets you chain any automations,
workflows, and agents into your own sequence without writing a manifest by
hand:

1. Click **+ New Pipeline**, name it, and pick a module for Step 1 from the
   dropdown (grouped by tier, so you can mix automations, workflows, and
   agents in any order — including reusing the same module twice).
2. **+ Add Step** for each subsequent step. Every field on a step shows a
   small "Static value" dropdown next to it — switch it to **"From Step N:
   &lt;output&gt;"** to wire that field straight from an earlier step's declared
   output instead of typing a literal, e.g. feeding Step 2's insight into
   Step 3's `notify_slack` field. Need just a piece of a nested output (e.g.
   `insight.risk_level` instead of the whole `insight` dict)? Fill in the
   small "nested key" box that appears once a mapping is selected.
3. Use the **▲/▼** buttons to reorder a step, or **×** to remove any step
   (not just the last one). Both rewrite this pipeline's own explicit
   mappings so they still point at the right step afterward, and both are
   blocked outright if doing so would leave a mapping pointing at a later
   step, or at a step that no longer exists. Neither can know about a step
   that implicitly reads a shared context key another step happens to
   produce *without* a declared mapping (any module output lands in the
   shared context regardless of mapping) — that kind of hidden coupling can
   still change behavior on reorder or removal.
4. **Save & Launch** does exactly what it says in one click: it writes the
   pipeline to `pipelines/<slug>.yaml` (the same manifest convention as every
   other module folder — check it into git like anything else here) and
   immediately runs it through the same live tracker/thought-stream/toast UI
   as any other run.
5. Saved pipelines get their own card in a **Saved Pipelines** section with a
   Run button, so you don't have to rebuild them each time. Click **Graph**
   to see that pipeline's steps and data-mapping dependencies rendered as an
   SVG DAG.

A mapped field is resolved at the moment that step runs, by reading whatever
context key the source step's declared output landed under — so it works even
when the two modules' field names don't already match (`engine/orchestrator.py`'s
`StepSpec.seed`, `engine/templating.py`'s `resolve_path`). A saved pipeline is
validated on save: every step's module must exist, and every mapping must
point at an *earlier* step's *declared* output (a nested path's base name is
what's checked — the manifest doesn't describe a nested output's shape), or
you get a clear error back instead of a silent bad reference.

## Adding a new module

1. Pick the tier folder (`automations/`, `workflows/`, or `agents/`).
2. Add `my_module.py` with a class implementing `BaseModule` (see any `example_*.py`
   for the shape — just a `name`, `tier`, `description`, and a `run(context)` method).
   Call `context.emit("thought", "...")` or `context.emit("tool_call", "...")` from
   inside `run()` if you want the dashboard's live tracker/thought stream to show
   your module's own progress, not just start/finish.
3. Add `my_module.yaml` next to it:
   ```yaml
   name: my_module
   tier: automation   # or workflow / agent
   entrypoint: automations.my_module:MyModuleClass
   description: What this module does.
   enabled: true
   inputs:                     # optional — renders as a form on the module's card
     - name: threshold
       label: Threshold
       type: number             # text | number | select | toggle
       default: 0.1
   outputs:                     # optional — makes these mappable in the pipeline builder
     - name: my_result
       label: My Result
   ```
4. Run `python cli.py list` (or refresh the dashboard) to confirm it's discovered.

No core engine code changes are required — the registry scans each tier folder for
manifests at pipeline-build time, the dashboard reads the same `inputs` schema to
render the card's form, and the pipeline builder reads `outputs` to populate the
"map from an earlier step" dropdown.

## Diagnostics, telemetry, and everyday UI

A few things live in the header/subheader on every page load:

- **System health beacon** — click it for a breakdown of four checks: the state
  store is reachable, every manifest parses, every entrypoint actually imports
  and instantiates, and which optional environment variables aren't set. Backed
  by `GET /api/health` (`webapp/health.py`) — a broken manifest or a typo'd
  entrypoint shows up here with a specific reason, not as a confusing error the
  first time someone clicks Run.
- **Metrics ticker** — total runs, success rate, and average duration across
  everything ever run, from `GET /api/metrics`.
- **Recent Runs** — the last 15 runs with status and duration, plus an
  **Export CSV** link (`GET /api/runs.csv`) for the full history.
- **Search bar** — filters every module and saved-pipeline card by name/tier/
  description as you type; a tier section with zero matches hides itself.
- **Favorites** — click the ☆ on any card to pin it to a shelf at the top;
  clicking a favorite's own Run button scrolls to and runs the *actual* card
  elsewhere on the page (so its live tracker/thought stream shows up in its
  one true location, not a second copy).
- **Collapsible sections** — every tier section, Favorites, Saved Pipelines,
  and Recent Runs can be collapsed via the chevron next to its heading; state
  persists in `localStorage`.
- **Light/dark theme toggle** — persists across reloads.
- **Notifications** — the bell icon keeps the last 30 toasts (success and
  error) so a background run's result isn't missed if you look away.
- **Clone a saved pipeline** — the "Clone" button on a saved-pipeline card
  duplicates it under a new name (`POST /api/pipelines/{slug}/duplicate`) so
  you can branch off an existing chain without rebuilding it.
- **System Info / Agent Thoughts / Raw Output tabs** — the full-pipeline and
  builder run panels split their live log into three views instead of one
  undifferentiated dump: a timestamped step log, the aggregated agent
  thought/tool-call stream, and the final JSON context. Whichever tab is open
  gets a **Copy** button, and the Raw Output tab has a **Pretty/Compact** toggle.

## Data ingestion, templates, and code inspection

- **Upload a CSV, PDF, or JSON file** in the dashboard's **Data Ingestion**
  section (drag a file onto the drop zone, or use "Choose file"). A CSV
  becomes structured JSON records (auto-cleansed — trimmed, deduped, blank
  rows dropped); a PDF gets its text extracted; a JSON file (an array of
  records, or a single object) is used as-is, no separate derived copy
  needed. Files are hashed — dropping the exact same file twice is reported
  as a duplicate and skipped, not reprocessed. Uploads over 20 MB are
  rejected before ever being fully read into memory. Converted/raw artifacts
  land in `artifacts/` with a small table, a **keyword search** box that
  full-text searches every ingested file's extracted content, and a
  **Purge older than N hours** control (never touches `orchestrator.db` or
  `pipelines/`, no matter what age you set).
- **Folder watcher** — drop a `.csv`/`.pdf`/`.json` file straight into
  `watched_input/` (no browser needed at all) and it's ingested within a
  couple seconds by a lightweight polling loop (`webapp/watcher.py`, no
  `watchdog` dependency). The dashboard's Folder Watcher feed shows what it
  picked up, live.
- **Template fields** — a manifest input can declare `type: template` to get a
  textarea that supports `{variable}` and `{variable.nested.path}`
  interpolation against whatever's currently in the shared context (a sibling
  input on the same card, or an earlier pipeline step's output) — resolved
  right before that step runs, so the module itself never has to know
  templating exists. Click one of the suggested `{name}` chips under the
  textarea to insert it at the cursor. See `churn_response_agent`'s
  `custom_note` field for a working example.
- **View source + lint** — click **&lt;/&gt;** on any module card to see its
  actual `.py` source and a live `ruff check` result (green "no issues" or a
  list of line/column/rule findings), read-only, scoped to exactly that
  module's own file — never an arbitrary path.

## Reliability, scheduling, and inspection tools

- **Result caching with force-refresh** — a single-module card run is cached
  by a hash of `{tier, name, inputs}` (`webapp/cache.py`); re-running with the
  exact same inputs returns instantly with a "⚡ cached result" badge instead
  of re-executing. Check **Force refresh** on a card to bypass the cache for
  that run. Scoped deliberately to single-module runs, not pipelines — a
  pipeline step's inputs can depend on live upstream context, so "same
  inputs" isn't well-defined the same way there.
- **Per-step execution timeout** — every `StepSpec` the dashboard builds now
  carries a `timeout_seconds` (`engine/orchestrator.py`); a step that runs
  longer is recorded as a failed step with a clear message. Python can't
  forcibly kill a running thread, so this only stops the orchestrator from
  *waiting* on it — documented explicitly in the error text.
- **Graceful shutdown** — the FastAPI app tracks every in-flight background
  run thread and gives them a few seconds to finish (or at least stop being
  waited on) before the process exits, instead of runs disappearing mid-flight.
- **Cron-style scheduler** — schedule any module or saved pipeline to run on
  a recurring interval from the **Schedules** section (create, pause/resume,
  delete). This is a plain "every N seconds" timer (`webapp/scheduler.py`),
  not a real cron-expression engine — no new dependency, and it covers the
  actual local-tool use case without pretending to parse minute/hour/day-of-
  week syntax nothing here would use.
- **CSV auto-cleansing on ingest** — every CSV upload is automatically
  trimmed of whitespace, has blank rows dropped, exact-duplicate rows
  removed, and empty cells turned into `null` (`ingestion.cleanse_records`).
  The ingestion result shows a "🧹 auto-cleansed" summary when anything
  changed. No schema inference or type coercion — just cleanup.
- **Keyword search over artifacts** — the Data Ingestion panel's search box
  does a case-insensitive full-text search across every ingested file's
  *extracted* content (the JSON/text conversion, not the raw upload) and
  shows a snippet per match.
- **Process performance ticker** — the header shows this process's own live
  CPU%, RSS memory, thread count, and uptime (`webapp/perf.py`, via
  `psutil`) — not a system-wide monitor, just "how is this one process doing."
- **Run comparison / diff tool** — pick any two runs in **Recent Runs** and
  click **Compare** to see a step-by-step, key-level diff of their outputs
  side by side.
- **DAG visualizer** — click **Graph** on a saved pipeline card to render its
  steps and data-mapping dependencies as an SVG graph. Since a mapped field
  can pull from *any* earlier step (not just the one immediately before it),
  this is a genuine DAG, not just a straight line — skip-edges are drawn as
  labeled arcs above the sequential backbone.
- **Keyboard shortcuts** — `/` focuses the search bar, `Esc` closes open
  dropdown panels, `?` shows a quick shortcut reminder toast.
- **Regex tester** — a small client-side widget under **Tools**: type a
  pattern and flags, paste test text, see matches highlighted live and
  captured groups listed. Pure JS, no backend round-trip.
- **Slowest-step highlight** — after any multi-step run finishes, whichever
  step took the longest wall-clock time gets a highlighted row and a
  "🐢 slowest (Nms)" badge in its tracker, so the bottleneck is visible at a
  glance instead of having to read every duration.

## Pipeline reordering, nested mappings, and hardening

- **Reorder pipeline steps** — the visual builder's ▲/▼ buttons swap a step's
  position, rewiring every explicit "map from Step N" reference so it still
  points at the right step afterward. A move that would make an explicit
  mapping reference a later step instead of an earlier one is blocked
  outright. This can't account for a step that implicitly reads a shared
  context key another step happens to produce *without* a declared mapping —
  every module's output lands in the shared context regardless of mapping,
  so that kind of hidden coupling is a real, disclosed limitation, not an
  oversight.
- **Nested-path mapping and templates** — both the pipeline builder's field
  mapping and `{variable}` template interpolation now support a dotted path
  (`insight.risk_level`) reaching into a nested key of a declared output,
  not just the whole output value (`engine/templating.py`'s `resolve_path`).
- **Upload/ingestion size limit** — every upload (`/api/ingest/*`) and every
  file the folder watcher picks up is capped at 20 MB. Uploads are read in
  chunks and rejected the moment they exceed the cap, never fully buffered
  into memory first; the watcher checks a file's size on disk before ever
  reading it.
- **JSON file ingestion** — upload (or drop into `watched_input/`) a `.json`
  file — an array of records, or a single object — and get back the same
  row-count/columns/preview shape as a CSV. Unlike CSV/PDF, no separate
  converted artifact is written; the raw upload already is the reusable,
  searchable artifact.
- **Background-threaded linting** — the module source/lint viewer
  (`GET /api/modules/{tier}/{name}/source`) now runs the file read + `ruff`
  subprocess in a dedicated thread pool instead of on the request's own
  thread, so a slow lint or a large file can't sit in front of unrelated
  requests.
- **Secrets redaction** — any dict value whose key looks like a secret
  (`password`, `token`, `api_key`, `secret`, `credential`, ...) is replaced
  with a fixed placeholder before it's ever persisted to the state store or
  streamed over SSE (`engine/redaction.py`). The live `ExecutionContext` a
  later step actually reads is untouched, so a module that legitimately
  needs a real secret value still gets one — only the audit trail (run
  history, log tabs, the JSON backup) is masked. This is a best-effort,
  key-name-based check, not a scan of arbitrary string content.
- **State backup/export** — the health panel's dropdown has two downloads: a
  portable JSON snapshot of every run, step, schedule, ingested-file record,
  and saved pipeline (`GET /api/backup/export`), and the raw SQLite file
  itself (`GET /api/backup/db`).
- **Rate limiting on run triggers** — module runs, pipeline runs, and
  save-and-launch are capped at 30 requests per 10 seconds per client
  (`webapp/ratelimit.py`), an in-memory sliding window with no external
  dependency — a guard against an accidental request storm (a stuck retry
  loop, a misconfigured schedule), not multi-tenant abuse prevention.

## Concurrency, memory, and recovery

- **Arbitrary step removal** — the builder's **×** now removes any step, not
  just the last one, shifting every later mapping's step index down by one
  and blocking the removal outright if another step's explicit mapping
  depends on the one being removed.
- **Parallel branch execution** — `engine.ParallelGroup` runs a set of
  independent steps concurrently in one pipeline slot (each still gets its
  own `StepRecord`/tracker events, tagged `parallel: true` with a
  `branch_index`), then merges their outputs back before the next step.
  Construct one in Python (`Orchestrator([Double(),
  ParallelGroup(steps=[StepA(), StepB()]), AddOne()])`) or visually via the
  builder's **+ Add Parallel Group** (see below). Branches must be
  genuinely independent; two branches writing the same output key is
  undefined (whichever merges last wins).
- **SSE reconnect/replay** — the live run stream now records events in an
  append-only log per run instead of draining a queue, and pairs this with
  SSE's native `id:`/`Last-Event-ID` mechanism, so a dropped connection
  (network blip, browser reload) resumes exactly where it left off via a
  plain `EventSource` — no custom client-side retry logic needed.
- **Persistent cross-run agent memory** — `context.memory` is a key/value
  store any module can read or write that survives across *separate* runs,
  unlike `context.variables` (scoped to one run). `churn_response_agent`
  demonstrates it: it remembers the last run's risk level and calls out
  when a completely independent later run's risk level has changed. Browse,
  delete, or clear it from the new **Agent Memory** section, or via
  `GET /api/memory`.
- **Run history retention** — a **Purge older than N hours** control next to
  Recent Runs (`POST /api/runs/purge`) deletes old finished runs and their
  steps, mirroring the existing artifact purge; a still-in-progress run is
  never a candidate no matter how old it started.
- **Backup restore** — the health panel's **Restore backup…** upload
  complements the existing export: it re-populates runs/steps/schedules/
  ingested-file records/memory/pipelines from a previously exported JSON
  snapshot. Strictly additive — it fills in whatever isn't already present
  by id/key/slug and never overwrites existing data, so restoring the same
  snapshot twice (or into a store that already has some of this data) is
  always safe.
- **Compact/dense view** — a header toggle next to the theme switch shrinks
  card padding and text size to fit more on screen at once, persisted in
  `localStorage` the same way the theme is.

## Resilience, conditions, and outbound HTTP

- **Generic HTTP request module** — `automations/http_request.py` calls any
  user-supplied URL (method, JSON-object headers, JSON-or-raw body, and
  timeout all configurable per run) and returns a structured
  `http_response` (status code, ok flag, headers, parsed-JSON-or-text body,
  elapsed ms). A non-2xx status is a *result*, not a failure — only an
  unreachable host, timeout, or malformed input raises. It deliberately
  opts out of the fixed one-click demo pipeline
  (`include_in_full_pipeline: false` in its manifest) so that stays fast
  and offline; it's fully runnable standalone and in hand-built pipelines.
- **Conditional pipeline steps** — every builder step has a **Run only
  if…** row: pick a context key (dot-paths reach into nested outputs, e.g.
  `insight.risk_level`), an operator (`equals`, `does not equal`,
  `contains`, `gt`, `lt`, `truthy`, `falsy`), and a value. When the
  condition doesn't hold at the moment the step would run, the step is
  *skipped* — a `step_skipped` event (⏭️ in the tracker, with the reason in
  its tooltip), not a failure, and the run continues. Engine-level:
  `StepSpec(condition=lambda ctx: ..., condition_label="...")`; a condition
  that raises counts as false. Conditions reference context keys, never
  step numbers, so reordering steps needs no condition rewiring.
- **Module circuit breaker** — after N *consecutive* failures (default 3;
  per-module override via `circuit_breaker_threshold` in its manifest) a
  module's breaker trips: every launch path — its Run button, the full
  pipeline, saved pipelines, schedules — refuses to run it (HTTP 409, and
  a blocked schedule records the reason as its last status). The card shows
  a ⛔ banner with a **Reset breaker** button (`POST
  /api/breakers/{tier}/{name}/reset`; `GET /api/breakers` lists state). A
  success closes a failure streak but never closes an already-open breaker —
  only an explicit reset does, so a flaky module can't quietly re-arm itself.
- **XLSX export** — **Export XLSX** next to the CSV export downloads the
  same run history as a spreadsheet with a second **Steps** sheet of
  per-step detail (module, tier, success, error, timing) — the thing a flat
  CSV can't carry. Uses `openpyxl`, imported lazily.
- **Run-history search** — a search box in Recent Runs does full-text,
  case-insensitive search across every recorded step's output JSON and
  error text (`GET /api/runs/search?q=`), newest first, with a snippet and
  a ❌ marker on hits found in a failed step's error. Step outputs are
  redacted before they're ever logged, so snippets can't leak secrets.
- **Environment & config viewer** — a dashboard section listing every env
  var declared in `.env.example` (set/missing, with secret-shaped names
  only ever revealing their value's *length*) plus the live values of the
  app's operational knobs: step timeout, breaker threshold, rate limit,
  upload cap, cache age, watcher/scheduler intervals, SSE retention, and
  the state-store path (`GET /api/environment`).
- **Parallel groups in the visual builder** — **+ Add Parallel Group** adds
  a slot whose branches (two or more) run concurrently: pick a module per
  branch, map branch fields from any step *above* the group, optionally
  name the group. Saved as `type: parallel` + `branches:` in the pipeline
  YAML; a later step can map from the group's slot (it offers every
  branch's declared outputs), and the live tracker shows one indented row
  per branch under a group header. Validation enforces ≥2 branches and
  blocks branch mappings that reference the group itself or anything after
  it.

## Retries, webhooks, editing, and run history tools

- **Automatic step retries with backoff** — any builder step (or parallel
  branch) can turn on **Retry on failure**: max retries + a backoff seconds
  value that doubles each attempt (`retry_backoff_seconds * 2**attempt`). A
  failing step retries in place — no re-seed, same context — emitting a
  `step_retrying` event (🔁 in the tracker, with attempt/delay in its
  tooltip) before each wait; only exhausting every retry counts as a real
  `step_failed`, and only the terminal outcome reaches the circuit breaker.
- **Inbound webhook trigger** — every saved pipeline gets a
  `POST /api/pipelines/{slug}/webhook` URL (shown, with a copy button, on
  its card): POST a JSON body and it overrides step 1's own input fields
  (unknown keys are ignored, same as any other input dict here) before
  launching — no auth, consistent with this app's single-user/local scope,
  but it still respects the run rate limit and a tripped circuit breaker
  like any other launch path.
- **Edit a saved pipeline** — an **Edit** action on a saved-pipeline card
  loads its full definition (steps, mappings, conditions, retries, parallel
  groups) back into the builder. Renaming is disabled while editing (a
  banner explains why) so **Save & Launch** always overwrites the same
  slug/file — no orphaned duplicate under a new name. Clone still exists
  for making an actual differently-named copy.
- **Run detail drill-down** — clicking a Recent Runs row expands it in
  place with full per-step detail (tier/name/success/timing/output/error),
  fetched from the existing `GET /api/runs/{run_id}` and cached client-side
  (a run's recorded steps never change once finished).
- **Per-module performance stats** — every module card now shows
  `N runs · X% success · avg Yms`, sourced from `GET /api/modules/stats`
  (also folded into `GET /api/modules` per card) — total runs, success
  rate, and average duration grouped by (tier, name).
- **Pipeline YAML export/import** — **Export** on a saved-pipeline card
  downloads its own YAML file directly (for sharing/backup outside a full
  state-store snapshot); **Import pipeline…** next to the builder uploads
  one back in, validated through the exact same path a builder save uses.
- **Re-run a past run** — every run's drill-down has a
  **↻ Re-run with these inputs** button. Each step now records the full
  resolved input snapshot it actually ran with (`GET /api/runs/{run_id}`
  exposes `inputs` per step) — covering *both* seeding paths (`StepSpec.seed`
  for pipeline steps, and the run's own `initial_context` for standalone
  module/scheduled runs). `POST /api/runs/{run_id}/rerun` replays each
  recorded step with its own module + recorded inputs as a brand-new run;
  it doesn't reconstruct the original pipeline's mappings/conditions/groups
  (that topology isn't part of run history), just the per-step facts that
  are. A secret-shaped input is redacted before it's ever logged, so a
  re-run can't recover or resend the real secret value.

## Agent delegation, trends, scheduling, tagging, and the command palette

- **Agent-to-agent delegation** — `churn_response_agent` can hand a
  high-risk case off to a second, specialized agent (`escalation_agent`)
  by writing a `handoff` dict into shared context; the receiving agent
  reads it straight out of context (no field mapping needed) and either
  declines cleanly or picks a specialist and drafts a plan. This is
  genuine agent-to-agent delegation — one agent deciding *another agent*
  should take over — not just the usual tier-to-tier handoff every module
  already does.
- **Run history trend sparkline** — an inline SVG bar chart above Recent
  Runs shows the last 15 runs' duration (bar height) and outcome (bar
  color, reusing the same reserved status colors as everywhere else in
  the app) at a glance, with a legend and a hover tooltip. No charting
  library added.
- **Daily-at-a-time scheduling** — schedules can now fire once a day at a
  specific `HH:MM` (local time), alongside the existing every-N-seconds
  interval mode. Pick the mode from a Frequency dropdown when creating a
  schedule.
- **Artifact tagging** — assign free-text tags to an ingested artifact
  (an inline "+ tag" chip input, click a chip's × to remove) and filter
  the artifact list by tag, alongside the existing keyword search.
- **Command palette** — press **Ctrl/Cmd+K** to open a searchable overlay
  that runs a module, jumps to a saved pipeline, opens the builder, or
  toggles theme/density, all from the keyboard.
- **Bulk run selection** — checkboxes on Recent Runs rows (plus a
  select-all checkbox) enable a "Delete selected" action, for pruning
  specific runs rather than everything older than a cutoff.

## XLSX ingestion, pipeline versioning, and a shared agent blackboard

- **XLSX ingestion** — upload (or drop into the folder watcher) an .xlsx
  spreadsheet, not just CSV/PDF/JSON. The first sheet's row 1 becomes
  headers; everything after gets the same auto-cleansing (trim/blank-row/
  duplicate-row) a CSV upload already gets.
- **Pipeline version history** — every time a saved pipeline is overwritten
  (a builder re-save, or importing under an existing name), its previous
  definition is archived. A **History** action on the pipeline card lists
  past versions, shows a diff against the current definition, and can
  restore an old version back to current — restoring is itself undoable,
  since it archives the pre-restore definition on the way in.
- **Global scheduler pause** — a **⏸ Pause all** / **▶ Resume all** toggle
  in the Schedules section stops every schedule from firing for a
  maintenance window, without touching any individual schedule's own
  enabled/disabled state.
- **Resource usage alert banner** — the existing CPU/memory ticker now
  flags unusually high usage with a highlighted stat and a banner, so it's
  noticeable without watching the numbers.
- **Pipeline builder undo** — Ctrl/Cmd+Z or an **↶ Undo** button reverts
  the last structural change (add/remove a step or branch, reorder, swap a
  step's module) in the visual pipeline builder.
- **Shared agent blackboard** — a second multi-agent collaboration pattern
  alongside the handoff above: any agent in a run can post a free-form,
  timestamped note any other agent can read, without either side needing
  to know who else is participating. Shown as a **🗒 Shared Agent
  Blackboard** section in a run's detail drill-down when notes exist.

## Module toggles, URL ingestion, templates, audit trail, and self-test

- **Module enable/disable toggle** — a card's own **On/Off** switch turns a
  module off at runtime, independent of its manifest's own `enabled` flag —
  no YAML edit needed, and it persists across a restart. A disabled module
  blocks every launch path (standalone run, full pipeline, saved pipeline,
  schedule, webhook, rerun) exactly like a tripped circuit breaker.
- **Ingest a file from a URL** — paste a link to a `.csv`/`.json`/`.xlsx`
  file next to the upload dropzone instead of only dragging a local file;
  it's fetched with the same streaming size cap and duplicate-content
  detection direct uploads already get.
- **Pipeline starter templates** — a **Pipeline Templates** gallery offers
  three ready-made pipelines built entirely from the bundled modules (the
  basic churn-response chain, the multi-agent handoff-with-escalation
  chain, and an outbound-webhook notifier). **Use this template** clones one
  into your own saved pipelines to customize.
- **Recent Actions audit trail** — a **Recent Actions** panel logs every
  destructive/administrative action (run purge, bulk-delete runs, artifact
  purge, backup restore, circuit breaker reset, scheduler pause/resume,
  pipeline version restore) with a timestamp and detail, newest first, so
  it's never a guess what happened and when.
- **One-click module self-test** — a **🧪 Self-Test** button actually runs
  every enabled module once with its own manifest's default inputs (not
  just checking that manifests parse and entrypoints instantiate, which
  `/api/health` already does) and reports a pass/fail per module with error
  detail and timing, in a disposable context that never touches real run
  history, circuit breaker counts, or persistent agent memory.

## Pipeline lifecycle, breaker thresholds, comparison, and module scaffolding

- **Delete a saved pipeline** — a **Delete** button (with a confirm prompt)
  on each saved-pipeline card removes its current definition. Its archived
  version history is kept, not wiped, so it stays restorable.
- **Save without launching** — a **Save** button next to **Save & Launch**
  in the builder persists a pipeline as a draft without immediately running
  it, for one that isn't ready to fire yet or is only ever meant to be
  triggered by a schedule or webhook.
- **Runtime-configurable circuit breaker threshold** — each module card has
  its own **Breaker trips after N failure(s)** control, overriding the
  manifest's own `circuit_breaker_threshold` without a YAML edit, with a
  one-click revert to the manifest default.
- **Diff two saved pipelines** — a **Compare** row under Saved Pipelines
  picks any two and shows a key-level diff of their definitions, the same
  tool already used for run comparison and pipeline version history.
- **Audit log CSV export** — an **Export CSV** link next to the Recent
  Actions panel downloads the full administrative-action history.
- **New-module scaffolding wizard** — a small form under **Tools** generates
  a starter `.py` + `.yaml` pair for a brand-new automation, workflow, or
  agent — the exact boilerplate this README's "Adding a new module" section
  otherwise asks you to hand-write — and it's discoverable immediately, no
  restart needed.

## Pipeline validation/import, module duplication, artifact viewing, and presets

- **Validate a pipeline without saving** — a **Validate** button next to
  **Save** / **Save & Launch** checks the current builder definition
  (module references, mappings, condition/retry shapes) and reports errors
  without writing anything to disk or launching a run.
- **Import a pipeline from a URL** — paste a link to a `.yaml` pipeline
  export next to **Import pipeline…** to bring it in the same way an
  uploaded file already does.
- **Duplicate an existing module** — a **⧉** button on any module card
  clones its manifest and source under a new name in the same tier, a
  working starting point (keeps the original's inputs/outputs/`run()`
  logic) rather than an empty stub.
- **Artifact content viewer** — a **View** button on each ingested artifact
  expands its full extracted content inline — a table for structured
  records, plain text for extracted PDF text — not just a search-result
  snippet.
- **Saved input presets** — save the current values in a module card's
  input form under a name, then reload that exact combination later with
  one click instead of retyping it.
- **Bulk tag artifacts** — select multiple ingested files with checkboxes
  and apply one tag to all of them at once, instead of one at a time.

## Pipeline tagging, module usage lookup, run notes, schema summaries, and a notification center

- **Tag saved pipelines** — add/remove free-form tags on any saved pipeline
  and filter the Saved Pipelines section by tag, mirroring the existing
  artifact-tagging feature.
- **Search inside pipeline definitions** — a dedicated search box over
  saved pipelines matches keywords anywhere in a pipeline's YAML (input
  values, mapped fields, conditions), not just its name/description the way
  the header search omnibar already does.
- **"Used by" reverse lookup** — every module card shows which saved
  pipelines reference it, so you can see the blast radius before disabling,
  deleting, or duplicating a module; clicking a pipeline name jumps
  straight to its card.
- **Free-text notes on past runs** — attach, edit, or clear a note on any
  run in Recent Runs, for your own future reference (e.g. "expected
  failure, ignore") — a small 📝 marks runs that have one.
- **Column schema summary on ingest** — the artifact content viewer shows a
  lightweight schema alongside any CSV/XLSX table: column names, an
  inferred type per column (int/float/bool/date/text), and the row count.
- **Persistent alert/notification center** — unlike a toast (gone on
  reload), the bell icon's panel now also lists durable alerts that survive
  a page refresh: circuit breaker trips, scheduled-run failures, and
  resource-usage warnings, with an unread badge.

## Step duplication, weekly scheduling, tag directory, notification muting, bulk pipeline export, and step notes

- **Duplicate a pipeline builder step** — a **⧉** button next to a step's
  move/remove controls clones it (or a whole parallel group) with the same
  module, inputs, and mappings, inserted right after the original.
- **Weekly scheduling** — beyond "every N seconds" and "daily at a time,"
  schedule a module or pipeline for a specific day of the week plus time of
  day (e.g. "every Monday at 09:00").
- **Artifact tag directory** — a tag cloud above the artifacts list shows
  every tag currently in use and how many files carry it; click one to
  filter by it instead of typing a tag you already have to know.
- **Mute notification kinds** — turn off circuit-breaker-trip,
  scheduled-run-failure, or resource-usage-alert notifications individually
  from the bell icon's panel; a muted kind is never stored, not just hidden.
- **Export all saved pipelines** — download every saved pipeline's YAML in
  one zip from the Saved Pipelines section, distinct from exporting a
  single pipeline or the full state-store backup snapshot.
- **Per-step author notes** — attach optional free-text documentation to a
  pipeline step (or parallel group) right in the builder — purely
  descriptive, saved with the pipeline, and shown as a tooltip in the DAG
  view. Distinct from a run's own after-the-fact note: this documents the
  pipeline's *design*, not one particular execution of it.

## Artifact favorites/bulk-untag, quick-fill from last run, run-notes search, version branching, and notification cleanup

- **Favorite artifacts** — the existing module/pipeline favorites system
  now covers artifacts too; a starred artifact appears in the Favorites
  section with a one-click **View** shortcut.
- **Bulk remove a tag from selected artifacts** — the inverse of bulk-tag:
  select multiple files and strip one tag from all of them at once.
- **Quick-fill from a module's last run** — a **↺ Use last run's inputs**
  button fills a module's form with whatever values it actually ran with
  most recently, no named preset required ahead of time.
- **Search across run notes** — a dedicated search box finds past runs by
  keyword in your own notes on them, distinct from the existing search
  over step outputs/errors.
- **Branch a pipeline version into a new pipeline** — alongside restoring
  an archived version in place, "Branch as new…" saves it as a brand-new
  saved pipeline instead, leaving the current one untouched.
- **Clear read notifications** — a "Clear read" button in the bell icon's
  Alerts section deletes already-read alerts outright, keeping the list
  from growing unbounded (distinct from mark-all-read, which just flips
  the read flag).

## One-time scheduling, tag renaming, schema comparison, notification export, and recently viewed

- **One-time future-scheduled run** — a fourth schedule type, alongside
  interval/daily/weekly: pick "Once at a date/time" and it fires exactly
  once at that instant, then disables itself automatically.
- **Rename a tag across all artifacts at once** — a ✎ button next to each
  chip in the artifact tag directory renames that tag everywhere it's
  used in one action, merging into an existing tag rather than
  duplicating it if the target name is already present on some file.
- **Compare two artifacts' schemas** — select exactly two table-shaped
  files (CSV or ingested JSON) and a new **Compare schemas** button shows
  which columns are unique to each side and which shared columns
  disagree on inferred type.
- **Export notifications/alerts to CSV** — an **Export CSV** link in the
  bell icon's Alerts section downloads the full alert history, mirroring
  the existing audit-log CSV export.
- **Recently Viewed quick-access strip** — a new section tracks the last
  8 modules run, pipelines run, or artifacts opened, most-recent-first,
  entirely in `localStorage` — no server round trip, and it hides itself
  automatically once empty.

## Schedule fire notifications, favorites clearing, artifact CSV export, schedule duplication, and pipeline bulk-delete

- **Notify when a one-time schedule fires** — a `schedule_once_fired`
  alert appears in the bell icon once a "once" schedule's run actually
  succeeds, so a set-and-forget schedule doesn't require checking the
  Schedules list manually to know it ran.
- **Clear all favorites** — a **Clear all** button in the Favorites
  section heading empties every favorite in one action.
- **Export the artifacts list to CSV** — an **Export CSV** link next to
  Purge downloads every ingested file's name, size, tags, and modified
  time.
- **Duplicate a schedule** — a **Duplicate** button per schedule reopens
  the create-schedule form pre-filled with that schedule's settings, for
  editing before saving as a new one.
- **Bulk-delete saved pipelines** — checkbox-select multiple saved
  pipelines and remove them all at once with **Delete selected**.

## Raw artifact downloads, schedule bulk actions, memory export, notification mute-all, and version-to-version pipeline diff

- **Download an artifact's raw file** — a ⬇ button next to **View**
  downloads the original uploaded bytes, distinct from the in-page
  content viewer which shows extracted/derived content.
- **Bulk pause/resume/delete schedules** — checkbox-select multiple
  schedules and act on all of them at once.
- **Export agent memory to CSV** — an **Export CSV** link in the Agent
  Memory panel downloads every remembered key/value pair (secrets still
  redacted).
- **Mute-all/unmute-all notifications** — quick buttons in the bell
  icon's Preferences section flip every notification kind's mute state
  at once.
- **Version-to-version pipeline diff** — checkbox-select any two
  archived versions of a saved pipeline in its History panel to diff
  them against each other, not just against the current definition.

## Schedule select-all, notification search, bulk pipeline import, bulk artifact favoriting, and clearing Recently Viewed

- **Select-all for schedules** — a header checkbox selects (or clears)
  every schedule row at once before a bulk pause/resume/delete.
- **Search across notifications** — a search box in the bell icon's
  Alerts section filters alerts by keyword.
- **Bulk import pipelines from a zip** — an **Import zip…** picker
  imports every `.yaml` file inside a bundle at once, the counterpart to
  the existing **Export all** zip download.
- **Bulk favorite selected artifacts** — checkbox-select multiple
  artifacts and add them all to Favorites with one click.
- **Clear Recently Viewed** — a **Clear** button empties the Recently
  Viewed strip in one action.

## Pipeline select-all, audit-log search, notification bulk actions, run JSON export, and memory search

- **Select-all for saved pipelines** — a header checkbox selects (or
  clears) every saved pipeline before a bulk delete.
- **Search the audit log** — a search box above Recent Actions filters
  the administrative-action history by keyword.
- **Bulk mark-read/delete for notifications** — checkbox-select
  individual alerts and mark them read or delete them, read or unread,
  in one action.
- **Export a run's full detail as JSON** — a **⬇ Download JSON** link in
  the run detail drill-down downloads that run's complete steps,
  blackboard, and note.
- **Search agent memory** — a search box in the Agent Memory panel
  filters remembered key/value pairs by keyword.

## Notification select-all, artifact bulk delete, module stats CSV export, and schedule/template filters

- **Select-all for notification alerts** — a header checkbox in the bell
  icon's Alerts section selects (or clears) every alert at once before a
  bulk mark-read/delete.
- **Bulk delete selected artifacts** — checkbox-select multiple
  artifacts and remove them all in one click, the finer-grained
  counterpart to the existing age-based purge.
- **Export per-module performance stats to CSV** — a Module Performance
  Stats panel in Tools shows each module's run count/success
  rate/average duration, with an **Export CSV** download link.
- **Filter schedules by keyword** — a filter box above the Schedules
  list narrows visible rows by target name, tier, or kind.
- **Filter the pipeline template gallery** — a filter box above the
  Pipeline Templates gallery narrows visible cards by name,
  description, or module chain.

## Schedule CSV export, pipeline rename, bulk module enable/disable, and a problems filter

- **Export the schedule list to CSV** — an **Export CSV** link next to
  the Schedules heading downloads every schedule's target, cadence, and
  status.
- **Rename a saved pipeline in place** — a **Rename** button on each
  saved pipeline card changes its display name (and slug) without
  cloning it, migrating its tags and repointing any schedule that
  targets it by slug.
- **Bulk enable/disable modules** — checkbox-select modules across every
  tier and enable or disable them all in one click, mirroring the
  existing bulk pause/resume for schedules.
- **Bulk remove a tag from selected saved pipelines** — the pipeline
  counterpart to bulk-untagging artifacts: checkbox-select multiple
  saved pipelines and strip one tag from all of them at once.
- **"Show only disabled / tripped" module filter** — a toggle above the
  Modules directory narrows every tier down to just the modules that
  need attention, pairing naturally with bulk enable/disable.

## Bulk pipeline tagging, bulk breaker resets, an audit log reset, and version pruning

- **Bulk-add a tag to selected saved pipelines** — the counterpart to
  bulk-removing a tag: checkbox-select multiple saved pipelines and
  apply one tag to all of them at once.
- **Bulk reset circuit breakers** — checkbox-select modules across every
  tier and reset their circuit breakers in one click, pairing with the
  "show only disabled / tripped" filter to spot and clear problem
  modules together.
- **Clear the audit log** — a **Clear** button next to the Recent
  Actions heading empties the administrative-action history in one
  action, mirroring the existing clear-all controls for notifications
  and favorites.
- **Pipeline version history pruning** — a "Keep latest N" control in
  each pipeline's History panel deletes its older archived versions,
  keeping only the most recent ones.

## Bulk artifact download, bulk breaker threshold clearing, a used-by CSV, and bulk pipeline duplication

- **Bulk download selected artifacts as a zip** — checkbox-select
  multiple artifacts and download them all as a single zip file, the
  finer-grained counterpart to the pipeline library's **Export all**.
- **Bulk clear breaker threshold overrides** — checkbox-select modules
  and revert their circuit breaker threshold to the manifest default in
  one click, distinct from bulk-resetting the trip state itself.
- **Export the module "used by" reverse lookup to CSV** — an **Export
  used-by CSV** link above the Modules directory downloads which saved
  pipelines reference each module.
- **Bulk duplicate selected saved pipelines** — checkbox-select multiple
  saved pipelines and clone them all at once, each getting the same
  non-colliding "(copy)" name as the single-pipeline duplicate action.

## Age-based audit log purging, webhook testing, runtime rate limits, and memory import

- **Purge the audit log by age** — a "Purge older than" control next to
  the existing full **Clear** button trims old audit entries without
  wiping the whole log, mirroring the artifact/run-history purge pattern.
- **Send a test webhook** — a **▶ Send test** button on each saved
  pipeline card fires a real POST at its own webhook endpoint and shows
  the live run progress inline, so a webhook integration can be verified
  without an external tool.
- **Runtime-configurable rate limit** — the run-triggering rate limiter's
  max-requests/window can be tuned from the Environment & Config panel
  without a restart, mirroring the existing circuit breaker threshold
  override.
- **Import agent memory from a JSON file** — an **Import…** picker in the
  Agent Memory panel restores or seeds `{key: value}` pairs, the restore
  counterpart to the existing memory CSV export.

## Schedule favorites/labels, DAG SVG export, and run status filter chips

- **Favorite a schedule** — a ★ toggle on each schedule row adds it to the
  same Favorites strip used by modules, pipelines, and artifacts.
- **Free-text label per schedule** — an **Add label**/**Edit label** button
  on each schedule row attaches a short note (e.g. "Nightly metrics pull")
  so schedules against the same target are easy to tell apart at a glance;
  the schedule CSV export includes it too.
- **Download a pipeline's DAG graph as an SVG file** — a **Download SVG**
  button above the graph saves the rendered dependency diagram as a
  standalone file.
- **Run status filter chips** — All/Completed/Failed/Running chips above
  Recent Runs narrow the table to one status without a page reload.

## Schedule bulk favoriting/label clearing, memory JSON export, and artifact storage stats

- **Bulk favorite selected schedules** — checkbox-select multiple schedules
  and add them all to Favorites at once.
- **Bulk clear labels from selected schedules** — checkbox-select multiple
  schedules and blank out their labels in one click.
- **Export agent memory to JSON** — an **Export JSON** link next to the
  existing Export CSV button downloads memory in the same `{key: value}`
  shape the **Import…** picker expects, for a clean round trip.
- **Artifact storage stats** — the Artifacts panel heading shows the total
  file count and combined size of whatever's currently listed (respecting
  the active tag filter).

## Schedule JSON export/sorting, breaker CSV export, and pipeline JSON export

- **Export selected schedules as JSON** — checkbox-select multiple
  schedules and download just those as a JSON file, complementing the
  existing full-list CSV export.
- **Sort schedules** — a Next run / Name dropdown above the schedule list
  reorders it, alongside the existing keyword filter.
- **Export circuit breaker states to CSV** — an **Export breakers CSV**
  link near the Modules directory downloads every tracked module's breaker
  state.
- **Export a saved pipeline as JSON** — an **Export JSON** link on each
  pipeline card downloads its definition in JSON, alongside the existing
  YAML export.

## Notification/artifact bulk export, notification sort, and a tag CSV export

- **Bulk export selected notifications to CSV** — checkbox-select multiple
  alerts and download just those as a CSV.
- **Sort notifications** — a Newest first / Kind dropdown in the Alerts
  panel reorders the list.
- **Export artifact tag directory to CSV** — an **Export tags CSV** link
  above the Artifacts list downloads every tag and its usage count.
- **Bulk export selected artifacts' metadata to CSV** — checkbox-select
  multiple artifacts and download just their metadata (name/size/tags) as
  a CSV, distinct from the existing bulk zip-of-content download.

## Schedule run-now, artifact rename, and a bigger command palette

- **"Run now" for a schedule** — a **▶ Run now** button fires a schedule's
  target immediately, once, without touching its next scheduled run or
  cadence.
- **Rename an artifact file** — a **Rename** button renames the
  underlying file (distinct from the existing rename-a-tag feature),
  carrying its tags along to the new name.
- **A bigger command palette** — "Jump to Schedules/Agent Memory/Recent
  Runs/Ingestion" section shortcuts and a per-schedule jump command join
  the existing Run/Pipeline-jump/theme/density commands.

## Desktop alerts, automatic backups, and bulk schedule run-now

- **Desktop browser notifications for critical alerts** — an opt-in checkbox
  in the notifications panel requests browser notification permission and,
  once granted, pops a native desktop notification whenever a `breaker_tripped`
  or `schedule_failed` alert lands, even if the dashboard tab isn't focused.
- **Automatic periodic backup snapshot to disk** — a background timer
  (mirroring the existing scheduler's architecture) writes a full state
  snapshot to `backups/` on a configurable interval, pruning down to a
  configurable number of most-recent files. A new Environment & Config panel
  row lets you enable/disable it, set the interval and retention count, and
  trigger an immediate backup on demand.
- **Bulk "Run now" for selected schedules** — the existing per-schedule
  **▶ Run now** action is now also available as a bulk button across a
  checkbox selection, firing every selected schedule's target immediately
  (independently — one failure doesn't block the rest) without touching any
  of their next-run cadences.

## Backup snapshot listing/restore, backup-failure alerts, and bulk schedule duplication

- **List and restore from an automatic backup snapshot** — the automatic
  backup panel now lists every timestamped snapshot sitting in `backups/`
  (size and modified time included), each with its own **Restore** button
  that restores directly from that file on disk (the same additive merge
  as the existing upload-based restore) without having to find and
  re-upload it by hand.
- **Notify on automatic backup failure** — if a scheduled backup write
  ever fails (disk full, permissions, etc.), it now raises a
  `backup_failed` alert in the notification center instead of silently
  vanishing into a background thread, and the background timer itself
  keeps running so the next scheduled attempt still retries.
- **Bulk duplicate selected schedules** — a **⧉ Duplicate selected**
  button joins the other bulk schedule actions, actually cloning each
  checkbox-selected schedule into a brand new schedule row with the same
  target/inputs/cadence (distinct from the existing per-row **Duplicate**
  button, which only pre-fills the create-schedule form for manual
  review). An interval/daily/weekly clone gets its own freshly computed
  next-run time; a one-time clone carries its `next_run_at` over as-is.

## One-click pipeline scheduling, snapshot cleanup, artifact notes, and a JSON run export

- **One-click "Schedule this pipeline" shortcut** — a **🕐 Schedule** button
  on every saved pipeline card pre-fills the create-schedule form (kind and
  target already set) and scrolls to it, the same shortcut the existing
  per-schedule "Duplicate" button already offered for schedules.
- **Delete an individual automatic backup snapshot** — each row in the
  automatic-backup snapshot list (added last batch) now has its own
  **Delete** button, for discarding a specific snapshot early without
  waiting for the configured keep-count to prune it.
- **Free-text note field for artifacts** — an **Add/Edit note** button per
  artifact stores a single free-text comment (distinct from tags, which
  are short structured keywords), mirroring the existing per-schedule
  label. The note travels with the file if it's later renamed.
- **Export full run history as JSON** — a new **Export JSON** button joins
  the existing CSV/XLSX run-history exports, downloading the same summary
  list as a `.json` file (distinct from the existing single-run JSON
  export, which downloads one run's full nested detail).

## Backup snapshot CSV/zip exports, bulk artifact import, and a schedule preview

- **Export the automatic backup snapshot list to CSV** — an **Export
  snapshots CSV** link joins the JSON listing added last batch, downloading
  filename/size/modified-time for every snapshot, mirroring every other
  list-to-CSV export in this app.
- **Bulk import artifacts from a zip bundle** — an **Import zip…** control
  in the ingestion panel ingests every CSV/JSON/XLSX/PDF file inside a zip
  at once through the same per-type ingestion path (and content-hash
  dedupe) as a direct single-file upload, mirroring the existing bulk
  pipeline zip import.
- **Download all automatic backup snapshots as a zip** — a **Download all
  as zip** link bundles every snapshot in `backups/` into a single
  download, for offline safekeeping of the whole automatic-backup history
  at once.
- **Next-occurrences preview for daily/weekly schedules** — a **Preview
  next** button on any daily or weekly schedule shows its next 5 fire
  times inline, computed by chaining the existing next-run-time helpers
  forward.

## Backup snapshot age-purge, snapshot comparison, and a command palette jump

- **Age-based purge for automatic backup snapshots** — a "Purge snapshots
  older than N hours" control joins the panel, deleting snapshots by the
  timestamp encoded in their own filename, as a companion to the existing
  keep-count-based pruning (always keep exactly N most recent) for a
  manual "clear out anything past this age" instead.
- **Compare two automatic backup snapshots** — checkbox-select exactly two
  snapshots and click **Compare selected** to see the delta in each
  record type (runs/steps/schedules/ingested files/pipelines/memory)
  between them, mirroring the existing pipeline-version diff and
  artifact-schema compare features.
- **Command palette auto-backup jump** — "Jump to Environment & Config
  (auto backup)" joins the palette's existing section shortcuts.

## Tech stack

- **Language:** Python 3.11+ — first-class async/sync support and native SDKs
  (Anthropic, LangChain, CrewAI, Autogen) for the agent tier.
- **State:** SQLite today (`engine/state_store.py`), zero setup, file-based, good
  enough for a single-process synchronous engine. The store is a small enough
  surface to swap for Postgres or add a Redis-backed context cache later without
  touching the orchestrator.
- **Config:** YAML manifests per module — human-readable, diffable, and how the
  registry achieves "drop a file in, don't touch the core" extensibility. The
  same manifest now doubles as the dashboard's form schema (`inputs:`).
- **Dashboard:** FastAPI + a single static HTML/CSS/JS page (Tailwind-free, no
  build step) — a thin REST layer over the same `Orchestrator`/`StateStore` the
  CLI uses, so there's exactly one engine and one history behind both.
- **Pipeline builder:** no new tech — user-built pipelines are just another YAML
  manifest folder (`pipelines/`), validated and executed through the same
  `Orchestrator`/`StepSpec` machinery as everything else.
- **Ingestion:** stdlib `csv` for CSV parsing, `pypdf` for PDF text extraction.
- **Exports:** `openpyxl` for the XLSX run-history export (lazily imported);
  CSV export is stdlib.
- **Outbound HTTP:** `httpx` (already a FastAPI test dependency) powers the
  `http_request` automation module — no new dependency for it.
- **Linting:** `ruff` invoked as a subprocess, scoped to exactly the file a
  module's own manifest `entrypoint` resolves to.
- **Performance ticker:** `psutil`, reading only this process's own resource
  usage — not a system-wide monitor.
- **CI:** GitHub Actions running `pytest` (engine + API tests) and a CLI smoke
  test on every push.

Deliberately **not** included yet: Celery, Redis, LangGraph. The blueprint calls
for a synchronous, dependable core first — see
[`docs/ARCHITECTURE.md`](docs/ARCHITECTURE.md#roadmap) for when and why each of
those gets added.
