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
  `branch_index`), then merges their outputs back before the next step. Not
  yet exposed in the no-code visual builder (a genuinely separate UI
  surface) — construct one directly in Python: `Orchestrator([Double(),
  ParallelGroup(steps=[StepA(), StepB()]), AddOne()])`. Branches must be
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
- **Ingestion:** stdlib `csv` for CSV parsing, `pypdf` for PDF text extraction —
  the only two new runtime dependencies added since the initial scaffold.
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
