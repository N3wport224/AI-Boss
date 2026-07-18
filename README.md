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
│   ├── context.py           #   ExecutionContext: the shared context window
│   ├── orchestrator.py      #   Sequential runner that threads context between modules
│   ├── registry.py          #   Discovers modules + reads manifests (incl. input schemas)
│   └── state_store.py       #   SQLite-backed run/step history
├── webapp/                  # Visual control center (FastAPI + vanilla JS, no build step)
│   ├── main.py               #   REST API: list modules, run one, run the pipeline, history
│   ├── pipelines.py           #   Storage/validation for user-built pipelines
│   ├── events.py               #   SSE event bus for live run streaming
│   ├── health.py                #   Startup diagnostics (manifests, entrypoints, state store)
│   ├── ingestion.py              #   CSV/PDF upload, hash-dedupe, artifact storage/purge
│   ├── watcher.py                 #   Polling-based folder watcher (no watchdog dependency)
│   ├── linting.py                  #   Read-only per-module source + ruff lint view
│   └── static/                      #   index.html / styles.css / app.js — the dashboard itself
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
├── watched_input/                     # Drop a .csv/.pdf here for auto-ingestion — gitignored
├── tests/
│   ├── test_orchestrator.py
│   ├── test_webapp.py
│   ├── test_pipelines.py
│   ├── test_diagnostics.py
│   ├── test_ingestion.py
│   ├── test_templating.py
│   ├── test_watcher.py
│   └── test_linting.py
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
   Step 3's `notify_slack` field.
3. **Save & Launch** does exactly what it says in one click: it writes the
   pipeline to `pipelines/<slug>.yaml` (the same manifest convention as every
   other module folder — check it into git like anything else here) and
   immediately runs it through the same live tracker/thought-stream/toast UI
   as any other run.
4. Saved pipelines get their own card in a **Saved Pipelines** section with a
   Run button, so you don't have to rebuild them each time.

A mapped field is resolved at the moment that step runs, by reading whatever
context key the source step's declared output landed under — so it works even
when the two modules' field names don't already match (`engine/orchestrator.py`'s
`StepSpec.seed`). A saved pipeline is validated on save: every step's module
must exist, and every mapping must point at an *earlier* step's *declared*
output, or you get a clear error back instead of a silent bad reference.

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

- **Upload a CSV or PDF** in the dashboard's **Data Ingestion** section (drag
  a file onto the drop zone, or use "Choose file"). A CSV becomes structured
  JSON records; a PDF gets its text extracted. Files are hashed — dropping the
  exact same file twice is reported as a duplicate and skipped, not
  reprocessed. Converted artifacts land in `artifacts/` with a small table and
  a **Purge older than N hours** control (never touches `orchestrator.db` or
  `pipelines/`, no matter what age you set).
- **Folder watcher** — drop a `.csv`/`.pdf` straight into `watched_input/`
  (no browser needed at all) and it's ingested within a couple seconds by a
  lightweight polling loop (`webapp/watcher.py`, no `watchdog` dependency).
  The dashboard's Folder Watcher feed shows what it picked up, live.
- **Template fields** — a manifest input can declare `type: template` to get a
  textarea that supports `{variable}` interpolation against whatever's
  currently in the shared context (a sibling input on the same card, or an
  earlier pipeline step's output) — resolved right before that step runs, so
  the module itself never has to know templating exists. Click one of the
  suggested `{name}` chips under the textarea to insert it at the cursor. See
  `churn_response_agent`'s `custom_note` field for a working example.
- **View source + lint** — click **&lt;/&gt;** on any module card to see its
  actual `.py` source and a live `ruff check` result (green "no issues" or a
  list of line/column/rule findings), read-only, scoped to exactly that
  module's own file — never an arbitrary path.

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
- **CI:** GitHub Actions running `pytest` (engine + API tests) and a CLI smoke
  test on every push.

Deliberately **not** included yet: Celery, Redis, LangGraph. The blueprint calls
for a synchronous, dependable core first — see
[`docs/ARCHITECTURE.md`](docs/ARCHITECTURE.md#roadmap) for when and why each of
those gets added.
