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
│   └── static/                #   index.html / styles.css / app.js — the dashboard itself
├── automations/              # Tier 1 — drop in a <name>.py + <name>.yaml pair
│   ├── example_automation.py
│   └── example_automation.yaml
├── workflows/                 # Tier 2 — same pattern
│   ├── example_workflow.py
│   └── example_workflow.yaml
├── agents/                     # Tier 3 — same pattern
│   ├── example_agent.py
│   └── example_agent.yaml
├── tests/
│   ├── test_orchestrator.py
│   └── test_webapp.py
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
   ```
4. Run `python cli.py list` (or refresh the dashboard) to confirm it's discovered.

No core engine code changes are required — the registry scans each tier folder for
manifests at pipeline-build time, and the dashboard reads the same `inputs` schema
to render the card's form.

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
- **CI:** GitHub Actions running `pytest` (engine + API tests) and a CLI smoke
  test on every push.

Deliberately **not** included yet: Celery, Redis, LangGraph. The blueprint calls
for a synchronous, dependable core first — see
[`docs/ARCHITECTURE.md`](docs/ARCHITECTURE.md#roadmap) for when and why each of
those gets added.
