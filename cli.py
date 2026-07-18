#!/usr/bin/env python3
"""Unified control layer for the orchestration engine: run the pipeline, list modules,
or inspect execution history. This is the "dashboard" the blueprint calls for, as a CLI."""
import argparse
import sys
from pathlib import Path

from engine import Orchestrator, StateStore, discover

ROOT = Path(__file__).parent
TIER_DIRS = {
    "automation": ROOT / "automations",
    "workflow": ROOT / "workflows",
    "agent": ROOT / "agents",
}


def build_pipeline():
    """Assemble the full tier-1 -> tier-2 -> tier-3 pipeline from whatever is on disk."""
    pipeline = []
    for tier in ("automation", "workflow", "agent"):
        pipeline.extend(discover(TIER_DIRS[tier]))
    return pipeline


def cmd_list(_args):
    for module in build_pipeline():
        print(f"[{module.tier.value:9}] {module.name:25} {module.description}")


def cmd_run(args):
    store = StateStore(args.db)
    orchestrator = Orchestrator(
        build_pipeline(), state_store=store, stop_on_error=not args.continue_on_error
    )
    context = orchestrator.run()

    print("Final shared context:")
    for key, value in context.variables.items():
        print(f"  {key}: {value}")

    print("\nExecution history:")
    for step in context.history:
        status = "OK" if step.success else f"FAILED ({step.error})"
        print(f"  [{step.tier:9}] {step.name:25} {status}")


def cmd_status(args):
    store = StateStore(args.db)
    for run in store.recent_runs(args.limit):
        print(
            f"run {run['id']:>3} | {run['status']:10} | "
            f"started {run['started_at']} | finished {run['finished_at']}"
        )
        for step in store.steps_for_run(run["id"]):
            mark = "OK" if step["success"] else "FAIL"
            print(f"    - [{step['tier']:9}] {step['name']:25} {mark}")


def main():
    parser = argparse.ArgumentParser(description="AI-Boss orchestration engine")
    parser.add_argument("--db", default="orchestrator.db", help="Path to the SQLite state store")
    sub = parser.add_subparsers(dest="command", required=True)

    sub.add_parser(
        "list", help="List all discovered automations, workflows, and agents"
    ).set_defaults(func=cmd_list)

    run_parser = sub.add_parser("run", help="Run the full pipeline once, synchronously")
    run_parser.add_argument(
        "--continue-on-error", action="store_true", help="Keep running remaining steps after a failure"
    )
    run_parser.set_defaults(func=cmd_run)

    status_parser = sub.add_parser("status", help="Show recent run history from the state store")
    status_parser.add_argument("--limit", type=int, default=10)
    status_parser.set_defaults(func=cmd_status)

    args = parser.parse_args()
    args.func(args)


if __name__ == "__main__":
    sys.exit(main())
