import time
from concurrent.futures import ThreadPoolExecutor
from concurrent.futures import TimeoutError as FutureTimeoutError
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Callable, Optional, Sequence, Union

from .base import BaseModule
from .context import ExecutionContext, StepRecord
from .redaction import redact_secrets
from .state_store import StateStore

EventCallback = Callable[[dict], None]
SeedFn = Callable[[ExecutionContext], dict]

# Shared across every timed run — module.run() calls that exceed their step's
# timeout are abandoned (not killed; Python can't forcibly stop a thread), so
# this pool's size just bounds how many abandoned calls can pile up at once.
# Sized with some headroom over a single ParallelGroup's typical branch count,
# since a group submits every branch to this same pool at once.
_EXECUTOR = ThreadPoolExecutor(max_workers=16, thread_name_prefix="module-run")


@dataclass
class StepSpec:
    """One step in a pipeline: a module, plus values to seed into the shared context
    right before it runs.

    `seed` is a function of the *current* context rather than a plain dict, because
    by the time this step runs, every earlier step's output is already sitting in
    `context.variables` — this is how a no-code pipeline builder wires "Step 1's
    output X" into "Step 3's input Y" even when the two field names don't match:
    `seed = lambda ctx: {"Y": ctx.get("X")}`.

    `timeout_seconds`, if set, bounds how long this step's `module.run()` may run
    before it's treated as a failure (a `TimeoutError`, handled the same as any
    other exception the module raises).

    `condition`, if set, is evaluated against the context right before the step
    would seed and run; a falsy result skips the step entirely (no seed, no
    module.run(), a `step_skipped` event instead of started/completed). Skipping
    is not a failure — the run continues to the next step either way.
    `condition_label` is a human-readable rendering of the condition, carried on
    the skip event so dashboards can say *why* a step was skipped.

    `max_retries`, if greater than 0, retries a failing `module.run()` (or a
    timeout) up to that many additional times before counting it as a real
    `step_failed`. Each retry waits `retry_backoff_seconds * 2**attempt`
    (exponential backoff; attempt 0 for the first retry) and emits a
    `step_retrying` event first, so a dashboard can show "retry 1/3" instead
    of the step silently going quiet. A retried attempt reuses the same
    seeded context — `seed()` runs once, before the first attempt, not again
    per retry.
    """

    module: BaseModule
    seed: SeedFn = field(default=lambda ctx: {})
    timeout_seconds: Optional[float] = None
    condition: Optional[Callable[[ExecutionContext], bool]] = None
    condition_label: str = ""
    max_retries: int = 0
    retry_backoff_seconds: float = 0.0


@dataclass
class ParallelGroup:
    """A set of steps to run concurrently in one pipeline slot.

    The orchestrator waits for every branch to finish before continuing to
    the next top-level step; each branch still gets its own StepRecord and
    step_started/step_completed/step_failed events (tagged `parallel: True`
    plus a `branch_index`), so nothing about a single step's own behavior
    needs to know it ran inside a group.

    Branches must not depend on each other — there's no ordering guarantee
    among them. Each branch's `seed(context)` runs sequentially, before any
    branch's `module.run()` starts, against context as it stood *before* the
    group began — never a sibling branch's output. If two branches write the
    same output key, whichever merges back into context last (in listed
    order) wins; that's an inherent risk of "independent" branches actually
    not being independent, not a bug to guard against here.
    """

    steps: Sequence[Union[BaseModule, StepSpec]]
    name: str = "parallel_group"


def _as_step(item: Union[BaseModule, StepSpec]) -> StepSpec:
    return item if isinstance(item, StepSpec) else StepSpec(module=item)


def _await_result(future, module_name: str, timeout_seconds: Optional[float]):
    try:
        return future.result(timeout=timeout_seconds)
    except FutureTimeoutError:
        raise TimeoutError(
            f"'{module_name}' did not finish within {timeout_seconds}s. (Python cannot "
            "forcibly stop a running thread, so it may still be executing in the "
            "background — this only stops the orchestrator from waiting on it.)"
        ) from None


def _condition_allows(step: StepSpec, context: ExecutionContext) -> bool:
    """True if the step should run. A condition that *raises* counts as False —
    skipping a step is the safe failure mode for a broken condition, and the
    declarative evaluator (engine.conditions) never raises anyway."""
    if step.condition is None:
        return True
    try:
        return bool(step.condition(context))
    except Exception:
        return False


def _run_module(module: BaseModule, context: ExecutionContext, timeout_seconds: Optional[float]) -> dict:
    if timeout_seconds is None:
        return module.run(context)
    future = _EXECUTOR.submit(module.run, context)
    return _await_result(future, module.name, timeout_seconds)


def _run_with_retries(step: StepSpec, context: ExecutionContext, emit: EventCallback, event_extra: dict) -> dict:
    """Run `step.module` via `_run_module`, retrying on any exception up to
    `step.max_retries` times with exponential backoff. Re-raises the last
    exception once retries are exhausted (or immediately if max_retries is 0,
    identical to calling `_run_module` directly)."""
    attempt = 0
    while True:
        try:
            return _run_module(step.module, context, step.timeout_seconds)
        except Exception as exc:
            if attempt >= step.max_retries:
                raise
            delay = step.retry_backoff_seconds * (2**attempt)
            emit(
                {
                    "kind": "step_retrying",
                    "attempt": attempt + 1,
                    "max_retries": step.max_retries,
                    "delay_seconds": delay,
                    "error": str(exc),
                    **event_extra,
                }
            )
            if delay > 0:
                time.sleep(delay)
            attempt += 1


def _await_branch_with_retries(
    branch: StepSpec, first_future, context: ExecutionContext, emit: EventCallback, event_extra: dict
):
    """Await a parallel branch's already-submitted future, retrying by
    resubmitting `branch.module.run` to the shared executor on failure (up to
    `branch.max_retries` times, same exponential backoff as a sequential
    step). Retrying blocks only this branch's slot in the collection loop —
    every other branch's future keeps running independently in the
    background regardless of how long this branch's retries take."""
    future = first_future
    attempt = 0
    while True:
        try:
            return _await_result(future, branch.module.name, branch.timeout_seconds)
        except Exception as exc:
            if attempt >= branch.max_retries:
                raise
            delay = branch.retry_backoff_seconds * (2**attempt)
            emit(
                {
                    "kind": "step_retrying",
                    "attempt": attempt + 1,
                    "max_retries": branch.max_retries,
                    "delay_seconds": delay,
                    "error": str(exc),
                    **event_extra,
                }
            )
            if delay > 0:
                time.sleep(delay)
            attempt += 1
            future = _EXECUTOR.submit(branch.module.run, context)


class Orchestrator:
    """Runs a fixed sequence of steps synchronously, threading shared context between them.

    Each module's return value is merged into the same ExecutionContext before the
    next module runs, so a rule-based automation's output becomes an AI workflow's
    input, and that workflow's output becomes an agent's input, in one dependable pass.
    Steps may be bare `BaseModule` instances (the common case — nothing extra to
    configure), `StepSpec`s carrying per-step static/mapped input values, for
    pipelines assembled from steps that don't already share context key names,
    or `ParallelGroup`s of either, run concurrently in one pipeline slot.

    An optional `on_event` callback passed to `run()` receives a live stream of
    step_started / step_completed / step_failed / run_completed / run_failed
    events (plus whatever a module itself `emit()`s), so a caller — e.g. the
    dashboard's SSE endpoint — can watch a run happen instead of only seeing its
    final result.
    """

    def __init__(
        self,
        pipeline: Sequence[Union[BaseModule, StepSpec, ParallelGroup]],
        state_store: Optional[StateStore] = None,
        stop_on_error: bool = True,
    ):
        self.pipeline: list[Union[StepSpec, ParallelGroup]] = [
            item if isinstance(item, ParallelGroup) else _as_step(item) for item in pipeline
        ]
        self.state_store = state_store
        self.stop_on_error = stop_on_error

    def run(self, initial_context: Optional[dict] = None, on_event: Optional[EventCallback] = None) -> ExecutionContext:
        def emit(event: dict) -> None:
            if on_event is not None:
                on_event(event)

        context = ExecutionContext(initial_context, on_event=emit, state_store=self.state_store)
        run_id = self.state_store.start_run() if self.state_store else None
        had_failure = False

        for index, step in enumerate(self.pipeline):
            if isinstance(step, ParallelGroup):
                group_failed = self._run_parallel_group(step, index, context, run_id, emit)
                if group_failed:
                    had_failure = True
                    if self.stop_on_error:
                        if self.state_store:
                            self.state_store.finish_run(run_id, "failed")
                        error = f"Parallel group '{step.name}' had a failing branch."
                        emit({"kind": "run_failed", "error": error, "context": redact_secrets(context.variables)})
                        raise RuntimeError(error)
                continue

            module = step.module
            if not _condition_allows(step, context):
                emit(
                    {
                        "kind": "step_skipped",
                        "index": index,
                        "tier": module.tier.value,
                        "name": module.name,
                        "condition": step.condition_label,
                    }
                )
                continue
            context.update(step.seed(context))
            # A snapshot of everything this module can see when it runs — not
            # just its own StepSpec.seed() output. A module fed via the run's
            # `initial_context` (the standalone "run this module" and
            # scheduled-module paths seed that way, not through StepSpec.seed)
            # would otherwise have no recorded inputs at all to replay later.
            resolved_inputs = dict(context.variables)
            context.active_module = (module.tier.value, module.name)
            emit({"kind": "step_started", "index": index, "tier": module.tier.value, "name": module.name})
            started_at = datetime.now(timezone.utc)

            try:
                output = (
                    _run_with_retries(step, context, emit, {"index": index, "tier": module.tier.value, "name": module.name})
                    or {}
                )
            except Exception as exc:
                finished_at = datetime.now(timezone.utc)
                record = StepRecord(
                    module.name, module.tier.value, started_at, finished_at, False, {}, str(exc),
                    inputs=redact_secrets(resolved_inputs),
                )
                context.record(record)
                if self.state_store:
                    self.state_store.log_step(run_id, record)
                emit(
                    {
                        "kind": "step_failed",
                        "index": index,
                        "tier": module.tier.value,
                        "name": module.name,
                        "error": str(exc),
                        "duration_ms": round((finished_at - started_at).total_seconds() * 1000, 1),
                    }
                )
                had_failure = True

                if self.stop_on_error:
                    if self.state_store:
                        self.state_store.finish_run(run_id, "failed")
                    emit({"kind": "run_failed", "error": str(exc), "context": redact_secrets(context.variables)})
                    raise
                continue

            finished_at = datetime.now(timezone.utc)
            context.update(output)  # the real, unredacted output — later steps may legitimately need it
            safe_output = redact_secrets(output)
            record = StepRecord(
                module.name, module.tier.value, started_at, finished_at, True, safe_output,
                inputs=redact_secrets(resolved_inputs),
            )
            context.record(record)
            if self.state_store:
                self.state_store.log_step(run_id, record)
            emit(
                {
                    "kind": "step_completed",
                    "index": index,
                    "tier": module.tier.value,
                    "name": module.name,
                    "output": safe_output,
                    "duration_ms": round((finished_at - started_at).total_seconds() * 1000, 1),
                }
            )

        if self.state_store:
            self.state_store.finish_run(run_id, "failed" if had_failure else "completed")
        emit(
            {
                "kind": "run_failed" if had_failure else "run_completed",
                "context": redact_secrets(context.variables),
            }
        )
        return context

    def _run_parallel_group(
        self,
        group: ParallelGroup,
        index: int,
        context: ExecutionContext,
        run_id: Optional[int],
        emit: EventCallback,
    ) -> bool:
        """Run every branch in `group` concurrently, returning True if any
        branch failed. Seeding happens sequentially (a plain dict merge, no
        need for concurrency) before any branch's module.run() is submitted,
        so no branch ever sees a sibling's output during setup."""
        all_branches = [_as_step(b) for b in group.steps]
        emit({"kind": "group_started", "index": index, "name": group.name, "branch_count": len(all_branches)})

        # Conditions are evaluated against context as it stood before the
        # group began (same view every branch's seed gets); a skipped branch
        # neither seeds nor runs.
        branches = []
        for branch_index, branch in enumerate(all_branches):
            if not _condition_allows(branch, context):
                emit(
                    {
                        "kind": "step_skipped",
                        "index": index,
                        "branch_index": branch_index,
                        "parallel": True,
                        "tier": branch.module.tier.value,
                        "name": branch.module.name,
                        "condition": branch.condition_label,
                    }
                )
                continue
            branches.append((branch_index, branch))

        # A snapshot of everything each branch's module can see when it runs —
        # not just its own seed() output (see the equivalent comment in the
        # main run() loop for why: some seeding paths merge straight into
        # context.variables rather than through a StepSpec.seed).
        resolved_inputs_by_branch = {}
        for branch_index, branch in branches:
            context.update(branch.seed(context))
            resolved_inputs_by_branch[branch_index] = dict(context.variables)

        started_ats = [datetime.now(timezone.utc) for _ in branches]
        futures = [_EXECUTOR.submit(branch.module.run, context) for _, branch in branches]

        any_failed = False
        for (branch_index, branch), future, started_at in zip(branches, futures, started_ats):
            module = branch.module
            emit(
                {
                    "kind": "step_started",
                    "index": index,
                    "branch_index": branch_index,
                    "parallel": True,
                    "tier": module.tier.value,
                    "name": module.name,
                }
            )

            try:
                output = (
                    _await_branch_with_retries(
                        branch,
                        future,
                        context,
                        emit,
                        {"index": index, "branch_index": branch_index, "parallel": True, "tier": module.tier.value, "name": module.name},
                    )
                    or {}
                )
            except Exception as exc:
                finished_at = datetime.now(timezone.utc)
                record = StepRecord(
                    module.name, module.tier.value, started_at, finished_at, False, {}, str(exc),
                    inputs=redact_secrets(resolved_inputs_by_branch[branch_index]),
                )
                context.record(record)
                if self.state_store:
                    self.state_store.log_step(run_id, record)
                emit(
                    {
                        "kind": "step_failed",
                        "index": index,
                        "branch_index": branch_index,
                        "parallel": True,
                        "tier": module.tier.value,
                        "name": module.name,
                        "error": str(exc),
                        "duration_ms": round((finished_at - started_at).total_seconds() * 1000, 1),
                    }
                )
                any_failed = True
                continue

            finished_at = datetime.now(timezone.utc)
            context.update(output)
            safe_output = redact_secrets(output)
            record = StepRecord(
                module.name, module.tier.value, started_at, finished_at, True, safe_output,
                inputs=redact_secrets(resolved_inputs_by_branch[branch_index]),
            )
            context.record(record)
            if self.state_store:
                self.state_store.log_step(run_id, record)
            emit(
                {
                    "kind": "step_completed",
                    "index": index,
                    "branch_index": branch_index,
                    "parallel": True,
                    "tier": module.tier.value,
                    "name": module.name,
                    "output": safe_output,
                    "duration_ms": round((finished_at - started_at).total_seconds() * 1000, 1),
                }
            )

        emit({"kind": "group_completed", "index": index, "name": group.name, "had_failure": any_failed})
        return any_failed
