from concurrent.futures import ThreadPoolExecutor
from concurrent.futures import TimeoutError as FutureTimeoutError
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Callable, Optional, Sequence, Union

from .base import BaseModule
from .context import ExecutionContext, StepRecord
from .state_store import StateStore

EventCallback = Callable[[dict], None]
SeedFn = Callable[[ExecutionContext], dict]

# Shared across every timed run — module.run() calls that exceed their step's
# timeout are abandoned (not killed; Python can't forcibly stop a thread), so
# this pool's size just bounds how many abandoned calls can pile up at once.
_EXECUTOR = ThreadPoolExecutor(max_workers=8, thread_name_prefix="module-run")


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
    """

    module: BaseModule
    seed: SeedFn = field(default=lambda ctx: {})
    timeout_seconds: Optional[float] = None


def _as_step(item: Union[BaseModule, StepSpec]) -> StepSpec:
    return item if isinstance(item, StepSpec) else StepSpec(module=item)


def _run_module(module: BaseModule, context: ExecutionContext, timeout_seconds: Optional[float]) -> dict:
    if timeout_seconds is None:
        return module.run(context)

    future = _EXECUTOR.submit(module.run, context)
    try:
        return future.result(timeout=timeout_seconds)
    except FutureTimeoutError:
        raise TimeoutError(
            f"'{module.name}' did not finish within {timeout_seconds}s. (Python cannot "
            "forcibly stop a running thread, so it may still be executing in the "
            "background — this only stops the orchestrator from waiting on it.)"
        ) from None


class Orchestrator:
    """Runs a fixed sequence of steps synchronously, threading shared context between them.

    Each module's return value is merged into the same ExecutionContext before the
    next module runs, so a rule-based automation's output becomes an AI workflow's
    input, and that workflow's output becomes an agent's input, in one dependable pass.
    Steps may be bare `BaseModule` instances (the common case — nothing extra to
    configure) or `StepSpec`s carrying per-step static/mapped input values, for
    pipelines assembled from steps that don't already share context key names.

    An optional `on_event` callback passed to `run()` receives a live stream of
    step_started / step_completed / step_failed / run_completed / run_failed
    events (plus whatever a module itself `emit()`s), so a caller — e.g. the
    dashboard's SSE endpoint — can watch a run happen instead of only seeing its
    final result.
    """

    def __init__(
        self,
        pipeline: Sequence[Union[BaseModule, StepSpec]],
        state_store: Optional[StateStore] = None,
        stop_on_error: bool = True,
    ):
        self.pipeline: list[StepSpec] = [_as_step(item) for item in pipeline]
        self.state_store = state_store
        self.stop_on_error = stop_on_error

    def run(self, initial_context: Optional[dict] = None, on_event: Optional[EventCallback] = None) -> ExecutionContext:
        def emit(event: dict) -> None:
            if on_event is not None:
                on_event(event)

        context = ExecutionContext(initial_context, on_event=emit)
        run_id = self.state_store.start_run() if self.state_store else None
        had_failure = False

        for index, step in enumerate(self.pipeline):
            module = step.module
            context.update(step.seed(context))
            context.active_module = (module.tier.value, module.name)
            emit({"kind": "step_started", "index": index, "tier": module.tier.value, "name": module.name})
            started_at = datetime.now(timezone.utc)

            try:
                output = _run_module(module, context, step.timeout_seconds) or {}
            except Exception as exc:
                finished_at = datetime.now(timezone.utc)
                record = StepRecord(module.name, module.tier.value, started_at, finished_at, False, {}, str(exc))
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
                    emit({"kind": "run_failed", "error": str(exc), "context": context.variables})
                    raise
                continue

            finished_at = datetime.now(timezone.utc)
            context.update(output)
            record = StepRecord(module.name, module.tier.value, started_at, finished_at, True, output)
            context.record(record)
            if self.state_store:
                self.state_store.log_step(run_id, record)
            emit(
                {
                    "kind": "step_completed",
                    "index": index,
                    "tier": module.tier.value,
                    "name": module.name,
                    "output": output,
                    "duration_ms": round((finished_at - started_at).total_seconds() * 1000, 1),
                }
            )

        if self.state_store:
            self.state_store.finish_run(run_id, "failed" if had_failure else "completed")
        emit(
            {
                "kind": "run_failed" if had_failure else "run_completed",
                "context": context.variables,
            }
        )
        return context
