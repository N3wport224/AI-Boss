from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Callable, Optional, Sequence, Union

from .base import BaseModule
from .context import ExecutionContext, StepRecord
from .state_store import StateStore

EventCallback = Callable[[dict], None]
SeedFn = Callable[[ExecutionContext], dict]


@dataclass
class StepSpec:
    """One step in a pipeline: a module, plus values to seed into the shared context
    right before it runs.

    `seed` is a function of the *current* context rather than a plain dict, because
    by the time this step runs, every earlier step's output is already sitting in
    `context.variables` — this is how a no-code pipeline builder wires "Step 1's
    output X" into "Step 3's input Y" even when the two field names don't match:
    `seed = lambda ctx: {"Y": ctx.get("X")}`.
    """

    module: BaseModule
    seed: SeedFn = field(default=lambda ctx: {})


def _as_step(item: Union[BaseModule, StepSpec]) -> StepSpec:
    return item if isinstance(item, StepSpec) else StepSpec(module=item)


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
                output = module.run(context) or {}
            except Exception as exc:
                record = StepRecord(
                    module.name, module.tier.value, started_at, datetime.now(timezone.utc), False, {}, str(exc)
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
                    }
                )
                had_failure = True

                if self.stop_on_error:
                    if self.state_store:
                        self.state_store.finish_run(run_id, "failed")
                    emit({"kind": "run_failed", "error": str(exc), "context": context.variables})
                    raise
                continue

            context.update(output)
            record = StepRecord(
                module.name, module.tier.value, started_at, datetime.now(timezone.utc), True, output
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
                    "output": output,
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
