from datetime import datetime, timezone
from typing import Callable, Optional, Sequence

from .base import BaseModule
from .context import ExecutionContext, StepRecord
from .state_store import StateStore

EventCallback = Callable[[dict], None]


class Orchestrator:
    """Runs a fixed sequence of modules synchronously, threading shared context between them.

    Each module's return value is merged into the same ExecutionContext before the
    next module runs, so a rule-based automation's output becomes an AI workflow's
    input, and that workflow's output becomes an agent's input, in one dependable pass.

    An optional `on_event` callback passed to `run()` receives a live stream of
    step_started / step_completed / step_failed / run_completed / run_failed
    events (plus whatever a module itself `emit()`s), so a caller — e.g. the
    dashboard's SSE endpoint — can watch a run happen instead of only seeing its
    final result.
    """

    def __init__(
        self,
        pipeline: Sequence[BaseModule],
        state_store: Optional[StateStore] = None,
        stop_on_error: bool = True,
    ):
        self.pipeline = list(pipeline)
        self.state_store = state_store
        self.stop_on_error = stop_on_error

    def run(self, initial_context: Optional[dict] = None, on_event: Optional[EventCallback] = None) -> ExecutionContext:
        def emit(event: dict) -> None:
            if on_event is not None:
                on_event(event)

        context = ExecutionContext(initial_context, on_event=emit)
        run_id = self.state_store.start_run() if self.state_store else None
        had_failure = False

        for module in self.pipeline:
            context.active_module = (module.tier.value, module.name)
            emit({"kind": "step_started", "tier": module.tier.value, "name": module.name})
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
                emit({"kind": "step_failed", "tier": module.tier.value, "name": module.name, "error": str(exc)})
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
            emit({"kind": "step_completed", "tier": module.tier.value, "name": module.name, "output": output})

        if self.state_store:
            self.state_store.finish_run(run_id, "failed" if had_failure else "completed")
        emit(
            {
                "kind": "run_failed" if had_failure else "run_completed",
                "context": context.variables,
            }
        )
        return context
