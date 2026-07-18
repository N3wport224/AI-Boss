from datetime import datetime, timezone
from typing import Optional, Sequence

from .base import BaseModule
from .context import ExecutionContext, StepRecord
from .state_store import StateStore


class Orchestrator:
    """Runs a fixed sequence of modules synchronously, threading shared context between them.

    Each module's return value is merged into the same ExecutionContext before the
    next module runs, so a rule-based automation's output becomes an AI workflow's
    input, and that workflow's output becomes an agent's input, in one dependable pass.
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

    def run(self, initial_context: Optional[dict] = None) -> ExecutionContext:
        context = ExecutionContext(initial_context)
        run_id = self.state_store.start_run() if self.state_store else None

        for module in self.pipeline:
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
                    self.state_store.finish_run(run_id, "failed")
                if self.stop_on_error:
                    raise
                continue

            context.update(output)
            record = StepRecord(
                module.name, module.tier.value, started_at, datetime.now(timezone.utc), True, output
            )
            context.record(record)
            if self.state_store:
                self.state_store.log_step(run_id, record)

        if self.state_store:
            self.state_store.finish_run(run_id, "completed")
        return context
