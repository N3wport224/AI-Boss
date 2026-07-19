from dataclasses import dataclass, field
from datetime import datetime
from typing import Any, Callable, Optional


@dataclass
class StepRecord:
    name: str
    tier: str
    started_at: datetime
    finished_at: datetime
    success: bool
    output: dict
    error: Optional[str] = None
    # The step's own resolved input values (after static defaults, mappings,
    # and template interpolation) — what it actually ran with, not just its
    # manifest defaults. Lets a historical run be faithfully replayed (see
    # POST /api/runs/{run_id}/rerun) without needing the original pipeline
    # definition, mappings, or conditions to still exist.
    inputs: dict = field(default_factory=dict)


class MemoryStore:
    """A namespaced, best-effort persistent key/value interface exposed on
    `ExecutionContext` as `context.memory` — unlike `context.variables`
    (scoped to a single run), a value written here is visible to a later,
    completely independent run, so one run's agent can leave a note for the
    next one to read.

    Backed by `StateStore`'s `memory` table when one is attached (accepted
    as `Any` here, not imported by type, to avoid a context<->state_store
    import cycle — it only needs to look like a `StateStore` at runtime).
    Falls back to a plain in-memory dict scoped to just this one
    `ExecutionContext` when no store is attached (e.g. an `ExecutionContext`
    built directly in a test, with no persistence intended).
    """

    def __init__(self, state_store: Optional[Any] = None):
        self._state_store = state_store
        self._fallback: dict[str, Any] = {}

    def get(self, key: str, default: Any = None) -> Any:
        if self._state_store is not None:
            return self._state_store.get_memory(key, default)
        return self._fallback.get(key, default)

    def set(self, key: str, value: Any) -> None:
        if self._state_store is not None:
            self._state_store.set_memory(key, value)
        else:
            self._fallback[key] = value

    def delete(self, key: str) -> None:
        if self._state_store is not None:
            self._state_store.delete_memory(key)
        else:
            self._fallback.pop(key, None)

    def all(self) -> list[dict]:
        if self._state_store is not None:
            return self._state_store.all_memory()
        return [{"key": k, "value": v} for k, v in self._fallback.items()]


class ExecutionContext:
    """Shared state threaded through every module in a pipeline run.

    This is the "shared context window" the blueprint calls for: automations,
    workflows, and agents all read from and write to the same `variables` dict,
    so a tier-1 output is directly available as a tier-3 input with no extra glue.

    Modules can also call `emit()` mid-`run()` to report live progress — a
    thought, a tool call, anything worth surfacing to a watching UI. The
    Orchestrator wires this to `active_module` before each module runs so every
    emitted event is automatically tagged with which tier/module produced it.
    """

    def __init__(
        self,
        initial: Optional[dict] = None,
        on_event: Optional[Callable[[dict], None]] = None,
        state_store: Optional[Any] = None,
    ):
        self.variables: dict[str, Any] = dict(initial or {})
        self.history: list[StepRecord] = []
        self.active_module: Optional[tuple[str, str]] = None
        self._on_event = on_event
        self.memory = MemoryStore(state_store)

    def get(self, key: str, default: Any = None) -> Any:
        return self.variables.get(key, default)

    def set(self, key: str, value: Any) -> None:
        self.variables[key] = value

    def update(self, values: dict) -> None:
        self.variables.update(values)

    def record(self, step: StepRecord) -> None:
        self.history.append(step)

    def emit(self, kind: str, message: str, **extra: Any) -> None:
        """Report a live event (e.g. "thought", "tool_call") from within a module's run()."""
        if self._on_event is None:
            return
        event: dict[str, Any] = {"kind": kind, "message": message}
        if self.active_module is not None:
            event["tier"], event["name"] = self.active_module
        event.update(extra)
        self._on_event(event)

    def to_dict(self) -> dict:
        return {
            "variables": self.variables,
            "history": [
                {
                    "name": h.name,
                    "tier": h.tier,
                    "success": h.success,
                    "started_at": h.started_at.isoformat(),
                    "finished_at": h.finished_at.isoformat(),
                    "error": h.error,
                }
                for h in self.history
            ],
        }
