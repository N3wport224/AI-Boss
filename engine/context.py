from dataclasses import dataclass
from datetime import datetime
from typing import Any, Optional


@dataclass
class StepRecord:
    name: str
    tier: str
    started_at: datetime
    finished_at: datetime
    success: bool
    output: dict
    error: Optional[str] = None


class ExecutionContext:
    """Shared state threaded through every module in a pipeline run.

    This is the "shared context window" the blueprint calls for: automations,
    workflows, and agents all read from and write to the same `variables` dict,
    so a tier-1 output is directly available as a tier-3 input with no extra glue.
    """

    def __init__(self, initial: Optional[dict] = None):
        self.variables: dict[str, Any] = dict(initial or {})
        self.history: list[StepRecord] = []

    def get(self, key: str, default: Any = None) -> Any:
        return self.variables.get(key, default)

    def set(self, key: str, value: Any) -> None:
        self.variables[key] = value

    def update(self, values: dict) -> None:
        self.variables.update(values)

    def record(self, step: StepRecord) -> None:
        self.history.append(step)

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
