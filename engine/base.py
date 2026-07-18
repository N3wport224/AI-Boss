from abc import ABC, abstractmethod
from enum import Enum
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from .context import ExecutionContext


class Tier(str, Enum):
    """The three execution tiers the orchestrator distinguishes between."""

    AUTOMATION = "automation"  # deterministic, rule-based
    WORKFLOW = "workflow"      # data-driven / ML pipeline
    AGENT = "agent"            # autonomous, NLP-capable


class BaseModule(ABC):
    """Contract every automation, workflow, and agent implements to join a pipeline."""

    name: str
    tier: Tier
    description: str = ""

    @abstractmethod
    def run(self, context: "ExecutionContext") -> dict:
        """Execute this step and return a dict to merge into the shared context."""
        raise NotImplementedError
