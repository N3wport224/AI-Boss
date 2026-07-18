from engine import BaseModule, Orchestrator, Tier
from engine.context import ExecutionContext


class Double(BaseModule):
    name = "double"
    tier = Tier.AUTOMATION

    def run(self, context: ExecutionContext) -> dict:
        return {"value": context.get("value", 0) * 2}


class AddOne(BaseModule):
    name = "add_one"
    tier = Tier.WORKFLOW

    def run(self, context: ExecutionContext) -> dict:
        return {"value": context.get("value", 0) + 1}


class Fails(BaseModule):
    name = "fails"
    tier = Tier.AGENT

    def run(self, context: ExecutionContext) -> dict:
        raise RuntimeError("boom")


def test_pipeline_threads_context_sequentially():
    orchestrator = Orchestrator([Double(), AddOne()])
    context = orchestrator.run({"value": 5})

    assert context.get("value") == 11  # (5 * 2) + 1
    assert [step.name for step in context.history] == ["double", "add_one"]
    assert all(step.success for step in context.history)


def test_stop_on_error_halts_pipeline_and_reraises():
    orchestrator = Orchestrator([Double(), Fails(), AddOne()])
    try:
        orchestrator.run({"value": 5})
        assert False, "expected RuntimeError to propagate"
    except RuntimeError:
        pass


def test_continue_on_error_runs_remaining_steps():
    orchestrator = Orchestrator([Double(), Fails(), AddOne()], stop_on_error=False)
    context = orchestrator.run({"value": 5})

    assert context.get("value") == 11  # Fails() never wrote "value", AddOne still ran
    names_and_success = [(step.name, step.success) for step in context.history]
    assert names_and_success == [("double", True), ("fails", False), ("add_one", True)]
