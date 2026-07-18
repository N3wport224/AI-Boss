import time

from engine import BaseModule, Orchestrator, StepSpec, Tier
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


class Echo(BaseModule):
    """Reads whatever key its manifest/pipeline wires as `heard`, for mapping tests."""

    name = "echo"
    tier = Tier.AGENT

    def run(self, context: ExecutionContext) -> dict:
        return {"echoed": context.get("heard")}


class Fails(BaseModule):
    name = "fails"
    tier = Tier.AGENT

    def run(self, context: ExecutionContext) -> dict:
        raise RuntimeError("boom")


class Slow(BaseModule):
    name = "slow"
    tier = Tier.AUTOMATION

    def run(self, context: ExecutionContext) -> dict:
        time.sleep(0.3)
        return {"done": True}


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


def test_step_spec_maps_an_earlier_steps_output_into_a_differently_named_field():
    # Double() writes "value"; Echo only ever reads "heard" — a StepSpec seed is
    # what a no-code pipeline builder uses to wire "Step 1 output -> Step 2 input"
    # when the names don't already line up.
    steps = [
        StepSpec(module=Double()),
        StepSpec(module=Echo(), seed=lambda ctx: {"heard": ctx.get("value")}),
    ]
    orchestrator = Orchestrator(steps)
    context = orchestrator.run({"value": 5})

    assert context.get("value") == 10
    assert context.get("echoed") == 10
    assert [s.name for s in context.history] == ["double", "echo"]


def test_step_spec_list_can_mix_bare_modules_and_specs():
    steps = [Double(), StepSpec(module=AddOne())]
    orchestrator = Orchestrator(steps)
    context = orchestrator.run({"value": 5})

    assert context.get("value") == 11


def test_step_spec_timeout_fails_a_slow_step():
    # Python can't forcibly kill a thread, so a timeout only stops the
    # orchestrator from waiting on it — the step is recorded as a failure.
    steps = [StepSpec(module=Slow(), timeout_seconds=0.05)]
    orchestrator = Orchestrator(steps, stop_on_error=False)
    context = orchestrator.run({})

    step = context.history[0]
    assert step.success is False
    assert "did not finish within 0.05s" in step.error
    assert context.get("done") is None
