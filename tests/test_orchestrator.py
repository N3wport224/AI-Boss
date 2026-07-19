import time

import pytest

from engine import BaseModule, Orchestrator, ParallelGroup, StepSpec, Tier
from engine.context import ExecutionContext
from engine.state_store import StateStore


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


class EmitsApiKey(BaseModule):
    name = "emits_api_key"
    tier = Tier.AUTOMATION

    def run(self, context: ExecutionContext) -> dict:
        return {"api_key": "sk-real-secret-value", "status": "ok"}


class ReadsApiKey(BaseModule):
    """Proves a later step still sees the *real* value via shared context —
    only what's recorded/emitted is redacted, not the live working state."""

    name = "reads_api_key"
    tier = Tier.WORKFLOW

    def run(self, context: ExecutionContext) -> dict:
        return {"saw_real_key": context.get("api_key") == "sk-real-secret-value"}


class SleepAndEcho(BaseModule):
    """Sleeps briefly then writes a value under its own key — used to prove
    parallel branches actually run concurrently (wall time well under the
    sum of every branch's sleep)."""

    name = "sleep_and_echo"
    tier = Tier.AUTOMATION

    def __init__(self, output_key: str, sleep_seconds: float = 0.15):
        self.output_key = output_key
        self.sleep_seconds = sleep_seconds

    def run(self, context: ExecutionContext) -> dict:
        time.sleep(self.sleep_seconds)
        return {self.output_key: True}


class AlwaysFails(BaseModule):
    name = "always_fails"
    tier = Tier.AGENT

    def run(self, context: ExecutionContext) -> dict:
        raise RuntimeError("branch boom")


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


def test_secret_shaped_output_is_redacted_in_history_and_events_but_not_in_live_context():
    events = []
    orchestrator = Orchestrator([EmitsApiKey(), ReadsApiKey()])
    context = orchestrator.run({}, on_event=events.append)

    # The recorded StepRecord (what StateStore persists) never sees the real value.
    emits_step = context.history[0]
    assert emits_step.output["api_key"] == "***REDACTED***"
    assert emits_step.output["status"] == "ok"

    # A later step reading the same context key still got the real value.
    assert context.get("saw_real_key") is True

    # Every emitted event (step_completed, run_completed) is redacted too.
    step_completed_events = [e for e in events if e["kind"] == "step_completed" and e["name"] == "emits_api_key"]
    assert step_completed_events[0]["output"]["api_key"] == "***REDACTED***"

    run_completed = next(e for e in events if e["kind"] == "run_completed")
    assert run_completed["context"]["api_key"] == "***REDACTED***"


def test_parallel_group_runs_branches_concurrently_and_merges_outputs():
    group = ParallelGroup(
        steps=[
            SleepAndEcho("branch_a", 0.15),
            SleepAndEcho("branch_b", 0.15),
            SleepAndEcho("branch_c", 0.15),
        ]
    )
    orchestrator = Orchestrator([group])

    start = time.monotonic()
    context = orchestrator.run({})
    elapsed = time.monotonic() - start

    assert context.get("branch_a") is True
    assert context.get("branch_b") is True
    assert context.get("branch_c") is True
    # Sequential would take >= 0.45s; concurrent should be close to 0.15s.
    assert elapsed < 0.35

    branch_records = [s for s in context.history if s.name == "sleep_and_echo"]
    assert len(branch_records) == 3
    assert all(s.success for s in branch_records)


def test_parallel_group_emits_group_and_tagged_branch_events():
    events = []
    group = ParallelGroup(steps=[SleepAndEcho("a", 0.01), SleepAndEcho("b", 0.01)], name="my_group")
    orchestrator = Orchestrator([group])
    orchestrator.run({}, on_event=events.append)

    kinds = [e["kind"] for e in events]
    assert kinds[0] == "group_started"
    assert kinds.count("step_started") == 2
    assert kinds.count("step_completed") == 2
    assert "group_completed" in kinds

    group_started = next(e for e in events if e["kind"] == "group_started")
    assert group_started["name"] == "my_group"
    assert group_started["branch_count"] == 2

    branch_started_indices = {e["branch_index"] for e in events if e["kind"] == "step_started" and e.get("parallel")}
    assert branch_started_indices == {0, 1}

    group_completed = next(e for e in events if e["kind"] == "group_completed")
    assert group_completed["had_failure"] is False


def test_parallel_group_seeds_each_branch_before_any_branch_runs():
    group = ParallelGroup(
        steps=[
            StepSpec(module=Echo(), seed=lambda ctx: {"heard": "first"}),
            StepSpec(module=Double(), seed=lambda ctx: {"value": 10}),
        ]
    )
    orchestrator = Orchestrator([group])
    context = orchestrator.run({})

    assert context.get("echoed") == "first"
    assert context.get("value") == 20


def test_parallel_group_failing_branch_continues_pipeline_when_stop_on_error_is_false():
    group = ParallelGroup(steps=[SleepAndEcho("ok", 0.01), AlwaysFails()])
    orchestrator = Orchestrator([group, AddOne()], stop_on_error=False)
    context = orchestrator.run({"value": 5})

    assert context.get("ok") is True
    assert context.get("value") == 6  # AddOne after the group still ran

    names_and_success = [(s.name, s.success) for s in context.history]
    assert ("always_fails", False) in names_and_success
    assert ("add_one", True) in names_and_success


def test_parallel_group_failing_branch_stops_pipeline_when_stop_on_error_is_true():
    group = ParallelGroup(steps=[SleepAndEcho("ok", 0.01), AlwaysFails()])
    orchestrator = Orchestrator([group, AddOne()])  # stop_on_error=True by default

    try:
        orchestrator.run({"value": 5})
        assert False, "expected RuntimeError to propagate"
    except RuntimeError as exc:
        assert "parallel_group" in str(exc)


def test_parallel_group_can_be_mixed_with_ordinary_steps_in_one_pipeline():
    group = ParallelGroup(steps=[SleepAndEcho("side_effect", 0.01)])
    orchestrator = Orchestrator([Double(), group, AddOne()])
    context = orchestrator.run({"value": 5})

    assert context.get("value") == 11  # (5 * 2) + 1, group didn't disturb the chain
    assert context.get("side_effect") is True
    assert [s.name for s in context.history] == ["double", "sleep_and_echo", "add_one"]


class RemembersLastValue(BaseModule):
    """Writes this run's `value` to persistent memory and reports whatever
    was remembered from a *previous, separate* run — proves context.memory
    survives across independent Orchestrator.run() calls, unlike
    context.variables, which starts fresh every run."""

    name = "remembers_last_value"
    tier = Tier.AGENT

    def run(self, context: ExecutionContext) -> dict:
        previous = context.memory.get("last_value")
        context.memory.set("last_value", context.get("value"))
        return {"previous_value": previous}


def test_context_memory_persists_across_independent_runs_sharing_a_state_store(tmp_path):
    store = StateStore(str(tmp_path / "memory_test.db"))

    first_run = Orchestrator([RemembersLastValue()], state_store=store).run({"value": 42})
    assert first_run.get("previous_value") is None  # nothing remembered yet

    second_run = Orchestrator([RemembersLastValue()], state_store=store).run({"value": 99})
    assert second_run.get("previous_value") == 42  # the *first* run's value, from a separate run

    store.close()


def test_context_memory_falls_back_to_a_local_dict_with_no_state_store_attached():
    # No state_store at all -> memory still works, just scoped to this one context.
    context = ExecutionContext({})
    assert context.memory.get("anything") is None
    context.memory.set("anything", "value")
    assert context.memory.get("anything") == "value"


def test_conditional_step_skips_when_condition_is_false_and_run_continues():
    events = []
    conditional = StepSpec(
        module=Double(),
        condition=lambda ctx: ctx.get("value", 0) > 100,
        condition_label="value gt 100",
    )
    orchestrator = Orchestrator([conditional, AddOne()])
    context = orchestrator.run({"value": 5}, on_event=events.append)

    assert context.get("value") == 6  # Double never ran, AddOne did

    kinds = [e["kind"] for e in events]
    assert "step_skipped" in kinds
    skipped = next(e for e in events if e["kind"] == "step_skipped")
    assert skipped["name"] == "double"
    assert skipped["condition"] == "value gt 100"
    assert kinds[-1] == "run_completed"  # skipping is not a failure

    assert [s.name for s in context.history] == ["add_one"]  # no record for the skipped step


def test_conditional_step_runs_normally_when_condition_is_true():
    conditional = StepSpec(module=Double(), condition=lambda ctx: ctx.get("value", 0) > 1)
    context = Orchestrator([conditional]).run({"value": 5})
    assert context.get("value") == 10


def test_condition_can_read_an_earlier_steps_output():
    # Double turns 5 into 10; AddOne only runs if value is exactly 10.
    gated = StepSpec(module=AddOne(), condition=lambda ctx: ctx.get("value") == 10)
    context = Orchestrator([Double(), gated]).run({"value": 5})
    assert context.get("value") == 11


def test_a_raising_condition_skips_the_step_instead_of_crashing_the_run():
    def broken_condition(ctx):
        raise RuntimeError("condition bug")

    conditional = StepSpec(module=Double(), condition=broken_condition)
    context = Orchestrator([conditional, AddOne()]).run({"value": 5})
    assert context.get("value") == 6  # Double skipped, run survived


def test_parallel_group_branch_condition_skips_only_that_branch():
    events = []
    group = ParallelGroup(
        steps=[
            SleepAndEcho("runs", 0.01),
            StepSpec(module=AlwaysFails(), condition=lambda ctx: False, condition_label="never"),
        ]
    )
    orchestrator = Orchestrator([group], stop_on_error=False)
    context = orchestrator.run({}, on_event=events.append)

    assert context.get("runs") is True
    # The failing branch was skipped, so the group had no failure at all.
    group_completed = next(e for e in events if e["kind"] == "group_completed")
    assert group_completed["had_failure"] is False

    skipped = next(e for e in events if e["kind"] == "step_skipped")
    assert skipped["parallel"] is True
    assert skipped["branch_index"] == 1
    assert skipped["name"] == "always_fails"


class FailsNTimes(BaseModule):
    """Fails its first `fail_count` calls, then succeeds — a controllable
    stand-in for a flaky external dependency."""

    name = "fails_n_times"
    tier = Tier.AUTOMATION

    def __init__(self, fail_count: int):
        self.fail_count = fail_count
        self.calls = 0

    def run(self, context: ExecutionContext) -> dict:
        self.calls += 1
        if self.calls <= self.fail_count:
            raise RuntimeError(f"transient failure {self.calls}")
        return {"succeeded_on_call": self.calls}


def test_step_retries_and_eventually_succeeds_within_the_retry_budget():
    module = FailsNTimes(fail_count=2)
    step = StepSpec(module=module, max_retries=3, retry_backoff_seconds=0.01)
    events = []

    context = Orchestrator([step]).run({}, on_event=events.append)

    assert module.calls == 3
    assert context.get("succeeded_on_call") == 3
    kinds = [e["kind"] for e in events]
    assert kinds == ["step_started", "step_retrying", "step_retrying", "step_completed", "run_completed"]

    retries = [e for e in events if e["kind"] == "step_retrying"]
    assert [r["attempt"] for r in retries] == [1, 2]
    assert [r["max_retries"] for r in retries] == [3, 3]
    # Exponential backoff: attempt 0 waits backoff*2**0, attempt 1 waits backoff*2**1.
    assert retries[0]["delay_seconds"] == pytest.approx(0.01)
    assert retries[1]["delay_seconds"] == pytest.approx(0.02)


def test_step_retries_are_exhausted_and_the_step_really_fails():
    module = FailsNTimes(fail_count=5)
    step = StepSpec(module=module, max_retries=2, retry_backoff_seconds=0.01)
    events = []

    context = Orchestrator([step], stop_on_error=False).run({}, on_event=events.append)

    assert module.calls == 3  # 1 initial attempt + 2 retries
    kinds = [e["kind"] for e in events]
    assert kinds == ["step_started", "step_retrying", "step_retrying", "step_failed", "run_failed"]
    failure_record = next(s for s in context.history if s.name == "fails_n_times")
    assert failure_record.success is False


def test_zero_retries_is_identical_to_no_retry_behavior():
    module = FailsNTimes(fail_count=1)
    step = StepSpec(module=module)  # max_retries defaults to 0
    events = []

    Orchestrator([step], stop_on_error=False).run({}, on_event=events.append)

    assert module.calls == 1
    assert not any(e["kind"] == "step_retrying" for e in events)
    assert [e["kind"] for e in events] == ["step_started", "step_failed", "run_failed"]


def test_parallel_group_branch_retries_independently_of_other_branches():
    flaky = FailsNTimes(fail_count=1)
    group = ParallelGroup(
        steps=[
            SleepAndEcho("stable", 0.01),
            StepSpec(module=flaky, max_retries=2, retry_backoff_seconds=0.01),
        ]
    )
    events = []
    context = Orchestrator([group]).run({}, on_event=events.append)

    assert context.get("stable") is True
    assert context.get("succeeded_on_call") == 2
    retries = [e for e in events if e["kind"] == "step_retrying"]
    assert len(retries) == 1
    assert retries[0]["branch_index"] == 1
    assert retries[0]["parallel"] is True


def test_step_record_captures_resolved_inputs_seeded_via_step_spec():
    step = StepSpec(module=Double(), seed=lambda ctx: {"value": 21})
    context = Orchestrator([step]).run({})

    record = next(s for s in context.history if s.name == "double")
    assert record.inputs == {"value": 21}


def test_step_record_captures_inputs_seeded_via_initial_context_not_step_seed():
    """The standalone 'run this module' and scheduled-module paths seed a
    step by passing values as the run's initial_context, not through
    StepSpec.seed — inputs recording must cover that path too, not just
    explicit seed() closures."""
    step = StepSpec(module=Double())  # default seed: lambda ctx: {}
    context = Orchestrator([step]).run({"value": 55})

    record = next(s for s in context.history if s.name == "double")
    assert record.inputs == {"value": 55}


def test_step_record_captures_inputs_even_when_the_step_fails():
    step = StepSpec(module=Fails(), seed=lambda ctx: {"marker": "present"})
    context = Orchestrator([step], stop_on_error=False).run({})

    record = next(s for s in context.history if s.name == "fails")
    assert record.success is False
    assert record.inputs == {"marker": "present"}


def test_secret_shaped_recorded_input_is_redacted():
    class ReadsSecret(BaseModule):
        name = "reads_secret"
        tier = Tier.AGENT

        def run(self, context: ExecutionContext) -> dict:
            return {"used": context.get("api_key")}

    step = StepSpec(module=ReadsSecret())
    context = Orchestrator([step]).run({"api_key": "sk-real-secret-value"})

    record = next(s for s in context.history if s.name == "reads_secret")
    assert record.inputs["api_key"] == "***REDACTED***"
    # The live context (what a later step could still legitimately use) keeps
    # the real value — only the recorded/emitted audit trail is masked.
    assert context.get("api_key") == "sk-real-secret-value"


def test_parallel_branch_step_record_captures_its_own_resolved_inputs():
    group = ParallelGroup(
        steps=[
            StepSpec(module=Echo(), seed=lambda ctx: {"heard": "branch_a_value"}),
            StepSpec(module=Double(), seed=lambda ctx: {"value": 9}),
        ]
    )
    context = Orchestrator([group]).run({})

    echo_record = next(s for s in context.history if s.name == "echo")
    double_record = next(s for s in context.history if s.name == "double")
    assert echo_record.inputs["heard"] == "branch_a_value"
    assert double_record.inputs["value"] == 9
