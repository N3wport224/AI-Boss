import time

from webapp.events import RunEventBus


def test_stream_replays_events_published_before_the_reader_ever_connects():
    bus = RunEventBus()
    stream_id = bus.create()
    bus.publish(stream_id, {"kind": "step_started", "name": "a"})
    bus.publish(stream_id, {"kind": "step_completed", "name": "a"})
    bus.close(stream_id)

    events = list(bus.stream(stream_id))
    assert [e["kind"] for _, e in events] == ["step_started", "step_completed"]
    assert [i for i, _ in events] == [0, 1]


def test_a_second_reader_can_replay_from_scratch_after_a_first_reader_already_drained_it():
    # This is the actual reconnect scenario: one "connection" reads through
    # the whole stream, then a second (simulating a reconnect) asks for
    # everything again from the top — with the old queue-based bus this
    # would come back empty, since a queue.Queue.get() permanently removes
    # what it reads.
    bus = RunEventBus()
    stream_id = bus.create()
    bus.publish(stream_id, {"kind": "step_started", "name": "a"})
    bus.publish(stream_id, {"kind": "step_completed", "name": "a"})
    bus.close(stream_id)

    first_reader = list(bus.stream(stream_id))
    second_reader = list(bus.stream(stream_id))

    assert [e["kind"] for _, e in first_reader] == ["step_started", "step_completed"]
    assert [e["kind"] for _, e in second_reader] == ["step_started", "step_completed"]


def test_reconnect_resumes_from_a_given_index_instead_of_replaying_everything():
    bus = RunEventBus()
    stream_id = bus.create()
    bus.publish(stream_id, {"kind": "step_started", "name": "a"})
    bus.publish(stream_id, {"kind": "step_completed", "name": "a"})
    bus.publish(stream_id, {"kind": "run_completed"})
    bus.close(stream_id)

    # A client that already saw index 0 (step_started) reconnects asking for
    # index 1 onward, same as a browser resuming via Last-Event-ID: 0.
    resumed = list(bus.stream(stream_id, from_index=1))
    assert [e["kind"] for _, e in resumed] == ["step_completed", "run_completed"]


def test_stream_waits_for_events_published_after_the_reader_catches_up():
    bus = RunEventBus()
    stream_id = bus.create()
    bus.publish(stream_id, {"kind": "step_started", "name": "a"})

    received = []

    def consume():
        for index, event in bus.stream(stream_id, timeout=2.0):
            received.append(event)
            if event["kind"] == "run_completed":
                break

    import threading

    thread = threading.Thread(target=consume)
    thread.start()
    time.sleep(0.1)  # let the reader catch up to the one published event and start waiting
    bus.publish(stream_id, {"kind": "run_completed"})
    thread.join(timeout=2.0)

    assert [e["kind"] for e in received] == ["step_started", "run_completed"]


def test_stream_returns_immediately_for_an_unknown_stream_id():
    bus = RunEventBus()
    assert list(bus.stream(999999)) == []


def test_closed_stream_events_are_pruned_after_retention_window(monkeypatch):
    from webapp import events as events_module

    bus = RunEventBus()
    stream_id = bus.create()
    bus.publish(stream_id, {"kind": "run_completed"})
    bus.close(stream_id)

    # Simulate the retention window having already elapsed.
    monkeypatch.setattr(events_module, "_RETENTION_SECONDS", 0.0)
    time.sleep(0.01)

    bus.create()  # pruning happens lazily on the next create()
    assert list(bus.stream(stream_id)) == []
