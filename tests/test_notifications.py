"""Batch 13: persistent alert/notification center -- unlike a toast (gone on
reload) or the audit log (a record of user-initiated actions), this is a
durable feed of events the app itself decides are worth surfacing: a circuit
breaker tripping, a scheduled run failing, and (client-triggered) a
resource-usage alert crossing its threshold.
"""
import json

import pytest
from fastapi.testclient import TestClient

from engine.state_store import StateStore
from webapp.main import app, store

client = TestClient(app)

UNREACHABLE_URL = "http://127.0.0.1:9/nope"


@pytest.fixture(autouse=True)
def reset_breaker_and_threshold_state():
    for entry in store.all_module_health():
        store.reset_breaker(entry["tier"], entry["name"])
    for tier, name in list(store.all_breaker_threshold_overrides()):
        store.clear_breaker_threshold_override(tier, name)
    yield
    for entry in store.all_module_health():
        store.reset_breaker(entry["tier"], entry["name"])
    for tier, name in list(store.all_breaker_threshold_overrides()):
        store.clear_breaker_threshold_override(tier, name)


def _collect_stream(stream_id):
    events = []
    with client.stream("GET", f"/api/stream/{stream_id}") as response:
        for line in response.iter_lines():
            if not line.startswith("data: "):
                continue
            event = json.loads(line[len("data: "):])
            events.append(event)
            if event["kind"] in ("run_completed", "run_failed"):
                break
    return events


# ---- Store-level behavior ----

def test_add_and_list_notifications_newest_first(tmp_path):
    s = StateStore(str(tmp_path / "notifications.db"))
    first = s.add_notification("breaker_tripped", "first event")
    second = s.add_notification("schedule_failed", "second event")

    listed = s.list_notifications()
    assert listed[0]["id"] == second["id"]
    assert listed[1]["id"] == first["id"]
    assert listed[0]["read"] is False
    s.close()


def test_unread_count_and_mark_one_read(tmp_path):
    s = StateStore(str(tmp_path / "notifications_unread.db"))
    a = s.add_notification("info", "a")
    s.add_notification("info", "b")
    assert s.unread_notification_count() == 2

    assert s.mark_notification_read(a["id"]) is True
    assert s.unread_notification_count() == 1
    assert s.mark_notification_read(999999) is False
    s.close()


def test_mark_all_read(tmp_path):
    s = StateStore(str(tmp_path / "notifications_all.db"))
    s.add_notification("info", "a")
    s.add_notification("info", "b")
    marked = s.mark_all_notifications_read()
    assert marked == 2
    assert s.unread_notification_count() == 0
    s.close()


def test_unread_only_filter(tmp_path):
    s = StateStore(str(tmp_path / "notifications_filter.db"))
    a = s.add_notification("info", "a")
    s.add_notification("info", "b")
    s.mark_notification_read(a["id"])

    unread = s.list_notifications(unread_only=True)
    assert len(unread) == 1
    assert unread[0]["message"] == "b"
    s.close()


# ---- End-to-end through the webapp ----

def test_circuit_breaker_trip_creates_a_notification():
    client.patch("/api/breakers/automation/http_request/threshold", json={"threshold": 1})
    res = client.post(
        "/api/modules/automation/http_request/run",
        json={"inputs": {"url": UNREACHABLE_URL, "timeout": 2}},
    )
    _collect_stream(res.json()["stream_id"])

    notifications = client.get("/api/notifications").json()
    assert any(
        n["kind"] == "breaker_tripped" and "automation" in n["message"] and "http_request" in n["message"]
        for n in notifications
    )


def test_create_list_and_mark_read_via_the_api():
    create_res = client.post("/api/notifications", json={"kind": "resource_alert", "message": "CPU at 95%"})
    assert create_res.status_code == 200
    body = create_res.json()
    assert body["kind"] == "resource_alert"
    assert body["read"] is False

    listed = client.get("/api/notifications").json()
    assert any(n["id"] == body["id"] for n in listed)

    unread_before = client.get("/api/notifications/unread-count").json()["count"]
    assert unread_before >= 1

    mark_res = client.post(f"/api/notifications/{body['id']}/read")
    assert mark_res.status_code == 200
    assert mark_res.json() == {"id": body["id"], "read": True}

    listed_after = client.get("/api/notifications").json()
    marked = next(n for n in listed_after if n["id"] == body["id"])
    assert marked["read"] is True


def test_create_notification_rejects_a_blank_message():
    res = client.post("/api/notifications", json={"kind": "info", "message": "   "})
    assert res.status_code == 400


def test_mark_unknown_notification_read_404s():
    res = client.post("/api/notifications/99999999/read")
    assert res.status_code == 404


def test_mark_all_read_via_the_api():
    client.post("/api/notifications", json={"kind": "info", "message": "one"})
    client.post("/api/notifications", json={"kind": "info", "message": "two"})

    res = client.post("/api/notifications/mark-all-read")
    assert res.status_code == 200
    assert res.json()["marked"] >= 2
    assert client.get("/api/notifications/unread-count").json()["count"] == 0


def test_unread_only_query_param_via_the_api():
    a = client.post("/api/notifications", json={"kind": "info", "message": "stays unread"}).json()
    client.post(f"/api/notifications/{a['id']}/read")
    client.post("/api/notifications", json={"kind": "info", "message": "still unread"})

    unread = client.get("/api/notifications", params={"unread_only": True}).json()
    assert all(n["read"] is False for n in unread)
    assert any(n["message"] == "still unread" for n in unread)
    assert not any(n["id"] == a["id"] for n in unread)


# ---- Schedule failure notification ----

def test_notify_schedule_error_writes_a_notification_for_a_module_schedule():
    from webapp.main import _notify_schedule_error

    fake_schedule = {"kind": "module", "tier": "automation", "name": "notif_fake_module"}
    _notify_schedule_error(fake_schedule, "boom")

    notifications = client.get("/api/notifications").json()
    assert any(
        n["kind"] == "schedule_failed" and "notif_fake_module" in n["message"] and "boom" in n["message"]
        for n in notifications
    )


def test_notify_schedule_error_writes_a_notification_for_a_pipeline_schedule():
    from webapp.main import _notify_schedule_error

    fake_schedule = {"kind": "pipeline", "name": "notif_fake_pipeline"}
    _notify_schedule_error(fake_schedule, "pipeline boom")

    notifications = client.get("/api/notifications").json()
    assert any(
        n["kind"] == "schedule_failed" and "notif_fake_pipeline" in n["message"] and "pipeline boom" in n["message"]
        for n in notifications
    )


def test_scheduler_on_error_hook_is_wired_to_the_apps_own_scheduler():
    """webapp.main._scheduler is constructed with on_error=_notify_schedule_error
    (not left at the Scheduler default of None) -- the actual integration
    point this whole feature depends on."""
    from webapp.main import _notify_schedule_error, _scheduler

    assert _scheduler.on_error is _notify_schedule_error
