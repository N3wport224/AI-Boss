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


# ---- Batch 14: notification mute preferences ----

@pytest.fixture(autouse=True)
def reset_notification_mutes():
    for kind in ("breaker_tripped", "schedule_failed", "resource_alert", "schedule_once_fired"):
        store.set_notification_kind_muted(kind, False)
    yield
    for kind in ("breaker_tripped", "schedule_failed", "resource_alert", "schedule_once_fired"):
        store.set_notification_kind_muted(kind, False)


def test_store_level_mute_round_trip(tmp_path):
    s = StateStore(str(tmp_path / "mute.db"))
    assert s.is_notification_kind_muted("resource_alert") is False

    s.set_notification_kind_muted("resource_alert", True)
    assert s.is_notification_kind_muted("resource_alert") is True
    assert s.all_notification_mute_state() == {"resource_alert": True}

    s.set_notification_kind_muted("resource_alert", False)
    assert s.is_notification_kind_muted("resource_alert") is False
    s.close()


def test_add_notification_is_a_no_op_when_its_kind_is_muted(tmp_path):
    s = StateStore(str(tmp_path / "mute_add.db"))
    s.set_notification_kind_muted("resource_alert", True)

    result = s.add_notification("resource_alert", "should not be stored")
    assert result is None
    assert s.list_notifications() == []

    # An unmuted kind is unaffected.
    other = s.add_notification("breaker_tripped", "should be stored")
    assert other is not None
    assert len(s.list_notifications()) == 1
    s.close()


def test_get_notification_preferences_defaults_to_all_unmuted():
    prefs = client.get("/api/notifications/preferences").json()
    assert prefs == {
        "breaker_tripped": False,
        "schedule_failed": False,
        "resource_alert": False,
        "schedule_once_fired": False,
    }


def test_set_and_read_a_notification_preference_via_the_api():
    res = client.put("/api/notifications/preferences/resource_alert", json={"muted": True})
    assert res.status_code == 200
    assert res.json() == {"kind": "resource_alert", "muted": True}

    prefs = client.get("/api/notifications/preferences").json()
    assert prefs["resource_alert"] is True
    assert prefs["breaker_tripped"] is False


def test_set_preference_for_an_unknown_kind_400s():
    res = client.put("/api/notifications/preferences/not_a_real_kind", json={"muted": True})
    assert res.status_code == 400


# ---- Batch 18: mute-all/unmute-all notification preferences ----

def test_mute_all_notification_preferences_mutes_every_kind():
    res = client.put("/api/notifications/preferences", json={"muted": True})
    assert res.status_code == 200
    assert all(muted is True for muted in res.json().values())

    prefs = client.get("/api/notifications/preferences").json()
    assert all(muted is True for muted in prefs.values())


def test_unmute_all_notification_preferences_after_muting_all():
    client.put("/api/notifications/preferences", json={"muted": True})
    res = client.put("/api/notifications/preferences", json={"muted": False})
    assert res.status_code == 200
    assert all(muted is False for muted in res.json().values())

    prefs = client.get("/api/notifications/preferences").json()
    assert all(muted is False for muted in prefs.values())


def test_mute_all_actually_suppresses_new_notifications_of_every_kind():
    client.put("/api/notifications/preferences", json={"muted": True})
    res = client.post("/api/notifications", json={"kind": "resource_alert", "message": "should be muted"})
    assert res.json() == {"created": False, "kind": "resource_alert", "muted": True}


def test_create_notification_endpoint_reports_muted_instead_of_creating():
    client.put("/api/notifications/preferences/resource_alert", json={"muted": True})

    res = client.post("/api/notifications", json={"kind": "resource_alert", "message": "muted alert"})
    assert res.status_code == 200
    assert res.json() == {"created": False, "kind": "resource_alert", "muted": True}

    notifications = client.get("/api/notifications").json()
    assert not any(n["message"] == "muted alert" for n in notifications)


def test_create_notification_endpoint_still_creates_when_unmuted():
    res = client.post("/api/notifications", json={"kind": "breaker_tripped", "message": "unmuted alert"})
    assert res.status_code == 200
    body = res.json()
    assert body["created"] is True
    assert body["kind"] == "breaker_tripped"

    notifications = client.get("/api/notifications").json()
    assert any(n["message"] == "unmuted alert" for n in notifications)


# ---- Batch 15: clear all read notifications ----

def test_clear_read_notifications_only_removes_read_ones(tmp_path):
    s = StateStore(str(tmp_path / "clear_read.db"))
    a = s.add_notification("info", "read one")
    s.add_notification("info", "unread one")
    s.mark_notification_read(a["id"])

    cleared = s.clear_read_notifications()
    assert cleared == 1

    remaining = s.list_notifications()
    assert len(remaining) == 1
    assert remaining[0]["message"] == "unread one"
    s.close()


def test_clear_read_notifications_via_the_api():
    a = client.post("/api/notifications", json={"kind": "info", "message": "to be cleared"}).json()
    client.post("/api/notifications", json={"kind": "info", "message": "stays around"})
    client.post(f"/api/notifications/{a['id']}/read")

    res = client.post("/api/notifications/clear-read")
    assert res.status_code == 200
    assert res.json()["cleared"] >= 1

    remaining = client.get("/api/notifications").json()
    assert not any(n["id"] == a["id"] for n in remaining)
    assert any(n["message"] == "stays around" for n in remaining)


# ---- Batch 16: export notifications/alerts to CSV ----

def test_notifications_csv_export_contains_header_and_created_rows():
    unique_message = "csv export marker message 3031"
    created = client.post("/api/notifications", json={"kind": "info", "message": unique_message}).json()

    res = client.get("/api/notifications.csv")
    assert res.status_code == 200
    assert res.headers["content-type"].startswith("text/csv")
    assert "attachment; filename=notifications.csv" in res.headers["content-disposition"]

    lines = res.text.strip().splitlines()
    assert lines[0] == "id,kind,message,created_at,read"
    assert any(str(created["id"]) in line and unique_message in line for line in lines[1:])


def test_notifications_csv_export_respects_limit():
    for i in range(5):
        client.post("/api/notifications", json={"kind": "info", "message": f"limit test message {3032 + i}"})

    res = client.get("/api/notifications.csv", params={"limit": 2})
    assert res.status_code == 200
    lines = res.text.strip().splitlines()
    assert len(lines) - 1 == 2  # header + exactly 2 data rows


# ---- Batch 17: notify when a one-time schedule successfully fires ----

def test_scheduler_on_once_fired_hook_is_wired_to_the_apps_own_scheduler():
    """webapp.main._scheduler is constructed with
    on_once_fired=_notify_once_schedule_fired (not left at the Scheduler
    default of None) -- the actual integration point this feature depends on."""
    from webapp.main import _notify_once_schedule_fired, _scheduler

    assert _scheduler.on_once_fired is _notify_once_schedule_fired


def test_notify_once_schedule_fired_writes_a_notification_for_a_module_schedule():
    from webapp.main import _notify_once_schedule_fired

    before = store.unread_notification_count()
    _notify_once_schedule_fired({"kind": "module", "tier": "automation", "name": "fetch_raw_metrics"})
    after = store.unread_notification_count()
    assert after == before + 1

    latest = store.list_notifications(limit=1)[0]
    assert latest["kind"] == "schedule_once_fired"
    assert "fetch_raw_metrics" in latest["message"]


def test_notify_once_schedule_fired_writes_a_notification_for_a_pipeline_schedule():
    from webapp.main import _notify_once_schedule_fired

    _notify_once_schedule_fired({"kind": "pipeline", "name": "My Saved Pipeline"})
    latest = store.list_notifications(limit=1)[0]
    assert latest["kind"] == "schedule_once_fired"
    assert "My Saved Pipeline" in latest["message"]


def test_schedule_once_fired_is_a_registered_notification_kind():
    from webapp.main import NOTIFICATION_KINDS

    assert "schedule_once_fired" in NOTIFICATION_KINDS


# ---- Batch 19: search across notifications ----

def test_search_notifications_finds_a_keyword_in_the_message():
    unique_marker = "batch19searchmarker8081"
    client.post("/api/notifications", json={"kind": "info", "message": f"CPU spike detected: {unique_marker}"})

    res = client.get("/api/notifications/search", params={"q": unique_marker})
    assert res.status_code == 200
    body = res.json()
    assert body["query"] == unique_marker
    assert any(unique_marker in r["message"] for r in body["results"])


def test_search_notifications_is_case_insensitive():
    unique_marker = "Batch19CaseMarker8082"
    client.post("/api/notifications", json={"kind": "info", "message": f"Something about {unique_marker} happened"})

    res = client.get("/api/notifications/search", params={"q": unique_marker.lower()})
    assert any(unique_marker in r["message"] for r in res.json()["results"])


def test_search_notifications_returns_empty_for_a_blank_query():
    res = client.get("/api/notifications/search", params={"q": ""})
    assert res.status_code == 200
    assert res.json()["results"] == []


def test_search_notifications_finds_nothing_for_an_unmatched_keyword():
    res = client.get("/api/notifications/search", params={"q": "zzz_never_used_notification_zzz"})
    assert res.json()["results"] == []


# ---- Batch 20: checkbox-based bulk mark-read/delete for notifications ----

def test_bulk_mark_notifications_read_marks_only_the_selected_ones():
    a = client.post("/api/notifications", json={"kind": "info", "message": "bulk read a"}).json()
    b = client.post("/api/notifications", json={"kind": "info", "message": "bulk read b"}).json()
    c = client.post("/api/notifications", json={"kind": "info", "message": "bulk read c stays unread"}).json()

    res = client.post("/api/notifications/bulk-mark-read", json={"notification_ids": [a["id"], b["id"]]})
    assert res.status_code == 200
    assert res.json()["marked"] == 2

    listed = {n["id"]: n for n in client.get("/api/notifications").json()}
    assert listed[a["id"]]["read"] is True
    assert listed[b["id"]]["read"] is True
    assert listed[c["id"]]["read"] is False


def test_bulk_mark_notifications_read_is_a_no_op_for_unknown_ids():
    res = client.post("/api/notifications/bulk-mark-read", json={"notification_ids": [999999999]})
    assert res.status_code == 200
    assert res.json()["marked"] == 0


def test_bulk_delete_notifications_removes_read_and_unread_alike():
    a = client.post("/api/notifications", json={"kind": "info", "message": "bulk delete a"}).json()
    b = client.post("/api/notifications", json={"kind": "info", "message": "bulk delete b unread"}).json()
    client.post(f"/api/notifications/{a['id']}/read")

    res = client.post("/api/notifications/bulk-delete", json={"notification_ids": [a["id"], b["id"]]})
    assert res.status_code == 200
    assert res.json()["deleted"] == 2

    remaining_ids = {n["id"] for n in client.get("/api/notifications").json()}
    assert a["id"] not in remaining_ids
    assert b["id"] not in remaining_ids


def test_bulk_delete_notifications_is_a_no_op_on_an_empty_selection():
    res = client.post("/api/notifications/bulk-delete", json={"notification_ids": []})
    assert res.status_code == 200
    assert res.json()["deleted"] == 0


def test_bulk_export_notifications_returns_only_selected_as_csv():
    a = client.post("/api/notifications", json={"kind": "info", "message": "bulk export a"}).json()
    b = client.post("/api/notifications", json={"kind": "info", "message": "bulk export b"}).json()
    other = client.post("/api/notifications", json={"kind": "info", "message": "bulk export not selected"}).json()

    res = client.post("/api/notifications/bulk-export", json={"notification_ids": [a["id"], b["id"]]})
    assert res.status_code == 200
    assert res.headers["content-type"].startswith("text/csv")
    assert "attachment; filename=notifications_selected.csv" in res.headers["content-disposition"]

    lines = res.text.strip().splitlines()
    assert lines[0] == "id,kind,message,created_at,read"
    body_ids = {int(line.split(",")[0]) for line in lines[1:]}
    assert body_ids == {a["id"], b["id"]}
    assert other["id"] not in body_ids

    client.post(
        "/api/notifications/bulk-delete", json={"notification_ids": [a["id"], b["id"], other["id"]]}
    )
