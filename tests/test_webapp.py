import json
import time
from datetime import datetime, timezone

from fastapi.testclient import TestClient

from webapp.main import app

client = TestClient(app)


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


def test_list_modules_groups_by_tier_and_includes_input_schema():
    res = client.get("/api/modules")
    assert res.status_code == 200

    body = res.json()
    assert set(body.keys()) == {"automation", "workflow", "agent"}

    automation = body["automation"][0]
    assert automation["name"] == "fetch_raw_metrics"
    assert {f["name"] for f in automation["inputs"]} == {"signups", "churn", "revenue"}


def test_run_single_module_streams_progress_and_completion():
    res = client.post(
        "/api/modules/automation/fetch_raw_metrics/run",
        json={"inputs": {"signups": 200, "churn": 5, "revenue": 999}},
    )
    assert res.status_code == 200
    stream_id = res.json()["stream_id"]

    events = _collect_stream(stream_id)
    kinds = [e["kind"] for e in events]
    assert kinds == ["step_started", "step_completed", "run_completed"]

    completed = events[1]
    assert completed["tier"] == "automation"
    assert completed["name"] == "fetch_raw_metrics"
    assert completed["output"]["raw_metrics"] == {"signups": 200.0, "churn": 5.0, "revenue": 999.0}


def test_stream_reconnect_with_last_event_id_resumes_from_where_it_left_off():
    res = client.post(
        "/api/modules/automation/fetch_raw_metrics/run",
        json={"inputs": {"signups": 1, "churn": 1, "revenue": 1}, "force_refresh": True},
    )
    stream_id = res.json()["stream_id"]

    # Drain the whole stream once, recording each SSE "id:" line alongside its event.
    ids_and_kinds = []
    with client.stream("GET", f"/api/stream/{stream_id}") as response:
        current_id = None
        for line in response.iter_lines():
            if line.startswith("id: "):
                current_id = int(line[len("id: "):])
            elif line.startswith("data: "):
                event = json.loads(line[len("data: "):])
                ids_and_kinds.append((current_id, event["kind"]))
                if event["kind"] in ("run_completed", "run_failed"):
                    break

    assert [kind for _, kind in ids_and_kinds] == ["step_started", "step_completed", "run_completed"]
    first_id = ids_and_kinds[0][0]

    # Reconnect claiming we already saw the first event — the browser's own
    # Last-Event-ID reconnect behavior — and confirm we resume after it
    # instead of replaying the whole stream again.
    resumed_kinds = []
    with client.stream(
        "GET", f"/api/stream/{stream_id}", headers={"Last-Event-ID": str(first_id)}
    ) as response:
        for line in response.iter_lines():
            if not line.startswith("data: "):
                continue
            event = json.loads(line[len("data: "):])
            resumed_kinds.append(event["kind"])
            if event["kind"] in ("run_completed", "run_failed"):
                break

    assert resumed_kinds == ["step_completed", "run_completed"]


def test_run_unknown_module_returns_404():
    res = client.post("/api/modules/automation/does_not_exist/run", json={"inputs": {}})
    assert res.status_code == 404


def test_run_full_pipeline_streams_agent_thoughts_and_tool_calls():
    res = client.post("/api/pipeline/run", json={"inputs": {}})
    assert res.status_code == 200
    stream_id = res.json()["stream_id"]

    events = _collect_stream(stream_id)

    step_kinds = [e["kind"] for e in events if e["kind"] in ("step_started", "step_completed")]
    assert step_kinds == [
        "step_started", "step_completed",
        "step_started", "step_completed",
        "step_started", "step_completed",
    ]

    agent_thoughts = [e for e in events if e["kind"] == "thought" and e.get("name") == "churn_response_agent"]
    agent_tool_calls = [e for e in events if e["kind"] == "tool_call" and e.get("name") == "churn_response_agent"]
    assert len(agent_thoughts) > 0
    assert len(agent_tool_calls) > 0

    final = events[-1]
    assert final["kind"] == "run_completed"
    assert "agent_decision" in final["context"]
    # The agent's custom_note template default interpolates real upstream values.
    assert "128" in final["context"]["custom_note"]
    assert "14" in final["context"]["custom_note"]
    assert "{signups}" not in final["context"]["custom_note"]


def test_run_module_result_is_cached_and_force_refresh_bypasses_it(monkeypatch):
    import automations.example_automation as example_automation

    call_count = {"n": 0}
    original_run = example_automation.DataFetchAutomation.run

    def counting_run(self, context):
        call_count["n"] += 1
        return original_run(self, context)

    monkeypatch.setattr(example_automation.DataFetchAutomation, "run", counting_run)

    payload = {"inputs": {"signups": 321, "churn": 9, "revenue": 555}}

    res1 = client.post("/api/modules/automation/fetch_raw_metrics/run", json=payload)
    events1 = _collect_stream(res1.json()["stream_id"])
    assert events1[-1]["kind"] == "run_completed"
    assert call_count["n"] == 1

    res2 = client.post("/api/modules/automation/fetch_raw_metrics/run", json=payload)
    body2 = res2.json()
    assert body2.get("cached") is True
    events2 = _collect_stream(body2["stream_id"])
    assert call_count["n"] == 1  # cache hit — the module never ran again
    completed2 = next(e for e in events2 if e["kind"] == "step_completed")
    assert completed2.get("cached") is True
    assert completed2["output"] == events1[1]["output"]

    res3 = client.post(
        "/api/modules/automation/fetch_raw_metrics/run",
        json={**payload, "force_refresh": True},
    )
    body3 = res3.json()
    assert "cached" not in body3
    _collect_stream(body3["stream_id"])
    assert call_count["n"] == 2  # force_refresh skipped the cache


def test_schedule_crud_and_validation():
    res = client.post(
        "/api/schedules",
        json={
            "kind": "module",
            "tier": "automation",
            "name": "fetch_raw_metrics",
            "inputs": {"signups": 1, "churn": 1, "revenue": 1},
            "interval_seconds": 30,
        },
    )
    assert res.status_code == 200
    schedule = res.json()
    assert schedule["enabled"] is True
    schedule_id = schedule["id"]

    listed = client.get("/api/schedules").json()
    assert any(s["id"] == schedule_id for s in listed)

    patched = client.patch(f"/api/schedules/{schedule_id}", json={"enabled": False})
    assert patched.json()["enabled"] is False

    deleted = client.delete(f"/api/schedules/{schedule_id}")
    assert deleted.json() == {"deleted": schedule_id}
    assert not any(s["id"] == schedule_id for s in client.get("/api/schedules").json())


def test_schedule_label_can_be_set_and_cleared():
    res = client.post(
        "/api/schedules",
        json={
            "kind": "module",
            "tier": "automation",
            "name": "fetch_raw_metrics",
            "inputs": {"signups": 1, "churn": 1, "revenue": 1},
            "interval_seconds": 30,
        },
    )
    schedule_id = res.json()["id"]
    assert res.json()["label"] == ""

    labeled = client.patch(f"/api/schedules/{schedule_id}", json={"label": "Nightly metrics pull"})
    assert labeled.status_code == 200
    assert labeled.json()["label"] == "Nightly metrics pull"

    listed = {s["id"]: s for s in client.get("/api/schedules").json()}
    assert listed[schedule_id]["label"] == "Nightly metrics pull"

    cleared = client.patch(f"/api/schedules/{schedule_id}", json={"label": ""})
    assert cleared.json()["label"] == ""

    client.delete(f"/api/schedules/{schedule_id}")


def test_schedule_update_rejects_empty_payload_and_unknown_id():
    res = client.post(
        "/api/schedules",
        json={
            "kind": "module",
            "tier": "automation",
            "name": "fetch_raw_metrics",
            "inputs": {"signups": 1, "churn": 1, "revenue": 1},
            "interval_seconds": 30,
        },
    )
    schedule_id = res.json()["id"]

    empty_res = client.patch(f"/api/schedules/{schedule_id}", json={})
    assert empty_res.status_code == 400

    missing_res = client.patch("/api/schedules/9999999", json={"label": "does not matter"})
    assert missing_res.status_code == 404

    client.delete(f"/api/schedules/{schedule_id}")


def test_schedules_csv_export_has_a_header_and_a_row_for_a_created_schedule():
    res = client.post(
        "/api/schedules",
        json={
            "kind": "module",
            "tier": "automation",
            "name": "fetch_raw_metrics",
            "inputs": {"signups": 1, "churn": 1, "revenue": 1},
            "interval_seconds": 30,
        },
    )
    schedule_id = res.json()["id"]

    csv_res = client.get("/api/schedules.csv")
    assert csv_res.status_code == 200
    assert csv_res.headers["content-type"].startswith("text/csv")
    assert "attachment; filename=schedules.csv" in csv_res.headers["content-disposition"]

    lines = csv_res.text.strip().splitlines()
    assert lines[0] == (
        "id,kind,tier,name,label,schedule_type,interval_seconds,daily_time,day_of_week,"
        "enabled,next_run_at,last_run_at,last_status"
    )
    assert any(line.startswith(f"{schedule_id},module,automation,fetch_raw_metrics,") for line in lines[1:])

    client.delete(f"/api/schedules/{schedule_id}")


def _create_test_schedule():
    res = client.post(
        "/api/schedules",
        json={
            "kind": "module",
            "tier": "automation",
            "name": "fetch_raw_metrics",
            "inputs": {"signups": 1, "churn": 1, "revenue": 1},
            "interval_seconds": 30,
        },
    )
    return res.json()["id"]


def test_bulk_set_schedules_enabled_pauses_and_resumes_a_selection():
    id_a = _create_test_schedule()
    id_b = _create_test_schedule()

    pause_res = client.post("/api/schedules/bulk-set-enabled", json={"schedule_ids": [id_a, id_b], "enabled": False})
    assert pause_res.status_code == 200
    assert set(pause_res.json()["updated"]) == {id_a, id_b}
    assert pause_res.json()["enabled"] is False

    listed = {s["id"]: s for s in client.get("/api/schedules").json()}
    assert listed[id_a]["enabled"] is False
    assert listed[id_b]["enabled"] is False

    resume_res = client.post("/api/schedules/bulk-set-enabled", json={"schedule_ids": [id_a, id_b], "enabled": True})
    assert set(resume_res.json()["updated"]) == {id_a, id_b}
    listed_after = {s["id"]: s for s in client.get("/api/schedules").json()}
    assert listed_after[id_a]["enabled"] is True
    assert listed_after[id_b]["enabled"] is True


def test_bulk_set_schedules_enabled_skips_unknown_ids():
    id_a = _create_test_schedule()
    res = client.post("/api/schedules/bulk-set-enabled", json={"schedule_ids": [id_a, 9999999], "enabled": False})
    assert res.status_code == 200
    assert res.json()["updated"] == [id_a]


def test_bulk_clear_schedule_labels_blanks_out_selected_and_skips_unknown_ids():
    id_a = _create_test_schedule()
    id_b = _create_test_schedule()
    id_keep = _create_test_schedule()

    client.patch(f"/api/schedules/{id_a}", json={"label": "Nightly A"})
    client.patch(f"/api/schedules/{id_b}", json={"label": "Nightly B"})
    client.patch(f"/api/schedules/{id_keep}", json={"label": "Keep me"})

    res = client.post("/api/schedules/bulk-clear-label", json={"schedule_ids": [id_a, id_b, 9999999]})
    assert res.status_code == 200
    assert set(res.json()["updated"]) == {id_a, id_b}

    listed = {s["id"]: s for s in client.get("/api/schedules").json()}
    assert listed[id_a]["label"] == ""
    assert listed[id_b]["label"] == ""
    assert listed[id_keep]["label"] == "Keep me"

    client.post("/api/schedules/bulk-delete", json={"schedule_ids": [id_a, id_b, id_keep]})


def test_bulk_set_schedule_labels_applies_to_selected_and_skips_unknown_ids():
    id_a = _create_test_schedule()
    id_b = _create_test_schedule()
    id_untouched = _create_test_schedule()

    res = client.post(
        "/api/schedules/bulk-set-label", json={"schedule_ids": [id_a, id_b, 9999999], "label": "Batch 49 label"}
    )
    assert res.status_code == 200
    assert set(res.json()["updated"]) == {id_a, id_b}
    assert res.json()["label"] == "Batch 49 label"

    listed = {s["id"]: s for s in client.get("/api/schedules").json()}
    assert listed[id_a]["label"] == "Batch 49 label"
    assert listed[id_b]["label"] == "Batch 49 label"
    assert listed[id_untouched]["label"] == ""

    client.post("/api/schedules/bulk-delete", json={"schedule_ids": [id_a, id_b, id_untouched]})


def test_bulk_set_schedule_labels_is_a_no_op_on_an_empty_selection():
    res = client.post("/api/schedules/bulk-set-label", json={"schedule_ids": [], "label": "unused"})
    assert res.status_code == 200
    assert res.json()["updated"] == []


def test_bulk_export_schedules_returns_only_selected_and_skips_unknown_ids():
    id_a = _create_test_schedule()
    id_b = _create_test_schedule()
    id_other = _create_test_schedule()

    res = client.post("/api/schedules/bulk-export", json={"schedule_ids": [id_a, id_b, 9999999]})
    assert res.status_code == 200
    assert res.headers["content-type"].startswith("application/json")
    assert "attachment; filename=schedules_selected.json" in res.headers["content-disposition"]

    body = res.json()
    returned_ids = {s["id"] for s in body}
    assert returned_ids == {id_a, id_b}
    assert id_other not in returned_ids

    client.post("/api/schedules/bulk-delete", json={"schedule_ids": [id_a, id_b, id_other]})


def test_bulk_export_schedules_csv_returns_only_selected_and_skips_unknown_ids():
    id_a = _create_test_schedule()
    id_b = _create_test_schedule()
    id_other = _create_test_schedule()

    res = client.post("/api/schedules/bulk-export-csv", json={"schedule_ids": [id_a, id_b, 9999999]})
    assert res.status_code == 200
    assert res.headers["content-type"].startswith("text/csv")
    assert "attachment; filename=schedules_selected.csv" in res.headers["content-disposition"]

    lines = res.text.strip().splitlines()
    assert lines[0] == (
        "id,kind,tier,name,label,schedule_type,interval_seconds,"
        "daily_time,day_of_week,enabled,next_run_at,last_run_at,last_status"
    )
    returned_ids = {int(line.split(",")[0]) for line in lines[1:]}
    assert returned_ids == {id_a, id_b}
    assert id_other not in returned_ids

    client.post("/api/schedules/bulk-delete", json={"schedule_ids": [id_a, id_b, id_other]})


def test_bulk_export_schedules_csv_is_just_a_header_on_an_empty_selection():
    res = client.post("/api/schedules/bulk-export-csv", json={"schedule_ids": []})
    assert res.status_code == 200
    lines = res.text.strip().splitlines()
    assert len(lines) == 1


def test_run_schedule_now_executes_immediately_without_touching_cadence():
    schedule_id = _create_test_schedule()
    before = next(s for s in client.get("/api/schedules").json() if s["id"] == schedule_id)
    # Track ids, not counts: once the suite has accumulated 200+ runs both
    # before/after reads cap at the limit and a length comparison can never
    # detect the new run. A fresh run always has an unseen id at the top.
    ids_before = {r["id"] for r in client.get("/api/runs?limit=200").json()}

    res = client.post(f"/api/schedules/{schedule_id}/run-now")
    assert res.status_code == 200
    assert res.json() == {"triggered": True}

    new_ids = set()
    for _ in range(30):
        new_ids = {r["id"] for r in client.get("/api/runs?limit=200").json()} - ids_before
        if new_ids:
            break
        time.sleep(0.1)
    assert new_ids, "expected run-now to actually launch a new run"

    after = next(s for s in client.get("/api/schedules").json() if s["id"] == schedule_id)
    assert after["last_run_at"] == before["last_run_at"]
    assert after["next_run_at"] == before["next_run_at"]
    assert after["last_status"] == before["last_status"]

    client.delete(f"/api/schedules/{schedule_id}")


def test_run_schedule_now_404s_for_an_unknown_schedule():
    res = client.post("/api/schedules/9999999/run-now")
    assert res.status_code == 404


def test_bulk_run_schedules_now_triggers_every_selected_and_reports_unknown_ids():
    id_a = _create_test_schedule()
    id_b = _create_test_schedule()
    # Same id-based detection as the single run-now test: length comparisons
    # go blind once the suite has 200+ accumulated runs.
    ids_before = {r["id"] for r in client.get("/api/runs?limit=200").json()}

    res = client.post("/api/schedules/bulk-run-now", json={"schedule_ids": [id_a, id_b, 9999999]})
    assert res.status_code == 200
    body = res.json()
    assert set(body["triggered"]) == {id_a, id_b}
    assert body["failed"] == {"9999999": "No schedule with this id."}

    new_ids = set()
    for _ in range(30):
        new_ids = {r["id"] for r in client.get("/api/runs?limit=200").json()} - ids_before
        if len(new_ids) >= 2:
            break
        time.sleep(0.1)
    assert len(new_ids) >= 2, "expected both selected schedules to actually launch a run"

    client.post("/api/schedules/bulk-delete", json={"schedule_ids": [id_a, id_b]})


def test_bulk_run_schedules_now_is_a_no_op_on_an_empty_selection():
    res = client.post("/api/schedules/bulk-run-now", json={"schedule_ids": []})
    assert res.status_code == 200
    assert res.json() == {"triggered": [], "failed": {}}


def test_bulk_duplicate_schedules_clones_each_selected_schedule_and_skips_unknown_ids():
    id_a = _create_test_schedule()
    id_b = _create_test_schedule()

    res = client.post("/api/schedules/bulk-duplicate", json={"schedule_ids": [id_a, id_b, 9999999]})
    assert res.status_code == 200
    duplicated = res.json()["duplicated"]
    assert len(duplicated) == 2

    originals = {id_a, id_b}
    new_ids = {s["id"] for s in duplicated}
    assert new_ids.isdisjoint(originals), "duplicates must be brand new schedule rows, not the originals"

    for clone in duplicated:
        assert clone["kind"] == "module"
        assert clone["tier"] == "automation"
        assert clone["name"] == "fetch_raw_metrics"
        assert clone["inputs"] == {"signups": 1, "churn": 1, "revenue": 1}
        assert clone["interval_seconds"] == 30
        assert clone["schedule_type"] == "interval"
        assert clone["enabled"] is True

    client.post("/api/schedules/bulk-delete", json={"schedule_ids": [id_a, id_b, *new_ids]})


def test_bulk_duplicate_schedules_is_a_no_op_on_an_empty_selection():
    res = client.post("/api/schedules/bulk-duplicate", json={"schedule_ids": []})
    assert res.status_code == 200
    assert res.json() == {"duplicated": []}


def test_bulk_duplicate_a_daily_schedule_recomputes_its_own_next_occurrence():
    create_res = client.post(
        "/api/schedules",
        json={
            "kind": "module",
            "tier": "automation",
            "name": "fetch_raw_metrics",
            "inputs": {},
            "schedule_type": "daily",
            "daily_time": "23:59",
        },
    )
    original_id = create_res.json()["id"]

    res = client.post("/api/schedules/bulk-duplicate", json={"schedule_ids": [original_id]})
    assert res.status_code == 200
    duplicated = res.json()["duplicated"]
    assert len(duplicated) == 1
    clone = duplicated[0]
    assert clone["schedule_type"] == "daily"
    assert clone["daily_time"] == "23:59"
    assert clone["next_run_at"]

    client.post("/api/schedules/bulk-delete", json={"schedule_ids": [original_id, clone["id"]]})


def test_next_occurrences_returns_evenly_spaced_future_fire_times_for_a_daily_schedule():
    create_res = client.post(
        "/api/schedules",
        json={
            "kind": "module",
            "tier": "automation",
            "name": "fetch_raw_metrics",
            "inputs": {},
            "schedule_type": "daily",
            "daily_time": "23:58",
        },
    )
    schedule_id = create_res.json()["id"]

    res = client.get(f"/api/schedules/{schedule_id}/next-occurrences?count=3")
    assert res.status_code == 200
    occurrences = res.json()["occurrences"]
    assert len(occurrences) == 3
    parsed = [datetime.fromisoformat(o) for o in occurrences]
    assert parsed[0] < parsed[1] < parsed[2]

    client.post("/api/schedules/bulk-delete", json={"schedule_ids": [schedule_id]})


def test_next_occurrences_400s_for_an_interval_or_once_schedule():
    schedule_id = _create_test_schedule()
    res = client.get(f"/api/schedules/{schedule_id}/next-occurrences")
    assert res.status_code == 400
    client.post("/api/schedules/bulk-delete", json={"schedule_ids": [schedule_id]})


def test_next_occurrences_404s_for_an_unknown_schedule():
    res = client.get("/api/schedules/9999999/next-occurrences")
    assert res.status_code == 404


def test_bulk_delete_schedules_removes_every_selected_one():
    id_a = _create_test_schedule()
    id_b = _create_test_schedule()
    id_keep = _create_test_schedule()

    res = client.post("/api/schedules/bulk-delete", json={"schedule_ids": [id_a, id_b]})
    assert res.status_code == 200
    assert set(res.json()["deleted"]) == {id_a, id_b}

    remaining_ids = {s["id"] for s in client.get("/api/schedules").json()}
    assert id_a not in remaining_ids
    assert id_b not in remaining_ids
    assert id_keep in remaining_ids


def test_schedule_rejects_too_short_interval():
    res = client.post(
        "/api/schedules",
        json={"kind": "module", "tier": "automation", "name": "fetch_raw_metrics", "inputs": {}, "interval_seconds": 1},
    )
    assert res.status_code == 400


def test_schedule_rejects_unknown_module():
    res = client.post(
        "/api/schedules",
        json={"kind": "module", "tier": "automation", "name": "does_not_exist", "inputs": {}, "interval_seconds": 30},
    )
    assert res.status_code == 404


def test_schedule_rejects_unknown_pipeline():
    res = client.post(
        "/api/schedules",
        json={"kind": "pipeline", "name": "does-not-exist", "inputs": {}, "interval_seconds": 30},
    )
    assert res.status_code == 404


def test_daily_schedule_create_and_shows_next_run_in_the_future():
    res = client.post(
        "/api/schedules",
        json={
            "kind": "module",
            "tier": "automation",
            "name": "fetch_raw_metrics",
            "inputs": {"signups": 1, "churn": 1, "revenue": 1},
            "schedule_type": "daily",
            "daily_time": "09:30",
        },
    )
    assert res.status_code == 200
    schedule = res.json()
    assert schedule["schedule_type"] == "daily"
    assert schedule["daily_time"] == "09:30"
    assert datetime.fromisoformat(schedule["next_run_at"]) > datetime.now(timezone.utc)

    listed = client.get("/api/schedules").json()
    assert any(s["id"] == schedule["id"] and s["daily_time"] == "09:30" for s in listed)

    client.delete(f"/api/schedules/{schedule['id']}")


def test_daily_schedule_rejects_malformed_time():
    res = client.post(
        "/api/schedules",
        json={
            "kind": "module", "tier": "automation", "name": "fetch_raw_metrics", "inputs": {},
            "schedule_type": "daily", "daily_time": "25:99",
        },
    )
    assert res.status_code == 400


def test_daily_schedule_requires_daily_time():
    res = client.post(
        "/api/schedules",
        json={
            "kind": "module", "tier": "automation", "name": "fetch_raw_metrics", "inputs": {},
            "schedule_type": "daily",
        },
    )
    assert res.status_code == 400


def test_weekly_schedule_create_and_shows_next_run_in_the_future():
    res = client.post(
        "/api/schedules",
        json={
            "kind": "module",
            "tier": "automation",
            "name": "fetch_raw_metrics",
            "inputs": {"signups": 1, "churn": 1, "revenue": 1},
            "schedule_type": "weekly",
            "daily_time": "09:30",
            "day_of_week": 2,
        },
    )
    assert res.status_code == 200
    schedule = res.json()
    assert schedule["schedule_type"] == "weekly"
    assert schedule["daily_time"] == "09:30"
    assert schedule["day_of_week"] == 2
    assert datetime.fromisoformat(schedule["next_run_at"]) > datetime.now(timezone.utc)

    listed = client.get("/api/schedules").json()
    assert any(s["id"] == schedule["id"] and s["day_of_week"] == 2 for s in listed)

    client.delete(f"/api/schedules/{schedule['id']}")


def test_weekly_schedule_rejects_malformed_time():
    res = client.post(
        "/api/schedules",
        json={
            "kind": "module", "tier": "automation", "name": "fetch_raw_metrics", "inputs": {},
            "schedule_type": "weekly", "daily_time": "25:99", "day_of_week": 1,
        },
    )
    assert res.status_code == 400


def test_weekly_schedule_requires_daily_time():
    res = client.post(
        "/api/schedules",
        json={
            "kind": "module", "tier": "automation", "name": "fetch_raw_metrics", "inputs": {},
            "schedule_type": "weekly", "day_of_week": 1,
        },
    )
    assert res.status_code == 400


def test_weekly_schedule_requires_a_valid_day_of_week():
    res = client.post(
        "/api/schedules",
        json={
            "kind": "module", "tier": "automation", "name": "fetch_raw_metrics", "inputs": {},
            "schedule_type": "weekly", "daily_time": "09:00", "day_of_week": 7,
        },
    )
    assert res.status_code == 400

    res_missing = client.post(
        "/api/schedules",
        json={
            "kind": "module", "tier": "automation", "name": "fetch_raw_metrics", "inputs": {},
            "schedule_type": "weekly", "daily_time": "09:00",
        },
    )
    assert res_missing.status_code == 400


def test_once_schedule_create_and_shows_next_run_in_the_future():
    from datetime import datetime, timedelta, timezone as tz

    run_at = (datetime.now(tz.utc) + timedelta(hours=2)).isoformat()
    res = client.post(
        "/api/schedules",
        json={
            "kind": "module",
            "tier": "automation",
            "name": "fetch_raw_metrics",
            "inputs": {"signups": 1, "churn": 1, "revenue": 1},
            "schedule_type": "once",
            "run_at": run_at,
        },
    )
    assert res.status_code == 200
    schedule = res.json()
    assert schedule["schedule_type"] == "once"
    assert datetime.fromisoformat(schedule["next_run_at"]) > datetime.now(timezone.utc)

    client.delete(f"/api/schedules/{schedule['id']}")


def test_once_schedule_rejects_a_past_run_at():
    from datetime import datetime, timedelta, timezone as tz

    run_at = (datetime.now(tz.utc) - timedelta(hours=1)).isoformat()
    res = client.post(
        "/api/schedules",
        json={
            "kind": "module", "tier": "automation", "name": "fetch_raw_metrics", "inputs": {},
            "schedule_type": "once", "run_at": run_at,
        },
    )
    assert res.status_code == 400


def test_once_schedule_requires_run_at():
    res = client.post(
        "/api/schedules",
        json={
            "kind": "module", "tier": "automation", "name": "fetch_raw_metrics", "inputs": {},
            "schedule_type": "once",
        },
    )
    assert res.status_code == 400


def test_once_schedule_rejects_a_malformed_run_at():
    res = client.post(
        "/api/schedules",
        json={
            "kind": "module", "tier": "automation", "name": "fetch_raw_metrics", "inputs": {},
            "schedule_type": "once", "run_at": "not-a-real-datetime",
        },
    )
    assert res.status_code == 400


def test_schedule_rejects_unknown_schedule_type():
    res = client.post(
        "/api/schedules",
        json={
            "kind": "module", "tier": "automation", "name": "fetch_raw_metrics", "inputs": {},
            "schedule_type": "weekly", "interval_seconds": 30,
        },
    )
    assert res.status_code == 400


def test_scheduler_pause_resume_status_roundtrip():
    try:
        assert client.get("/api/scheduler/status").json() == {"paused": False}

        pause_res = client.post("/api/scheduler/pause")
        assert pause_res.status_code == 200
        assert pause_res.json() == {"paused": True}
        assert client.get("/api/scheduler/status").json() == {"paused": True}

        resume_res = client.post("/api/scheduler/resume")
        assert resume_res.status_code == 200
        assert resume_res.json() == {"paused": False}
        assert client.get("/api/scheduler/status").json() == {"paused": False}
    finally:
        client.post("/api/scheduler/resume")  # never leave the shared app-level scheduler paused for later tests


def test_run_detail_and_compare_endpoints():
    res1 = client.post(
        "/api/modules/automation/fetch_raw_metrics/run",
        json={"inputs": {"signups": 10, "churn": 1, "revenue": 100}, "force_refresh": True},
    )
    _collect_stream(res1.json()["stream_id"])

    res2 = client.post(
        "/api/modules/automation/fetch_raw_metrics/run",
        json={"inputs": {"signups": 20, "churn": 2, "revenue": 200}, "force_refresh": True},
    )
    _collect_stream(res2.json()["stream_id"])

    runs = client.get("/api/runs?limit=2").json()
    run_b_id, run_a_id = runs[0]["id"], runs[1]["id"]  # newest first

    detail = client.get(f"/api/runs/{run_a_id}")
    assert detail.status_code == 200
    assert detail.json()["steps"][0]["output"]["raw_metrics"]["signups"] == 10.0

    compare = client.get("/api/runs/compare", params={"a": run_a_id, "b": run_b_id})
    assert compare.status_code == 200
    body = compare.json()
    assert len(body["steps"]) == 1
    diff = body["steps"][0]["output_diff"]
    assert "raw_metrics" in diff
    assert diff["raw_metrics"]["a"]["signups"] == 10.0
    assert diff["raw_metrics"]["b"]["signups"] == 20.0


def test_run_detail_404s_for_unknown_run():
    assert client.get("/api/runs/999999").status_code == 404


# ---- Batch 20: export a single run's full detail as JSON ----

def test_download_run_detail_json_matches_the_regular_detail_endpoint():
    res = client.post(
        "/api/modules/automation/fetch_raw_metrics/run",
        json={"inputs": {"signups": 5, "churn": 1, "revenue": 50}, "force_refresh": True},
    )
    _collect_stream(res.json()["stream_id"])
    run_id = client.get("/api/runs?limit=1").json()[0]["id"]

    download = client.get(f"/api/runs/{run_id}.json")
    assert download.status_code == 200
    assert download.headers["content-type"].startswith("application/json")
    assert f"filename=run_{run_id}.json" in download.headers["content-disposition"]

    regular = client.get(f"/api/runs/{run_id}")
    assert download.json() == regular.json()


def test_download_run_detail_json_404s_for_unknown_run():
    assert client.get("/api/runs/999999.json").status_code == 404


def test_compare_404s_when_a_run_has_no_steps():
    res = client.post(
        "/api/modules/automation/fetch_raw_metrics/run",
        json={"inputs": {"signups": 1, "churn": 1, "revenue": 1}, "force_refresh": True},
    )
    _collect_stream(res.json()["stream_id"])
    run_id = client.get("/api/runs?limit=1").json()[0]["id"]

    assert client.get("/api/runs/compare", params={"a": run_id, "b": 999999}).status_code == 404


def test_run_note_can_be_set_and_is_returned_by_list_and_detail():
    res = client.post(
        "/api/modules/automation/fetch_raw_metrics/run",
        json={"inputs": {"signups": 1, "churn": 1, "revenue": 1}, "force_refresh": True},
    )
    _collect_stream(res.json()["stream_id"])
    run_id = client.get("/api/runs?limit=1").json()[0]["id"]

    put_res = client.put(f"/api/runs/{run_id}/note", json={"note": "  Expected failure, ignore.  "})
    assert put_res.status_code == 200
    assert put_res.json() == {"run_id": run_id, "note": "Expected failure, ignore."}

    detail = client.get(f"/api/runs/{run_id}")
    assert detail.json()["note"] == "Expected failure, ignore."

    listed = client.get("/api/runs?limit=1").json()
    assert listed[0]["note"] == "Expected failure, ignore."


def test_run_note_can_be_cleared_with_an_empty_string():
    res = client.post(
        "/api/modules/automation/fetch_raw_metrics/run",
        json={"inputs": {"signups": 2, "churn": 1, "revenue": 1}, "force_refresh": True},
    )
    _collect_stream(res.json()["stream_id"])
    run_id = client.get("/api/runs?limit=1").json()[0]["id"]

    client.put(f"/api/runs/{run_id}/note", json={"note": "temporary"})
    clear_res = client.put(f"/api/runs/{run_id}/note", json={"note": ""})
    assert clear_res.json() == {"run_id": run_id, "note": ""}
    assert client.get(f"/api/runs/{run_id}").json()["note"] == ""


def test_run_note_defaults_to_empty_string_when_never_set():
    res = client.post(
        "/api/modules/automation/fetch_raw_metrics/run",
        json={"inputs": {"signups": 3, "churn": 1, "revenue": 1}, "force_refresh": True},
    )
    _collect_stream(res.json()["stream_id"])
    run_id = client.get("/api/runs?limit=1").json()[0]["id"]
    assert client.get(f"/api/runs/{run_id}").json()["note"] == ""


def test_run_note_404s_for_an_unknown_run():
    res = client.put("/api/runs/999999/note", json={"note": "x"})
    assert res.status_code == 404


def test_search_run_notes_finds_a_keyword_in_a_notes_text():
    res = client.post(
        "/api/modules/automation/fetch_raw_metrics/run",
        json={"inputs": {"signups": 4, "churn": 1, "revenue": 1}, "force_refresh": True},
    )
    _collect_stream(res.json()["stream_id"])
    run_id = client.get("/api/runs?limit=1").json()[0]["id"]
    client.put(f"/api/runs/{run_id}/note", json={"note": "Flaky upstream vendor, expected failure."})

    search_res = client.get("/api/runs/search-notes", params={"q": "flaky upstream"})
    assert search_res.status_code == 200
    body = search_res.json()
    assert body["query"] == "flaky upstream"
    assert any(r["run_id"] == run_id for r in body["results"])


def test_search_run_notes_is_case_insensitive():
    res = client.post(
        "/api/modules/automation/fetch_raw_metrics/run",
        json={"inputs": {"signups": 5, "churn": 1, "revenue": 1}, "force_refresh": True},
    )
    _collect_stream(res.json()["stream_id"])
    run_id = client.get("/api/runs?limit=1").json()[0]["id"]
    client.put(f"/api/runs/{run_id}/note", json={"note": "UPPERCASE MARKER"})

    search_res = client.get("/api/runs/search-notes", params={"q": "uppercase marker"})
    assert any(r["run_id"] == run_id for r in search_res.json()["results"])


def test_search_run_notes_returns_empty_for_a_blank_query():
    res = client.get("/api/runs/search-notes", params={"q": ""})
    assert res.json()["results"] == []


def test_search_run_notes_finds_nothing_for_an_unmatched_keyword():
    res = client.get("/api/runs/search-notes", params={"q": "zzz_nonexistent_note_keyword_zzz"})
    assert res.json()["results"] == []


def test_run_endpoints_are_rate_limited_per_client():
    from webapp.main import _run_rate_limiter

    payload = {"inputs": {"signups": 1, "churn": 1, "revenue": 1}, "force_refresh": True}
    for _ in range(_run_rate_limiter.max_requests):
        res = client.post("/api/modules/automation/fetch_raw_metrics/run", json=payload)
        assert res.status_code == 200

    over_limit = client.post("/api/modules/automation/fetch_raw_metrics/run", json=payload)
    assert over_limit.status_code == 429
    assert "Too many run requests" in over_limit.json()["detail"]


def test_get_rate_limit_reports_the_hardcoded_default_when_no_override_exists():
    res = client.get("/api/ratelimit")
    assert res.status_code == 200
    body = res.json()
    assert body["max_requests"] == 30
    assert body["window_seconds"] == 10.0
    assert body["overridden"] is False


def test_set_rate_limit_overrides_the_live_limiter_immediately():
    res = client.patch("/api/ratelimit", json={"max_requests": 2, "window_seconds": 60})
    assert res.status_code == 200
    assert res.json() == {"max_requests": 2, "window_seconds": 60}

    listed = client.get("/api/ratelimit").json()
    assert listed == {"max_requests": 2, "window_seconds": 60, "overridden": True}

    payload = {"inputs": {"signups": 1, "churn": 1, "revenue": 1}, "force_refresh": True}
    ok1 = client.post("/api/modules/automation/fetch_raw_metrics/run", json=payload)
    ok2 = client.post("/api/modules/automation/fetch_raw_metrics/run", json=payload)
    over_limit = client.post("/api/modules/automation/fetch_raw_metrics/run", json=payload)
    assert ok1.status_code == 200
    assert ok2.status_code == 200
    assert over_limit.status_code == 429


def test_clear_rate_limit_reverts_to_the_hardcoded_default():
    client.patch("/api/ratelimit", json={"max_requests": 2, "window_seconds": 60})
    res = client.delete("/api/ratelimit")
    assert res.status_code == 200
    assert res.json() == {"max_requests": 30, "window_seconds": 10.0}

    listed = client.get("/api/ratelimit").json()
    assert listed["overridden"] is False


def test_set_rate_limit_rejects_a_non_positive_max_requests():
    res = client.patch("/api/ratelimit", json={"max_requests": 0, "window_seconds": 10})
    assert res.status_code == 400


def test_set_rate_limit_rejects_a_non_positive_window():
    res = client.patch("/api/ratelimit", json={"max_requests": 10, "window_seconds": 0})
    assert res.status_code == 400


def test_run_module_failure_streams_step_failed_then_run_failed(monkeypatch):
    import automations.example_automation as example_automation

    def boom(self, context):
        raise RuntimeError("simulated failure")

    monkeypatch.setattr(example_automation.DataFetchAutomation, "run", boom)

    res = client.post("/api/modules/automation/fetch_raw_metrics/run", json={"inputs": {}})
    stream_id = res.json()["stream_id"]

    events = _collect_stream(stream_id)
    kinds = [e["kind"] for e in events]
    assert kinds == ["step_started", "step_failed", "run_failed"]
    assert "simulated failure" in events[1]["error"]


def test_module_stats_start_at_zero_and_update_after_a_real_run():
    # The module-level store is shared across this whole test file, so other
    # tests may have already recorded runs (including failures) for this
    # same module — assert the *delta* a fresh success adds, not an absolute
    # count or rate.
    res = client.get("/api/modules")
    fetch = next(m for m in res.json()["automation"] if m["name"] == "fetch_raw_metrics")
    before_runs = fetch["stats"]["total_runs"]
    before_successes = round((fetch["stats"]["success_rate"] or 0) * before_runs)

    launch = client.post(
        "/api/modules/automation/fetch_raw_metrics/run",
        json={"inputs": {"signups": 5, "churn": 1, "revenue": 10}, "force_refresh": True},
    )
    _collect_stream(launch.json()["stream_id"])

    after = client.get("/api/modules").json()
    fetch_after = next(m for m in after["automation"] if m["name"] == "fetch_raw_metrics")
    assert fetch_after["stats"]["total_runs"] == before_runs + 1
    after_successes = round(fetch_after["stats"]["success_rate"] * fetch_after["stats"]["total_runs"])
    assert after_successes == before_successes + 1
    assert fetch_after["stats"]["avg_duration_seconds"] is not None


def test_module_stats_endpoint_reports_per_tier_and_name():
    client.post(
        "/api/modules/automation/fetch_raw_metrics/run",
        json={"inputs": {"signups": 1, "churn": 1, "revenue": 1}, "force_refresh": True},
    )
    res = client.get("/api/modules/stats")
    assert res.status_code == 200

    entries = res.json()
    fetch_entry = next(e for e in entries if e["tier"] == "automation" and e["name"] == "fetch_raw_metrics")
    assert fetch_entry["total_runs"] >= 1
    assert 0 <= fetch_entry["success_rate"] <= 1
    assert fetch_entry["avg_duration_seconds"] >= 0
    assert fetch_entry["success_count"] <= fetch_entry["total_runs"]


def test_module_stats_reflect_failures_alongside_successes():
    import tempfile
    from datetime import datetime, timezone

    from engine.context import StepRecord
    from engine.state_store import StateStore

    isolated = StateStore(f"{tempfile.mkdtemp()}/stats_test.db")
    now = datetime.now(timezone.utc)
    run_id = isolated.start_run()
    isolated.log_step(run_id, StepRecord("thing", "automation", now, now, True, {}))
    isolated.log_step(run_id, StepRecord("thing", "automation", now, now, False, {}, "boom"))
    isolated.finish_run(run_id, "failed")

    stats = isolated.module_stats()
    entry = next(s for s in stats if s["name"] == "thing")
    assert entry["total_runs"] == 2
    assert entry["success_count"] == 1
    assert entry["success_rate"] == 0.5
    isolated.close()


def test_module_with_no_runs_reports_zero_stats_not_an_error():
    res = client.get("/api/modules")
    for tier_modules in res.json().values():
        for module in tier_modules:
            assert "stats" in module
            if module["stats"]["total_runs"] == 0:
                assert module["stats"]["success_rate"] is None
                assert module["stats"]["avg_duration_seconds"] is None


# ---- Batch 21: export per-module performance stats to CSV ----

def test_module_stats_csv_export_has_a_header_and_a_row_for_a_run_module():
    client.post(
        "/api/modules/automation/fetch_raw_metrics/run",
        json={"inputs": {"signups": 3, "churn": 1, "revenue": 30}, "force_refresh": True},
    )
    res = client.get("/api/modules/stats.csv")
    assert res.status_code == 200
    assert res.headers["content-type"].startswith("text/csv")
    assert "attachment; filename=module_stats.csv" in res.headers["content-disposition"]

    lines = res.text.strip().splitlines()
    assert lines[0] == "tier,name,total_runs,success_count,success_rate,avg_duration_seconds"
    assert any("fetch_raw_metrics" in line for line in lines[1:])


def test_module_stats_json_export_matches_the_plain_json_listing():
    run_res = client.post(
        "/api/modules/automation/fetch_raw_metrics/run",
        json={"inputs": {"signups": 4, "churn": 1, "revenue": 40}, "force_refresh": True},
    )
    _collect_stream(run_res.json()["stream_id"])  # wait for the background run to actually land in the store

    res = client.get("/api/modules/stats.json")
    assert res.status_code == 200
    assert res.headers["content-type"].startswith("application/json")
    assert "attachment; filename=module_stats.json" in res.headers["content-disposition"]

    rows = res.json()
    assert rows == client.get("/api/modules/stats").json()
    assert any(r["tier"] == "automation" and r["name"] == "fetch_raw_metrics" for r in rows)
