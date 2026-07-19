"""Batch 12: saved input presets per module -- a named set of input values
for a module's own card, so a user can reapply a combination they use often
instead of retyping it every time.
"""
import pytest
from fastapi.testclient import TestClient

from engine.state_store import StateStore
from webapp.main import app, store

client = TestClient(app)


@pytest.fixture(autouse=True)
def cleanup_presets():
    """Presets live in the shared, process-wide StateStore -- clean up
    whatever this file creates so it never leaks into other test files."""
    yield
    for preset in store.list_input_presets("automation", "fetch_raw_metrics"):
        store.delete_input_preset("automation", "fetch_raw_metrics", preset["preset_name"])


# ---- Store-level behavior ----

def test_store_save_list_delete_round_trip(tmp_path):
    s = StateStore(str(tmp_path / "presets.db"))
    assert s.list_input_presets("automation", "m") == []

    saved = s.save_input_preset("automation", "m", "Big Customer", {"signups": 500, "churn": 2})
    assert saved["preset_name"] == "Big Customer"
    assert saved["inputs"] == {"signups": 500, "churn": 2}

    listed = s.list_input_presets("automation", "m")
    assert len(listed) == 1
    assert listed[0]["inputs"] == {"signups": 500, "churn": 2}

    # Saving under the same name overwrites rather than duplicating.
    s.save_input_preset("automation", "m", "Big Customer", {"signups": 999, "churn": 2})
    listed = s.list_input_presets("automation", "m")
    assert len(listed) == 1
    assert listed[0]["inputs"]["signups"] == 999

    assert s.delete_input_preset("automation", "m", "Big Customer") is True
    assert s.list_input_presets("automation", "m") == []
    assert s.delete_input_preset("automation", "m", "Big Customer") is False
    s.close()


def test_presets_are_scoped_per_module(tmp_path):
    s = StateStore(str(tmp_path / "presets_scope.db"))
    s.save_input_preset("automation", "module_a", "Shared Name", {"x": 1})
    s.save_input_preset("workflow", "module_b", "Shared Name", {"x": 2})

    assert s.list_input_presets("automation", "module_a")[0]["inputs"] == {"x": 1}
    assert s.list_input_presets("workflow", "module_b")[0]["inputs"] == {"x": 2}
    s.close()


# ---- API surface ----

def test_save_and_list_presets_through_the_api():
    res = client.post(
        "/api/modules/automation/fetch_raw_metrics/presets",
        json={"preset_name": "Whale Account", "inputs": {"signups": 1000, "churn": 1, "revenue": 99999}},
    )
    assert res.status_code == 200
    body = res.json()
    assert body["preset_name"] == "Whale Account"

    listed = client.get("/api/modules/automation/fetch_raw_metrics/presets").json()
    assert [p["preset_name"] for p in listed] == ["Whale Account"]
    assert listed[0]["inputs"]["signups"] == 1000


def test_save_preset_rejects_blank_name():
    res = client.post(
        "/api/modules/automation/fetch_raw_metrics/presets",
        json={"preset_name": "   ", "inputs": {}},
    )
    assert res.status_code == 400


def test_save_preset_for_unknown_module_404s():
    res = client.post(
        "/api/modules/automation/does_not_exist/presets",
        json={"preset_name": "Whatever", "inputs": {}},
    )
    assert res.status_code == 404


def test_delete_preset_through_the_api():
    client.post(
        "/api/modules/automation/fetch_raw_metrics/presets",
        json={"preset_name": "Temp Preset", "inputs": {"signups": 1}},
    )
    res = client.delete("/api/modules/automation/fetch_raw_metrics/presets/Temp%20Preset")
    assert res.status_code == 200
    assert res.json() == {"deleted": "Temp Preset"}

    listed = client.get("/api/modules/automation/fetch_raw_metrics/presets").json()
    assert not any(p["preset_name"] == "Temp Preset" for p in listed)


def test_delete_unknown_preset_404s():
    res = client.delete("/api/modules/automation/fetch_raw_metrics/presets/does_not_exist_preset")
    assert res.status_code == 404


def test_list_presets_for_unknown_module_404s():
    res = client.get("/api/modules/automation/does_not_exist/presets")
    assert res.status_code == 404


def test_saving_under_an_existing_name_overwrites_it():
    client.post(
        "/api/modules/automation/fetch_raw_metrics/presets",
        json={"preset_name": "Overwrite Me", "inputs": {"signups": 1}},
    )
    client.post(
        "/api/modules/automation/fetch_raw_metrics/presets",
        json={"preset_name": "Overwrite Me", "inputs": {"signups": 2}},
    )
    listed = client.get("/api/modules/automation/fetch_raw_metrics/presets").json()
    matching = [p for p in listed if p["preset_name"] == "Overwrite Me"]
    assert len(matching) == 1
    assert matching[0]["inputs"]["signups"] == 2
