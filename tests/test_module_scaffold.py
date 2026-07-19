"""Batch 11: new-module scaffolding wizard -- generates a starter manifest
YAML plus a matching Python class stub for a brand-new automation/workflow/
agent, so a user can start building one from the dashboard without
hand-writing the boilerplate described in the README's "Adding a new
module" section.

IMPORTANT: unlike pipelines/artifacts/watched_input (ephemeral runtime dirs),
scaffolding writes into the real automations/workflows/agents source
directories -- every test here must clean up whatever it creates so nothing
leaks into git status.
"""
import shutil

import pytest
from fastapi.testclient import TestClient

from webapp import scaffold
from webapp.main import TIER_DIRS, app

client = TestClient(app)

_TEST_MODULE_NAMES = [
    "scaffold_test_module",
    "scaffold_test_module_two",
    "___bad",
    "123_starts_with_digit",
    "duplicated_fetch_test",
    "duplicated_fetch_test_two",
]


@pytest.fixture(autouse=True)
def cleanup_scaffolded_files():
    yield
    for tier, directory in TIER_DIRS.items():
        for name in _TEST_MODULE_NAMES:
            for suffix in (".py", ".yaml"):
                path = directory / f"{name}{suffix}"
                if path.exists():
                    path.unlink()


# ---- webapp.scaffold module-level behavior ----

def test_scaffold_module_writes_a_valid_py_and_yaml_pair(tmp_path):
    automation_dir = tmp_path / "automations"
    tier_dirs = {"automation": automation_dir, "workflow": tmp_path / "workflows", "agent": tmp_path / "agents"}

    result = scaffold.scaffold_module("automation", "Scaffold Test Module", "Does a thing.", tier_dirs)
    assert result["name"] == "scaffold_test_module"
    assert result["class_name"] == "ScaffoldTestModule"

    py_path = automation_dir / "scaffold_test_module.py"
    yaml_path = automation_dir / "scaffold_test_module.yaml"
    assert py_path.exists()
    assert yaml_path.exists()

    import ast
    ast.parse(py_path.read_text())  # must be syntactically valid Python

    import yaml as yaml_lib
    manifest = yaml_lib.safe_load(yaml_path.read_text())
    assert manifest["name"] == "scaffold_test_module"
    assert manifest["tier"] == "automation"
    assert manifest["entrypoint"] == "automations.scaffold_test_module:ScaffoldTestModule"
    assert manifest["enabled"] is True
    assert manifest["inputs"] == []
    assert manifest["outputs"] == []


def test_scaffold_module_is_actually_importable_and_runnable(tmp_path):
    """Not just valid syntax -- the generated class must really instantiate
    and its run() must return a dict, same contract every hand-written
    module honors."""
    import sys

    automation_dir = tmp_path / "automations_pkg"
    automation_dir.mkdir()
    (tmp_path / "__init__.py").write_text("")
    (automation_dir / "__init__.py").write_text("")
    sys.path.insert(0, str(tmp_path))
    try:
        tier_dirs = {"automation": automation_dir}
        result = scaffold.scaffold_module("automation", "Importable Thing", "", tier_dirs)

        from engine.registry import instantiate

        manifest_entrypoint = f"automations_pkg.{result['name']}:{result['class_name']}"
        module = instantiate(manifest_entrypoint)
        assert module.name == "importable_thing"
        assert module.run(context=None) == {}
    finally:
        sys.path.remove(str(tmp_path))
        sys.modules.pop(f"automations_pkg.importable_thing", None)


def test_scaffold_module_handles_tricky_description_safely(tmp_path):
    tier_dirs = {"automation": tmp_path}
    tricky = 'has "quotes", a backslash \\ and a newline\nhere'
    result = scaffold.scaffold_module("automation", "Scaffold Test Module", tricky, tier_dirs)

    py_path = tmp_path / f"{result['name']}.py"
    import ast
    ast.parse(py_path.read_text())  # must still be valid Python

    import yaml as yaml_lib
    manifest = yaml_lib.safe_load((tmp_path / f"{result['name']}.yaml").read_text())
    assert manifest["description"] == tricky


def test_scaffold_rejects_unknown_tier(tmp_path):
    with pytest.raises(scaffold.ScaffoldError):
        scaffold.scaffold_module("bogus_tier", "Whatever", "", {"automation": tmp_path})


def test_scaffold_rejects_a_name_with_no_letters_or_numbers(tmp_path):
    with pytest.raises(scaffold.ScaffoldError):
        scaffold.scaffold_module("automation", "!!!", "", {"automation": tmp_path})


def test_scaffold_rejects_a_name_that_would_start_with_a_digit(tmp_path):
    with pytest.raises(scaffold.ScaffoldError):
        scaffold.scaffold_module("automation", "123 starts with digit", "", {"automation": tmp_path})


def test_scaffold_rejects_a_collision_with_an_existing_file(tmp_path):
    tier_dirs = {"automation": tmp_path}
    scaffold.scaffold_module("automation", "Scaffold Test Module", "", tier_dirs)
    with pytest.raises(scaffold.ScaffoldError):
        scaffold.scaffold_module("automation", "Scaffold Test Module", "", tier_dirs)


# ---- End-to-end through the webapp ----

def test_scaffold_endpoint_creates_files_and_module_is_immediately_listed():
    res = client.post(
        "/api/modules/scaffold",
        json={"tier": "workflow", "name": "Scaffold Test Module", "description": "A brand new workflow."},
    )
    assert res.status_code == 200
    body = res.json()
    assert body["tier"] == "workflow"
    assert body["name"] == "scaffold_test_module"

    listed = client.get("/api/modules").json()
    assert any(m["name"] == "scaffold_test_module" for m in listed["workflow"])


def test_scaffolded_module_can_actually_be_run():
    client.post("/api/modules/scaffold", json={"tier": "automation", "name": "Scaffold Test Module", "description": ""})

    res = client.post("/api/modules/automation/scaffold_test_module/run", json={"inputs": {}})
    assert res.status_code == 200

    stream_id = res.json()["stream_id"]
    events = []
    with client.stream("GET", f"/api/stream/{stream_id}") as response:
        import json as json_lib
        for line in response.iter_lines():
            if not line.startswith("data: "):
                continue
            event = json_lib.loads(line[len("data: "):])
            events.append(event)
            if event["kind"] in ("run_completed", "run_failed"):
                break
    assert events[-1]["kind"] == "run_completed"


def test_scaffold_endpoint_rejects_duplicate_name_in_same_tier():
    client.post("/api/modules/scaffold", json={"tier": "agent", "name": "Scaffold Test Module", "description": ""})
    res = client.post("/api/modules/scaffold", json={"tier": "agent", "name": "Scaffold Test Module", "description": ""})
    assert res.status_code == 400


def test_scaffold_endpoint_rejects_unknown_tier():
    res = client.post("/api/modules/scaffold", json={"tier": "not_a_tier", "name": "Scaffold Test Module Two"})
    assert res.status_code == 400


def test_scaffold_endpoint_rejects_blank_name():
    res = client.post("/api/modules/scaffold", json={"tier": "automation", "name": "   "})
    assert res.status_code == 400


# ---- Batch 12: duplicate an existing module as a scaffold starting point ----

def test_duplicate_module_copies_manifest_and_renames_class_and_name(tmp_path):
    import ast

    tier_dirs = {"automation": tmp_path}
    scaffold.scaffold_module("automation", "Original Thing", "The original.", tier_dirs)

    result = scaffold.duplicate_module("automation", "original_thing", "Copied Thing", "", tier_dirs)
    assert result["name"] == "copied_thing"
    assert result["class_name"] == "CopiedThing"

    new_source = (tmp_path / "copied_thing.py").read_text()
    ast.parse(new_source)  # must still be valid Python
    assert "class CopiedThing(BaseModule):" in new_source
    assert 'name = "copied_thing"' in new_source
    # The original module's own name/class must be untouched.
    original_source = (tmp_path / "original_thing.py").read_text()
    assert "class OriginalThing(BaseModule):" in original_source
    assert 'name = "original_thing"' in original_source

    import yaml as yaml_lib
    manifest = yaml_lib.safe_load((tmp_path / "copied_thing.yaml").read_text())
    assert manifest["name"] == "copied_thing"
    assert manifest["entrypoint"] == "automations.copied_thing:CopiedThing"
    # Description carried over from the source since none was given.
    assert manifest["description"] == "The original."


def test_duplicate_module_keeps_the_sources_inputs_and_outputs(tmp_path):
    tier_dirs = {"workflow": tmp_path}
    scaffold.scaffold_module("workflow", "Has Fields", "", tier_dirs)
    # Give it a real input/output schema, like a hand-written module would have.
    import yaml as yaml_lib

    manifest_path = tmp_path / "has_fields.yaml"
    manifest = yaml_lib.safe_load(manifest_path.read_text())
    manifest["inputs"] = [{"name": "threshold", "label": "Threshold", "type": "number", "default": 0.5}]
    manifest["outputs"] = [{"name": "verdict", "label": "Verdict"}]
    manifest_path.write_text(yaml_lib.safe_dump(manifest, sort_keys=False))

    result = scaffold.duplicate_module("workflow", "has_fields", "Has Fields Copy", "", tier_dirs)
    copied_manifest = yaml_lib.safe_load((tmp_path / f"{result['name']}.yaml").read_text())
    assert copied_manifest["inputs"] == manifest["inputs"]
    assert copied_manifest["outputs"] == manifest["outputs"]


def test_duplicate_module_can_override_the_description(tmp_path):
    tier_dirs = {"automation": tmp_path}
    scaffold.scaffold_module("automation", "Desc Source", "Original description.", tier_dirs)

    result = scaffold.duplicate_module("automation", "desc_source", "Desc Target", "A brand new description.", tier_dirs)
    new_source = (tmp_path / f"{result['name']}.py").read_text()
    assert "A brand new description." in new_source

    import yaml as yaml_lib
    manifest = yaml_lib.safe_load((tmp_path / f"{result['name']}.yaml").read_text())
    assert manifest["description"] == "A brand new description."


def test_duplicate_unknown_source_module_raises():
    with pytest.raises(scaffold.ScaffoldError):
        scaffold.duplicate_module("automation", "does_not_exist_at_all", "New Name", "", {"automation": "/tmp"})


def test_duplicate_module_rejects_a_collision_with_an_existing_target(tmp_path):
    tier_dirs = {"automation": tmp_path}
    scaffold.scaffold_module("automation", "Source One", "", tier_dirs)
    scaffold.scaffold_module("automation", "Target One", "", tier_dirs)

    with pytest.raises(scaffold.ScaffoldError):
        scaffold.duplicate_module("automation", "source_one", "Target One", "", tier_dirs)


def test_duplicate_module_endpoint_creates_a_working_clone_of_a_real_bundled_module():
    res = client.post(
        "/api/modules/automation/fetch_raw_metrics/duplicate",
        json={"new_name": "Duplicated Fetch Test", "description": ""},
    )
    assert res.status_code == 200
    body = res.json()
    assert body["name"] == "duplicated_fetch_test"

    listed = client.get("/api/modules").json()
    duplicated = next(m for m in listed["automation"] if m["name"] == "duplicated_fetch_test")
    # The duplicate keeps fetch_raw_metrics' real input schema, not an empty one.
    assert {f["name"] for f in duplicated["inputs"]} == {"signups", "churn", "revenue"}

    run_res = client.post(
        "/api/modules/automation/duplicated_fetch_test/run",
        json={"inputs": {"signups": 5, "churn": 1, "revenue": 2}},
    )
    assert run_res.status_code == 200
    stream_id = run_res.json()["stream_id"]
    events = []
    import json as json_lib
    with client.stream("GET", f"/api/stream/{stream_id}") as response:
        for line in response.iter_lines():
            if not line.startswith("data: "):
                continue
            event = json_lib.loads(line[len("data: "):])
            events.append(event)
            if event["kind"] in ("run_completed", "run_failed"):
                break
    assert events[-1]["kind"] == "run_completed"


def test_duplicate_module_endpoint_404_equivalent_for_unknown_source_is_400():
    res = client.post(
        "/api/modules/automation/does_not_exist/duplicate",
        json={"new_name": "Duplicated Fetch Test Two"},
    )
    assert res.status_code == 400
