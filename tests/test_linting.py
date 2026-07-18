import asyncio

from fastapi.testclient import TestClient

from webapp.main import app

client = TestClient(app)


def test_read_module_source_async_runs_off_the_event_loop_thread():
    from webapp import linting

    result = asyncio.run(linting.read_module_source_async("agents.example_agent:ChurnResponseAgent"))
    assert result["path"].endswith("agents/example_agent.py")
    assert "class ChurnResponseAgent" in result["source"]
    assert result["issues"] == []


def test_module_source_returns_clean_source_with_no_issues():
    res = client.get("/api/modules/agent/churn_response_agent/source")
    assert res.status_code == 200

    body = res.json()
    assert body["path"].endswith("agents/example_agent.py")
    assert "class ChurnResponseAgent" in body["source"]
    assert body["issues"] == []


def test_module_source_flags_real_lint_issues(tmp_path, monkeypatch):
    from webapp import linting

    bad_file = tmp_path / "bad_module.py"
    bad_file.write_text("import os\nx = 1\n")

    monkeypatch.setattr(linting, "resolve_source_path", lambda entrypoint: bad_file)

    res = client.get("/api/modules/agent/churn_response_agent/source")
    assert res.status_code == 200
    issues = res.json()["issues"]
    assert any(issue["code"] == "F401" for issue in issues)  # unused `os` import


def test_module_source_404s_for_unknown_module():
    res = client.get("/api/modules/agent/does_not_exist/source")
    assert res.status_code == 404


def test_module_source_404s_when_entrypoint_cannot_be_resolved(monkeypatch):
    from webapp import linting

    def boom(entrypoint):
        raise linting.SourceNotFoundError("nope")

    monkeypatch.setattr(linting, "resolve_source_path", boom)
    res = client.get("/api/modules/agent/churn_response_agent/source")
    assert res.status_code == 404
