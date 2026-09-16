from __future__ import annotations

import json
from pathlib import Path
from types import SimpleNamespace
from typing import Any

import pytest
from workflows import (
    Workflow,
    step,
)
from workflows.events import StartEvent, StopEvent


def test_preflight_validate_success(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    # Import late to patch within module namespace
    import llama_agents.appserver.app as app_mod

    # Stub config/env setup to avoid real IO
    monkeypatch.setattr(app_mod, "get_deployment_config", lambda: SimpleNamespace())
    monkeypatch.setattr(app_mod, "load_environment_variables", lambda *a, **k: None)
    monkeypatch.setattr(app_mod, "validate_required_env_vars", lambda *a, **k: None)

    # Provide empty workflows and stub Deployment
    monkeypatch.setattr(
        app_mod,
        "load_workflow_server",
        lambda cfg: SimpleNamespace(
            get_workflows=lambda: {},
            serializer=None,
            additional_events={},
        ),
    )
    monkeypatch.setattr(
        app_mod, "Deployment", lambda workflows, **options: SimpleNamespace()
    )

    # Should not raise
    app_mod.preflight_validate(cwd=tmp_path, deployment_file=tmp_path / "deploy.yaml")


def test_preflight_validate_collects_errors(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    import llama_agents.appserver.app as app_mod

    monkeypatch.setattr(app_mod, "get_deployment_config", lambda: SimpleNamespace())
    monkeypatch.setattr(app_mod, "load_environment_variables", lambda *a, **k: None)
    monkeypatch.setattr(app_mod, "validate_required_env_vars", lambda *a, **k: None)
    monkeypatch.setattr(
        app_mod, "Deployment", lambda workflows, **options: SimpleNamespace()
    )

    class BadWorkflow:
        def _validate(self) -> None:
            raise ValueError("boom")

    monkeypatch.setattr(
        app_mod,
        "load_workflow_server",
        lambda cfg: SimpleNamespace(
            get_workflows=lambda: {"svc": BadWorkflow()},
            serializer=None,
            additional_events={},
        ),
    )

    with pytest.raises(app_mod.PreflightValidationError) as ei:
        app_mod.preflight_validate(
            cwd=tmp_path, deployment_file=tmp_path / "deploy.yaml"
        )
    # Ensure error content is reflected
    assert isinstance(ei.value, app_mod.PreflightValidationError)
    assert ("svc", "boom") in ei.value.errors


def test_start_preflight_in_target_venv_invokes_uv(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    import llama_agents.appserver.app as app_mod

    # Simulate settings resolving to project subdir under provided cwd
    proj = tmp_path / "proj"
    proj.mkdir()
    # Replace settings object with minimal namespace
    monkeypatch.setattr(
        app_mod, "settings", SimpleNamespace(resolved_config_parent=proj)
    )

    # No-op configure_settings
    monkeypatch.setattr(app_mod, "configure_settings", lambda *a, **k: None)

    captured: dict[str, Any] = {}

    def _fake_run_process(
        args: list[str],
        *,
        cwd: Path,
        env: dict | None = None,
        line_transform: Any | None = None,
    ) -> int:
        captured["args"] = args
        captured["cwd"] = cwd
        return 0

    monkeypatch.setattr(app_mod, "run_process", _fake_run_process)

    df = tmp_path / "deploy.yaml"
    app_mod.start_preflight_in_target_venv(cwd=tmp_path, deployment_file=df)

    assert captured["cwd"] == proj.relative_to(tmp_path)
    assert captured["args"][:6] == [
        "uv",
        "run",
        "--no-progress",
        "python",
        "-m",
        "llama_agents.appserver.app",
    ]
    assert "--preflight" in captured["args"]
    assert "--deployment-file" in captured["args"]


def test_start_preflight_in_target_venv_skip_env_validation(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    import llama_agents.appserver.app as app_mod

    proj = tmp_path / "proj"
    proj.mkdir()
    monkeypatch.setattr(
        app_mod, "settings", SimpleNamespace(resolved_config_parent=proj)
    )
    monkeypatch.setattr(app_mod, "configure_settings", lambda *a, **k: None)

    captured: dict[str, Any] = {}

    def _fake_run_process(
        args: list[str],
        *,
        cwd: Path,
        env: dict | None = None,
        line_transform: Any | None = None,
    ) -> int:
        captured["args"] = args
        return 0

    monkeypatch.setattr(app_mod, "run_process", _fake_run_process)

    df = tmp_path / "deploy.yaml"
    app_mod.start_preflight_in_target_venv(
        cwd=tmp_path, deployment_file=df, skip_env_validation=True
    )

    assert "--skip-env-validation" in captured["args"]


def test_export_json_graph_strips_event_type_and_writes_file(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    import llama_agents.appserver.app as app_mod

    # Stub config/env setup to avoid real IO
    monkeypatch.setattr(app_mod, "get_deployment_config", lambda: SimpleNamespace())
    monkeypatch.setattr(app_mod, "load_environment_variables", lambda *a, **k: None)
    monkeypatch.setattr(app_mod, "validate_required_env_vars", lambda *a, **k: None)

    # Use a real, minimal LlamaIndex workflow instead of mocking the graph.
    class _SimpleWorkflow(Workflow):
        @step
        async def start(self, ev: StartEvent) -> StopEvent:
            return StopEvent(result=ev.input)

    wf = _SimpleWorkflow(timeout=5, verbose=False)
    monkeypatch.setattr(app_mod, "load_workflows", lambda cfg: {"my_workflow": wf})

    output = tmp_path / "graph.json"
    app_mod.export_json_graph(
        cwd=tmp_path,
        deployment_file=tmp_path / "deploy.yaml",
        output=output,
    )

    data = json.loads(output.read_text(encoding="utf-8"))
    assert isinstance(data, dict)
    assert "my_workflow" in data
    assert list(data.keys()) == ["my_workflow"]
    assert isinstance(data["my_workflow"], dict)
    assert isinstance(data["my_workflow"].get("nodes"), list)
    assert isinstance(data["my_workflow"].get("edges"), list)


def test_start_export_json_graph_in_target_venv_invokes_uv(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    import llama_agents.appserver.app as app_mod

    proj = tmp_path / "proj"
    proj.mkdir()
    monkeypatch.setattr(
        app_mod, "settings", SimpleNamespace(resolved_config_parent=proj)
    )
    monkeypatch.setattr(app_mod, "configure_settings", lambda *a, **k: None)

    captured: dict[str, Any] = {}

    def _fake_run_process(
        args: list[str],
        *,
        cwd: Path,
        env: dict | None = None,
        line_transform: Any | None = None,
    ) -> int:
        captured["args"] = args
        captured["cwd"] = cwd
        return 0

    monkeypatch.setattr(app_mod, "run_process", _fake_run_process)

    df = tmp_path / "deploy.yaml"
    output = tmp_path / "graph.json"
    app_mod.start_export_json_graph_in_target_venv(
        cwd=tmp_path,
        deployment_file=df,
        output=output,
    )

    assert captured["cwd"] == proj.relative_to(tmp_path)
    assert captured["args"][:6] == [
        "uv",
        "run",
        "--no-progress",
        "python",
        "-m",
        "llama_agents.appserver.app",
    ]
    assert "--export-json-graph" in captured["args"]
    assert "--deployment-file" in captured["args"]
    assert "--export-output" in captured["args"]
