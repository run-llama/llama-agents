from __future__ import annotations

import os
import subprocess
from pathlib import Path

import click
from llama_agents.cli.output import status

from ..app import app
from ..options import global_options


@app.group(
    help="LlamaAgents x Bedrock AgentCore deployment utilities",
    no_args_is_help=True,
)
@global_options
def agentcore() -> None:
    """LlamaAgents x Bedrock AgentCore deployment utilities"""
    pass


@agentcore.command("run", help="Run AgentCore server")
def run() -> None:
    start_app()


@agentcore.command(
    "test",
    help="Run AgentCore server locally with a local SQLite store (no AWS required). "
    "Send requests to POST http://localhost:8080/invocations",
)
def test() -> None:
    start_app(local=True)


@agentcore.command(
    "export",
    help="Export generated code to a `.agentcore` folder in the current working directory.",
)
def export() -> None:
    export_generated_entrypoint_code()


def export_generated_entrypoint_code() -> None:
    from llama_agents.agentcore.export import (
        export_generated_entrypoint_code as _export,
    )

    _export()


def start_app_in_target_venv(path: Path, *, local: bool = False) -> None:
    from llama_agents.appserver.process_utils import run_process
    from llama_agents.appserver.workflow_loader import (
        _exclude_venv_warning,
    )

    args = [
        "uv",
        "run",
        "--no-progress",
        "python",
        "-m",
        "llama_agents.agentcore.main",
    ]
    if local:
        args.append("--local")
    else:
        args.append("--run")

    run_process(
        args,
        cwd=path,
        env=os.environ.copy(),
        line_transform=_exclude_venv_warning,
    )


def start_app(local: bool = False) -> None:
    from llama_agents.appserver.deployment_config_parser import get_deployment_config
    from llama_agents.appserver.settings import configure_settings, settings
    from llama_agents.appserver.workflow_loader import (
        inject_appserver_into_target,
    )

    configure_settings(deployment_file_path=Path.cwd(), app_root=Path.cwd())
    cfg = get_deployment_config()
    inject_appserver_into_target(cfg, source_root=Path.cwd())
    base_dir = Path.cwd()
    path = settings.resolved_config_parent.relative_to(base_dir)
    try:
        start_app_in_target_venv(path, local=local)
    except subprocess.CalledProcessError as exc:
        status("failed to run agentcore; see errors above")
        raise click.exceptions.Exit(exc.returncode)
