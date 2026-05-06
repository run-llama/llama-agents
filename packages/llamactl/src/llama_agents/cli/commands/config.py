from __future__ import annotations

from typing import Any

import click
from llama_agents.cli.interactive import is_interactive_session
from llama_agents.cli.output import status
from pydantic import BaseModel, ConfigDict

from ..app import app
from ..options import global_options, output_option, render_output
from .auth import _get_service, _list_projects


class ConfigContext(BaseModel):
    model_config = ConfigDict(extra="forbid")

    environment: str | None
    profile: str | None
    project_id: str | None
    project_name: str | None = None


@app.group(
    invoke_without_command=True,
    help="Show local llamactl configuration",
)
@click.pass_context
@global_options
@output_option
def config(ctx: click.Context, output: str) -> None:
    if ctx.invoked_subcommand is not None:
        return
    context = _build_config_context()
    render_output(context, output, text_renderer=lambda: _render_config_text(context))


@config.command("destroy", hidden=True)
@global_options
def destroy_database() -> None:
    """Destroy the database."""
    from llama_agents.cli.config._config import ConfigManager

    if is_interactive_session() and not click.confirm(
        "Are you sure you want to destroy all of your local logins? This action cannot be undone."
    ):
        return
    ConfigManager(init_database=False).destroy_database()
    status("database destroyed")


@config.command("show-db", hidden=True)
@global_options
def show_database() -> None:
    """Show the database path."""
    path = _get_service().config_manager().db_path
    status(path)


def _build_config_context() -> ConfigContext:
    service = _get_service()
    current_env = service.get_current_environment()
    auth_svc = service.current_auth_service()
    profile = auth_svc.get_current_profile()
    project_id = profile.project_id if profile else None
    return ConfigContext(
        environment=current_env.api_url if current_env else None,
        profile=profile.name if profile else None,
        project_id=project_id,
        project_name=_resolve_project_name(auth_svc, project_id),
    )


def _resolve_project_name(auth_svc: Any, project_id: str | None) -> str | None:
    if project_id is None:
        return None
    try:
        projects = _list_projects(auth_svc)
    except Exception:
        return None
    project = next(
        (candidate for candidate in projects if candidate.project_id == project_id),
        None,
    )
    return project.project_name if project else None


def _render_config_text(context: ConfigContext) -> None:
    project = context.project_id or "(none)"
    if context.project_id and context.project_name:
        project = f"{context.project_id} ({context.project_name})"
    click.echo(f"environment:  {context.environment or '(none)'}")
    click.echo(f"profile:      {context.profile or '(none)'}")
    click.echo(f"project:      {project}")
