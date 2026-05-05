from __future__ import annotations

from importlib import metadata as importlib_metadata
from typing import TYPE_CHECKING

import click
from llama_agents.cli.config.schema import Environment
from llama_agents.cli.interactive import is_interactive_session, select_or_exit
from llama_agents.cli.param_types import EnvironmentType
from llama_agents.cli.styles import WARNING
from packaging import version as packaging_version
from rich import print as rprint

from ..display import EnvDisplay
from ..options import global_options, output_option, render_output
from .auth import auth

if TYPE_CHECKING:
    from llama_agents.cli.config.env_service import EnvService


def _env_service() -> EnvService:
    """Return the shared EnvService instance via a local import.

    This keeps CLI startup light while remaining easy to patch in tests via
    ``llama_agents.cli.config.env_service.service``.
    """
    from ..config.env_service import service

    return service


@auth.group(
    name="env",
    help="Manage environments (control plane API URLs)",
    no_args_is_help=True,
)
@global_options
def env_group() -> None:
    pass


@env_group.command("list")
@global_options
@output_option
def list_environments_cmd(output: str) -> None:
    try:
        service = _env_service()
        envs = service.list_environments()
        current_env = service.get_current_environment()

        if not envs and output == "text":
            rprint(f"[{WARNING}]No environments found[/]")
            return

        current_url = current_env.api_url if current_env else None
        displays = [
            EnvDisplay.from_environment(env, current_url=current_url) for env in envs
        ]
        render_output(displays, output)
    except Exception as e:
        rprint(f"[red]Error: {e}[/red]")
        raise click.Abort()


@env_group.command("add")
@click.argument("api_url", required=False)
@global_options
def add_environment_cmd(api_url: str | None) -> None:
    try:
        service = _env_service()
        if not api_url:
            if not is_interactive_session():
                raise click.ClickException(
                    "Pass <api_url> as an argument. To see existing environments, run: llamactl auth env list"
                )
            current_env = service.get_current_environment()
            entered = click.prompt(
                "Enter control plane API URL",
                default=current_env.api_url if current_env else "",
                show_default=current_env is not None,
            )
            if not entered:
                rprint(f"[{WARNING}]No environment entered[/]")
                return
            api_url = entered.strip()

        if api_url is None:
            raise click.ClickException("API URL is required")
        api_url = api_url.rstrip("/")
        env = service.probe_environment(api_url)
        service.create_or_update_environment(env)
        rprint(
            f"[green]Added environment[/green] {env.api_url} (requires_auth={env.requires_auth}, min_llamactl_version={env.min_llamactl_version or '-'})."
        )
        _maybe_warn_min_version(env.min_llamactl_version)
    except click.ClickException:
        raise
    except Exception as e:
        rprint(f"[red]Failed to add environment: {e}[/red]")
        raise click.Abort()


@env_group.command("delete")
@click.argument("api_url", required=False, type=EnvironmentType())
@global_options
def delete_environment_cmd(api_url: str | None) -> None:
    try:
        service = _env_service()
        if not api_url:
            result = _select_environment(
                service.list_environments(),
                service.get_current_environment(),
                "Select environment to delete",
            )
            api_url = result.api_url

        if api_url is None:
            raise click.ClickException("API URL is required")
        api_url = api_url.rstrip("/")
        deleted = service.delete_environment(api_url)
        if not deleted:
            raise click.ClickException(f"Environment '{api_url}' not found")
        rprint(
            f"[green]Deleted environment[/green] {api_url} and all associated profiles"
        )
    except click.ClickException:
        raise
    except Exception as e:
        rprint(f"[red]Failed to delete environment: {e}[/red]")
        raise click.Abort()


@env_group.command("switch")
@click.argument("api_url", required=False, type=EnvironmentType())
@global_options
def switch_environment_cmd(api_url: str | None) -> None:
    try:
        service = _env_service()
        selected_url = api_url

        if not selected_url:
            result = _select_environment(
                service.list_environments(),
                service.get_current_environment(),
                "Select environment",
            )
            selected_url = result.api_url

        selected_url = selected_url.rstrip("/")

        # Ensure environment exists and switch
        env = service.switch_environment(selected_url)
        try:
            env = service.auto_update_env(env)
        except Exception as e:
            rprint(f"[{WARNING}]Failed to resolve environment: {e}[/]")
            return
        service.current_auth_service().select_any_profile()
        rprint(f"[green]Switched to environment[/green] {env.api_url}")
        _maybe_warn_min_version(env.min_llamactl_version)
    except click.ClickException:
        raise
    except Exception as e:
        rprint(f"[red]Failed to switch environment: {e}[/red]")
        raise click.Abort()


def _get_cli_version() -> str | None:
    try:
        return importlib_metadata.version("llamactl")
    except Exception:
        return None


def _maybe_warn_min_version(min_required: str | None) -> None:
    if not min_required:
        return
    current = _get_cli_version()
    if not current:
        return
    try:
        if packaging_version.parse(current) < packaging_version.parse(min_required):
            rprint(
                f"[{WARNING}]Warning:[/] This environment requires llamactl >= [bold]{min_required}[/bold], you have [bold]{current}[/bold]."
            )
    except Exception:
        # If packaging is not available or parsing fails, skip strict comparison
        pass


def _select_environment(
    envs: list[Environment],
    current_env: Environment,
    message: str = "Select environment",
) -> Environment:
    if not envs:
        raise click.ClickException(
            "No environments found. This is a bug and shouldn't happen."
        )
    items = []
    current_idx = 0
    for i, env in enumerate(envs):
        label = env.api_url
        if env.api_url == current_env.api_url:
            label += " [current]"
            current_idx = i
        items.append((env, label))
    return select_or_exit(
        items,
        message,
        hint_flag="<api_url>",
        hint_command="llamactl auth env list",
        selected=current_idx,
    )
