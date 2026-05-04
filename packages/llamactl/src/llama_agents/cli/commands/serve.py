from __future__ import annotations

import asyncio
import logging
import os
from pathlib import Path
from typing import TYPE_CHECKING, Literal

import click
from click.exceptions import Abort, Exit
from llama_agents.cli.commands.auth import validate_authenticated_profile
from llama_agents.cli.env_settings import read_env_settings
from llama_agents.cli.interactive import select_or_exit
from llama_agents.cli.options import (
    interactive_option,
    native_tls_option,
)
from llama_agents.cli.styles import WARNING
from llama_agents.cli.utils.capabilities import probe_organizations_support
from llama_agents.cli.utils.redact import redact_api_key
from llama_agents.core.config import DEFAULT_DEPLOYMENT_FILE_PATH
from llama_agents.core.deployment_config import (
    read_deployment_config_from_git_root_or_cwd,
)
from llama_agents.core.schema.projects import OrgSummary, ProjectSummary
from rich import print as rprint

from ..app import app

if TYPE_CHECKING:
    from llama_agents.cli.config.schema import Auth

logger = logging.getLogger(__name__)
_ClickPath = getattr(click, "Path")


@app.command(
    "serve",
    help="Serve a LlamaDeploy app locally for development and testing",
)
@click.argument(
    "deployment_file",
    required=False,
    default=DEFAULT_DEPLOYMENT_FILE_PATH,
    type=_ClickPath(dir_okay=True, resolve_path=True, path_type=Path),
)
@click.option(
    "--no-install", is_flag=True, help="Skip installing python and js dependencies"
)
@click.option(
    "--no-reload", is_flag=True, help="Skip reloading the API server on code changes"
)
@click.option("--no-open-browser", is_flag=True, help="Skip opening the browser")
@click.option(
    "--preview",
    is_flag=True,
    help="Preview mode pre-builds the UI to static files, like a production build",
)
@click.option("--port", type=int, help="The port to run the API server on")
@click.option("--ui-port", type=int, help="The port to run the UI proxy server on")
@click.option(
    "--log-level",
    type=click.Choice(
        ["DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"], case_sensitive=False
    ),
    help="The log level to run the API server at",
)
@click.option(
    "--log-format",
    type=click.Choice(["console", "json"], case_sensitive=False),
    help="The format to use for logging",
)
@click.option(
    "--persistence",
    type=click.Choice(["memory", "local", "cloud"]),
    help="The persistence mode to use for the workflow server",
)
@click.option(
    "--local-persistence-path",
    type=_ClickPath(dir_okay=True, resolve_path=True, path_type=Path),
    help="The path to the sqlite database to use for the workflow server if using local persistence",
)
@click.option(
    "--host",
    type=str,
    help="The host to run the API server on. Default is 127.0.0.1. Use 0.0.0.0 to allow remote access.",
)
@interactive_option
@native_tls_option
def serve(
    deployment_file: Path,
    no_install: bool,
    no_reload: bool,
    no_open_browser: bool,
    preview: bool,
    port: int | None = None,
    ui_port: int | None = None,
    log_level: str | None = None,
    log_format: str | None = None,
    persistence: Literal["memory", "local", "cloud"] | None = None,
    local_persistence_path: Path | None = None,
    host: str | None = None,
    interactive: bool = False,
) -> None:
    """Run llama_deploy API Server in the foreground. Reads the deployment configuration from the current directory. Can optionally specify a deployment file path."""
    if not deployment_file.exists():
        rprint(f"[red]Deployment file '{deployment_file}' not found[/red]")
        raise click.Abort()

    # Early check: appserver requires a pyproject.toml in the config directory
    config_dir = deployment_file if deployment_file.is_dir() else deployment_file.parent
    if not (config_dir / "pyproject.toml").exists():
        rprint(
            "[red]No pyproject.toml found at[/red] "
            f"[bold]{config_dir}[/bold].\n"
            "Add a pyproject.toml to your project and re-run 'llamactl serve'."
        )
        raise click.Abort()

    try:
        # Pre-check: if the template requires llama cloud access, ensure credentials
        _maybe_inject_llama_cloud_credentials(
            deployment_file, interactive, require_cloud=persistence == "cloud"
        )

        # Defer heavy appserver imports until the `serve` command is actually invoked
        from llama_agents.appserver.app import (
            prepare_server,
            start_server_in_target_venv,
        )
        from llama_agents.appserver.deployment_config_parser import (
            get_deployment_config,
        )

        prepare_server(
            deployment_file=deployment_file,
            install=not no_install,
            build=preview,
        )
        deployment_config = get_deployment_config()
        _print_connection_summary()
        start_server_in_target_venv(
            cwd=Path.cwd(),
            deployment_file=deployment_file,
            proxy_ui=not preview,
            reload=not no_reload,
            open_browser=not no_open_browser,
            port=port,
            ui_port=ui_port,
            log_level=log_level.upper() if log_level else None,
            log_format=log_format.lower() if log_format else None,
            persistence=persistence if persistence else "local",
            local_persistence_path=str(local_persistence_path)
            if local_persistence_path and persistence == "local"
            else None,
            cloud_persistence_name=f"_public:serve_workflows_{deployment_config.name}"
            if persistence == "cloud"
            else None,
            host=host,
        )

    except (Exit, Abort):
        raise

    except KeyboardInterrupt:
        logger.debug("Shutting down...")

    except Exception as e:
        rprint(f"[red]Error: {e}[/red]")
        raise click.Abort()


def _set_env_vars_from_profile(profile: Auth) -> None:
    if profile.api_key:
        _set_env_vars(profile.api_key, profile.api_url)
    _set_project_id_from_profile(profile)


def _set_env_vars_from_env(env_vars: dict[str, str]) -> None:
    key = env_vars.get("LLAMA_CLOUD_API_KEY")
    url = env_vars.get("LLAMA_CLOUD_BASE_URL", "https://api.cloud.llamaindex.ai")
    # Also propagate project id if present in the environment
    _set_project_id_from_env(env_vars)
    if key:
        _set_env_vars(key, url)


def _set_env_vars(key: str, url: str) -> None:
    os.environ["LLAMA_CLOUD_API_KEY"] = key
    os.environ["LLAMA_CLOUD_BASE_URL"] = url
    # Inject prefixed copies for local dev. PUBLIC_ is the canonical convention
    # templates should read; VITE_ and NEXT_PUBLIC_ are kept for framework
    # defaults. These are only set by the CLI so production deployments must
    # opt in explicitly rather than always exposing the token to client code.
    for prefix in ["PUBLIC_", "VITE_", "NEXT_PUBLIC_"]:
        os.environ[f"{prefix}LLAMA_CLOUD_API_KEY"] = key
        os.environ[f"{prefix}LLAMA_CLOUD_BASE_URL"] = url


def _set_project_id_from_env(env_vars: dict[str, str]) -> None:
    project_id = env_vars.get("LLAMA_AGENTS_PROJECT_ID") or env_vars.get(
        "LLAMA_DEPLOY_PROJECT_ID"
    )
    if project_id:
        _set_project_id(project_id)


def _set_project_id_from_profile(profile: Auth) -> None:
    if profile.project_id:
        _set_project_id(profile.project_id)


def _set_project_id(project_id: str) -> None:
    os.environ["LLAMA_AGENTS_PROJECT_ID"] = project_id
    os.environ["LLAMA_DEPLOY_PROJECT_ID"] = project_id


def _maybe_inject_llama_cloud_credentials(
    deployment_file: Path, interactive: bool, require_cloud: bool
) -> None:
    """If the deployment config indicates Llama Cloud usage, ensure LLAMA_CLOUD_API_KEY is set.

    Behavior:
    - If LLAMA_CLOUD_API_KEY is already set, use it.
    - Else, try to read current profile's api_key and inject.
    - If no profile/api_key and session is interactive, prompt to log in and inject afterward.
    - If user declines or session is non-interactive, warn that deployment may not work.
    """
    from llama_agents.appserver.workflow_loader import parse_environment_variables
    from llama_agents.cli.config.env_service import service

    # Read config directly to avoid cached global settings
    try:
        config = read_deployment_config_from_git_root_or_cwd(
            Path.cwd(), deployment_file
        )
    except Exception:
        rprint(
            "[red]Error: Could not read a deployment config. This doesn't appear to be a valid llama-deploy project.[/red]"
        )
        raise click.Abort()

    if not config.llama_cloud and not require_cloud:
        return

    vars = parse_environment_variables(
        config, deployment_file.parent if deployment_file.is_file() else deployment_file
    )

    # Ensure project id is available to the app and UI processes
    _set_project_id_from_env({**os.environ, **vars})

    settings = read_env_settings()
    existing = None
    if not settings.cloud_auth_disabled:
        existing = settings.llama_cloud_api_key or vars.get("LLAMA_CLOUD_API_KEY")
    if existing:
        _set_env_vars_from_env({**os.environ, **vars})
        if interactive:
            if not read_env_settings().llama_agents_project_id:
                _maybe_select_project_for_env_key()
        return

    env = service.get_current_environment()
    if not env.requires_auth:
        rprint(
            f"[{WARNING}]Warning: This app requires Llama Cloud authentication, and no LLAMA_CLOUD_API_KEY is present. The app may not work.[/]"
        )
        return

    auth_svc = service.current_auth_service()
    profile = auth_svc.get_current_profile()
    if profile and profile.api_key:
        _set_env_vars_from_profile(profile)
        return

    # No key available; consider prompting if interactive
    if interactive:
        should_login = click.confirm(
            "This deployment requires Llama Cloud. Login now to inject credentials? Otherwise the app may not work.",
            default=True,
        )
        if should_login:
            authed = validate_authenticated_profile(True)
            if authed.api_key:
                _set_env_vars_from_profile(authed)
                return
        rprint(
            f"[{WARNING}]Warning: No Llama Cloud credentials configured. The app may not work.[/]"
        )
        return

    # Non-interactive session
    rprint(
        f"[{WARNING}]Warning: LLAMA_CLOUD_API_KEY is not set and no logged-in profile was found. The app may not work.[/]"
    )


def _maybe_select_project_for_env_key() -> None:
    """When using an env API key, ensure LLAMA_AGENTS_PROJECT_ID is set.

    If more than one project exists, prompt the user to select one.
    """
    from llama_agents.core.client.manage_client import ControlPlaneClient

    settings = read_env_settings()
    api_key = settings.llama_cloud_api_key
    base_url = settings.normalized_base_url
    if not api_key:
        return
    try:
        supports_organizations = probe_organizations_support()

        async def _run() -> tuple[OrgSummary | None, list[ProjectSummary]]:
            async with ControlPlaneClient.ctx(base_url, api_key, None) as client:
                org: OrgSummary | None = None
                if supports_organizations:
                    organizations = await client.list_organizations()
                    org = next(
                        (o for o in organizations if o.is_default),
                        organizations[0] if organizations else None,
                    )
                org_id = org.org_id if org is not None else None
                projects = await client.list_projects(org_id=org_id)
                return org, projects

        org, projects = asyncio.run(_run())
        if not projects:
            return
        if len(projects) == 1:
            _set_project_id(projects[0].project_id)
            return

        if org is not None:
            rprint(f"Projects for organization [bold]{org.org_name}[/]")

        # Multiple: prompt selection
        choice = select_or_exit(
            [
                (p.project_id, f"{p.project_name} ({p.deployment_count} deployments)")
                for p in projects
            ],
            "Select a project",
            hint_flag="LLAMA_AGENTS_PROJECT_ID",
            hint_command="llamactl auth project <project_id>",
        )
        _set_project_id(choice)
    except Exception:
        # Best-effort; if we fail to list, do nothing
        pass


def _print_connection_summary() -> None:
    settings = read_env_settings()
    if not settings.has_cloud_connection_summary:
        return

    base_url = settings.llama_cloud_base_url
    project_id = settings.llama_agents_project_id
    api_key = settings.llama_cloud_api_key
    redacted = redact_api_key(api_key)
    env_text = base_url or "-"
    proj_text = project_id or "-"
    rprint(
        f"Connecting to environment: [bold]{env_text}[/], project: [bold]{proj_text}[/], api key: [bold]{redacted}[/]"
    )
