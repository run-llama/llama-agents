# SPDX-License-Identifier: MIT
# Copyright (c) 2026 LlamaIndex Inc.
from __future__ import annotations

import os
from contextlib import asynccontextmanager
from typing import TYPE_CHECKING, AsyncGenerator

import click
from llama_agents.cli.config.schema import DEFAULT_ENVIRONMENT
from rich import print as rprint

if TYPE_CHECKING:
    from llama_agents.core.client.manage_client import ControlPlaneClient, ProjectClient


_ENV_VAR_AUTH_PROFILE_WARNING_EMITTED = False
_MISSING_ENV_PROJECT_MESSAGE = (
    "LLAMA_CLOUD_API_KEY is set but LLAMA_DEPLOY_PROJECT_ID is missing. "
    "Set it or pass --project."
)


def _env_var_client_or_none() -> tuple[str, str] | None:
    if os.environ.get("LLAMA_CLOUD_USE_PROFILE") == "1":
        return None

    api_key = os.environ.get("LLAMA_CLOUD_API_KEY")
    if not api_key:
        return None

    base_url = os.environ.get("LLAMA_CLOUD_BASE_URL") or DEFAULT_ENVIRONMENT.api_url
    return base_url.rstrip("/"), api_key


def _warn_if_env_auth_overrides_profile() -> None:
    global _ENV_VAR_AUTH_PROFILE_WARNING_EMITTED

    if _ENV_VAR_AUTH_PROFILE_WARNING_EMITTED:
        return
    if os.environ.get("_LLAMACTL_COMPLETE"):
        return

    from llama_agents.cli.config.env_service import service

    try:
        profile = service.current_auth_service().get_current_profile()
    except Exception:
        return
    if not profile:
        return

    click.echo(
        "Using LLAMA_CLOUD_API_KEY from environment "
        f"(overriding profile '{profile.name}'). "
        "Set LLAMA_CLOUD_USE_PROFILE=1 to use the profile instead.",
        err=True,
    )
    _ENV_VAR_AUTH_PROFILE_WARNING_EMITTED = True


def get_control_plane_client() -> ControlPlaneClient:
    from llama_agents.core.client.manage_client import ControlPlaneClient

    env_auth = _env_var_client_or_none()
    if env_auth:
        _warn_if_env_auth_overrides_profile()
        base_url, api_key = env_auth
        return ControlPlaneClient(base_url, api_key, None)

    from llama_agents.cli.config.env_service import service

    auth_svc = service.current_auth_service()
    profile = auth_svc.get_current_profile()
    if profile:
        resolved_base_url = profile.api_url.rstrip("/")
        resolved_api_key = profile.api_key
        return ControlPlaneClient(
            resolved_base_url, resolved_api_key, auth_svc.auth_middleware()
        )

    # Fallback: allow env-scoped client construction for env operations
    env = service.get_current_environment()
    resolved_base_url = env.api_url.rstrip("/")
    return ControlPlaneClient(resolved_base_url)


def get_project_client(project_id_override: str | None = None) -> ProjectClient:
    """Return a ProjectClient bound to env auth or the active profile.

    If ``project_id_override`` is provided, the client uses that project ID
    instead of the env/profile default. This mirrors ``kubectl -n <ns>``.
    """
    from llama_agents.core.client.manage_client import ProjectClient

    env_auth = _env_var_client_or_none()
    if env_auth:
        project_id = project_id_override or os.environ.get("LLAMA_DEPLOY_PROJECT_ID")
        if not project_id:
            raise SystemExit(_MISSING_ENV_PROJECT_MESSAGE)
        _warn_if_env_auth_overrides_profile()
        base_url, api_key = env_auth
        return ProjectClient(base_url, project_id, api_key, None)

    from llama_agents.cli.config.env_service import service

    auth_svc = service.current_auth_service()
    profile = auth_svc.get_current_profile()
    if not profile:
        rprint("\n[bold red]No profile configured![/bold red]")
        rprint("\nTo get started, create a profile with:")
        if auth_svc.env.requires_auth:
            rprint("[cyan]llamactl auth login[/cyan]")
        else:
            rprint("[cyan]llamactl auth token[/cyan]")
        raise SystemExit(1)
    project_id = project_id_override or profile.project_id
    return ProjectClient(
        profile.api_url, project_id, profile.api_key, auth_svc.auth_middleware()
    )


@asynccontextmanager
async def project_client_context(
    project_id_override: str | None = None,
) -> AsyncGenerator[ProjectClient, None]:
    client = get_project_client(project_id_override=project_id_override)
    try:
        yield client
    finally:
        try:
            await client.aclose()
        except Exception:
            pass
