# SPDX-License-Identifier: MIT
# Copyright (c) 2026 LlamaIndex Inc.
from __future__ import annotations

from contextlib import asynccontextmanager
from dataclasses import dataclass
from typing import TYPE_CHECKING, Any, AsyncGenerator

import click
from llama_agents.cli.env_settings import LlamactlEnvSettings, read_env_settings
from rich import print as rprint

if TYPE_CHECKING:
    from llama_agents.core.client.manage_client import ControlPlaneClient, ProjectClient


_ENV_VAR_AUTH_PROFILE_WARNING_EMITTED = False


@dataclass(frozen=True)
class _AuthContext:
    base_url: str
    project_id: str
    api_key: str | None
    auth_middleware: Any | None


def _env_auth_context_or_none(
    settings: LlamactlEnvSettings,
    project_id_override: str | None,
) -> _AuthContext | None:
    if settings.cloud_auth_disabled:
        return None

    if not settings.has_complete_cloud_auth:
        return None

    api_key = settings.llama_cloud_api_key
    project_id = settings.llama_deploy_project_id
    assert api_key is not None
    assert project_id is not None

    return _AuthContext(
        base_url=settings.normalized_base_url,
        project_id=project_id_override or project_id,
        api_key=api_key,
        auth_middleware=None,
    )


def _profile_auth_context_or_none(
    project_id_override: str | None,
) -> _AuthContext | None:
    from llama_agents.cli.config.env_service import service

    auth_svc = service.current_auth_service()
    profile = auth_svc.get_current_profile()
    if profile is None:
        return None

    return _AuthContext(
        base_url=profile.api_url.rstrip("/"),
        project_id=project_id_override or profile.project_id,
        api_key=profile.api_key,
        auth_middleware=auth_svc.auth_middleware(),
    )


def _auth_context_or_none(
    project_id_override: str | None = None,
) -> _AuthContext | None:
    settings = read_env_settings()
    context = _env_auth_context_or_none(settings, project_id_override)
    if context is not None:
        _warn_if_env_auth_overrides_profile(settings)
        return context
    return _profile_auth_context_or_none(project_id_override)


def _warn_if_env_auth_overrides_profile(settings: LlamactlEnvSettings) -> None:
    global _ENV_VAR_AUTH_PROFILE_WARNING_EMITTED

    if _ENV_VAR_AUTH_PROFILE_WARNING_EMITTED:
        return
    if settings.completion_active:
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

    context = _auth_context_or_none()
    if context is not None:
        return ControlPlaneClient(
            context.base_url, context.api_key, context.auth_middleware
        )

    # Fallback: allow env-scoped client construction for env operations
    from llama_agents.cli.config.env_service import service

    env = service.get_current_environment()
    resolved_base_url = env.api_url.rstrip("/")
    return ControlPlaneClient(resolved_base_url)


def get_project_client(project_id_override: str | None = None) -> ProjectClient:
    """Return a ProjectClient bound to env auth or the active profile.

    If ``project_id_override`` is provided, the client uses that project ID
    instead of the env/profile default. This mirrors ``kubectl -n <ns>``.
    """
    from llama_agents.core.client.manage_client import ProjectClient

    context = _auth_context_or_none(project_id_override)
    if context is not None:
        return ProjectClient(
            context.base_url,
            context.project_id,
            context.api_key,
            context.auth_middleware,
        )

    from llama_agents.cli.config.env_service import service

    auth_svc = service.current_auth_service()
    rprint("\n[bold red]No profile configured![/bold red]")
    rprint("\nTo get started, create a profile with:")
    if auth_svc.env.requires_auth:
        rprint("[cyan]llamactl auth login[/cyan]")
    else:
        rprint("[cyan]llamactl auth token[/cyan]")
    raise SystemExit(1)


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
