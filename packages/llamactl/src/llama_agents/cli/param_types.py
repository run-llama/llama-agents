# SPDX-License-Identifier: MIT
# Copyright (c) 2026 LlamaIndex Inc.
from __future__ import annotations

import asyncio
from concurrent.futures import ThreadPoolExecutor
from typing import Any

import click
from click.shell_completion import CompletionItem
from llama_agents.cli.templates import ALL_TEMPLATES


def _safe_fetch(fn: Any, timeout: float = 2.0) -> list[Any]:
    """Run a fetch function in a thread with a timeout. Returns [] on failure."""
    pool = ThreadPoolExecutor(max_workers=1)
    try:
        future = pool.submit(fn)
        return future.result(timeout=timeout)
    except (Exception, SystemExit):
        return []
    finally:
        pool.shutdown(wait=False)


def _fetch_deployments(
    project_id_override: str | None = None,
) -> list[CompletionItem]:
    from llama_agents.cli.client import get_project_client

    client = get_project_client(project_id_override=project_id_override)
    deployments = asyncio.run(client.list_deployments())
    return [CompletionItem(d.id) for d in deployments]


def _fetch_projects() -> list[CompletionItem]:
    from llama_agents.cli.client import get_control_plane_client

    client = get_control_plane_client()
    projects = asyncio.run(client.list_projects())
    return [
        CompletionItem(
            p.project_id,
            help=f"{p.project_name} ({p.deployment_count} deployments)",
        )
        for p in projects
    ]


def _fetch_organizations() -> list[CompletionItem]:
    from llama_agents.cli.client import get_control_plane_client

    client = get_control_plane_client()
    organizations = asyncio.run(client.list_organizations())
    return [
        CompletionItem(
            o.org_id,
            help=f"{o.org_name}{' (default)' if o.is_default else ''}",
        )
        for o in organizations
    ]


def _fetch_deployment_history(
    deployment_id: str, project_id_override: str | None = None
) -> list[CompletionItem]:
    from llama_agents.cli.client import get_project_client

    client = get_project_client(project_id_override=project_id_override)

    async def _fetch() -> Any:
        return await client.get_deployment_history(deployment_id)

    history = asyncio.run(_fetch())
    return [
        CompletionItem(item.git_sha, help=item.released_at.isoformat())
        for item in history.history
    ]


def _filter(items: list[CompletionItem], incomplete: str) -> list[CompletionItem]:
    lower = incomplete.lower()
    return [item for item in items if item.value.lower().startswith(lower)]


class DeploymentType(click.ParamType):
    name = "deployment"

    def shell_complete(
        self, ctx: click.Context, param: click.Parameter, incomplete: str
    ) -> list[CompletionItem]:
        project_id_override = ctx.params.get("project")
        return _filter(
            _safe_fetch(lambda: _fetch_deployments(project_id_override)),
            incomplete,
        )


class ProfileType(click.ParamType):
    name = "profile"

    def shell_complete(
        self, ctx: click.Context, param: click.Parameter, incomplete: str
    ) -> list[CompletionItem]:
        def _fetch() -> list[CompletionItem]:
            from llama_agents.cli.config.env_service import service

            auth_svc = service.current_auth_service()
            profiles = auth_svc.list_profiles()
            return [CompletionItem(p.name, help=p.api_url) for p in profiles]

        return _filter(_safe_fetch(_fetch), incomplete)


class ProjectType(click.ParamType):
    name = "project"

    def shell_complete(
        self, ctx: click.Context, param: click.Parameter, incomplete: str
    ) -> list[CompletionItem]:
        return _filter(_safe_fetch(_fetch_projects), incomplete)


class OrgType(click.ParamType):
    name = "org"

    def shell_complete(
        self, ctx: click.Context, param: click.Parameter, incomplete: str
    ) -> list[CompletionItem]:
        return _filter(_safe_fetch(_fetch_organizations), incomplete)


class EnvironmentType(click.ParamType):
    name = "environment"

    def shell_complete(
        self, ctx: click.Context, param: click.Parameter, incomplete: str
    ) -> list[CompletionItem]:
        def _fetch() -> list[CompletionItem]:
            from llama_agents.cli.config.env_service import service

            envs = service.list_environments()
            current = service.get_current_environment()
            return [
                CompletionItem(
                    e.api_url,
                    help="(current)" if e.api_url == current.api_url else "",
                )
                for e in envs
            ]

        return _filter(_safe_fetch(_fetch), incomplete)


class TemplateType(click.ParamType):
    name = "template"

    def shell_complete(
        self, ctx: click.Context, param: click.Parameter, incomplete: str
    ) -> list[CompletionItem]:
        return _filter(
            [CompletionItem(t.id, help=t.description) for t in ALL_TEMPLATES],
            incomplete,
        )


class GitShaType(click.ParamType):
    name = "git_sha"

    def shell_complete(
        self, ctx: click.Context, param: click.Parameter, incomplete: str
    ) -> list[CompletionItem]:
        deployment_id = ctx.params.get("deployment_id")
        if not deployment_id:
            return []
        project_id_override = ctx.params.get("project")
        return _filter(
            _safe_fetch(
                lambda: _fetch_deployment_history(deployment_id, project_id_override)
            ),
            incomplete,
        )
