# SPDX-License-Identifier: MIT
# Copyright (c) 2026 LlamaIndex Inc.

from __future__ import annotations

from typing import Any

import click
from llama_agents.cli.interactive import select_or_exit
from llama_agents.cli.output import status, warning
from llama_agents.cli.param_types import OrgType
from llama_agents.cli.utils.capabilities import probe_organizations_support

from ..app import app
from ..display import OrgDisplay
from ..options import global_options, render_output, simple_output_option
from .auth import (
    _get_service,
    _list_organizations,
    _list_projects,
    validate_authenticated_profile,
)


@app.group(
    help="Inspect organizations.",
    no_args_is_help=True,
)
@global_options
def organizations() -> None:
    pass


def _active_org_id(
    auth_svc: Any, *, current_project_id: str, fallback_org_id: str | None
) -> str | None:
    try:
        projects = _list_projects(auth_svc)
    except Exception:
        return fallback_org_id
    active_project = next(
        (project for project in projects if project.project_id == current_project_id),
        None,
    )
    return active_project.org_id if active_project is not None else fallback_org_id


@organizations.command("get")
@global_options
@simple_output_option
def get_organizations(output: str) -> None:
    """List organizations available to the current profile."""
    try:
        auth_svc = _get_service().current_auth_service()
        if not probe_organizations_support():
            if output == "text":
                warning("this server does not support organizations")
                return
            render_output([], output)
            return

        orgs = _list_organizations(auth_svc)
        if not orgs and output == "text":
            status("no organizations found")
            return

        default_org = next((o.org_id for o in orgs if o.is_default), None)
        current_org_id = default_org
        profile = auth_svc.get_current_profile()
        if profile is not None:
            current_org_id = _active_org_id(
                auth_svc,
                current_project_id=profile.project_id,
                fallback_org_id=default_org,
            )
        displays = [
            OrgDisplay.from_org_summary(org, current_org_id=current_org_id)
            for org in orgs
        ]
        render_output(displays, output)

    except Exception as e:
        raise click.ClickException(str(e)) from e


@organizations.command("use")
@click.argument("org_id", required=False, type=OrgType())
@global_options
def use_organization(org_id: str | None) -> None:
    """Set the active project to one from the selected organization."""
    auth_svc = _get_service().current_auth_service()

    try:
        if not probe_organizations_support():
            raise click.ClickException("this server does not support organizations")

        profile = validate_authenticated_profile()
        orgs = _list_organizations(auth_svc)
        if not orgs:
            status("no organizations found")
            return

        if org_id:
            selected_org = next((org for org in orgs if org.org_id == org_id), None)
            if selected_org is None:
                raise click.ClickException(f"Organization {org_id} not found")
        else:
            current_idx = next(
                (i for i, org in enumerate(orgs) if org.is_default),
                0,
            )
            selected_org = select_or_exit(
                [
                    (
                        org,
                        f"{org.org_name}  {org.org_id}"
                        f"{' [default]' if org.is_default else ''}",
                    )
                    for org in orgs
                ],
                "Select an organization",
                hint_flag="<org_id>",
                hint_command="llamactl organizations get",
                selected=current_idx,
            )

        projects = _list_projects(auth_svc, org_id=selected_org.org_id)
        if not projects:
            raise click.ClickException(
                f"Organization {selected_org.org_id} has no projects"
            )

        project = projects[0]
        if profile.project_id == project.project_id:
            status(f"already using organization {selected_org.org_name}")
            return

        auth_svc.set_project(profile.name, project.project_id)
        status(
            f"switched organization {selected_org.org_name} "
            f"and project {project.project_name}"
        )
    except click.ClickException:
        raise
    except Exception as e:
        raise click.ClickException(str(e)) from e
