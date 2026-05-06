from __future__ import annotations

import click
from llama_agents.cli.interactive import is_interactive_session, select_or_exit
from llama_agents.cli.output import status
from llama_agents.cli.param_types import OrgType, ProjectType

from ..app import app
from ..display import ProjectDisplay
from ..options import global_options, output_option, render_output
from .auth import (
    _discover_organization,
    _get_service,
    _list_projects,
    validate_authenticated_profile,
)


@app.group(
    help="Manage projects",
    no_args_is_help=True,
)
@global_options
def projects() -> None:
    pass


@projects.command("get")
@click.argument("project_id", required=False, type=ProjectType())
@click.option(
    "--org",
    "org_id",
    default=None,
    type=OrgType(),
    help="Organization ID to scope projects to",
)
@global_options
@output_option
def get_projects(project_id: str | None, org_id: str | None, output: str) -> None:
    """List projects available to the current profile."""
    try:
        auth_svc = _get_service().current_auth_service()
        profile = validate_authenticated_profile()
        if org_id is None:
            org = _discover_organization(auth_svc)
            if org is not None:
                org_id = org.org_id

        projects = _list_projects(auth_svc, org_id=org_id)
        if project_id:
            projects = [
                project for project in projects if project.project_id == project_id
            ]
            if not projects:
                raise click.ClickException(f"Project {project_id} not found")

        if not projects and output == "text":
            status("no projects found")
            return

        displays = [
            ProjectDisplay.from_project_summary(
                project, current_project_id=profile.project_id
            )
            for project in projects
        ]
        render_output(
            displays[0] if project_id and len(displays) == 1 else displays, output
        )

    except click.ClickException:
        raise
    except Exception as e:
        raise click.ClickException(str(e)) from e


@projects.command("use")
@click.argument("project_id", required=False, type=ProjectType())
@click.option(
    "--org",
    "org_id",
    default=None,
    type=OrgType(),
    help="Organization ID to scope projects to",
)
@global_options
def use_project(project_id: str | None, org_id: str | None) -> None:
    """Set the active project for the current profile."""
    auth_svc = _get_service().current_auth_service()
    profile = validate_authenticated_profile()

    try:
        if org_id is None:
            org = _discover_organization(auth_svc)
            if org is not None:
                org_id = org.org_id

        if project_id and profile.project_id == project_id:
            return

        if project_id:
            if auth_svc.env.requires_auth:
                projects = _list_projects(auth_svc, org_id=org_id)
                if not next(
                    (
                        project
                        for project in projects
                        if project.project_id == project_id
                    ),
                    None,
                ):
                    raise click.ClickException(f"Project {project_id} not found")
            auth_svc.set_project(profile.name, project_id)
            status(f"switched project {project_id}")
            return

        projects = _list_projects(auth_svc, org_id=org_id)

        if not projects:
            status("no projects found")
            return

        current_project_id = profile.project_id
        items = []
        current_idx = 0
        for i, project in enumerate(projects):
            label = f"{project.project_id}  {project.project_name} ({project.deployment_count} deployments)"
            if project.project_id == current_project_id:
                label += " [current]"
                current_idx = i
            items.append((project.project_id, label))
        if not auth_svc.env.requires_auth:
            items.append(("__CREATE__", "Create new project"))

        result = select_or_exit(
            items,
            "Select a project",
            hint_flag="<project_id>",
            hint_command="llamactl projects use <project_id>",
            selected=current_idx,
        )
        if result == "__CREATE__":
            if not is_interactive_session():
                raise click.ClickException("Pass <project_id> to choose one")
            result = click.prompt(
                "Enter project ID", default="", show_default=False
            ).strip()
        if result:
            selected_project = next(
                (project for project in projects if project.project_id == result), None
            )
            name = selected_project.project_name if selected_project else result
            auth_svc.set_project(profile.name, result)
            status(f"switched project {name}")
        else:
            status("no project selected")
    except click.ClickException:
        raise
    except Exception as e:
        raise click.ClickException(str(e)) from e
