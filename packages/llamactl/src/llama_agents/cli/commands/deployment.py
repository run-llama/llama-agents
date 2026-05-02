"""CLI commands for managing LlamaDeploy deployments.

This command group lets you list, create, edit, refresh, and delete deployments.
A deployment points the control plane at your Git repository and deployment file
(e.g., `llama_deploy.yaml`). The control plane pulls your code at the selected
git ref, reads the config, and runs your app.
"""

from __future__ import annotations

import asyncio
from pathlib import Path
from typing import Any

import click
import yaml
from llama_agents.cli.commands.auth import validate_authenticated_profile
from llama_agents.cli.param_types import DeploymentType, GitShaType
from llama_agents.cli.styles import WARNING
from llama_agents.core.git.git_util import is_git_repo
from llama_agents.core.schema import LogEvent
from llama_agents.core.schema.deployments import (
    INTERNAL_CODE_REPO_SCHEME,
    DeploymentHistoryResponse,
    DeploymentResponse,
    DeploymentUpdate,
)
from pydantic import ValidationError
from rich import print as rprint

from ..app import app, console
from ..apply_yaml import (
    ApplyYamlError,
    FieldError,
    annotate_yaml_with_errors,
    parse_apply_yaml,
    parse_delete_yaml_name,
)
from ..client import get_project_client, project_client_context
from ..display import (
    PUSH_MODE_REPO_URL,
    DeploymentDisplay,
    DeploymentSpec,
    ReleaseDisplay,
)
from ..local_context import gather_local_context
from ..log_format import parse_log_body, render_plain
from ..options import (
    global_options,
    interactive_option,
    output_option,
    output_option_with_template,
    project_option,
    render_output,
)
from ..render import short_sha
from ..utils.capabilities import probe_code_push_support
from ..utils.git_push import (
    configure_git_remote,
    get_api_key,
    get_deployment_git_url,
    internal_push_refspec,
    push_to_remote,
)
from ..yaml_template import render as render_yaml_template


class PushFailedError(click.ClickException):
    """Raised when apply's push step fails."""


class RepositoryValidationError(click.ClickException):
    """Raised when validate-repository blocks apply."""

    def __init__(self, message: str, path: tuple[str | int, ...]) -> None:
        self.path = path
        super().__init__(message)


_WIRE_SPEC_FIELDS = {
    "repo_url",
    "deployment_file_path",
    "git_ref",
    "appserver_version",
    "suspended",
    "personal_access_token",
}


def _error(path: tuple[str | int, ...], message: str) -> FieldError:
    return FieldError(path=path, severity="error", message=message)


def _remap_wire_path(
    loc: tuple[str | int, ...],
    *,
    display: DeploymentDisplay | None = None,
) -> tuple[str | int, ...]:
    parts = tuple(part for part in loc if part not in {"body", "query"})
    if not parts:
        if display is not None and display.name is not None:
            return ("name",)
        return ()
    if parts[0] == "id":
        return ("name", *parts[1:])
    if parts[0] == "display_name":
        return ("generate_name", *parts[1:])
    if parts[0] in _WIRE_SPEC_FIELDS:
        return ("spec", *parts)
    if parts[0] == "secrets":
        return ("spec", *parts)
    return ()


def _parse_null_create_secret_paths(message: str) -> list[tuple[str | int, ...]]:
    marker = "null values for:"
    if marker not in message:
        return []
    secret_text = message.split(marker, 1)[1].strip().rstrip(")")
    return [
        ("spec", "secrets", name.strip())
        for name in secret_text.split(",")
        if name.strip()
    ]


def _field_errors_from_parse_error(exc: ApplyYamlError) -> list[FieldError]:
    return [_error(detail.path, detail.message) for detail in exc.errors]


def _field_errors_from_value_error(exc: ValueError) -> list[FieldError]:
    message = str(exc)
    if "cannot create a deployment as suspended" in message:
        return [_error(("spec", "suspended"), message)]
    secret_paths = _parse_null_create_secret_paths(message)
    if secret_paths:
        return [_error(path, message) for path in secret_paths]
    if "generate_name is required" in message:
        return [_error(("generate_name",), message)]
    return [_error((), message)]


def _field_errors_from_validation_error(
    exc: ValidationError, *, display: DeploymentDisplay | None = None
) -> list[FieldError]:
    return [
        _error(
            _remap_wire_path(
                tuple(part for part in detail["loc"] if isinstance(part, (str, int))),
                display=display,
            ),
            str(detail["msg"]),
        )
        for detail in exc.errors()
    ]


def _field_errors_from_http_error(
    exc: Exception, *, display: DeploymentDisplay | None = None
) -> list[FieldError]:
    # Deferred: llamactl startup budget avoids importing httpx at module level.
    import httpx

    if not isinstance(exc, httpx.HTTPStatusError):
        return [_error((), str(exc))]
    try:
        detail = exc.response.json().get("detail")
    except ValueError:
        return [_error((), str(exc))]

    if isinstance(detail, list):
        errors: list[FieldError] = []
        for item in detail:
            if not isinstance(item, dict):
                errors.append(_error((), str(item)))
                continue
            loc = item.get("loc")
            message = str(item.get("msg", item))
            if isinstance(loc, (list, tuple)):
                path = _remap_wire_path(
                    tuple(part for part in loc if isinstance(part, (str, int))),
                    display=display,
                )
            else:
                path = ()
            errors.append(_error(path, message))
        return errors
    if isinstance(detail, str):
        return [_error((), detail)]
    return [_error((), str(exc))]


def _field_errors_from_exception(
    exc: Exception, *, display: DeploymentDisplay | None = None
) -> list[FieldError]:
    if isinstance(exc, ApplyYamlError):
        return _field_errors_from_parse_error(exc)
    if isinstance(exc, RepositoryValidationError):
        return [_error(exc.path, exc.message)]
    if isinstance(exc, PushFailedError):
        return [_error((), exc.message)]
    if isinstance(exc, click.ClickException):
        if "generate_name" in exc.message:
            return [_error(("generate_name",), exc.message)]
        return [_error((), exc.message)]
    if isinstance(exc, ValidationError):
        return _field_errors_from_validation_error(exc, display=display)
    if isinstance(exc, ValueError):
        return _field_errors_from_value_error(exc)
    return _field_errors_from_http_error(exc, display=display)


def _repository_error_path(
    message: str, display: DeploymentDisplay
) -> tuple[str | int, ...]:
    lowered = message.lower()
    if any(token in lowered for token in ("auth", "pat", "token", "403")):
        return ("spec", "personal_access_token")
    if display.spec.repo_url:
        return ("spec", "repo_url")
    return ()


def _read_apply_input(filename: str) -> str:
    if filename == "-":
        return click.get_text_stream("stdin").read()
    return Path(filename).read_text()


def _handle_annotated_apply_error(
    *,
    filename: str,
    text: str,
    errors: list[FieldError],
) -> None:
    annotated = annotate_yaml_with_errors(text, errors)
    if filename == "-":
        click.echo(annotated, nl=False)
    else:
        Path(filename).write_text(annotated)
        click.echo(f"apply failed; see annotations in {filename}", err=True)
    raise click.exceptions.Exit(1)


@app.group(
    help="Deploy your app to the cloud.",
    no_args_is_help=True,
)
@global_options
def deployments() -> None:
    """Manage deployments"""
    pass


def friendly_http_error(
    exc: Exception,
    *,
    deployment_id: str | None = None,
    project_id: str | None = None,
) -> str | None:
    """Translate well-known HTTP errors into a one-line CLI message.

    Returns ``None`` when the caller should fall back to the verbose default
    rendering. We only collapse the cases where a richer message would just
    be debug noise to the user — currently a 404 on a known deployment id.
    Other 4xx/5xx and non-HTTP errors keep their existing message so we
    don't swallow useful info on unexpected paths.
    """
    # Defer httpx import: `llamactl --help` is held to a no-httpx startup
    # budget by tests/test_cli_imports.py; only error paths need the type.
    import httpx

    if not isinstance(exc, httpx.HTTPStatusError):
        return None
    if exc.response.status_code != 404 or not deployment_id:
        return None
    msg = f"deployment '{deployment_id}' not found"
    if project_id:
        msg += f" in project '{project_id}'"
    return msg


def _do_get(
    deployment_id: str | None,
    interactive: bool,
    output: str,
    project: str | None,
) -> None:
    """Implementation of ``deployments get`` shared with the hidden ``list`` alias.

    No ``deployment_id`` → list all deployments (kubectl-style). With an ID →
    a single-row table for that deployment. Never launches the TUI; for a
    live view use ``deployments logs --follow``.
    """
    mode = output.lower()
    if mode == "template" and not deployment_id:
        raise click.ClickException("-o template requires a deployment name")

    validate_authenticated_profile(interactive)
    # Fall back to the user-supplied override if client construction itself
    # raises; `client.project_id` resolves the active project when no override.
    effective_project: str | None = project
    try:
        client = get_project_client(project_id_override=project)
        effective_project = client.project_id

        if not deployment_id:
            deployments = asyncio.run(client.list_deployments())

            if not deployments and mode == "text":
                rprint(
                    f"[{WARNING}]No deployments found for project {client.project_id}[/]"
                )
                return

            displays = [DeploymentDisplay.from_response(d) for d in deployments]
            render_output(displays, output)
            return

        deployment = asyncio.run(client.get_deployment(deployment_id))
        display = DeploymentDisplay.from_response(deployment)
        if mode == "template":
            click.echo(render_yaml_template(display), nl=False)
            return
        render_output(display, output)

    except Exception as e:
        friendly = friendly_http_error(
            e, deployment_id=deployment_id, project_id=effective_project
        )
        message = friendly if friendly is not None else str(e)
        rprint(f"[red]Error: {message}[/red]")
        raise click.Abort()


@deployments.command("get")
@global_options
@click.argument("deployment_id", required=False, type=DeploymentType())
@output_option_with_template
@project_option
@interactive_option
def get_deployment(
    deployment_id: str | None,
    interactive: bool,
    output: str,
    project: str | None,
) -> None:
    """Get one or more deployments.

    With no argument: lists all deployments in the project (kubectl-style).
    With a deployment ID: prints details for that deployment.

    Use ``-o json`` or ``-o yaml`` for machine-readable output. Use
    ``llamactl deployments logs <name> --follow`` to stream logs.
    """
    _do_get(deployment_id, interactive, output, project)


@deployments.command("list", hidden=True)
@global_options
@output_option
@project_option
@interactive_option
def list_deployments(
    interactive: bool,
    output: str,
    project: str | None,
) -> None:
    """Hidden alias for ``deployments get``. Kept for backward compatibility."""
    _do_get(None, interactive, output, project)


@deployments.command("template")
@global_options
def template_deployment() -> None:
    """Print an apply-shaped YAML scaffold for a new deployment.

    Reads the local working tree (git remote and ref, deployment config,
    .env, required secrets) and emits a YAML scaffold with ``##`` instruction
    comments. Edit the output, then run ``llamactl deployments apply -f
    <file>``. Offline by design — no auth profile required.
    """
    ctx = gather_local_context()

    cwd_name: str = Path.cwd().name
    preferred_name: str = ctx.generate_name or cwd_name
    secrets: dict[str, str | None] | None = None
    if ctx.required_secret_names:
        secrets = {name: f"${{{name}}}" for name in ctx.required_secret_names}

    if ctx.is_git_repo:
        spec = DeploymentSpec(
            repo_url=PUSH_MODE_REPO_URL,
            deployment_file_path=ctx.deployment_file_path,
            git_ref=ctx.git_ref,
            appserver_version=ctx.installed_appserver_version,
            secrets=secrets,
        )
        required: tuple[str, ...] = ()
    else:
        spec = DeploymentSpec(
            appserver_version=ctx.installed_appserver_version,
            secrets=secrets,
        )
        required = ("repo_url",)

    display = DeploymentDisplay(name=None, generate_name=preferred_name, spec=spec)

    head: list[str] = [f"WARNING: {warning}" for warning in ctx.warnings]
    if ctx.warnings:
        head.append("")
    head.append("Edit, then run: llamactl deployments apply -f <file>")
    if not ctx.is_git_repo:
        head.extend(
            [
                "",
                "NOT IN A GIT REPO — set repo_url, or cd into a working tree "
                "and re-run.",
            ]
        )

    field_alternatives: dict[str, tuple[str, str]] = {}
    if ctx.is_git_repo and ctx.repo_url:
        field_alternatives["repo_url"] = (
            ctx.repo_url,
            "auto-detected from your git remotes",
        )

    secret_comments: dict[str, str] = {}
    for name_ in ctx.required_secret_names:
        if name_ in ctx.available_secrets:
            secret_comments[name_] = "from your .env"
        else:
            secret_comments[name_] = "not in your .env — add it before apply"

    click.echo(
        render_yaml_template(
            display,
            head=head,
            secret_comments=secret_comments,
            field_alternatives=field_alternatives,
            required=required,
            name_example=preferred_name,
            scaffold_generate_name=True,
        ),
        nl=False,
    )


@deployments.command("create")
@global_options
@interactive_option
def create_deployment(
    interactive: bool,
) -> None:
    """Create a new deployment."""
    validate_authenticated_profile(interactive)

    if not interactive:
        raise click.ClickException("This command requires an interactive session.")

    # Keep this import local: `llamactl --help` eagerly imports command modules,
    # and import-time profiling showed Textual adds material startup cost here.
    # Avoid adding other local imports unless instrumentation shows they are slow.
    from ..textual.deployment_form import create_deployment_form

    deployment_form = create_deployment_form(
        server_supports_code_push=probe_code_push_support(),
    )
    if deployment_form is None:
        rprint(f"[{WARNING}]Cancelled[/]")
        return

    rprint(f"[green]Created deployment: {deployment_form.id}[/green]")


@deployments.command("configure-git-remote")
@global_options
@click.argument("deployment_id", required=False, type=DeploymentType())
@project_option
@interactive_option
def configure_git_remote_cmd(
    deployment_id: str | None, interactive: bool, project: str | None
) -> None:
    """Configure a git remote for a deployment.

    Sets up authentication and a git remote named 'llamaagents-<deployment_id>'
    so you can push with:
      git push llamaagents-<deployment_id>

    Tip: 'llamactl deployments update' handles pushing and redeployment in one
    step. This command is useful for troubleshooting git push issues.
    """
    validate_authenticated_profile(interactive)
    try:
        if not is_git_repo():
            raise click.ClickException("Not a git repository")

        deployment_id = select_deployment(
            deployment_id, interactive=interactive, project_id_override=project
        )
        if not deployment_id:
            rprint(f"[{WARNING}]No deployment selected[/]")
            return

        client = get_project_client(project_id_override=project)
        git_url = get_deployment_git_url(client.base_url, deployment_id)
        api_key = get_api_key()
        remote_name = configure_git_remote(
            git_url, api_key, client.project_id, deployment_id
        )

        rprint(
            f"[green]Configured git remote '{remote_name}' for {deployment_id}[/green]"
        )
        rprint(f"Push with: [cyan]git push {remote_name}[/cyan]")

    except click.ClickException:
        raise
    except Exception as e:
        rprint(f"[red]Error: {e}[/red]")
        raise click.Abort()


@deployments.command("delete")
@global_options
@click.argument("deployment_id", required=False, type=DeploymentType())
@click.option(
    "-f",
    "--filename",
    default=None,
    type=click.File("r"),
    help="Path to YAML file; name is read from the file. Mutually exclusive with positional ID.",
)
@project_option
@interactive_option
def delete_deployment(
    deployment_id: str | None,
    filename: click.utils.LazyFile | None,
    interactive: bool,
    project: str | None,
) -> None:
    """Delete a deployment"""
    # Keep this import local: the helper imports `questionary`, which import-time
    # profiling showed is a noticeable CLI startup cost. Avoid other local
    # imports unless instrumentation shows they are slow.
    from ..interactive_prompts.utils import confirm_action

    if filename is not None and deployment_id is not None:
        raise click.ClickException(
            "--filename and deployment ID are mutually exclusive"
        )

    if filename is not None:
        try:
            deployment_id = parse_delete_yaml_name(filename.read())
        except ApplyYamlError as exc:
            raise click.ClickException(str(exc)) from exc

    validate_authenticated_profile(interactive)
    try:
        client = get_project_client(project_id_override=project)

        deployment_id = select_deployment(
            deployment_id, interactive=interactive, project_id_override=project
        )
        if not deployment_id:
            rprint(f"[{WARNING}]No deployment selected[/]")
            return

        if interactive:
            if not confirm_action(f"Delete deployment '{deployment_id}'?"):
                rprint(f"[{WARNING}]Cancelled[/]")
                return

        asyncio.run(client.delete_deployment(deployment_id))
        rprint(f"[green]Deleted deployment: {deployment_id}[/green]")

    except Exception as e:
        rprint(f"[red]Error: {e}[/red]")
        raise click.Abort()


def _apply_push(
    client: Any,
    deployment_id: str,
    git_ref: str | None,
) -> None:
    """Push local code to the deployment's internal bare repo.

    Used in the push-then-save flow (existing push-mode → push-mode update)
    and also called by ``_apply_push_after_save`` for the save-then-push flow.
    Raises ``click.ClickException`` on failure.
    """
    api_key = get_api_key()
    git_url = get_deployment_git_url(client.base_url, deployment_id)
    remote_name = configure_git_remote(
        git_url, api_key, client.project_id, deployment_id
    )
    local_ref, target_ref = internal_push_refspec(git_ref)
    with console.status("pushing code..."):
        push_result = push_to_remote(
            remote_name, local_ref=local_ref, target_ref=target_ref
        )
    if push_result.returncode != 0:
        stderr = push_result.stderr.decode(errors="replace").strip()
        raise PushFailedError(
            f"push failed: {stderr}\n"
            f"To debug, try: llamactl deployments configure-git-remote {deployment_id}"
        )


def _apply_push_after_save(
    client: Any,
    deployment_id: str,
    git_ref: str | None,
) -> None:
    """Push after a successful create/update (bootstrap push).

    On push failure the save already succeeded, so the error message includes
    a recovery hint. Raises ``click.ClickException`` on failure.
    """
    try:
        _apply_push(client, deployment_id, git_ref)
    except PushFailedError as exc:
        click.echo(str(exc.message), err=True)
        raise PushFailedError(
            f"{exc.message}\n"
            "re-run `llamactl deployments apply -f <file>` to retry the push"
        ) from exc


async def _apply_deployment_from_yaml(
    client: Any, display: DeploymentDisplay, *, no_push: bool = False
) -> None:
    # Deferred: llamactl startup budget avoids importing httpx at module level.
    import httpx

    existing: DeploymentResponse | None = None
    is_update = False

    try:
        if display.name is not None:
            try:
                existing = await client.get_deployment(display.name)
                is_update = True
            except httpx.HTTPStatusError as exc:
                if exc.response.status_code == 404:
                    if display.generate_name is None:
                        raise click.ClickException(
                            "deployment not found and no generate_name provided "
                            "for create"
                        ) from exc
                else:
                    raise
        elif not display.generate_name:
            raise click.ClickException(
                "YAML must include top-level 'name' or 'generate_name'"
            )

        # Pre-flight validate-repository (skipped for push-mode, dry-run,
        # and when repo_url is unset on the update path).
        repo_url = display.spec.repo_url
        skip_validation = (
            repo_url is None
            or repo_url == ""
            or repo_url == INTERNAL_CODE_REPO_SCHEME
            or (is_update and "repo_url" not in display.spec.model_fields_set)
        )
        if not skip_validation:
            assert repo_url is not None  # guarded by skip_validation
            vr = await client.validate_repository(
                repo_url=repo_url,
                deployment_id=existing.id if existing else None,
                pat=display.spec.personal_access_token,
            )
            if not vr.accessible:
                raise RepositoryValidationError(
                    vr.message, _repository_error_path(vr.message, display)
                )

        # Push ordering matrix:
        #   (no deployment, push)    -> save then push (bootstrap)
        #   (push, push)             -> push then save (bare repo must hold
        #                              new ref before update resolves git_ref)
        #   (external, push)         -> save then push (switch into push mode)
        #   (*, external)            -> save only
        #   (*, same-as-current)     -> save only (repo_url omitted in YAML)
        current_is_push = (
            existing is not None and existing.repo_url == INTERNAL_CODE_REPO_SCHEME
        )
        if "repo_url" not in display.spec.model_fields_set:
            desired_is_push = current_is_push
        elif (
            display.spec.repo_url == ""
            or display.spec.repo_url == INTERNAL_CODE_REPO_SCHEME
        ):
            desired_is_push = True
        else:
            desired_is_push = False

        push_before_save = current_is_push and desired_is_push

        if desired_is_push and no_push:
            desired_is_push = False
            push_before_save = False

        if desired_is_push and not is_git_repo():
            rprint(
                f"[{WARNING}]Not in a git repo — skipping push, "
                "server will resolve from last pushed code[/]"
            )
            desired_is_push = False
            push_before_save = False

        # Execute: translate, optionally push, and call the API.
        if push_before_save:
            assert existing is not None and display.name is not None
            _apply_push(
                client,
                existing.id,
                display.spec.git_ref or existing.git_ref,
            )
            payload = display.to_update_payload()
            response = await client.update_deployment(display.name, payload)
            click.echo(f"updated {response.id}")
        elif is_update:
            assert display.name is not None
            payload = display.to_update_payload()
            response = await client.update_deployment(display.name, payload)
            click.echo(f"updated {response.id}")
            if desired_is_push:
                _apply_push_after_save(client, response.id, display.spec.git_ref)
        else:
            payload = display.to_create_payload()
            response = await client.create_deployment(payload)
            click.echo(f"created {response.id}")
            if desired_is_push:
                _apply_push_after_save(client, response.id, display.spec.git_ref)

    except (click.ClickException, ValueError, ValidationError):
        raise
    except httpx.HTTPStatusError:
        raise
    except Exception as exc:
        raise click.ClickException(str(exc)) from exc


@deployments.command("apply")
@global_options
@click.option(
    "-f",
    "--filename",
    required=True,
    type=click.Path(allow_dash=True, dir_okay=False, path_type=str),
    help="Path to YAML file, or '-' for stdin.",
)
@click.option(
    "--dry-run",
    is_flag=True,
    default=False,
    help="Validate and print the resolved payload without making API calls.",
)
@click.option(
    "--no-push",
    is_flag=True,
    default=False,
    help="Skip pushing local code even when the deployment uses push-mode.",
)
@click.option(
    "--annotate-on-error",
    is_flag=True,
    default=False,
    help="Write apply errors back into the YAML input.",
)
@project_option
def apply_deployment(
    filename: str,
    dry_run: bool,
    no_push: bool,
    annotate_on_error: bool,
    project: str | None,
) -> None:
    """Apply a deployment from a YAML file.

    Creates the deployment if it doesn't exist, or updates it if it does.
    Reads the file (or stdin with ``-f -``), resolves ``${VAR}`` references
    from the environment, and issues the appropriate API call.
    """
    text = _read_apply_input(filename)
    try:
        display = parse_apply_yaml(text)
    except ApplyYamlError as exc:
        if annotate_on_error and not dry_run:
            _handle_annotated_apply_error(
                filename=filename,
                text=text,
                errors=_field_errors_from_parse_error(exc),
            )
        raise click.ClickException(str(exc)) from exc

    if dry_run:
        if display.name:
            verdict = f"would upsert deployment '{display.name}'"
        elif display.generate_name:
            verdict = f"would create deployment '{display.generate_name}'"
        else:
            message = "YAML must include top-level 'name' or 'generate_name'"
            raise click.ClickException(message)

        click.echo(
            yaml.safe_dump(
                display.spec.as_redacted().model_dump(mode="json", exclude_unset=True),
                sort_keys=False,
            )
        )
        click.echo(verdict)
        return

    validate_authenticated_profile(interactive=False)
    client = get_project_client(project_id_override=project)
    try:
        asyncio.run(_apply_deployment_from_yaml(client, display, no_push=no_push))
    except Exception as exc:
        if annotate_on_error:
            _handle_annotated_apply_error(
                filename=filename,
                text=text,
                errors=_field_errors_from_exception(exc, display=display),
            )
        if isinstance(exc, click.ClickException):
            raise
        raise


@deployments.command("edit")
@global_options
@click.argument("deployment_id", required=False, type=DeploymentType())
@project_option
@interactive_option
def edit_deployment(
    deployment_id: str | None, interactive: bool, project: str | None
) -> None:
    """Interactively edit a deployment"""
    # Keep this import local: `llamactl --help` eagerly imports command modules,
    # and import-time profiling showed Textual adds material startup cost here.
    # Avoid adding other local imports unless instrumentation shows they are slow.
    from ..textual.deployment_form import edit_deployment_form

    validate_authenticated_profile(interactive)
    try:
        client = get_project_client(project_id_override=project)

        deployment_id = select_deployment(
            deployment_id, interactive=interactive, project_id_override=project
        )
        if not deployment_id:
            rprint(f"[{WARNING}]No deployment selected[/]")
            return

        current_deployment = asyncio.run(client.get_deployment(deployment_id))

        updated_deployment = edit_deployment_form(
            current_deployment,
            server_supports_code_push=probe_code_push_support(),
        )
        if updated_deployment is None:
            rprint(f"[{WARNING}]Cancelled[/]")
            return

        rprint(
            f"[green]Successfully updated deployment: {updated_deployment.id}[/green]"
        )

    except Exception as e:
        rprint(f"[red]Error: {e}[/red]")
        raise click.Abort()


def _push_internal_for_update(
    deployment_id: str,
    git_ref: str | None,
    base_url: str,
    project_id: str,
) -> None:
    """Push local code to the internal repo before updating.

    This ensures the S3-stored bare repo has the latest commits so the
    server can resolve the ref to a fresh SHA.
    """
    if not is_git_repo():
        rprint(
            f"[{WARNING}]Not in a git repo — skipping push, "
            "server will resolve from last pushed code[/]"
        )
        return

    api_key = get_api_key()
    git_url = get_deployment_git_url(base_url, deployment_id)
    remote_name = configure_git_remote(git_url, api_key, project_id, deployment_id)
    local_ref, target_ref = internal_push_refspec(git_ref)
    with console.status("Pushing code..."):
        push_result = push_to_remote(
            remote_name, local_ref=local_ref, target_ref=target_ref
        )
    if push_result.returncode != 0:
        stderr = push_result.stderr.decode(errors="replace").strip()
        rprint(f"[{WARNING}]Push failed: {stderr}[/]")
        rprint(
            f"[{WARNING}]Continuing with update using last pushed code. "
            f"To debug, try: llamactl deployments configure-git-remote {deployment_id}[/]"
        )


@deployments.command("update")
@global_options
@click.argument("deployment_id", required=False, type=DeploymentType())
@click.option(
    "--git-ref",
    help="Reference branch, tag, or commit SHA for the deployment. If not provided, the current reference and latest commit on it will be used.",
    default=None,
)
@project_option
@interactive_option
def refresh_deployment(
    deployment_id: str | None,
    git_ref: str | None,
    interactive: bool,
    project: str | None,
) -> None:
    """Update the deployment, pulling the latest code from it's branch"""
    validate_authenticated_profile(interactive)
    try:
        deployment_id = select_deployment(
            deployment_id, interactive=interactive, project_id_override=project
        )
        if not deployment_id:
            rprint(f"[{WARNING}]No deployment selected[/]")
            return

        # Single asyncio.run with one client: reusing a ProjectClient across
        # two asyncio.run calls binds the underlying httpx pool to a closed
        # loop and the next request raises "Event loop is closed".
        async def _do_update() -> tuple[DeploymentResponse, DeploymentResponse]:
            async with project_client_context(project_id_override=project) as client:
                current = await client.get_deployment(deployment_id)
                effective_git_ref = git_ref or current.git_ref
                if current.repo_url == INTERNAL_CODE_REPO_SCHEME:
                    _push_internal_for_update(
                        deployment_id,
                        effective_git_ref,
                        base_url=client.base_url,
                        project_id=client.project_id,
                    )
                # Re-resolves the branch to the latest commit SHA on the server.
                with console.status(f"Refreshing {current.display_name}..."):
                    updated = await client.update_deployment(
                        deployment_id,
                        DeploymentUpdate(git_ref=effective_git_ref),
                    )
                return current, updated

        current_deployment, updated_deployment = asyncio.run(_do_update())

        old_git_sha = current_deployment.git_sha or ""
        new_git_sha = updated_deployment.git_sha or ""
        old_short = short_sha(old_git_sha) if old_git_sha else "-"
        new_short = short_sha(new_git_sha) if new_git_sha else "-"

        if old_git_sha == new_git_sha:
            rprint(f"No changes: already at {new_short}")
        else:
            rprint(f"Updated: {old_short} → {new_short}")

    except Exception as e:
        rprint(f"[red]Error: {e}[/red]")
        raise click.Abort()


@deployments.command("history")
@global_options
@click.argument("deployment_id", required=False, type=DeploymentType())
@output_option
@project_option
@interactive_option
def show_history(
    deployment_id: str | None,
    interactive: bool,
    output: str,
    project: str | None,
) -> None:
    """Show release history for a deployment."""
    validate_authenticated_profile(interactive)
    try:
        deployment_id = select_deployment(
            deployment_id, interactive=interactive, project_id_override=project
        )
        if not deployment_id:
            rprint(f"[{WARNING}]No deployment selected[/]")
            return

        async def _fetch_history() -> DeploymentHistoryResponse:
            async with project_client_context(project_id_override=project) as client:
                return await client.get_deployment_history(deployment_id)

        history = asyncio.run(_fetch_history())
        items_sorted = sorted(
            history.history,
            key=lambda it: it.released_at,
            reverse=True,
        )

        if not items_sorted and output == "text":
            rprint(f"No history recorded for {deployment_id}")
            return

        displays = [ReleaseDisplay.from_response(item) for item in items_sorted]
        render_output(displays, output)
    except Exception as e:
        rprint(f"[red]Error: {e}[/red]")
        raise click.Abort()


@deployments.command("rollback")
@global_options
@click.argument("deployment_id", required=False, type=DeploymentType())
@click.option(
    "--git-sha", required=False, type=GitShaType(), help="Git SHA to roll back to"
)
@project_option
@interactive_option
def rollback(
    deployment_id: str | None,
    git_sha: str | None,
    interactive: bool,
    project: str | None,
) -> None:
    """Rollback a deployment to a previous git sha."""
    # Keep these imports local: profiling showed `questionary` is a noticeable
    # startup cost for `llamactl --help`. Avoid other local imports unless they
    # are measured and proven slow.
    import questionary

    from ..interactive_prompts.utils import confirm_action

    validate_authenticated_profile(interactive)
    try:
        deployment_id = select_deployment(
            deployment_id, interactive=interactive, project_id_override=project
        )
        if not deployment_id:
            rprint(f"[{WARNING}]No deployment selected[/]")
            return

        if not git_sha:
            # If not provided, prompt from history
            async def _fetch_current_and_history() -> tuple[
                DeploymentResponse, DeploymentHistoryResponse
            ]:
                async with project_client_context(
                    project_id_override=project
                ) as client:
                    current = await client.get_deployment(deployment_id)
                    hist = await client.get_deployment_history(deployment_id)
                    return current, hist

            current_deployment, history = asyncio.run(_fetch_current_and_history())
            current_sha = current_deployment.git_sha or ""

            items_sorted = sorted(
                history.history or [], key=lambda it: it.released_at, reverse=True
            )
            choices = []
            for it in items_sorted:
                short = short_sha(it.git_sha)
                suffix = (
                    " [current]" if current_sha and it.git_sha == current_sha else ""
                )
                choices.append(
                    questionary.Choice(
                        title=f"{short}{suffix} ({it.released_at})", value=it.git_sha
                    )
                )
            if not choices:
                rprint(f"[{WARNING}]No history available to rollback[/]")
                return
            git_sha = questionary.select("Select git sha:", choices=choices).ask()
            if not git_sha:
                rprint(f"[{WARNING}]Cancelled[/]")
                return

        if interactive and not confirm_action(
            f"Rollback '{deployment_id}' to {short_sha(git_sha)}?"
        ):
            rprint(f"[{WARNING}]Cancelled[/]")
            return

        async def _do_rollback() -> DeploymentResponse:
            async with project_client_context(project_id_override=project) as client:
                return await client.rollback_deployment(deployment_id, git_sha)

        updated = asyncio.run(_do_rollback())
        new_short = short_sha(updated.git_sha) if updated.git_sha else "-"
        rprint(f"[green]Rollback initiated[/green]: {deployment_id} → {new_short}")
    except Exception as e:
        rprint(f"[red]Error: {e}[/red]")
        raise click.Abort()


@deployments.command("logs")
@global_options
@click.argument("deployment_id", required=False, type=DeploymentType())
@click.option(
    "--follow",
    "-f",
    is_flag=True,
    default=False,
    help="Stream logs continuously until interrupted (Ctrl-C).",
)
@click.option(
    "--json",
    "json_lines",
    is_flag=True,
    default=False,
    help="Output one LogEvent JSON object per line (jsonl).",
)
@click.option(
    "--tail",
    "tail",
    type=click.IntRange(min=1),
    default=200,
    show_default=True,
    help="Number of lines to retrieve from the end of the logs initially.",
)
@click.option(
    "--since-seconds",
    "since_seconds",
    type=click.IntRange(min=0),
    default=None,
    help="Only return logs newer than this many seconds.",
)
@click.option(
    "--include-init-containers",
    is_flag=True,
    default=False,
    help="Include init container logs.",
)
@project_option
@interactive_option
def deployment_logs(
    deployment_id: str | None,
    follow: bool,
    json_lines: bool,
    tail: int,
    since_seconds: int | None,
    include_init_containers: bool,
    interactive: bool,
    project: str | None,
) -> None:
    """Stream or fetch logs for a deployment.

    By default, prints recent logs and exits. Use ``--follow`` to keep the
    stream open until you Ctrl-C. Use ``--json`` to emit one JSON
    ``LogEvent`` per line for downstream tooling (jsonl).
    """
    validate_authenticated_profile(interactive)

    deployment_id = select_deployment(
        deployment_id, interactive=interactive, project_id_override=project
    )
    if not deployment_id:
        rprint(f"[{WARNING}]No deployment selected[/]")
        return

    async def _consume() -> int:
        events_seen = 0
        async with project_client_context(project_id_override=project) as client:
            async for ev in client.stream_deployment_logs(
                deployment_id,
                include_init_containers=include_init_containers,
                tail_lines=tail,
                since_seconds=since_seconds,
                follow=follow,
            ):
                events_seen += 1
                _emit_log_event(ev, json_lines=json_lines)
        return events_seen

    try:
        events_seen = asyncio.run(_consume())
    except KeyboardInterrupt:
        # Clean exit on Ctrl-C; no traceback.
        return
    except Exception as e:
        rprint(f"[red]Error: {e}[/red]")
        raise click.Abort()

    if events_seen == 0 and not follow:
        click.echo("no logs available yet", err=True)


def _emit_log_event(ev: LogEvent, *, json_lines: bool) -> None:
    """Render a single LogEvent to stdout per the requested format."""
    if json_lines:
        click.echo(ev.model_dump_json())
        return

    parsed = parse_log_body(ev.text)
    body = render_plain(parsed)
    pod = f"{ev.pod}/{ev.container}"
    # Skip the envelope timestamp when the structured body already carries one,
    # otherwise structlog lines render with two side-by-side timestamps.
    ts = "" if parsed.timestamp else (ev.timestamp.isoformat() if ev.timestamp else "")
    prefix = " ".join(p for p in (ts, pod) if p)
    click.echo(f"{prefix} {body}" if prefix else body)


def select_deployment(
    deployment_id: str | None,
    interactive: bool,
    project_id_override: str | None = None,
) -> str | None:
    """
    Select a deployment interactively if ID not provided.
    Returns the selected deployment ID or None if cancelled.

    In non-interactive sessions, returns None if deployment_id is not provided.
    """
    # Keep this import local: profiling showed `questionary` is a noticeable
    # startup cost for `llamactl --help`. Avoid other local imports unless they
    # are measured and proven slow.
    import questionary

    if deployment_id:
        return deployment_id

    # Don't attempt interactive selection in non-interactive sessions
    if not interactive:
        return None
    client = get_project_client(project_id_override=project_id_override)
    deployments = asyncio.run(client.list_deployments())

    if not deployments:
        rprint(f"[{WARNING}]No deployments found for project {client.project_id}[/]")
        return None

    choices = []
    for deployment in deployments:
        deployment_id = deployment.id
        status = deployment.status
        choices.append(
            questionary.Choice(
                title=f"{deployment_id} - {status}",
                value=deployment_id,
            )
        )

    return questionary.select("Select deployment:", choices=choices).ask()
