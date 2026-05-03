"""CLI commands for managing LlamaDeploy deployments.

This command group lets you list, create, edit, refresh, and delete deployments.
A deployment points the control plane at your Git repository and deployment file
(e.g., `llama_deploy.yaml`). The control plane pulls your code at the selected
git ref, reads the config, and runs your app.
"""

from __future__ import annotations

import asyncio
import os
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Literal, NoReturn

import click
import yaml
from llama_agents.cli.commands.auth import validate_authenticated_profile
from llama_agents.cli.param_types import DeploymentType, GitShaType
from llama_agents.cli.styles import WARNING
from llama_agents.core.git.git_util import is_git_repo
from llama_agents.core.schema import LogEvent
from llama_agents.core.schema.deployments import (
    INTERNAL_CODE_REPO_SCHEME,
    DeploymentCreate,
    DeploymentHistoryResponse,
    DeploymentResponse,
    DeploymentUpdate,
)
from pydantic import ValidationError
from rich import print as rprint

from ..app import app, console
from ..apply_yaml import (
    SPEC_FIELDS,
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
    PayloadError,
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
from ..utils.git_push import (
    configure_git_remote,
    get_deployment_git_url,
    internal_push_refspec,
    push_to_remote,
)
from ..yaml_template import render as render_yaml_template

DeploymentApplyMode = Literal["apply", "create", "update"]
DeploymentOperationAction = Literal["create", "update"]


@dataclass(frozen=True)
class _DeploymentIntent:
    display: DeploymentDisplay
    mode: DeploymentApplyMode
    update_target: str | None = None


@dataclass(frozen=True)
class _ResolvedDeploymentOperation:
    action: DeploymentOperationAction
    display: DeploymentDisplay
    payload: DeploymentCreate | DeploymentUpdate
    existing: DeploymentResponse | None = None


class PushFailedError(click.ClickException):
    """Raised when apply's push step fails."""


class RepositoryValidationError(click.ClickException):
    """Raised when validate-repository blocks apply."""

    def __init__(self, message: str, path: tuple[str | int, ...]) -> None:
        self.path = path
        super().__init__(message)


def _error(path: tuple[str | int, ...], message: str) -> FieldError:
    return FieldError(path=path, message=message)


def _wire_path_from_loc(
    loc: tuple[Any, ...], *, display: DeploymentDisplay | None = None
) -> tuple[str | int, ...]:
    """Map an API/pydantic error ``loc`` back to the corresponding YAML path."""
    parts = tuple(
        part
        for part in loc
        if isinstance(part, (str, int)) and part not in {"body", "query"}
    )
    if not parts:
        if display is not None and display.name is not None:
            return ("name",)
        return ()
    if parts[0] == "id":
        return ("name", *parts[1:])
    if parts[0] == "display_name":
        return ("generate_name", *parts[1:])
    if parts[0] in SPEC_FIELDS or parts[0] == "secrets":
        return ("spec", *parts)
    return ()


def _http_error_to_field_errors(
    exc: Any, *, display: DeploymentDisplay | None = None
) -> list[FieldError]:
    """Extract structured field errors from an ``httpx.HTTPStatusError``."""
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
                path = _wire_path_from_loc(tuple(loc), display=display)
            else:
                path = ()
            errors.append(_error(path, message))
        return errors
    if isinstance(detail, str):
        return [_error((), detail)]
    return [_error((), str(exc))]


_PYDANTIC_VALUE_ERROR_PREFIX = "Value error, "


def _strip_pydantic_prefix(msg: str) -> str:
    if msg.startswith(_PYDANTIC_VALUE_ERROR_PREFIX):
        return msg[len(_PYDANTIC_VALUE_ERROR_PREFIX) :]
    return msg


def _validation_error_to_field_errors(
    exc: ValidationError, *, display: DeploymentDisplay
) -> list[FieldError]:
    return [
        _error(
            _wire_path_from_loc(tuple(d["loc"]), display=display),
            _strip_pydantic_prefix(str(d["msg"])),
        )
        for d in exc.errors()
    ]


def _validate_dry_run_payload(display: DeploymentDisplay) -> None:
    try:
        if display.name:
            display.to_update_payload()
        elif display.generate_name:
            display.to_create_payload()
        else:
            msg = "YAML must include top-level 'name' or 'generate_name'"
            raise ApplyYamlError(msg, errors=[_error(("generate_name",), msg)])
    except PayloadError as exc:
        raise ApplyYamlError(str(exc), errors=[_error(exc.path, str(exc))]) from exc
    except ValidationError as exc:
        raise ApplyYamlError(
            str(exc),
            errors=_validation_error_to_field_errors(exc, display=display),
            original_error=exc,
        ) from exc


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


def _format_apply_yaml_error(exc: ApplyYamlError) -> str:
    if len(exc.errors) <= 1:
        return str(exc)

    lines = ["deployment YAML has errors:"]
    for error in exc.errors:
        if error.path:
            path = ".".join(str(part) for part in error.path)
            lines.append(f"- {path}: {error.message}")
        else:
            lines.append(f"- {error.message}")
    return "\n".join(lines)


def _raise_apply_yaml_click_error(exc: ApplyYamlError) -> NoReturn:
    raise click.ClickException(_format_apply_yaml_error(exc)) from exc


def _new_deployment_template_yaml(
    *, action_hint: str = "Edit, then run: llamactl deployments apply -f <file>"
) -> str:
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
    head.append(action_hint)
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

    return render_yaml_template(
        display,
        head=head,
        secret_comments=secret_comments,
        field_alternatives=field_alternatives,
        required=required,
        name_example=preferred_name,
        scaffold_generate_name=True,
    )


def _existing_deployment_template_yaml(deployment: DeploymentResponse) -> str:
    return render_yaml_template(DeploymentDisplay.from_response(deployment))


def _parse_deployment_yaml_text(text: str) -> DeploymentDisplay:
    return parse_apply_yaml(text)


async def _apply_deployment_intent(
    *,
    project: str | None,
    intent: _DeploymentIntent,
    no_push: bool = False,
) -> None:
    async with project_client_context(project_id_override=project) as client:
        await _apply_deployment_from_yaml(
            client,
            intent.display,
            no_push=no_push,
            mode=intent.mode,
            update_target=intent.update_target,
        )


def _apply_deployment_display(
    display: DeploymentDisplay,
    *,
    project: str | None,
    no_push: bool = False,
    mode: DeploymentApplyMode = "apply",
    update_target: str | None = None,
) -> None:
    asyncio.run(
        _apply_deployment_intent(
            project=project,
            intent=_DeploymentIntent(
                display=display,
                mode=mode,
                update_target=update_target,
            ),
            no_push=no_push,
        )
    )


def _apply_deployment_yaml_text(
    text: str,
    *,
    project: str | None,
    no_push: bool = False,
    mode: DeploymentApplyMode = "apply",
    update_target: str | None = None,
) -> None:
    display = _parse_deployment_yaml_text(text)
    _apply_deployment_display(
        display,
        project=project,
        no_push=no_push,
        mode=mode,
        update_target=update_target,
    )


def _apply_deployment_yaml_file(
    *,
    filename: str,
    project: str | None,
    no_push: bool,
    mode: DeploymentApplyMode = "apply",
    update_target: str | None = None,
) -> None:
    text = _read_apply_input(filename)
    try:
        display = _parse_deployment_yaml_text(text)
    except ApplyYamlError as exc:
        _raise_apply_yaml_click_error(exc)

    try:
        _apply_deployment_display(
            display,
            project=project,
            no_push=no_push,
            mode=mode,
            update_target=update_target,
        )
    except ApplyYamlError as exc:
        _raise_apply_yaml_click_error(exc)


async def _fetch_deployment_for_editor(
    *, project: str | None, deployment_id: str
) -> tuple[str, DeploymentResponse]:
    async with project_client_context(project_id_override=project) as client:
        return client.project_id, await client.get_deployment(deployment_id)


def _ci_enabled() -> bool:
    return os.environ.get("CI", "").lower() not in {"", "0", "false", "no"}


def _requires_file_for_editor(interactive: bool) -> bool:
    return not interactive or _ci_enabled()


def _has_non_comment_yaml_lines(text: str) -> bool:
    return any(
        stripped and not stripped.startswith("#")
        for stripped in (line.lstrip() for line in text.splitlines())
    )


def _open_deployment_yaml_editor(current_text: str) -> str | None:
    return click.edit(text=current_text, extension=".yaml")


def _editor_cancelled() -> None:
    rprint(f"[{WARNING}]Cancelled[/]")


def _editor_text_unchanged(current_text: str, last_opened_text: str) -> bool:
    return current_text.rstrip("\n") == last_opened_text.rstrip("\n")


def _editor_noop(mode: DeploymentApplyMode) -> None:
    if mode == "create":
        rprint(f"[{WARNING}]No changes saved; fill in the YAML before creating[/]")
    else:
        rprint(f"[{WARNING}]No changes saved[/]")


def _editor_empty() -> None:
    rprint(f"[{WARNING}]No deployment YAML saved; nothing applied[/]")


def _editor_comments_only() -> None:
    rprint(f"[{WARNING}]No deployment fields saved; add YAML fields and run again[/]")


def _edit_deployment_yaml_loop(
    *,
    initial_yaml: str,
    project: str | None,
    no_push: bool,
    mode: DeploymentApplyMode,
    update_target: str | None = None,
) -> None:
    last_opened_text = initial_yaml
    while True:
        current_text = _open_deployment_yaml_editor(last_opened_text)

        if current_text is None:
            _editor_cancelled()
            return
        if _editor_text_unchanged(current_text, last_opened_text):
            _editor_noop(mode)
            return
        if not current_text.strip():
            _editor_empty()
            return
        if not _has_non_comment_yaml_lines(current_text):
            _editor_comments_only()
            return

        try:
            _apply_deployment_yaml_text(
                current_text,
                project=project,
                no_push=no_push,
                mode=mode,
                update_target=update_target,
            )
            return
        except ApplyYamlError as exc:
            last_opened_text = annotate_yaml_with_errors(current_text, exc.errors)


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
            click.echo(_existing_deployment_template_yaml(deployment), nl=False)
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
    click.echo(_new_deployment_template_yaml(), nl=False)


@deployments.command("create")
@global_options
@click.option(
    "-f",
    "--filename",
    default=None,
    type=click.Path(allow_dash=True, exists=True, dir_okay=False, path_type=str),
    help="Path to YAML file, or '-' for stdin.",
)
@click.option(
    "--no-push",
    is_flag=True,
    default=False,
    help="Skip pushing local code even when the deployment uses push-mode.",
)
@project_option
@interactive_option
def create_deployment(
    filename: str | None,
    no_push: bool,
    project: str | None,
    interactive: bool,
) -> None:
    """Create a new deployment."""
    if filename is not None:
        _apply_deployment_yaml_file(
            filename=filename,
            project=project,
            no_push=no_push,
            mode="create",
        )
        return

    if _requires_file_for_editor(interactive):
        raise click.ClickException("pass -f <file> for non-interactive create")

    _edit_deployment_yaml_loop(
        initial_yaml=_new_deployment_template_yaml(
            action_hint="Edit, save, and close to create the deployment"
        ),
        project=project,
        no_push=no_push,
        mode="create",
    )


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
        remote_name = configure_git_remote(
            git_url, client.api_key, client.project_id, deployment_id
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
    git_url = get_deployment_git_url(client.base_url, deployment_id)
    remote_name = configure_git_remote(
        git_url, client.api_key, client.project_id, deployment_id
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


def _create_identity_error() -> FieldError:
    msg = "set top-level 'name' or 'generate_name'"
    return _error(("name",), msg)


def _create_source_error() -> FieldError:
    msg = 'set spec.repo_url for create (use "" for push-mode)'
    return _error(("spec", "repo_url"), msg)


def _normalize_create_display(display: DeploymentDisplay) -> DeploymentDisplay:
    if display.name is not None and display.generate_name is None:
        return display.model_copy(update={"generate_name": display.name})
    return display


def _validate_create_intent(display: DeploymentDisplay) -> None:
    errors: list[FieldError] = []
    if not display.name and not display.generate_name:
        errors.append(_create_identity_error())
    if "repo_url" not in display.spec.model_fields_set or display.spec.repo_url is None:
        errors.append(_create_source_error())
    if errors:
        message = (
            errors[0].message if len(errors) == 1 else "deployment YAML has errors"
        )
        raise ApplyYamlError(message, errors=errors)


async def _resolve_deployment_operation(
    client: Any,
    intent: _DeploymentIntent,
) -> _ResolvedDeploymentOperation:
    # Deferred: llamactl startup budget avoids importing httpx at module level.
    import httpx

    display = intent.display

    if intent.mode == "create":
        display = _normalize_create_display(display)
        _validate_create_intent(display)
        return _ResolvedDeploymentOperation(
            action="create",
            display=display,
            payload=display.to_create_payload(),
        )

    if intent.mode == "update":
        update_target = intent.update_target or display.name
        if update_target is None:
            msg = "YAML must include top-level 'name' for edit"
            raise ApplyYamlError(msg, errors=[_error(("name",), msg)])
        if display.name is not None and display.name != update_target:
            msg = (
                f"YAML name '{display.name}' does not match deployment "
                f"'{update_target}'"
            )
            raise ApplyYamlError(msg, errors=[_error(("name",), msg)])

        display = display.model_copy(update={"name": update_target})
        existing = await client.get_deployment(update_target)
        return _ResolvedDeploymentOperation(
            action="update",
            display=display,
            payload=display.to_update_payload(),
            existing=existing,
        )

    if display.name is not None:
        try:
            existing = await client.get_deployment(display.name)
            return _ResolvedDeploymentOperation(
                action="update",
                display=display,
                payload=display.to_update_payload(),
                existing=existing,
            )
        except httpx.HTTPStatusError as exc:
            if exc.response.status_code != 404:
                raise

    display = _normalize_create_display(display)
    _validate_create_intent(display)
    return _ResolvedDeploymentOperation(
        action="create",
        display=display,
        payload=display.to_create_payload(),
    )


async def _execute_deployment_operation(
    client: Any,
    operation: _ResolvedDeploymentOperation,
    *,
    no_push: bool = False,
) -> None:
    display = operation.display
    existing = operation.existing
    is_update = operation.action == "update"

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
        assert repo_url is not None
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
    elif display.spec.repo_url in {"", INTERNAL_CODE_REPO_SCHEME}:
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

    if push_before_save:
        assert existing is not None and display.name is not None
        _apply_push(
            client,
            existing.id,
            display.spec.git_ref or existing.git_ref,
        )
        response = await client.update_deployment(display.name, operation.payload)
        click.echo(f"updated {response.id}")
    elif operation.action == "update":
        assert display.name is not None
        response = await client.update_deployment(display.name, operation.payload)
        click.echo(f"updated {response.id}")
        if desired_is_push:
            _apply_push_after_save(client, response.id, display.spec.git_ref)
    else:
        response = await client.create_deployment(operation.payload)
        click.echo(f"created {response.id}")
        if desired_is_push:
            _apply_push_after_save(client, response.id, display.spec.git_ref)


def _create_conflict_error(display: DeploymentDisplay) -> ApplyYamlError:
    deployment_id = display.name or display.generate_name or "deployment"
    msg = (
        f"deployment '{deployment_id}' already exists; use "
        "`llamactl deployments edit -f` or `llamactl deployments apply -f` "
        "to update"
    )
    return ApplyYamlError(msg, errors=[_error(("name",), msg)])


async def _apply_deployment_from_yaml(
    client: Any,
    display: DeploymentDisplay,
    *,
    no_push: bool = False,
    mode: DeploymentApplyMode = "apply",
    update_target: str | None = None,
) -> None:
    # Deferred: llamactl startup budget avoids importing httpx at module level.
    import httpx

    try:
        intent = _DeploymentIntent(
            display=display,
            mode=mode,
            update_target=update_target,
        )
        operation = await _resolve_deployment_operation(client, intent)
        await _execute_deployment_operation(client, operation, no_push=no_push)
    except ApplyYamlError:
        raise
    except RepositoryValidationError as exc:
        raise ApplyYamlError(
            exc.message, errors=[_error(exc.path, exc.message)]
        ) from exc
    except PushFailedError as exc:
        raise ApplyYamlError(exc.message, errors=[_error((), exc.message)]) from exc
    except PayloadError as exc:
        raise ApplyYamlError(str(exc), errors=[_error(exc.path, str(exc))]) from exc
    except ValidationError as exc:
        raise ApplyYamlError(
            str(exc),
            errors=_validation_error_to_field_errors(exc, display=display),
            original_error=exc,
        ) from exc
    except httpx.HTTPStatusError as exc:
        if (
            mode == "create"
            and exc.response.status_code == 409
            and display.name is not None
        ):
            raise _create_conflict_error(display) from exc
        raise ApplyYamlError(
            str(exc),
            errors=_http_error_to_field_errors(exc, display=display),
            original_error=exc,
        ) from exc
    except Exception as exc:
        raise ApplyYamlError(str(exc)) from exc


@deployments.command("apply")
@global_options
@click.option(
    "-f",
    "--filename",
    required=True,
    type=click.Path(allow_dash=True, exists=True, dir_okay=False, path_type=str),
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
        display = _parse_deployment_yaml_text(text)
    except ApplyYamlError as exc:
        if annotate_on_error and not dry_run:
            _handle_annotated_apply_error(
                filename=filename,
                text=text,
                errors=exc.errors,
            )
        _raise_apply_yaml_click_error(exc)

    if dry_run:
        try:
            _validate_dry_run_payload(display)
        except ApplyYamlError as exc:
            _raise_apply_yaml_click_error(exc)

        verdict = (
            f"would upsert deployment '{display.name}'"
            if display.name
            else f"would create deployment '{display.generate_name}'"
        )

        click.echo(
            yaml.safe_dump(
                display.spec.as_redacted().model_dump(mode="json", exclude_unset=True),
                sort_keys=False,
            )
        )
        click.echo(verdict)
        return

    try:
        _apply_deployment_display(display, project=project, no_push=no_push)
    except ApplyYamlError as exc:
        if annotate_on_error:
            _handle_annotated_apply_error(
                filename=filename,
                text=text,
                errors=exc.errors,
            )
        _raise_apply_yaml_click_error(exc)


@deployments.command("edit")
@global_options
@click.argument("deployment_id", required=False, type=DeploymentType())
@click.option(
    "-f",
    "--filename",
    default=None,
    type=click.Path(allow_dash=True, exists=True, dir_okay=False, path_type=str),
    help="Path to YAML file, or '-' for stdin.",
)
@click.option(
    "--no-push",
    is_flag=True,
    default=False,
    help="Skip pushing local code even when the deployment uses push-mode.",
)
@project_option
@interactive_option
def edit_deployment(
    deployment_id: str | None,
    filename: str | None,
    no_push: bool,
    interactive: bool,
    project: str | None,
) -> None:
    """Edit a deployment in $EDITOR."""
    if filename is not None:
        _apply_deployment_yaml_file(
            filename=filename,
            project=project,
            no_push=no_push,
            mode="update",
            update_target=deployment_id,
        )
        return

    if _requires_file_for_editor(interactive):
        raise click.ClickException("pass -f <file> for non-interactive edit")

    effective_project: str | None = project
    try:
        deployment_id = select_deployment(
            deployment_id, interactive=interactive, project_id_override=project
        )
        if not deployment_id:
            rprint(f"[{WARNING}]No deployment selected[/]")
            return

        effective_project, current_deployment = asyncio.run(
            _fetch_deployment_for_editor(project=project, deployment_id=deployment_id)
        )
        _edit_deployment_yaml_loop(
            initial_yaml=_existing_deployment_template_yaml(current_deployment),
            project=project,
            no_push=no_push,
            mode="update",
            update_target=deployment_id,
        )

    except Exception as e:
        friendly = friendly_http_error(
            e, deployment_id=deployment_id, project_id=effective_project
        )
        message = friendly if friendly is not None else str(e)
        rprint(f"[red]Error: {message}[/red]")
        raise click.Abort()


def _push_internal_for_update(
    deployment_id: str,
    git_ref: str | None,
    client: Any,
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

    git_url = get_deployment_git_url(client.base_url, deployment_id)
    remote_name = configure_git_remote(
        git_url, client.api_key, client.project_id, deployment_id
    )
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
                        client=client,
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
