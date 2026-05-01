import json
import logging
import os
from typing import Any, Callable, ParamSpec, TypeVar

import click
from llama_agents.cli.interactive_prompts.session_utils import is_interactive_session
from llama_agents.cli.param_types import ProjectType
from pydantic import BaseModel

from .debug import setup_file_logging

P = ParamSpec("P")
R = TypeVar("R")


def global_options(f: Callable[P, R]) -> Callable[P, R]:
    """Common decorator to add global options to command groups"""

    return native_tls_option(file_logging(f))


_BASE_OUTPUT_CHOICES = ("text", "json", "yaml", "wide")
_BASE_OUTPUT_HELP = (
    "Output format. 'json'/'yaml' for machine-readable output; "
    "'wide' for the text table with extra columns."
)


def _output_option(
    *extra_choices: str, help_text: str = _BASE_OUTPUT_HELP
) -> Callable[[Callable[P, R]], Callable[P, R]]:
    choices = list(_BASE_OUTPUT_CHOICES) + list(extra_choices)
    return click.option(
        "-o",
        "--output",
        "output",
        type=click.Choice(choices, case_sensitive=False),
        default="text",
        show_default=True,
        help=help_text,
    )


def output_option(f: Callable[P, R]) -> Callable[P, R]:
    """Add a `-o/--output` option for read commands.

    Choices: ``text`` (default), ``json``, ``yaml``, ``wide``. ``wide`` is
    text mode with the less-common columns interleaved into their natural
    positions (kubectl-style).
    """

    return _output_option()(f)


def output_option_with_template(f: Callable[P, R]) -> Callable[P, R]:
    """Variant of :func:`output_option` that adds the ``template`` choice.

    ``template`` emits the apply-shaped YAML scaffold (annotated with ``#!``
    docs, suitable for ``llamactl deployments apply -f``). It only makes sense
    on a single-row read (``deployments get <name>``) and is opted-in per
    command rather than advertised on every ``-o``-bearing command.
    """

    return _output_option(
        "template",
        help_text=(
            f"{_BASE_OUTPUT_HELP[:-1]}; "
            "'template' for an annotated YAML scaffold suitable for `apply`."
        ),
    )(f)


def project_option(f: Callable[P, R]) -> Callable[P, R]:
    """Add a ``--project`` option to override the active profile's project for a command.

    The value is exposed as the ``project`` keyword argument and should be
    threaded into ``get_project_client()`` or ``project_client_context()`` via
    the ``project_id_override`` parameter.
    """

    return click.option(
        "--project",
        "project",
        type=ProjectType(),
        default=None,
        help="Project ID to use for this command (overrides active profile).",
    )(f)


def render_output(
    payload: BaseModel | list[BaseModel] | Any,
    output: str,
    text_renderer: Callable[[], None] | None = None,
) -> None:
    """Render a payload according to ``output`` mode.

    - ``text`` / ``wide``: if ``payload`` is a ``BaseModel`` (or list of
      them) whose class declares ``Column`` annotations, the framework
      derives the table directly. Otherwise (or if ``text_renderer`` is
      provided as an explicit override) the supplied text renderer runs.
      ``wide`` includes ``wide=True`` columns; ``text`` excludes them.
    - ``json``: emit canonical JSON via ``click.echo`` (no Rich markup).
    - ``yaml``: emit YAML via ``click.echo`` (the naive Pydantic shape).

    ``payload`` may be a Pydantic model, a list of Pydantic models, or any
    JSON-serializable value. Structured outputs go through ``click.echo`` so
    they pipe cleanly even when Rich would otherwise insert markup.
    """

    # Defer the import: the framework lives in ``cli.display`` which has
    # heavier transitive imports than ``options.py`` itself.
    from llama_agents.cli.display import render_columns, resolve_columns

    mode = output.lower()
    if mode == "template":
        # ``-o template`` is the apply-shaped scaffold output and only makes
        # sense for ``deployments get <name>``. Each command that supports it
        # short-circuits before calling ``render_output``; reaching here with
        # ``template`` means the command does not.
        raise click.ClickException(
            "-o template is only supported for `llamactl deployments get <name>`"
        )
    if mode in {"text", "wide"}:
        rows: list[BaseModel] | None = None
        if isinstance(payload, BaseModel):
            rows = [payload]
        elif (
            isinstance(payload, list) and payload and isinstance(payload[0], BaseModel)
        ):
            rows = list(payload)
        if rows is not None and resolve_columns(type(rows[0])):
            render_columns(rows, wide=(mode == "wide"))
            return
        if isinstance(payload, list) and not payload:
            # Empty list in text/wide mode: caller is expected to have
            # emitted a status line ("No X found"). Silently no-op rather
            # than demanding a text_renderer for the degenerate case.
            return
        if text_renderer is None:
            raise click.ClickException("No text renderer available for this payload.")
        text_renderer()
        return

    def _to_json_safe(value: Any) -> Any:
        # Models with a custom ``to_output_dict`` (e.g. ``DeploymentDisplay``)
        # own their on-the-wire shape — including which null keys to keep —
        # so prefer that over a raw ``model_dump``.
        to_output = getattr(value, "to_output_dict", None)
        if callable(to_output):
            return to_output()
        if isinstance(value, BaseModel):
            return value.model_dump(mode="json")
        if isinstance(value, list):
            return [_to_json_safe(item) for item in value]
        if isinstance(value, dict):
            return {k: _to_json_safe(v) for k, v in value.items()}
        return value

    if mode == "json":
        click.echo(json.dumps(_to_json_safe(payload), indent=2))
        return
    if mode == "yaml":
        # Defer pyyaml import: it's a measurable chunk of CLI startup and
        # only paid for by the (rare) `-o yaml` path.
        import yaml

        click.echo(yaml.safe_dump(_to_json_safe(payload), sort_keys=False))
        return

    raise click.ClickException(f"Unknown output mode: {output}")


def interactive_option(f: Callable[P, R]) -> Callable[P, R]:
    """Add an interactive option to the command"""

    default = is_interactive_session()
    return click.option(
        "--interactive/--no-interactive",
        help="Run in interactive mode. If not provided, will default to the current session's interactive state.",
        is_flag=True,
        default=default,
    )(f)


def native_tls_option(f: Callable[P, R]) -> Callable[P, R]:
    """Enable native TLS to trust system configured trust store rather than python bundled trust stores.

    When enabled, we set:
    - UV_NATIVE_TLS=1 to instruct uv to use the platform trust store
    - LLAMA_DEPLOY_USE_TRUSTSTORE=1 to use system certificate store for Python httpx clients
    """

    def _enable_native_tls(
        ctx: click.Context, param: click.Parameter, value: bool
    ) -> bool:
        if value:
            # Don't override if user explicitly set a value
            os.environ.setdefault("UV_NATIVE_TLS", "1")
            os.environ.setdefault("LLAMA_DEPLOY_USE_TRUSTSTORE", "1")
        return value

    return click.option(
        "--native-tls",
        is_flag=True,
        help=(
            "Enable native TLS mode to use system certificate store rather than runtime defaults. Can be set via LLAMACTL_NATIVE_TLS=1"
        ),
        callback=_enable_native_tls,
        expose_value=False,
        is_eager=True,
        envvar=["LLAMACTL_NATIVE_TLS"],
    )(f)


def file_logging(f: Callable[P, R]) -> Callable[P, R]:
    def debug_callback(ctx: click.Context, param: click.Parameter, value: str) -> str:
        if value:
            setup_file_logging(level=logging._nameToLevel.get(value, logging.INFO))
        return value

    return click.option(
        "--log-level",
        type=click.Choice(
            ["DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"], case_sensitive=False
        ),
        help="Enable debug logging to file",
        callback=debug_callback,
        expose_value=False,
        is_eager=True,
        hidden=True,
    )(f)
