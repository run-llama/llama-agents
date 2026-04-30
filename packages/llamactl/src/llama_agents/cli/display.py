# SPDX-License-Identifier: MIT
# Copyright (c) 2026 LlamaIndex Inc.
"""Declarative column framework for tabular CLI read commands.

A "tabular read command" emits a CLI-side display model whose fields carry
:class:`Column` markers in their ``Annotated[]`` metadata. A small walker
(``resolve_columns``) reads the markers; ``render_columns`` derives a
plain-whitespace table; consumers compose this with ``render_output`` (in
``cli.options``) to dispatch text/json/yaml/wide.

The ``Annotated[]`` channel is intentionally open: future markers
(``YamlComment`` for pedagogical comments in template output, ``Alias`` for
legacy-name input tolerance on ``apply``, etc.) live alongside ``Column`` on
the same fields. Each consumer reads ``field.metadata`` and filters on its own
marker class via ``isinstance`` — markers do not register with the framework.

Per-command display models (``DeploymentDisplay``, ``ReleaseDisplay``,
``AuthProfileDisplay``, …) live with the commands that consume them and
import the primitives from this module.
"""

from __future__ import annotations

import functools
import types
from dataclasses import dataclass
from datetime import datetime
from typing import Any, Callable, Literal, Union, get_args, get_origin

from llama_agents.cli.render import format_iso_z, gh_short, short_sha, star_marker
from llama_agents.core.schema.deployments import (
    DeploymentResponse,
    LlamaDeploymentPhase,
    ReleaseHistoryItem,
)
from llama_agents.core.schema.projects import OrgSummary
from pydantic import BaseModel, ConfigDict
from typing_extensions import Annotated

SECRET_MASK = "********"

# Sentinel value of ``DeploymentSpec.repo_url`` indicating push-mode (the CLI
# pushes the local working tree on apply rather than pointing at a remote).
PUSH_MODE_REPO_URL = ""


def strip_masks(spec_data: dict[str, Any]) -> dict[str, Any]:
    """Remove :data:`SECRET_MASK` sentinels from a serialized spec dict.

    - ``secrets``: drop entries whose value equals the mask; drop the key
      entirely if no entries remain.
    - ``personal_access_token``: drop the key if its value equals the mask.

    The filter runs at the emit boundary so masked values never leak back
    into apply input via ``get | edit | apply`` round-trips.
    """
    out = dict(spec_data)
    secrets = out.get("secrets")
    if isinstance(secrets, dict):
        filtered = {k: v for k, v in secrets.items() if v != SECRET_MASK}
        if filtered:
            out["secrets"] = filtered
        else:
            out.pop("secrets", None)
    if out.get("personal_access_token") == SECRET_MASK:
        out.pop("personal_access_token", None)
    return out


@dataclass(frozen=True)
class Doc:
    """Marker placed in a field's ``Annotated[]`` metadata to attach a doc comment.

    Consumed by the YAML template renderer (``cli.yaml_template.render``):
    each ``Doc(text)`` becomes one ``## <text>`` line per ``\\n``-separated
    chunk above the field's key in the rendered output. ``Doc`` coexists with
    :class:`Column` on the same field and is read independently via
    ``isinstance`` filtering of ``field.metadata``.

    Args:
        text: The comment body. Rendered verbatim, prefixed with ``## ``. May
            contain ``\\n`` for multi-line guidance — each line emits as its
            own ``##`` comment in the output.
    """

    text: str


@dataclass(frozen=True)
class TrailingDoc:
    """Marker for a short YAML note rendered after a field's scalar value."""

    text: str


@dataclass(frozen=True)
class Column:
    """Marker placed in a field's ``Annotated[]`` metadata to declare a column.

    The marker is a pure data class; it carries no behaviour. The walker
    (:func:`resolve_columns`) discovers ``Column`` instances and the renderer
    (:func:`render_columns`) consumes them.

    Args:
        header: Column header rendered verbatim in the table.
        format: Optional cell formatter. Called with the raw field value
            (only when non-None). Must return a string.
        default: Cell text when the value (or any nested-model parent on the
            field path) is ``None``.
        wide: When ``True``, the column appears only under ``-o wide``.
    """

    header: str
    format: Callable[[Any], str] | None = None
    default: str = ""
    wide: bool = False


@dataclass(frozen=True)
class ResolvedColumn:
    """A walker-derived column: its declaration path plus the marker."""

    path: tuple[str, ...]
    column: Column


def _is_basemodel(tp: Any) -> bool:
    return isinstance(tp, type) and issubclass(tp, BaseModel)


def _unwrap_optional_model(annotation: Any) -> type[BaseModel] | None:
    """If ``annotation`` is ``BaseModel``, ``Optional[BaseModel]`` or
    ``BaseModel | None``, return the model class. Otherwise ``None``."""

    if _is_basemodel(annotation):
        return annotation  # type: ignore[return-value]
    origin = get_origin(annotation)
    if origin is Union or origin is types.UnionType:
        non_none = [a for a in get_args(annotation) if a is not type(None)]
        if len(non_none) == 1 and _is_basemodel(non_none[0]):
            return non_none[0]
    return None


@functools.cache
def resolve_columns(model_cls: type[BaseModel]) -> tuple[ResolvedColumn, ...]:
    """Walk a display model in declaration order and return its columns.

    Field annotations carrying a ``Column`` marker yield a leaf column at the
    field's path. Fields whose annotation is a ``BaseModel`` (or
    ``Optional[BaseModel]``) are descended into. All other fields are skipped.

    A field carrying multiple ``Column`` markers is a typo: the walker raises
    ``ValueError`` rather than silently picking one.

    Display models are assumed to form a tree, not a graph — circular
    references are not supported.
    """

    return tuple(_walk(model_cls, ()))


def _walk(model_cls: type[BaseModel], prefix: tuple[str, ...]) -> list[ResolvedColumn]:
    out: list[ResolvedColumn] = []
    for name, info in model_cls.model_fields.items():
        path = prefix + (name,)
        cols = [m for m in info.metadata if isinstance(m, Column)]
        if len(cols) > 1:
            raise ValueError(
                f"{model_cls.__name__}.{name}: multiple Column annotations on a single field"
            )
        if cols:
            out.append(ResolvedColumn(path=path, column=cols[0]))
            continue
        nested = _unwrap_optional_model(info.annotation)
        if nested is not None:
            out.extend(_walk(nested, path))
    return out


def _extract_cell(row: BaseModel, column: ResolvedColumn) -> str:
    value: Any = row
    for part in column.path:
        if value is None:
            return column.column.default
        value = getattr(value, part)
    if value is None:
        return column.column.default
    if column.column.format is not None:
        return column.column.format(value)
    return str(value)


def render_columns(
    rows: list[BaseModel] | list[Any],
    *,
    wide: bool = False,
) -> None:
    """Render ``rows`` as a plain-whitespace table using ``Column`` metadata.

    ``rows`` must be homogeneous; the row class is read from the first
    element. An empty list still emits headers (matches ``render_table``'s
    empty-row behaviour). Columns marked ``wide=True`` are filtered out unless
    ``wide`` is ``True``.
    """

    from llama_agents.cli.render import render_table  # local: avoid cycle

    if not rows:
        # No row to derive a class from. Caller is expected to have emitted a
        # status message ("No X found") before reaching here — but if not,
        # we silently emit nothing rather than guessing the column layout.
        return

    row_cls = type(rows[0])
    if not isinstance(rows[0], BaseModel):
        raise TypeError(
            f"render_columns expects BaseModel rows; got {row_cls.__name__}"
        )

    cols = [c for c in resolve_columns(row_cls) if wide or not c.column.wide]
    columns = [(c.column.header, c.column.header) for c in cols]
    table_rows: list[dict[str, str]] = []
    for row in rows:
        table_rows.append({c.column.header: _extract_cell(row, c) for c in cols})
    render_table(table_rows, columns)


class DeploymentSpec(BaseModel):
    """Editable deployment fields.

    Every editable field is ``Optional`` (or has a default of ``None``) so the
    same model serves three roles: (1) projection of a server-known deployment
    (``DeploymentDisplay.from_response`` populates everything explicitly),
    (2) input shape for ``apply`` (any subset is valid; create-time required
    fields are enforced by the apply translator, not this model), (3) input
    shape for partial updates (``model_dump(exclude_unset=True)`` produces a
    clean patch payload).

    ``personal_access_token`` is a leaky abstraction over the server-side
    ``GITHUB_PAT`` secret — it is surfaced here as a dedicated field rather
    than mixed into ``secrets`` so the apply input shape is explicit.
    """

    model_config = ConfigDict(extra="forbid")

    repo_url: Annotated[
        str | None,
        Column("REPO", format=gh_short, default="-"),
        Doc(
            '"" = push your local working tree on apply.\n'
            '"internal://" = reuse a previously-pushed working tree.\n'
            "https://… = remote git URL (GitHub, GitLab, etc.)."
        ),
    ] = None
    deployment_file_path: Annotated[
        str | None,
        TrailingDoc("pyproject.toml or llama_deploy.yaml"),
    ] = None
    git_ref: Annotated[
        str | None,
        Column("GIT_REF", default="-"),
    ] = None
    appserver_version: Annotated[
        str | None,
        Column("APPSERVER", default="-", wide=True),
        TrailingDoc("auto-pinned from local install"),
    ] = None
    # No Column: suspended state is already visible via status.phase.
    suspended: Annotated[
        bool | None,
        TrailingDoc("scale to zero without deleting"),
    ] = None
    # ``str | None`` value type matches ``DeploymentUpdate.secrets`` on the wire:
    # null values delete on apply.
    secrets: Annotated[
        dict[str, str | None] | None,
        Doc(
            "Secret env vars. ${VAR} reads from your local environment at apply time.\n"
            "Values are masked after apply — set them, don't expect to read back."
        ),
    ] = None
    personal_access_token: Annotated[
        str | None,
        TrailingDoc("private-repo access (GitHub PAT, GitLab access token)"),
    ] = None


class DeploymentStatus(BaseModel):
    """Read-only / system-set deployment status block.

    These fields reflect runtime state and are not editable via ``apply``.
    ``warning`` is intentionally always serialized (explicit ``null`` when no
    warning is present) — absence vs. explicit-null carries meaning for the
    future ``describe`` command.
    """

    model_config = ConfigDict(extra="forbid")

    # `LlamaDeploymentPhase | str` tolerates unknown phase values from a newer
    # server; the column renders whatever string the server sent.
    phase: Annotated[LlamaDeploymentPhase | str, Column("PHASE")]
    git_sha: Annotated[
        str | None, Column("GIT_SHA", format=short_sha, default="-", wide=True)
    ] = None
    apiserver_url: Annotated[
        str | None, Column("APISERVER_URL", default="-", wide=True)
    ] = None
    project_id: Annotated[str, Column("PROJECT", wide=True)]
    warning: str | None = None


class DeploymentDisplay(BaseModel):
    """CLI projection of a deployment.

    Top-level ``name`` is the stable id (immutable on update). ``spec``
    carries the editable surface. ``status`` carries everything set by the
    server. Use :meth:`from_response` to translate a ``DeploymentResponse``
    into this shape; use :meth:`to_output_dict` to obtain a dict suitable for
    JSON/YAML emission with the omit-when-empty rules applied.
    """

    model_config = ConfigDict(extra="forbid")

    # ``None`` is used by the ``deployments template`` command to render the
    # top-level key as a commented-out example; ``from_response`` always
    # populates it from the wire id.
    name: Annotated[
        str | None,
        Column("NAME", default="-"),
        Doc("Stable id for the deployment. Immutable on update."),
    ] = None
    # Slug seed used by the server when top-level ``name`` is unset. Surfaced
    # at the identity tier (sibling to ``name``) since it answers a
    # what-id-do-I-get question, not an editable-spec question. Wire-side
    # the field is still ``display_name`` on ``DeploymentResponse``; the CLI
    # flattens at :meth:`from_response`.
    generate_name: str | None = None
    spec: DeploymentSpec
    status: DeploymentStatus | None = None

    @classmethod
    def from_response(cls, r: DeploymentResponse) -> DeploymentDisplay:
        """Project a wire ``DeploymentResponse`` into the CLI display shape."""
        secret_names = r.secret_names or []
        secrets: dict[str, str | None] | None = (
            {name: SECRET_MASK for name in secret_names} if secret_names else None
        )
        pat = SECRET_MASK if r.has_personal_access_token else None
        spec = DeploymentSpec(
            repo_url=r.repo_url,
            deployment_file_path=r.deployment_file_path,
            git_ref=r.git_ref,
            appserver_version=r.appserver_version,
            suspended=r.suspended,
            secrets=secrets,
            personal_access_token=pat,
        )
        status = DeploymentStatus(
            phase=r.status,
            git_sha=r.git_sha,
            apiserver_url=str(r.apiserver_url) if r.apiserver_url else None,
            project_id=r.project_id,
            warning=r.warning,
        )
        return cls(name=r.id, generate_name=r.display_name, spec=spec, status=status)

    def to_output_dict(self) -> dict[str, Any]:
        """Return the dict shape used for JSON/YAML rendering.

        Omits fields inside ``spec`` whose value is None and strips the
        ``SECRET_MASK`` sentinel from ``secrets`` and ``personal_access_token``
        so a ``get | edit | apply`` round-trip can't push a literal ``********``
        back as the value. ``generate_name`` is emitted at the top level only
        when set. The nested ``status`` block is preserved verbatim so its
        ``warning`` key remains explicit even when ``null``.
        """
        spec_data = self.spec.model_dump(mode="json")
        spec_data = strip_masks({k: v for k, v in spec_data.items() if v is not None})
        data: dict[str, Any] = {"name": self.name}
        if self.generate_name is not None:
            data["generate_name"] = self.generate_name
        data["spec"] = spec_data
        if self.status is not None:
            data["status"] = self.status.model_dump(mode="json")
        return data


class ReleaseDisplay(BaseModel):
    """A single release-history entry, projected for table output."""

    model_config = ConfigDict(extra="forbid")

    released_at: Annotated[datetime, Column("RELEASED_AT", format=format_iso_z)]
    git_sha: Annotated[str, Column("GIT_SHA", format=short_sha)]
    image_tag: Annotated[str | None, Column("IMAGE_TAG", default="-")] = None

    @classmethod
    def from_response(cls, item: ReleaseHistoryItem) -> ReleaseDisplay:
        return cls(
            released_at=item.released_at,
            git_sha=item.git_sha,
            image_tag=item.image_tag,
        )


class AuthProfileDisplay(BaseModel):
    """A locally-stored auth profile, projected for ``auth list``.

    Secret material (``api_key``, OIDC tokens) is intentionally not surfaced.
    """

    model_config = ConfigDict(extra="forbid")

    name: Annotated[str, Column("NAME")]
    api_url: Annotated[str, Column("API_URL")]
    project_id: Annotated[str | None, Column("PROJECT_ID", default="-")] = None
    active: Annotated[bool, Column("ACTIVE", format=star_marker)] = False
    auth_type: Annotated[Literal["none", "token", "oidc"], Column("AUTH")] = "none"

    @classmethod
    def from_profile(
        cls, profile: Any, *, current_name: str | None
    ) -> AuthProfileDisplay:
        if profile.device_oidc:
            auth_type: Literal["none", "token", "oidc"] = "oidc"
        elif profile.api_key:
            auth_type = "token"
        else:
            auth_type = "none"
        return cls(
            name=profile.name,
            api_url=profile.api_url,
            project_id=profile.project_id,
            active=profile.name == current_name,
            auth_type=auth_type,
        )


def _bool_str_lower(value: bool) -> str:
    return "true" if value else "false"


class EnvDisplay(BaseModel):
    """A configured environment, projected for ``auth env list``.

    ``min_llamactl_version`` is intentionally omitted — it isn't part of the
    public env-list contract.
    """

    model_config = ConfigDict(extra="forbid")

    api_url: Annotated[str, Column("API_URL")]
    requires_auth: Annotated[bool, Column("REQUIRES_AUTH", format=_bool_str_lower)]
    active: Annotated[bool, Column("ACTIVE", format=star_marker)] = False

    @classmethod
    def from_environment(cls, env: Any, *, current_url: str | None) -> EnvDisplay:
        return cls(
            api_url=env.api_url,
            requires_auth=env.requires_auth,
            active=env.api_url == current_url,
        )


def _yes_if_true(value: bool) -> str:
    return "yes" if value else ""


class OrgDisplay(BaseModel):
    """An organization summary, projected for ``auth organizations``."""

    model_config = ConfigDict(extra="forbid")

    org_id: Annotated[str, Column("ORG_ID")]
    org_name: Annotated[str, Column("NAME")]
    is_default: Annotated[bool, Column("DEFAULT", format=_yes_if_true)]
    active: Annotated[bool, Column("ACTIVE", format=star_marker)] = False

    @classmethod
    def from_org_summary(
        cls, org: OrgSummary, *, current_org_id: str | None = None
    ) -> OrgDisplay:
        return cls(
            org_id=org.org_id,
            org_name=org.org_name,
            is_default=org.is_default,
            active=current_org_id is not None and org.org_id == current_org_id,
        )
