# SPDX-License-Identifier: MIT
# Copyright (c) 2026 LlamaIndex Inc.
"""YAML parsing and translation for ``llamactl deployments apply -f``.

Pure parsing + validation — no network, no client calls.  Takes YAML text,
resolves ``${VAR}`` environment variables, strips ``********`` mask sentinels,
validates against :class:`DeploymentDisplay`, and translates to
:class:`DeploymentCreate` / :class:`DeploymentUpdate` wire models.
"""

from __future__ import annotations

import os
import re
from typing import Any

import yaml
from llama_agents.cli.display import SECRET_MASK, DeploymentDisplay
from llama_agents.core.schema.deployments import DeploymentCreate, DeploymentUpdate
from pydantic import ValidationError

_ENV_VAR_RE = re.compile(r"\$\{([^}]+)\}")


class ApplyYamlError(Exception):
    """Base error for YAML apply parsing/validation failures."""


class UnresolvedEnvVarsError(ApplyYamlError):
    """Raised when ``${VAR}`` references cannot be resolved."""

    def __init__(self, unresolved: list[str]) -> None:
        self.unresolved = unresolved
        super().__init__(
            f"unresolved environment variables: {', '.join(sorted(unresolved))}"
        )


# ---------------------------------------------------------------------------
# Environment variable resolution
# ---------------------------------------------------------------------------


def resolve_env_vars(value: Any, *, strict: bool = True) -> Any:
    """Recursively resolve ``${VAR}`` patterns from ``os.environ``.

    For strings, replace every ``${VAR}`` with the matching env var.
    Dicts and lists are walked recursively.  Other types pass through.

    In strict mode, all unresolved vars are collected and raised as
    :class:`UnresolvedEnvVarsError`.  Non-strict mode leaves ``${VAR}``
    literals in place.
    """
    unresolved: list[str] = []
    result = _resolve(value, unresolved)
    if strict and unresolved:
        raise UnresolvedEnvVarsError(unresolved)
    return result


def _resolve(value: Any, unresolved: list[str]) -> Any:
    if isinstance(value, str):
        return _resolve_string(value, unresolved)
    if isinstance(value, dict):
        return {k: _resolve(v, unresolved) for k, v in value.items()}
    if isinstance(value, list):
        return [_resolve(item, unresolved) for item in value]
    return value


def _resolve_string(text: str, unresolved: list[str]) -> str:
    def _replacer(match: re.Match[str]) -> str:
        var = match.group(1)
        env_val = os.environ.get(var)
        if env_val is None:
            unresolved.append(var)
            return match.group(0)  # leave ${VAR} as-is
        return env_val

    return _ENV_VAR_RE.sub(_replacer, text)


# ---------------------------------------------------------------------------
# Mask stripping
# ---------------------------------------------------------------------------


def _strip_masks(data: dict[str, Any]) -> dict[str, Any]:
    """Drop ``********`` sentinel values from spec-level ``secrets`` and
    ``personal_access_token`` so masked round-trip values never reach pydantic
    or the wire models.
    """
    out = dict(data)

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


# ---------------------------------------------------------------------------
# Main parse entry point
# ---------------------------------------------------------------------------


def parse_apply_yaml(text: str, *, strict_env: bool = True) -> DeploymentDisplay:
    """Parse YAML apply input into a validated :class:`DeploymentDisplay`.

    1. ``yaml.safe_load`` → dict.
    2. Drop ``status`` (round-trip artifact from ``get -o yaml``).
    3. Resolve ``${VAR}`` env vars in ``spec`` values.
    4. Strip ``********`` mask sentinels from ``spec.secrets`` and
       ``spec.personal_access_token``.
    5. Validate against :class:`DeploymentDisplay` (pydantic handles
       ``extra="forbid"`` rejection for typos / excluded fields).
    6. Wrap :class:`~pydantic.ValidationError` into :class:`ApplyYamlError`.
    """
    try:
        raw = yaml.safe_load(text)
    except yaml.YAMLError as exc:
        raise ApplyYamlError(f"invalid YAML: {exc}") from exc

    if not isinstance(raw, dict):
        raise ApplyYamlError(
            f"expected a YAML mapping at the top level, got {type(raw).__name__}"
        )

    # Drop read-only status block.
    raw.pop("status", None)

    # Resolve env vars inside spec.
    spec = raw.get("spec")
    if isinstance(spec, dict):
        raw["spec"] = resolve_env_vars(spec, strict=strict_env)
        raw["spec"] = _strip_masks(raw["spec"])

    try:
        return DeploymentDisplay.model_validate(raw)
    except ValidationError as exc:
        raise ApplyYamlError(str(exc)) from exc


# ---------------------------------------------------------------------------
# Translation to wire models
# ---------------------------------------------------------------------------


def apply_payload_to_create(display: DeploymentDisplay) -> DeploymentCreate:
    """Translate a parsed :class:`DeploymentDisplay` into a
    :class:`DeploymentCreate` wire model.

    Raises :class:`ApplyYamlError` for create-time constraint violations.
    """
    spec = display.spec

    # Create-only restrictions.
    if spec.suspended is not None:
        raise ApplyYamlError(
            "cannot create a deployment as suspended; "
            "create it first, then update with --suspended"
        )

    if spec.secrets is not None:
        none_keys = [k for k, v in spec.secrets.items() if v is None]
        if none_keys:
            raise ApplyYamlError(
                f"cannot delete secrets on create "
                f"(null values for: {', '.join(sorted(none_keys))})"
            )

    # display_name is required on create.
    display_name = display.generate_name
    if display_name is None:
        raise ApplyYamlError("generate_name is required on create")

    # Build from only the fields the user actually set.
    spec_data = spec.model_dump(exclude_unset=True)

    # repo_url defaults to "" on create if not set.
    repo_url = spec_data.pop("repo_url", "")

    return DeploymentCreate(
        id=display.name,
        display_name=display_name,
        repo_url=repo_url,
        **spec_data,
    )


def apply_payload_to_update(display: DeploymentDisplay) -> DeploymentUpdate:
    """Translate a parsed :class:`DeploymentDisplay` into a
    :class:`DeploymentUpdate` wire model.

    Only includes fields that were explicitly set in the YAML input
    (via ``model_fields_set``).
    """
    spec = display.spec
    spec_set = spec.model_fields_set

    kwargs: dict[str, Any] = {}

    # display_name on update comes from generate_name if the user set it.
    if "generate_name" in display.model_fields_set:
        kwargs["display_name"] = display.generate_name

    # Map spec fields that were explicitly set.
    _DIRECT_FIELDS = (
        "repo_url",
        "deployment_file_path",
        "git_ref",
        "appserver_version",
        "suspended",
        "secrets",
    )
    for field in _DIRECT_FIELDS:
        if field in spec_set:
            kwargs[field] = getattr(spec, field)

    # PAT semantics:
    #   explicit null in YAML  → "" on wire (delete sentinel)
    #   string value           → set as given
    #   not present            → omit (None / unchanged)
    if "personal_access_token" in spec_set:
        pat = spec.personal_access_token
        kwargs["personal_access_token"] = "" if pat is None else pat

    return DeploymentUpdate(**kwargs)


# ---------------------------------------------------------------------------
# Lightweight delete helper
# ---------------------------------------------------------------------------


def parse_delete_yaml_name(text: str) -> str:
    """Extract the ``name`` field from YAML for a delete operation.

    No env resolution, no model validation — just pull the top-level
    ``name`` string.
    """
    try:
        raw = yaml.safe_load(text)
    except yaml.YAMLError as exc:
        raise ApplyYamlError(f"invalid YAML: {exc}") from exc

    if not isinstance(raw, dict):
        raise ApplyYamlError(
            f"expected a YAML mapping at the top level, got {type(raw).__name__}"
        )

    name = raw.get("name")
    if name is None:
        raise ApplyYamlError("missing required field: name")
    if not isinstance(name, str):
        raise ApplyYamlError(f"name must be a string, got {type(name).__name__}")
    return name
