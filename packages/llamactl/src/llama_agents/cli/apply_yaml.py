# SPDX-License-Identifier: MIT
# Copyright (c) 2026 LlamaIndex Inc.
"""YAML parsing for ``llamactl deployments apply -f``.

Pure parsing + validation — no network, no client calls.  Takes YAML text,
resolves ``${VAR}`` environment variables, strips ``********`` mask sentinels,
and validates against :class:`DeploymentDisplay`.
"""

from __future__ import annotations

import os
import re
from typing import Any

import yaml
from llama_agents.cli.display import DeploymentDisplay, DeploymentSpec
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


_ENV_STRING_SPEC_FIELDS = (
    "repo_url",
    "deployment_file_path",
    "git_ref",
    "appserver_version",
    "personal_access_token",
)


def _resolve_string(text: str, unresolved: list[str]) -> str:
    def _replacer(match: re.Match[str]) -> str:
        var = match.group(1)
        env_val = os.environ.get(var)
        if env_val is None:
            unresolved.append(var)
            return match.group(0)  # leave ${VAR} as-is
        return env_val

    return _ENV_VAR_RE.sub(_replacer, text)


def _resolve_spec_env_vars(spec: DeploymentSpec) -> DeploymentSpec:
    unresolved: list[str] = []
    updates: dict[str, Any] = {}

    for field in _ENV_STRING_SPEC_FIELDS:
        value = getattr(spec, field)
        if isinstance(value, str):
            updates[field] = _resolve_string(value, unresolved)

    if spec.secrets is not None:
        updates["secrets"] = {
            name: _resolve_string(value, unresolved)
            if isinstance(value, str)
            else value
            for name, value in spec.secrets.items()
        }

    if unresolved:
        raise UnresolvedEnvVarsError(unresolved)
    return spec.model_copy(update=updates)


def _load_yaml_mapping(text: str) -> dict[str, Any]:
    try:
        raw = yaml.safe_load(text)
    except yaml.YAMLError as exc:
        raise ApplyYamlError(f"invalid YAML: {exc}") from exc

    if not isinstance(raw, dict):
        raise ApplyYamlError(
            f"expected a YAML mapping at the top level, got {type(raw).__name__}"
        )
    return raw


# ---------------------------------------------------------------------------
# Main parse entry point
# ---------------------------------------------------------------------------


def parse_apply_yaml(text: str) -> DeploymentDisplay:
    """Parse YAML apply input into a validated :class:`DeploymentDisplay`.

    1. ``yaml.safe_load`` → dict.
    2. Drop ``status`` (round-trip artifact from ``get -o yaml``).
    3. Validate against :class:`DeploymentDisplay` (pydantic handles
       ``extra="forbid"`` rejection for typos / excluded fields).
    4. Resolve ``${VAR}`` env vars in typed string fields under ``spec``.
    5. Strip ``********`` mask sentinels from ``spec.secrets`` and
       ``spec.personal_access_token``.
    6. Wrap :class:`~pydantic.ValidationError` into :class:`ApplyYamlError`.
    """
    raw = _load_yaml_mapping(text)

    # Drop read-only status block.
    raw.pop("status", None)

    try:
        display = DeploymentDisplay.model_validate(raw)
    except ValidationError as exc:
        raise ApplyYamlError(str(exc)) from exc

    display = display.model_copy(update={"spec": _resolve_spec_env_vars(display.spec)})
    return display.without_mask_sentinels()


# ---------------------------------------------------------------------------
# Lightweight delete helper
# ---------------------------------------------------------------------------


def parse_delete_yaml_name(text: str) -> str:
    """Extract the ``name`` field from YAML for a delete operation.

    No env resolution, no model validation — just pull the top-level
    ``name`` string.
    """
    raw = _load_yaml_mapping(text)

    name = raw.get("name")
    if name is None:
        raise ApplyYamlError("missing required field: name")
    if not isinstance(name, str):
        raise ApplyYamlError(f"name must be a string, got {type(name).__name__}")
    return name
