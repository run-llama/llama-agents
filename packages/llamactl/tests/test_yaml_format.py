# SPDX-License-Identifier: MIT
# Copyright (c) 2026 LlamaIndex Inc.
"""Tests for ``cli.yaml_format`` — YAML parsing for ``deployments apply -f``."""

from __future__ import annotations

import textwrap

import pytest
from llama_agents.cli.yaml_format import (
    ApplyYamlError,
    UnresolvedEnvVarsError,
    apply_payload_to_create,
    apply_payload_to_update,
    parse_apply_yaml,
    parse_delete_yaml_name,
    resolve_env_vars,
)

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

MINIMAL_YAML = textwrap.dedent("""\
    name: my-app
    spec:
      repo_url: https://github.com/example/repo
""")


def _yaml_with_spec(**spec_fields: object) -> str:
    """Build a minimal YAML doc with arbitrary spec fields."""
    lines = ["name: my-app", "spec:"]
    for k, v in spec_fields.items():
        lines.append(f"  {k}: {v}")
    return "\n".join(lines) + "\n"


# ---------------------------------------------------------------------------
# parse_apply_yaml — basics
# ---------------------------------------------------------------------------


def test_parse_basic_name_and_repo() -> None:
    display = parse_apply_yaml(MINIMAL_YAML)
    assert display.name == "my-app"
    assert display.spec.repo_url == "https://github.com/example/repo"


def test_parse_drops_status_key() -> None:
    """Round-trip from ``get -o yaml`` includes ``status``; parse strips it."""
    doc = textwrap.dedent("""\
        name: my-app
        spec:
          repo_url: https://github.com/example/repo
        status:
          phase: Running
          project_id: proj_default
    """)
    display = parse_apply_yaml(doc)
    assert display.name == "my-app"
    assert display.status is None


def test_parse_unknown_spec_field_raises() -> None:
    doc = textwrap.dedent("""\
        name: my-app
        spec:
          repo_url: https://github.com/example/repo
          image_tag: latest
    """)
    with pytest.raises(ApplyYamlError, match="image_tag"):
        parse_apply_yaml(doc)


def test_parse_unknown_spec_field_rebuild_raises() -> None:
    doc = textwrap.dedent("""\
        name: my-app
        spec:
          repo_url: https://github.com/example/repo
          rebuild: true
    """)
    with pytest.raises(ApplyYamlError, match="rebuild"):
        parse_apply_yaml(doc)


# ---------------------------------------------------------------------------
# Environment variable resolution
# ---------------------------------------------------------------------------


def test_env_var_resolves(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("MY_REPO", "https://github.com/resolved/repo")
    doc = textwrap.dedent("""\
        name: my-app
        spec:
          repo_url: ${MY_REPO}
    """)
    display = parse_apply_yaml(doc)
    assert display.spec.repo_url == "https://github.com/resolved/repo"


def test_env_var_multiple_in_one_string(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("HOST", "example.com")
    monkeypatch.setenv("PORT", "8080")
    result = resolve_env_vars("https://${HOST}:${PORT}")
    assert result == "https://example.com:8080"


def test_env_var_missing_strict_raises(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.delenv("MISSING_VAR", raising=False)
    with pytest.raises(UnresolvedEnvVarsError) as exc_info:
        resolve_env_vars("${MISSING_VAR}", strict=True)
    assert "MISSING_VAR" in exc_info.value.unresolved


def test_env_var_multiple_missing_listed(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.delenv("AAA", raising=False)
    monkeypatch.delenv("BBB", raising=False)
    with pytest.raises(UnresolvedEnvVarsError) as exc_info:
        resolve_env_vars("${AAA} ${BBB}", strict=True)
    assert "AAA" in exc_info.value.unresolved
    assert "BBB" in exc_info.value.unresolved


def test_env_var_non_strict_leaves_placeholder(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.delenv("MISSING_VAR", raising=False)
    result = resolve_env_vars("${MISSING_VAR}", strict=False)
    assert result == "${MISSING_VAR}"


def test_env_var_in_secrets(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("SECRET_VAL", "s3cret")
    doc = textwrap.dedent("""\
        name: my-app
        spec:
          repo_url: https://github.com/example/repo
          secrets:
            MY_SECRET: ${SECRET_VAL}
    """)
    display = parse_apply_yaml(doc)
    assert display.spec.secrets is not None
    assert display.spec.secrets["MY_SECRET"] == "s3cret"


# ---------------------------------------------------------------------------
# Mask passthrough (strip SECRET_MASK values)
# ---------------------------------------------------------------------------


def test_mask_pat_stripped() -> None:
    doc = textwrap.dedent("""\
        name: my-app
        spec:
          repo_url: https://github.com/example/repo
          personal_access_token: "********"
    """)
    display = parse_apply_yaml(doc)
    # Masked PAT is stripped — field not set on the model.
    assert "personal_access_token" not in display.spec.model_fields_set


def test_mask_secret_entry_stripped() -> None:
    doc = textwrap.dedent("""\
        name: my-app
        spec:
          repo_url: https://github.com/example/repo
          secrets:
            FOO: "********"
    """)
    display = parse_apply_yaml(doc)
    # All entries masked → secrets key itself dropped.
    assert display.spec.secrets is None or "FOO" not in display.spec.secrets


def test_mask_partial_secrets() -> None:
    doc = textwrap.dedent("""\
        name: my-app
        spec:
          repo_url: https://github.com/example/repo
          secrets:
            FOO: "********"
            BAR: real-value
    """)
    display = parse_apply_yaml(doc)
    assert display.spec.secrets is not None
    assert "FOO" not in display.spec.secrets
    assert display.spec.secrets["BAR"] == "real-value"


# ---------------------------------------------------------------------------
# Null handling
# ---------------------------------------------------------------------------


def test_null_pat_preserved() -> None:
    doc = textwrap.dedent("""\
        name: my-app
        spec:
          repo_url: https://github.com/example/repo
          personal_access_token: null
    """)
    display = parse_apply_yaml(doc)
    assert display.spec.personal_access_token is None
    assert "personal_access_token" in display.spec.model_fields_set


def test_null_secret_value_preserved() -> None:
    doc = textwrap.dedent("""\
        name: my-app
        spec:
          repo_url: https://github.com/example/repo
          secrets:
            FOO: null
    """)
    display = parse_apply_yaml(doc)
    assert display.spec.secrets is not None
    assert display.spec.secrets["FOO"] is None


# ---------------------------------------------------------------------------
# generate_name
# ---------------------------------------------------------------------------


def test_generate_name_snake_case() -> None:
    doc = textwrap.dedent("""\
        generate_name: my-slug
        spec:
          repo_url: https://github.com/example/repo
    """)
    display = parse_apply_yaml(doc)
    assert display.generate_name == "my-slug"


# ---------------------------------------------------------------------------
# apply_payload_to_create
# ---------------------------------------------------------------------------


def test_create_with_name_sets_id() -> None:
    doc = textwrap.dedent("""\
        name: my-app
        generate_name: My App
        spec:
          repo_url: https://github.com/example/repo
    """)
    display = parse_apply_yaml(doc)
    payload = apply_payload_to_create(display)
    assert payload.id == "my-app"
    assert payload.display_name == "My App"


def test_create_without_name_uses_generate_name() -> None:
    doc = textwrap.dedent("""\
        generate_name: My App
        spec:
          repo_url: https://github.com/example/repo
    """)
    display = parse_apply_yaml(doc)
    payload = apply_payload_to_create(display)
    assert payload.id is None
    assert payload.display_name == "My App"


def test_create_without_name_or_generate_name_raises() -> None:
    doc = textwrap.dedent("""\
        spec:
          repo_url: https://github.com/example/repo
    """)
    display = parse_apply_yaml(doc)
    with pytest.raises(ApplyYamlError):
        apply_payload_to_create(display)


def test_create_suspended_raises() -> None:
    doc = textwrap.dedent("""\
        generate_name: My App
        spec:
          repo_url: https://github.com/example/repo
          suspended: true
    """)
    display = parse_apply_yaml(doc)
    with pytest.raises(ApplyYamlError, match="suspended"):
        apply_payload_to_create(display)


def test_create_secrets_with_null_value_raises() -> None:
    doc = textwrap.dedent("""\
        generate_name: My App
        spec:
          repo_url: https://github.com/example/repo
          secrets:
            FOO: null
    """)
    display = parse_apply_yaml(doc)
    with pytest.raises(ApplyYamlError):
        apply_payload_to_create(display)


def test_create_excludes_unset_fields() -> None:
    doc = textwrap.dedent("""\
        generate_name: My App
        spec:
          repo_url: https://github.com/example/repo
    """)
    display = parse_apply_yaml(doc)
    payload = apply_payload_to_create(display)
    # git_ref was not set, so it should default / not be explicitly configured
    assert payload.git_ref is None


def test_create_empty_repo_url_passthrough() -> None:
    """``repo_url: ""`` is push-mode; passes through to create."""
    doc = textwrap.dedent("""\
        generate_name: My App
        spec:
          repo_url: ""
    """)
    display = parse_apply_yaml(doc)
    payload = apply_payload_to_create(display)
    assert payload.repo_url == ""


# ---------------------------------------------------------------------------
# apply_payload_to_update
# ---------------------------------------------------------------------------


def test_update_only_includes_fields_set() -> None:
    doc = textwrap.dedent("""\
        name: my-app
        spec:
          git_ref: v2
    """)
    display = parse_apply_yaml(doc)
    payload = apply_payload_to_update(display)
    dumped = payload.model_dump(exclude_unset=True)
    assert "git_ref" in dumped
    # repo_url was not in the input, so not in the update
    assert "repo_url" not in dumped


def test_update_null_pat_becomes_delete_sentinel() -> None:
    doc = textwrap.dedent("""\
        name: my-app
        spec:
          personal_access_token: null
    """)
    display = parse_apply_yaml(doc)
    payload = apply_payload_to_update(display)
    assert payload.personal_access_token == ""


def test_update_secrets_null_values_preserved() -> None:
    doc = textwrap.dedent("""\
        name: my-app
        spec:
          secrets:
            FOO: null
            BAR: new-value
    """)
    display = parse_apply_yaml(doc)
    payload = apply_payload_to_update(display)
    assert payload.secrets is not None
    assert payload.secrets["FOO"] is None
    assert payload.secrets["BAR"] == "new-value"


def test_update_generate_name_maps_to_display_name() -> None:
    doc = textwrap.dedent("""\
        name: my-app
        generate_name: New Name
        spec:
          repo_url: https://github.com/example/repo
    """)
    display = parse_apply_yaml(doc)
    payload = apply_payload_to_update(display)
    assert payload.display_name == "New Name"


# ---------------------------------------------------------------------------
# parse_delete_yaml_name
# ---------------------------------------------------------------------------


def test_delete_returns_name() -> None:
    doc = textwrap.dedent("""\
        name: my-app
        spec:
          repo_url: https://github.com/example/repo
    """)
    assert parse_delete_yaml_name(doc) == "my-app"


def test_delete_missing_name_raises() -> None:
    doc = textwrap.dedent("""\
        spec:
          repo_url: https://github.com/example/repo
    """)
    with pytest.raises(ApplyYamlError):
        parse_delete_yaml_name(doc)


def test_delete_non_string_name_raises() -> None:
    doc = textwrap.dedent("""\
        name: 42
    """)
    with pytest.raises(ApplyYamlError):
        parse_delete_yaml_name(doc)


def test_delete_ignores_other_fields_no_env_resolution(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """No env resolution or model validation happens."""
    monkeypatch.delenv("MISSING", raising=False)
    doc = textwrap.dedent("""\
        name: my-app
        spec:
          repo_url: ${MISSING}
          bogus_field: whatever
    """)
    # Should succeed — only name is inspected.
    assert parse_delete_yaml_name(doc) == "my-app"


# ---------------------------------------------------------------------------
# Schema validation error messages
# ---------------------------------------------------------------------------


def test_validation_error_includes_spec_prefix() -> None:
    doc = textwrap.dedent("""\
        name: my-app
        spec:
          repo_url: https://github.com/example/repo
          bogus: nope
    """)
    with pytest.raises(ApplyYamlError, match="spec"):
        parse_apply_yaml(doc)


# ---------------------------------------------------------------------------
# resolve_env_vars — recursive walk
# ---------------------------------------------------------------------------


def test_resolve_env_vars_dict(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("VAL", "resolved")
    result = resolve_env_vars({"key": "${VAL}"})
    assert result == {"key": "resolved"}


def test_resolve_env_vars_list(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("ITEM", "x")
    result = resolve_env_vars(["${ITEM}", "literal"])
    assert result == ["x", "literal"]


def test_resolve_env_vars_non_string_passthrough() -> None:
    assert resolve_env_vars(42) == 42
    assert resolve_env_vars(True) is True
    assert resolve_env_vars(None) is None
