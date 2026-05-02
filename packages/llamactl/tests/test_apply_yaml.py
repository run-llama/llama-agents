# SPDX-License-Identifier: MIT
# Copyright (c) 2026 LlamaIndex Inc.
"""Tests for ``cli.apply_yaml`` — YAML parsing for ``deployments apply -f``."""

from __future__ import annotations

import textwrap

import pytest
from llama_agents.cli.apply_yaml import (
    ApplyYamlError,
    FieldError,
    UnresolvedEnvVarsError,
    annotate_yaml_with_errors,
    parse_apply_yaml,
    parse_delete_yaml_name,
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
    doc = textwrap.dedent("""\
        name: my-app
        spec:
          repo_url: https://${HOST}:${PORT}/repo
    """)
    display = parse_apply_yaml(doc)
    assert display.spec.repo_url == "https://example.com:8080/repo"


def test_env_var_missing_raises(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.delenv("MISSING_VAR", raising=False)
    doc = textwrap.dedent("""\
        name: my-app
        spec:
          repo_url: ${MISSING_VAR}
    """)
    with pytest.raises(UnresolvedEnvVarsError) as exc_info:
        parse_apply_yaml(doc)
    assert "MISSING_VAR" in exc_info.value.unresolved


def test_env_var_multiple_missing_listed(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.delenv("AAA", raising=False)
    monkeypatch.delenv("BBB", raising=False)
    doc = textwrap.dedent("""\
        name: my-app
        spec:
          repo_url: ${AAA}/${BBB}
    """)
    with pytest.raises(UnresolvedEnvVarsError) as exc_info:
        parse_apply_yaml(doc)
    assert "AAA" in exc_info.value.unresolved
    assert "BBB" in exc_info.value.unresolved


def test_env_var_missing_reports_all_paths(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.delenv("GITHUB_TOKEN", raising=False)
    monkeypatch.delenv("OPENAI_API_KEY", raising=False)
    doc = textwrap.dedent("""\
        generate_name: my-app
        spec:
          repo_url: https://github.com/example/repo
          personal_access_token: ${GITHUB_TOKEN}
          secrets:
            OPENAI_API_KEY: ${OPENAI_API_KEY}
    """)

    with pytest.raises(UnresolvedEnvVarsError) as exc_info:
        parse_apply_yaml(doc)

    assert exc_info.value.unresolved == ["GITHUB_TOKEN", "OPENAI_API_KEY"]
    assert [error.path for error in exc_info.value.errors] == [
        ("spec", "personal_access_token"),
        ("spec", "secrets", "OPENAI_API_KEY"),
    ]


def test_env_var_in_unknown_field_is_not_resolved(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.delenv("MISSING_VAR", raising=False)
    doc = textwrap.dedent("""\
        name: my-app
        spec:
          repo_url: https://github.com/example/repo
          image_tag: ${MISSING_VAR}
    """)
    with pytest.raises(ApplyYamlError, match="image_tag"):
        parse_apply_yaml(doc)


def test_env_var_in_non_string_field_is_not_resolved(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.delenv("MISSING_VAR", raising=False)
    doc = textwrap.dedent("""\
        name: my-app
        spec:
          repo_url: https://github.com/example/repo
          suspended: ${MISSING_VAR}
    """)
    with pytest.raises(ApplyYamlError, match="suspended"):
        parse_apply_yaml(doc)


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
# annotate_yaml_with_errors
# ---------------------------------------------------------------------------


def _field_error(path: tuple[str | int, ...], message: str) -> FieldError:
    return FieldError(path=path, severity="error", message=message)


def test_annotate_top_level_field() -> None:
    doc = "name: my-app\nspec:\n  repo_url: https://github.com/example/repo\n"

    annotated = annotate_yaml_with_errors(
        doc, [_field_error(("name",), "must be a valid DNS label")]
    )

    assert annotated.startswith("## ERROR: must be a valid DNS label\nname: my-app")


def test_annotate_spec_field_above_doc_block() -> None:
    doc = textwrap.dedent("""\
        name: my-app
        spec:
          ## https://... = remote git URL.
          ## Omit to keep the current value.
          repo_url: https://github.com/example/repo
    """)

    annotated = annotate_yaml_with_errors(
        doc, [_field_error(("spec", "repo_url"), "repo not found")]
    )

    assert "  ## ERROR: repo not found\n  ## https://... = remote git URL." in annotated
    assert "## Omit to keep the current value." in annotated


def test_annotate_secret_path_preserves_indentation() -> None:
    doc = textwrap.dedent("""\
        name: my-app
        spec:
          secrets:
            API_KEY: null
    """)

    annotated = annotate_yaml_with_errors(
        doc, [_field_error(("spec", "secrets", "API_KEY"), "cannot delete on create")]
    )

    assert "    ## ERROR: cannot delete on create\n    API_KEY: null" in annotated


def test_annotate_multiple_errors_same_field_preserves_order() -> None:
    doc = "generate_name: My App\nspec: {}\n"

    annotated = annotate_yaml_with_errors(
        doc,
        [
            _field_error(("generate_name",), "first"),
            _field_error(("generate_name",), "second"),
        ],
    )

    assert annotated.startswith(
        "## ERROR: first\n## ERROR: second\ngenerate_name: My App"
    )


def test_annotate_unresolved_path_prepends_with_path() -> None:
    doc = "name: my-app\nspec: {}\n"

    annotated = annotate_yaml_with_errors(
        doc, [_field_error(("spec", "missing"), "not valid")]
    )

    assert annotated.startswith("## ERROR: spec.missing: not valid\nname: my-app")


def test_annotate_is_idempotent_for_existing_error_lines() -> None:
    doc = textwrap.dedent("""\
        ## ERROR: old file error
        name: my-app
        spec:
          ## ERROR: old repo error
          repo_url: https://github.com/example/repo
    """)

    once = annotate_yaml_with_errors(
        doc, [_field_error(("spec", "repo_url"), "new repo error")]
    )
    twice = annotate_yaml_with_errors(
        once, [_field_error(("spec", "repo_url"), "new repo error")]
    )

    assert once == twice
    assert "old file error" not in once
    assert once.count("## ERROR: new repo error") == 1


def test_annotate_preserves_template_docs_that_are_not_errors() -> None:
    doc = textwrap.dedent("""\
        ## Edit, then run: llamactl deployments apply -f <file>
        name: my-app
        spec: {}
    """)

    annotated = annotate_yaml_with_errors(doc, [_field_error((), "file problem")])

    assert "## Edit, then run: llamactl deployments apply -f <file>" in annotated


def test_annotate_syntax_error_falls_back_to_file_level() -> None:
    doc = "name: [\n"

    annotated = annotate_yaml_with_errors(doc, [_field_error(("name",), "bad name")])

    assert annotated == "## ERROR: name: bad name\nname: [\n"


def test_annotate_multiline_error_comments_every_line() -> None:
    doc = "name: [\n"

    annotated = annotate_yaml_with_errors(
        doc, [_field_error((), "invalid YAML: first line\n  second line\nthird line")]
    )

    assert annotated == (
        "## ERROR: invalid YAML: first line\n"
        "## ERROR:   second line\n"
        "## ERROR: third line\n"
        "name: [\n"
    )


def test_annotate_multiline_error_is_idempotent() -> None:
    doc = "name: [\n"
    error = _field_error((), "invalid YAML: first line\nsecond line")

    once = annotate_yaml_with_errors(doc, [error])
    twice = annotate_yaml_with_errors(once, [error])

    assert once == twice
    assert once.count("## ERROR:") == 2


def test_annotate_non_mapping_falls_back_to_file_level() -> None:
    doc = "- name: my-app\n"

    annotated = annotate_yaml_with_errors(doc, [_field_error(("name",), "bad name")])

    assert annotated == "## ERROR: name: bad name\n- name: my-app\n"


def test_annotate_warning_severity() -> None:
    doc = "name: my-app\nspec: {}\n"

    annotated = annotate_yaml_with_errors(
        doc, [FieldError(path=("name",), severity="warning", message="check this")]
    )

    assert annotated.startswith("## WARNING: check this\nname: my-app")
