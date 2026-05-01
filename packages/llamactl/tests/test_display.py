# SPDX-License-Identifier: MIT
# Copyright (c) 2026 LlamaIndex Inc.
"""Tests for the CLI ``DeploymentDisplay`` projection model."""

from __future__ import annotations

from typing import Any

import pytest
from conftest import make_deployment
from llama_agents.cli.display import (
    SECRET_MASK,
    DeploymentDisplay,
    DeploymentSpec,
    DeploymentStatus,
)
from llama_agents.core.schema.deployments import DeploymentCreate, DeploymentUpdate
from pydantic import ValidationError

_CREATE_SPEC_FIELDS = frozenset(
    {
        "repo_url",
        "deployment_file_path",
        "git_ref",
        "appserver_version",
        "secrets",
        "personal_access_token",
    }
)
_UPDATE_SPEC_FIELDS = frozenset(
    {
        "repo_url",
        "deployment_file_path",
        "git_ref",
        "appserver_version",
        "suspended",
        "secrets",
        "personal_access_token",
    }
)
_HANDLED_SPEC_FIELDS = _CREATE_SPEC_FIELDS | _UPDATE_SPEC_FIELDS
_SERVICE_UPDATE_FIELDS = frozenset(
    {
        "git_sha",
        "static_assets_path",
        "image_tag",
        "bump_to_latest_appserver",
        "rebuild",
    }
)


def test_from_response_translates_spec_fields() -> None:
    response = make_deployment(
        "my-app",
        display_name="My App",
        repo_url="https://github.com/example/repo",
        deployment_file_path="llama_deploy.yaml",
        git_ref="main",
        appserver_version="0.4.2",
        suspended=True,
    )
    display = DeploymentDisplay.from_response(response)

    # ``name`` is the stable id, NOT the deprecated ``r.name`` alias.
    assert display.name == "my-app"
    # ``display_name`` from the wire surfaces at the identity tier as
    # ``generate_name`` — not on ``spec``.
    assert display.generate_name == "My App"
    assert isinstance(display.spec, DeploymentSpec)
    assert display.spec.repo_url == "https://github.com/example/repo"
    assert display.spec.deployment_file_path == "llama_deploy.yaml"
    assert display.spec.git_ref == "main"
    assert display.spec.appserver_version == "0.4.2"
    assert display.spec.suspended is True


def test_from_response_status_block() -> None:
    response = make_deployment(
        "my-app",
        git_sha="abc123",
        apiserver_url="http://my-app.svc.cluster.local/",
        warning="upgrade me",
    )
    display = DeploymentDisplay.from_response(response)

    assert isinstance(display.status, DeploymentStatus)
    assert display.status.phase == "Running"
    assert display.status.git_sha == "abc123"
    assert display.status.apiserver_url == "http://my-app.svc.cluster.local/"
    assert display.status.project_id == "proj_default"
    assert display.status.warning == "upgrade me"


def test_from_response_secrets_masked() -> None:
    response = make_deployment(
        "my-app", secret_names=["LLAMA_CLOUD_API_KEY", "OPENAI_API_KEY"]
    )
    display = DeploymentDisplay.from_response(response)

    assert display.spec.secrets == {
        "LLAMA_CLOUD_API_KEY": SECRET_MASK,
        "OPENAI_API_KEY": SECRET_MASK,
    }


def test_from_response_no_secrets_is_none() -> None:
    response = make_deployment("my-app", secret_names=[])
    display = DeploymentDisplay.from_response(response)
    assert display.spec.secrets is None

    response = make_deployment("my-app", secret_names=None)
    display = DeploymentDisplay.from_response(response)
    assert display.spec.secrets is None


def test_from_response_pat_masking() -> None:
    response = make_deployment("my-app", has_personal_access_token=True)
    display = DeploymentDisplay.from_response(response)
    assert display.spec.personal_access_token == SECRET_MASK

    response = make_deployment("my-app", has_personal_access_token=False)
    display = DeploymentDisplay.from_response(response)
    assert display.spec.personal_access_token is None


def test_spec_as_redacted_masks_secret_values() -> None:
    spec = DeploymentSpec(
        repo_url="https://github.com/example/repo",
        secrets={"API_KEY": "sk-test", "DELETE_ME": None},
        personal_access_token="ghp-test",
    )

    data = spec.as_redacted().model_dump(mode="json", exclude_unset=True)

    assert data == {
        "repo_url": "https://github.com/example/repo",
        "secrets": {"API_KEY": SECRET_MASK, "DELETE_ME": None},
        "personal_access_token": SECRET_MASK,
    }


def test_without_mask_sentinels_removes_apply_unsafe_masks() -> None:
    display = DeploymentDisplay(
        name="my-app",
        spec=DeploymentSpec(
            repo_url="https://github.com/example/repo",
            secrets={"REAL": "value", "MASKED": SECRET_MASK},
            personal_access_token=SECRET_MASK,
        ),
    )

    stripped = display.without_mask_sentinels()

    assert stripped.spec.secrets == {"REAL": "value"}
    assert "personal_access_token" not in stripped.spec.model_fields_set


def test_all_deployment_spec_fields_have_apply_semantics() -> None:
    assert set(DeploymentSpec.model_fields) == _HANDLED_SPEC_FIELDS


def test_create_payload_mapping_matches_wire_model_boundary() -> None:
    assert _CREATE_SPEC_FIELDS == set(DeploymentCreate.model_fields) - {
        "id",
        "display_name",
    }


def test_update_payload_mapping_matches_wire_model_boundary() -> None:
    assert _UPDATE_SPEC_FIELDS == (
        set(DeploymentUpdate.model_fields) - {"display_name"} - _SERVICE_UPDATE_FIELDS
    )


def test_to_create_payload_with_name_sets_id() -> None:
    display = DeploymentDisplay(
        name="my-app",
        generate_name="My App",
        spec=DeploymentSpec(repo_url="https://github.com/example/repo"),
    )

    payload = display.to_create_payload()

    assert payload.id == "my-app"
    assert payload.display_name == "My App"


def test_to_create_payload_without_name_uses_generate_name() -> None:
    display = DeploymentDisplay(
        generate_name="My App",
        spec=DeploymentSpec(repo_url="https://github.com/example/repo"),
    )

    payload = display.to_create_payload()

    assert payload.id is None
    assert payload.display_name == "My App"


def test_to_create_payload_without_generate_name_raises() -> None:
    display = DeploymentDisplay(
        spec=DeploymentSpec(repo_url="https://github.com/example/repo"),
    )

    with pytest.raises(ValueError, match="generate_name"):
        display.to_create_payload()


def test_to_create_payload_suspended_raises() -> None:
    display = DeploymentDisplay(
        generate_name="My App",
        spec=DeploymentSpec(
            repo_url="https://github.com/example/repo",
            suspended=True,
        ),
    )

    with pytest.raises(ValueError, match="suspended"):
        display.to_create_payload()


def test_to_create_payload_secrets_with_null_value_raises() -> None:
    display = DeploymentDisplay(
        generate_name="My App",
        spec=DeploymentSpec(
            repo_url="https://github.com/example/repo",
            secrets={"FOO": None},
        ),
    )

    with pytest.raises(ValueError, match="null values"):
        display.to_create_payload()


def test_to_create_payload_unset_fields_default() -> None:
    display = DeploymentDisplay(
        generate_name="My App",
        spec=DeploymentSpec(repo_url="https://github.com/example/repo"),
    )

    payload = display.to_create_payload()

    assert payload.git_ref is None


def test_to_create_payload_empty_repo_url_passthrough() -> None:
    display = DeploymentDisplay(
        generate_name="My App",
        spec=DeploymentSpec(repo_url=""),
    )

    payload = display.to_create_payload()

    assert payload.repo_url == ""


def test_to_update_payload_unset_fields_remain_none() -> None:
    display = DeploymentDisplay(name="my-app", spec=DeploymentSpec(git_ref="v2"))

    payload = display.to_update_payload()

    assert payload.git_ref == "v2"
    assert payload.repo_url is None


def test_to_update_payload_null_pat_becomes_delete_sentinel() -> None:
    display = DeploymentDisplay(
        name="my-app", spec=DeploymentSpec(personal_access_token=None)
    )

    payload = display.to_update_payload()

    assert payload.personal_access_token == ""


def test_to_update_payload_secrets_null_values_preserved() -> None:
    display = DeploymentDisplay(
        name="my-app",
        spec=DeploymentSpec(secrets={"FOO": None, "BAR": "new-value"}),
    )

    payload = display.to_update_payload()

    assert payload.secrets is not None
    assert payload.secrets["FOO"] is None
    assert payload.secrets["BAR"] == "new-value"


def test_to_update_payload_generate_name_maps_to_display_name() -> None:
    display = DeploymentDisplay(
        name="my-app",
        generate_name="New Name",
        spec=DeploymentSpec(repo_url="https://github.com/example/repo"),
    )

    payload = display.to_update_payload()

    assert payload.display_name == "New Name"


def test_to_output_dict_omits_empty_spec_fields() -> None:
    response = make_deployment("my-app")  # no secrets, no PAT
    data = DeploymentDisplay.from_response(response).to_output_dict()

    spec = data["spec"]
    assert "secrets" not in spec
    assert "personal_access_token" not in spec
    # Editable defaults still surface inside spec so apply round-trips cleanly.
    assert data["name"] == "my-app"
    assert spec["suspended"] is False


def test_to_output_dict_keeps_explicit_status_warning_null() -> None:
    response = make_deployment("my-app", warning=None)
    data = DeploymentDisplay.from_response(response).to_output_dict()
    assert data["status"]["warning"] is None


def test_to_output_dict_strips_mask_sentinels() -> None:
    """Mask sentinels are dropped at the emit boundary so a ``get | edit |
    apply`` round-trip can't push the literal ``********`` back as the value."""
    response = make_deployment(
        "my-app",
        secret_names=["KEY"],
        has_personal_access_token=True,
    )
    data = DeploymentDisplay.from_response(response).to_output_dict()
    # ``secrets`` had only mask values → key drops entirely.
    assert "secrets" not in data["spec"]
    # PAT mask drops too.
    assert "personal_access_token" not in data["spec"]
    # And ``SECRET_MASK`` should not appear anywhere serialized.
    assert SECRET_MASK not in repr(data)


def test_to_output_dict_emits_generate_name_when_set() -> None:
    response = make_deployment("my-app", display_name="My App")
    data = DeploymentDisplay.from_response(response).to_output_dict()
    assert data["generate_name"] == "My App"
    assert "display_name" not in data["spec"]
    assert "display_name" not in data


def test_display_model_forbids_extra_fields() -> None:
    """Adding an unknown wire field should fail loudly during translation."""
    with pytest.raises(ValidationError):
        DeploymentDisplay.model_validate(
            {
                "name": "x",
                "spec": {
                    "repo_url": "https://github.com/x/y",
                    "deployment_file_path": ".",
                },
                "novel_field": "leak",
            }
        )


def test_spec_model_forbids_extra_fields() -> None:
    with pytest.raises(ValidationError):
        DeploymentSpec.model_validate(
            {
                "repo_url": "https://github.com/x/y",
                "deployment_file_path": ".",
                "novel_field": "leak",
            }
        )


def test_spec_model_no_longer_accepts_display_name() -> None:
    """``display_name`` lives on ``DeploymentDisplay.generate_name`` now;
    the spec model rejects it as an unknown field."""
    with pytest.raises(ValidationError):
        DeploymentSpec.model_validate({"display_name": "x"})


def test_status_model_forbids_extra_fields() -> None:
    with pytest.raises(ValidationError):
        DeploymentStatus.model_validate(
            {"phase": "Running", "project_id": "proj", "extra_field": "x"}
        )


def test_no_legacy_aliases_in_output(monkeypatch: Any) -> None:
    """Sanity: deprecated wire aliases (``id``, ``llama_deploy_version``,
    ``has_personal_access_token``, ``secret_names``) must not leak."""
    response = make_deployment(
        "my-app",
        secret_names=["KEY"],
        has_personal_access_token=True,
        appserver_version="0.4.2",
    )
    data = DeploymentDisplay.from_response(response).to_output_dict()
    for forbidden in (
        "id",
        "llama_deploy_version",
        "has_personal_access_token",
        "secret_names",
    ):
        assert forbidden not in data


# ---------------------------------------------------------------------------
# Column framework — walker
# ---------------------------------------------------------------------------


from typing import Literal  # noqa: E402

from llama_agents.cli.display import (  # noqa: E402
    Column,
    render_columns,
    resolve_columns,
)
from pydantic import BaseModel  # noqa: E402
from typing_extensions import Annotated  # noqa: E402


class _Flat(BaseModel):
    name: Annotated[str, Column("NAME")]
    note: str  # no Column → skipped
    age: Annotated[int, Column("AGE", format=lambda v: f"~{v}")]


class _Inner(BaseModel):
    phase: Annotated[str, Column("PHASE")]
    secret: str  # no Column → skipped


class _Nested(BaseModel):
    name: Annotated[str, Column("NAME")]
    inner: _Inner
    optional_inner: _Inner | None = None


class _Wide(BaseModel):
    name: Annotated[str, Column("NAME")]
    extra: Annotated[str, Column("EXTRA", wide=True)] = "x"


def test_resolve_columns_flat_model_declaration_order() -> None:
    cols = resolve_columns(_Flat)
    assert [c.column.header for c in cols] == ["NAME", "AGE"]
    assert [c.path for c in cols] == [("name",), ("age",)]


def test_resolve_columns_descends_nested_models() -> None:
    cols = resolve_columns(_Nested)
    # Outer NAME, then inner PHASE (descended), then optional_inner PHASE.
    assert [c.column.header for c in cols] == ["NAME", "PHASE", "PHASE"]
    assert [c.path for c in cols] == [
        ("name",),
        ("inner", "phase"),
        ("optional_inner", "phase"),
    ]


def test_resolve_columns_skips_field_without_column() -> None:
    cols = resolve_columns(_Inner)
    # ``secret`` carries no Column → excluded.
    assert [c.column.header for c in cols] == ["PHASE"]


def test_resolve_columns_supports_multiple_independent_markers() -> None:
    """Forward-compat: extra markers on the same field don't perturb output."""

    class _Marker:
        pass

    class _M(BaseModel):
        name: Annotated[str, Column("NAME"), _Marker()]

    cols = resolve_columns(_M)
    assert len(cols) == 1
    assert cols[0].column.header == "NAME"


def test_resolve_columns_rejects_duplicate_columns_on_one_field() -> None:
    class _Bad(BaseModel):
        name: Annotated[str, Column("A"), Column("B")]

    with pytest.raises(ValueError, match="multiple Column"):
        resolve_columns(_Bad)


def test_render_columns_filters_wide(capsys: Any) -> None:
    rows = [_Wide(name="a"), _Wide(name="b", extra="z")]
    render_columns(rows)
    out = capsys.readouterr().out
    assert "EXTRA" not in out
    assert "NAME" in out

    render_columns(rows, wide=True)
    out = capsys.readouterr().out
    assert "EXTRA" in out
    assert "z" in out


def test_render_columns_applies_format_and_default(capsys: Any) -> None:
    class _M(BaseModel):
        ref: Annotated[str | None, Column("REF", default="-")] = None
        age: Annotated[int, Column("AGE", format=lambda v: f"~{v}")] = 0

    render_columns([_M(ref=None, age=3), _M(ref="main", age=7)])
    out = capsys.readouterr().out
    lines = out.strip().splitlines()
    assert "REF" in lines[0]
    # First row uses the default; format is applied to age.
    assert "-" in lines[1]
    assert "~3" in lines[1]
    assert "main" in lines[2]
    assert "~7" in lines[2]


def test_render_columns_propagates_none_through_missing_nested_model(
    capsys: Any,
) -> None:
    class _Inner2(BaseModel):
        phase: Annotated[str, Column("PHASE", default="-")]

    class _Outer(BaseModel):
        name: Annotated[str, Column("NAME")]
        inner: _Inner2 | None = None

    render_columns([_Outer(name="a", inner=None)])
    out = capsys.readouterr().out
    lines = out.strip().splitlines()
    assert "PHASE" in lines[0]
    # Missing nested model → cell renders the column's default.
    assert "-" in lines[1]


def test_resolve_columns_handles_optional_basemodel_union() -> None:
    cols = resolve_columns(_Nested)
    paths = {c.path for c in cols}
    assert ("optional_inner", "phase") in paths


def test_resolve_columns_is_cached() -> None:
    """Cache hit returns the same tuple instance."""
    a = resolve_columns(_Flat)
    b = resolve_columns(_Flat)
    assert a is b


def test_render_columns_literal_field_renders_value(capsys: Any) -> None:
    class _M(BaseModel):
        kind: Annotated[Literal["a", "b"], Column("KIND")] = "a"

    render_columns([_M()])
    out = capsys.readouterr().out
    assert "a" in out
