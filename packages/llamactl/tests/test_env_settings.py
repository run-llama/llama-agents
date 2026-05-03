# SPDX-License-Identifier: MIT
# Copyright (c) 2026 LlamaIndex Inc.
from __future__ import annotations

import pytest
from conftest import clear_llama_cloud_env, set_llama_cloud_env
from llama_agents.cli.config.schema import DEFAULT_ENVIRONMENT
from llama_agents.cli.env_settings import LlamactlEnvSettings


def test_base_url_defaults_to_current_default_environment(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    clear_llama_cloud_env(monkeypatch)

    settings = LlamactlEnvSettings()

    assert settings.llama_cloud_base_url == DEFAULT_ENVIRONMENT.api_url
    assert settings.normalized_base_url == DEFAULT_ENVIRONMENT.api_url.rstrip("/")


def test_base_url_normalized_value_strips_trailing_slash(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    set_llama_cloud_env(monkeypatch, base_url="https://api.example.test/")

    settings = LlamactlEnvSettings()

    assert settings.llama_cloud_base_url == "https://api.example.test/"
    assert settings.normalized_base_url == "https://api.example.test"


def test_empty_api_key_and_project_id_are_incomplete(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    clear_llama_cloud_env(monkeypatch)
    monkeypatch.setenv("LLAMA_CLOUD_API_KEY", "")
    monkeypatch.setenv("LLAMA_AGENTS_PROJECT_ID", "")

    settings = LlamactlEnvSettings()

    assert settings.llama_cloud_api_key is None
    assert settings.llama_agents_project_id is None
    assert settings.has_complete_cloud_auth is False


def test_cloud_auth_is_complete_with_api_key_and_project_id(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    set_llama_cloud_env(monkeypatch, api_key="env-api-key", project_id="env-project")

    settings = LlamactlEnvSettings()

    assert settings.has_complete_cloud_auth is True


def test_use_profile_env_value_one_parses_as_true(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    set_llama_cloud_env(monkeypatch, use_profile=True)

    settings = LlamactlEnvSettings()

    assert settings.llama_cloud_use_profile is True
    assert settings.cloud_auth_disabled is True


def test_lowercase_env_names_populate_normal_cloud_fields(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    clear_llama_cloud_env(monkeypatch)
    monkeypatch.setenv("llama_cloud_api_key", "lower-api-key")
    monkeypatch.setenv("llama_agents_project_id", "lower-project")
    monkeypatch.setenv("llama_cloud_base_url", "https://lower.example.test")
    monkeypatch.setenv("llama_cloud_use_profile", "1")

    settings = LlamactlEnvSettings()

    assert settings.llama_cloud_api_key == "lower-api-key"
    assert settings.llama_agents_project_id == "lower-project"
    assert settings.llama_cloud_base_url == "https://lower.example.test"
    assert settings.llama_cloud_use_profile is True


def test_completion_env_var_marks_completion_active(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    set_llama_cloud_env(monkeypatch, completion="zsh_source")

    settings = LlamactlEnvSettings()

    assert settings.llamactl_complete == "zsh_source"
    assert settings.completion_active is True
