# SPDX-License-Identifier: MIT
# Copyright (c) 2026 LlamaIndex Inc.
from __future__ import annotations

from typing import Any

from llama_agents.cli.config.schema import DEFAULT_ENVIRONMENT
from pydantic import Field, field_validator
from pydantic_settings import BaseSettings, SettingsConfigDict


class LlamactlEnvSettings(BaseSettings):
    model_config = SettingsConfigDict(extra="ignore")

    llama_cloud_api_key: str | None = None
    llama_cloud_base_url: str = DEFAULT_ENVIRONMENT.api_url
    llama_deploy_project_id: str | None = None
    llama_cloud_use_profile: bool = False
    llamactl_complete: str | None = Field(
        default=None,
        validation_alias="_LLAMACTL_COMPLETE",
    )

    @field_validator("llama_cloud_api_key", "llama_deploy_project_id", mode="before")
    @classmethod
    def _empty_string_is_unset(cls, value: Any) -> Any:
        if value == "":
            return None
        return value

    @field_validator("llama_cloud_base_url", mode="before")
    @classmethod
    def _empty_base_url_uses_default(cls, value: Any) -> Any:
        if value == "":
            return DEFAULT_ENVIRONMENT.api_url
        return value

    @property
    def normalized_base_url(self) -> str:
        return self.llama_cloud_base_url.rstrip("/")

    @property
    def has_complete_cloud_auth(self) -> bool:
        return bool(self.llama_cloud_api_key and self.llama_deploy_project_id)

    @property
    def has_cloud_connection_summary(self) -> bool:
        return bool(
            self.llama_cloud_api_key
            or self.llama_deploy_project_id
            or "llama_cloud_base_url" in self.model_fields_set
        )

    @property
    def completion_active(self) -> bool:
        return self.llamactl_complete is not None

    @property
    def cloud_auth_disabled(self) -> bool:
        return self.llama_cloud_use_profile


def read_env_settings() -> LlamactlEnvSettings:
    return LlamactlEnvSettings()
