# SPDX-License-Identifier: MIT
# Copyright (c) 2026 LlamaIndex Inc.
"""Build API keeps the builder's UI output path with the artifact."""

from __future__ import annotations

import io
from collections.abc import AsyncIterator
from unittest.mock import AsyncMock, patch

import boto3
import httpx
import pytest
from aiomoto import mock_aws
from llama_agents.control_plane.build_api.build_app import build_app
from llama_agents.control_plane.build_api.build_auth import authenticate_deployment
from llama_agents.control_plane.build_api.build_storage import BuildArtifactStorage
from llama_agents.core.schema.deployments import LlamaDeploymentCRD

BUILDS_URL = "/deployments/dep-1/builds"


@pytest.fixture
async def storage() -> AsyncIterator[BuildArtifactStorage]:
    with mock_aws():
        boto3.client("s3", region_name="us-east-1").create_bucket(Bucket="builds")
        yield BuildArtifactStorage(
            bucket="builds", region="us-east-1", access_key="x", secret_key="x"
        )


@pytest.fixture
async def client(storage: BuildArtifactStorage) -> AsyncIterator[httpx.AsyncClient]:
    crd = LlamaDeploymentCRD.model_validate(
        {
            "metadata": {"name": "dep-1", "namespace": "ns"},
            "spec": {"name": "dep-1", "projectId": "p", "repoUrl": "internal://"},
        }
    )
    build_app.dependency_overrides[authenticate_deployment] = lambda: crd
    try:
        with (
            patch(
                "llama_agents.control_plane.build_api.build_app.build_artifact_storage",
                storage,
            ),
            patch(
                "llama_agents.control_plane.build_api.build_app.gc_build_artifacts",
                AsyncMock(),
            ),
        ):
            async with httpx.AsyncClient(
                transport=httpx.ASGITransport(app=build_app), base_url="http://test"
            ) as c:
                yield c
    finally:
        build_app.dependency_overrides.clear()


@pytest.mark.parametrize("path", ['"ui/dist"', '""'])
async def test_head_returns_static_assets_path_uploaded_with_artifact(
    client: httpx.AsyncClient, path: str
) -> None:
    put = await client.put(
        f"{BUILDS_URL}/b1", content=b"tar", headers={"X-Static-Assets-Path": path}
    )
    assert put.status_code == 200

    head = await client.head(f"{BUILDS_URL}/b1")
    assert head.status_code == 200
    assert head.headers["X-Static-Assets-Path"] == path


async def test_head_omits_static_assets_path_for_artifact_without_metadata(
    client: httpx.AsyncClient, storage: BuildArtifactStorage
) -> None:
    await storage.upload_artifact_fileobj("dep-1", "old", io.BytesIO(b"tar"))

    head = await client.head(f"{BUILDS_URL}/old")
    assert head.status_code == 200
    assert "X-Static-Assets-Path" not in head.headers
    assert (await client.head(f"{BUILDS_URL}/missing")).status_code == 404
