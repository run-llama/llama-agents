# SPDX-License-Identifier: MIT
# Copyright (c) 2026 LlamaIndex Inc.
from __future__ import annotations

from pathlib import Path

import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient
from llama_agents.appserver.routers.ui_proxy import mount_static_files
from llama_agents.appserver.settings import ApiserverSettings
from llama_agents.core.deployment_config import DeploymentConfig, UIConfig


@pytest.mark.parametrize("method", ["GET", "HEAD"])
@pytest.mark.parametrize("root_path", ["", "/gateway"])
def test_ui_directory_redirect_keeps_public_path(
    tmp_path: Path, method: str, root_path: str
) -> None:
    output = tmp_path / "ui" / "dist"
    directory = output / "some page"
    directory.mkdir(parents=True)
    (directory / "index.html").write_text("page")
    (output / "asset.js").write_text("asset")
    app = FastAPI()
    mount_static_files(
        app,
        DeploymentConfig(name="dep", ui=UIConfig(directory="ui")),
        ApiserverSettings(app_root=tmp_path),
    )
    path = f"{root_path}/deployments/dep/ui/some%20page"
    with TestClient(app, root_path=root_path) as client:
        response = client.request(
            method,
            path + "?id=a%2Fb&next=%2Fother",
            headers={"Host": "deployment.svc.cluster.local"},
            follow_redirects=False,
        )
        assert response.status_code == 307
        assert response.headers["location"] == path + "/?id=a%2Fb&next=%2Fother"
        followed = client.request(method, response.headers["location"])
        assert followed.status_code == 200
        assert followed.text == ("page" if method == "GET" else "")
        assert client.get(f"{root_path}/deployments/dep/ui/asset.js").text == "asset"
        assert client.get(f"{root_path}/deployments/dep/ui/missing").status_code == 404
