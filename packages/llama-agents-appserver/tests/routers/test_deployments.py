from typing import Any
from unittest import mock

from fastapi import FastAPI
from fastapi.testclient import TestClient
from llama_agents.appserver.deployment import Deployment
from llama_agents.appserver.routers.deployments import (
    create_base_router,
    create_deployments_router,
)
from llama_agents.appserver.types import TaskDefinition


def build_app(name: str, deployment: Deployment) -> TestClient:
    app = FastAPI()
    app.include_router(create_base_router(name))
    app.include_router(create_deployments_router(name, deployment))
    return TestClient(app)


def test_create_and_run_task_basic() -> None:
    d = Deployment(workflows={})
    d._workflow_services = {"default": mock.MagicMock()}

    async def run_workflow(
        service_id: str, session_id: str | None = None, **run_kwargs: dict
    ) -> dict[str, bool]:
        return {"ok": True}

    d.run_workflow = mock.AsyncMock(side_effect=run_workflow)  # type: ignore

    client = build_app("dep", d)
    r = client.post("/deployments/dep/tasks/run", json={"input": "{}"})
    assert r.status_code == 200
    assert r.json() == {"ok": True}


def test_create_task_nowait_and_list() -> None:
    d = Deployment(workflows={})
    d._workflow_services = {"default": mock.MagicMock()}
    d.run_workflow_no_wait = mock.MagicMock(return_value=("hid", "sid"))  # type: ignore

    client = build_app("dep", d)
    r = client.post("/deployments/dep/tasks/create", json={"input": "{}"})
    assert r.status_code == 200
    td = TaskDefinition.model_validate(r.json())
    assert td.task_id == "hid"
    assert td.session_id == "sid"

    # handler still running
    d._handlers = {"hid": mock.MagicMock(is_done=mock.MagicMock(return_value=False))}
    d._handler_inputs = {"hid": "{}"}
    r2 = client.get("/deployments/dep/tasks")
    assert r2.status_code == 200
    assert len(r2.json()) == 1


def test_get_event_stream() -> None:
    d = Deployment(workflows={})

    class Ctx:
        class Q:
            def empty(self) -> bool:
                return True

        def __init__(self) -> None:
            self.streaming_queue = self.Q()

    class H:
        def is_done(self) -> bool:
            return True

        def __await__(self) -> Any:
            async def _w() -> str:
                return "done"

            return _w().__await__()

        ctx = Ctx()

    d._handlers = {"hid": H()}  # type: ignore[attr-defined]  # ty: ignore[invalid-assignment]

    client = build_app("dep", d)
    r = client.get("/deployments/dep/tasks/hid/events", params={"session_id": "s"})
    assert r.status_code == 200


def test_sessions_crud() -> None:
    d = Deployment(workflows={})
    # have a default to allow create
    d._default_service = mock.MagicMock()
    d._workflow_services = {"default": mock.MagicMock()}
    client = build_app("dep", d)

    r = client.post("/deployments/dep/sessions/create")
    assert r.status_code == 200
    sid = r.json()["session_id"]

    r = client.get(f"/deployments/dep/sessions/{sid}")
    assert r.status_code == 200

    r = client.get("/deployments/dep/sessions")
    assert r.status_code == 200

    r = client.post(f"/deployments/dep/sessions/delete?session_id={sid}")
    assert r.status_code == 200
