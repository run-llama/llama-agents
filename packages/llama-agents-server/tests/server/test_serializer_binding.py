from __future__ import annotations

import base64
import pickle
from pathlib import Path
from typing import Any, cast

import pytest
from llama_agents.server import MemoryWorkflowStore, WorkflowServer
from llama_agents.server._store.abstract_workflow_store import (
    AbstractWorkflowStore,
    HandlerQuery,
    PersistentHandler,
    Status,
    query_handlers,
    stream_workflow_ticks,
)
from llama_agents.server._store.sqlite.sqlite_workflow_store import SqliteWorkflowStore
from workflows import Context, Workflow, step
from workflows.context.serializers import (
    BaseSerializer,
    JsonSerializer,
    PickleSerializer,
)
from workflows.events import Event, StartEvent, StopEvent


class PythonEvent(Event):
    pass


class PythonWorkflow(Workflow):
    @step
    async def start(self, ctx: Context, ev: StartEvent) -> PythonEvent:
        await ctx.store.set("python_value", complex(1, 2))
        return PythonEvent.model_validate({"value": complex(3, 4)})

    @step
    async def finish(self, ctx: Context, ev: PythonEvent) -> StopEvent:
        assert await ctx.store.get("python_value") == complex(1, 2)
        assert ev.value == complex(3, 4)
        return StopEvent(result="ok")


class CustomSerializer(BaseSerializer):
    def __init__(self) -> None:
        self.encodes = 0
        self.decodes = 0

    def serialize(self, value: Any) -> str:
        self.encodes += 1
        return "custom:" + base64.b64encode(pickle.dumps(value)).decode()

    def deserialize(self, value: str) -> Any:
        self.decodes += 1
        assert value.startswith("custom:")
        return pickle.loads(base64.b64decode(value.removeprefix("custom:")))


@pytest.mark.parametrize("use_workflow_override", [True, False])
@pytest.mark.parametrize("serializer_type", [PickleSerializer, CustomSerializer])
async def test_bound_serializer_handles_internal_python_values(
    tmp_path: Path,
    use_workflow_override: bool,
    serializer_type: type[BaseSerializer],
) -> None:
    selected = serializer_type()
    workflow = PythonWorkflow(serializer=selected if use_workflow_override else None)
    store = SqliteWorkflowStore(db_path=str(tmp_path / "workflows.db"))
    server = WorkflowServer(
        workflow_store=store,
        serializer=JsonSerializer() if use_workflow_override else selected,
    )
    server.add_workflow("python", workflow)
    assert workflow.runtime.get_serializer(workflow) is selected
    server.add_workflow("python", workflow)
    assert workflow.runtime.get_serializer(workflow) is selected
    async with server._runtime_core.contextmanager():
        result = await server._runtime_core.run("python")
        completed = await server._service.await_workflow(result)
        assert completed.result is not None
        assert completed.result.value["result"] == "ok"
        assert result.run_id is not None
        ticks = [
            tick
            async for tick in stream_workflow_ticks(
                store, result.run_id, serializer=selected
            )
        ]
        assert ticks
        assert server._runtime_core._persistence is not None
        replayed = await server._runtime_core._persistence.context_from_ticks(
            workflow, result.run_id
        )
        assert replayed is not None
    if isinstance(selected, CustomSerializer):
        assert selected.encodes > 0
        assert selected.decodes > 0


class FirstStop(StopEvent):
    pass


class SecondStop(StopEvent):
    pass


async def test_store_selects_decoder_by_row_workflow_before_validation(
    tmp_path: Path,
) -> None:
    store = SqliteWorkflowStore(db_path=str(tmp_path / "results.db"))
    await store.start()
    for name, event_type in [("first", FirstStop), ("second", SecondStop)]:
        await store.update(
            PersistentHandler(
                handler_id=name,
                workflow_name=name,
                status="completed",
                result=event_type.model_validate({"result": name}),
            )
        )
    decoders = {
        "first": JsonSerializer(allowed_types=[FirstStop]),
        "second": JsonSerializer(allowed_types=[SecondStop]),
    }
    handlers = await store.query(HandlerQuery(), result_decoder=decoders.__getitem__)
    assert {type(handler.result) for handler in handlers} == {FirstStop, SecondStop}
    with pytest.raises(ValueError, match="Refusing to import"):
        await store.query(HandlerQuery(), result_decoder=lambda name: decoders["first"])


async def test_legacy_custom_query_signature_is_used_without_decoder(
    tmp_path: Path,
) -> None:
    class LegacyStore:
        async def query(self, query: HandlerQuery) -> list[PersistentHandler]:
            return []

    store = cast(AbstractWorkflowStore, LegacyStore())
    assert await query_handlers(store, HandlerQuery()) == []
    with pytest.raises(TypeError, match="result_decoder"):
        await query_handlers(
            store, HandlerQuery(), result_decoder=lambda name: JsonSerializer()
        )


async def test_legacy_status_override_receives_no_new_keyword(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    store = MemoryWorkflowStore()
    calls: list[str] = []

    async def update(
        run_id: str,
        *,
        status: Status | None = None,
        result: StopEvent | None = None,
        error: str | None = None,
    ) -> None:
        calls.append(run_id)
        await MemoryWorkflowStore.update_handler_status(
            store, run_id, status=status, result=result, error=error
        )

    monkeypatch.setattr(store, "update_handler_status", update)
    server = WorkflowServer(workflow_store=store, serializer=PickleSerializer())
    server.add_workflow("python", PythonWorkflow())
    async with server._runtime_core.contextmanager():
        result = await server._runtime_core.run("python")
        completed = await server._service.await_workflow(result)
        assert completed.status == "completed"
    assert calls
