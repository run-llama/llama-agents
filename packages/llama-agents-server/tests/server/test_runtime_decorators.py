# SPDX-License-Identifier: MIT
# Copyright (c) 2026 LlamaIndex Inc.
"""Tests for the base runtime decorator forwarding classes."""

from __future__ import annotations

from typing import Any, AsyncGenerator
from unittest.mock import MagicMock

from workflows.context.state_store import StateStore
from workflows.events import (
    Event,
    StopEvent,
)
from workflows.runtime.runtime_decorators import (
    BaseExternalRunAdapterDecorator,
    BaseInternalRunAdapterDecorator,
    BaseRuntimeDecorator,
)
from workflows.runtime.types.plugin import (
    ExternalRunAdapter,
    InternalRunAdapter,
    RegisteredWorkflow,
    Runtime,
    WaitResult,
    WaitResultTimeout,
)
from workflows.runtime.types.ticks import WorkflowTick

# -- Stubs -----------------------------------------------------------------


class StubInternalAdapter(InternalRunAdapter):
    def __init__(self) -> None:
        self.closed = False

    @property
    def run_id(self) -> str:
        return "r1"

    async def write_to_event_stream(self, event: Event) -> None:
        pass

    async def get_now(self) -> float:
        return 1.0

    async def send_event(self, tick: WorkflowTick) -> None:
        pass

    async def wait_receive(self, timeout_seconds: float | None = None) -> WaitResult:
        return WaitResultTimeout()

    async def close(self) -> None:
        self.closed = True

    def get_state_store(
        self, namespace: tuple[str, ...] = ()
    ) -> StateStore[Any] | None:
        return None


class StubExternalAdapter(ExternalRunAdapter):
    def __init__(self) -> None:
        self.closed = False

    @property
    def run_id(self) -> str:
        return "r1"

    async def send_event(self, tick: WorkflowTick) -> None:
        pass

    async def stream_published_events(self) -> AsyncGenerator[Event, None]:
        yield StopEvent(result="done")

    async def close(self) -> None:
        self.closed = True

    async def get_result(self) -> StopEvent:
        return StopEvent(result="done")

    def get_state_store(
        self, namespace: tuple[str, ...] = ()
    ) -> StateStore[Any] | None:
        return None


class StubRuntime(Runtime):
    def __init__(self) -> None:
        super().__init__()
        self.launched = False

    def register(self, workflow: Any) -> RegisteredWorkflow:
        return RegisteredWorkflow(
            workflow=workflow, workflow_run_fn=MagicMock(), steps={}
        )

    def run_workflow(
        self,
        run_id: str,
        workflow: Any,
        init_state: Any,
        start_event: Any = None,
        serialized_state: dict[str, Any] | None = None,
        serializer: Any = None,
    ) -> ExternalRunAdapter:
        return StubExternalAdapter()

    def get_internal_adapter(self, workflow: Any) -> InternalRunAdapter:
        return StubInternalAdapter()

    def get_external_adapter(self, run_id: str) -> ExternalRunAdapter:
        return StubExternalAdapter()

    async def launch(self) -> None:
        self.launched = True

    async def destroy(self) -> None:
        pass


# -- Tests -----------------------------------------------------------------


def test_runtime_decorator_forwards() -> None:
    inner = StubRuntime()
    dec = BaseRuntimeDecorator(inner)
    dec.launch_sync()
    assert inner.launched


async def test_internal_adapter_decorator_forwards() -> None:
    inner = StubInternalAdapter()
    dec = BaseInternalRunAdapterDecorator(inner)
    assert dec.run_id == "r1"
    assert await dec.get_now() == 1.0
    await dec.close()
    assert inner.closed


async def test_external_adapter_decorator_forwards() -> None:
    inner = StubExternalAdapter()
    dec = BaseExternalRunAdapterDecorator(inner)
    assert dec.run_id == "r1"
    result = await dec.get_result()
    assert result.result == "done"
    await dec.close()
    assert inner.closed


async def test_subclass_can_override_selectively() -> None:
    """Override one method; the rest still forward."""

    class Custom(BaseInternalRunAdapterDecorator):
        async def get_now(self) -> float:
            return 42.0

    inner = StubInternalAdapter()
    dec = Custom(inner)
    assert await dec.get_now() == 42.0
    assert dec.run_id == "r1"  # still forwarded


def test_runtime_decorator_forwards_untrack() -> None:
    from workflows import Workflow, step
    from workflows.events import StartEvent

    class SimpleWorkflow(Workflow):
        @step
        async def start(self, ev: StartEvent) -> StopEvent:
            return StopEvent(result="done")

    inner = StubRuntime()
    dec = BaseRuntimeDecorator(inner)
    wf = SimpleWorkflow(runtime=dec)
    assert wf in dec._pending
    dec.untrack_workflow(wf)
    assert wf not in dec._pending
