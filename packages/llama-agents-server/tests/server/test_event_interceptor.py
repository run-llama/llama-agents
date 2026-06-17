# SPDX-License-Identifier: MIT
# Copyright (c) 2026 LlamaIndex Inc.
"""Tests for EventInterceptorDecorator."""

from __future__ import annotations

from typing import Any
from unittest.mock import MagicMock

from llama_agents.server._runtime.event_interceptor import (
    EventInterceptorDecorator,
)
from workflows.context.state_store import StateStore
from workflows.events import Event, StopEvent
from workflows.runtime.types.plugin import (
    ExternalRunAdapter,
    InternalRunAdapter,
    RegisteredWorkflow,
    Runtime,
    WaitResult,
    WaitResultTimeout,
)
from workflows.runtime.types.ticks import WorkflowTick
from workflows.workflow import Workflow


class _RecordingInternalAdapter(InternalRunAdapter):
    """Adapter that records calls for assertion."""

    def __init__(self) -> None:
        self.events_written: list[Event] = []
        self.ticks_sent: list[WorkflowTick] = []
        self.closed = False

    @property
    def run_id(self) -> str:
        return "test-run"

    async def write_to_event_stream(self, event: Event) -> None:
        self.events_written.append(event)

    async def get_now(self) -> float:
        return 1.0

    async def send_event(self, tick: WorkflowTick) -> None:
        self.ticks_sent.append(tick)

    async def wait_receive(self, timeout_seconds: float | None = None) -> WaitResult:
        return WaitResultTimeout()

    async def close(self) -> None:
        self.closed = True

    def get_state_store(
        self, namespace: tuple[str, ...] = ()
    ) -> StateStore[Any] | None:
        return None


class _StubRuntime(Runtime):
    def __init__(self, adapter: InternalRunAdapter) -> None:
        super().__init__()
        self._adapter = adapter

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
        raise NotImplementedError

    def get_internal_adapter(self, workflow: Any) -> InternalRunAdapter:
        return self._adapter

    def get_external_adapter(self, run_id: str) -> ExternalRunAdapter:
        raise NotImplementedError

    async def launch(self) -> None:
        pass

    async def destroy(self) -> None:
        pass


async def test_write_to_event_stream_is_blocked() -> None:
    inner_adapter = _RecordingInternalAdapter()
    runtime = _StubRuntime(inner_adapter)
    decorator = EventInterceptorDecorator(runtime)

    wf = MagicMock(spec=Workflow)
    adapter = decorator.get_internal_adapter(wf)

    await adapter.write_to_event_stream(StopEvent(result="hello"))

    assert inner_adapter.events_written == [], "Events should not reach inner adapter"


async def test_other_methods_pass_through() -> None:
    inner_adapter = _RecordingInternalAdapter()
    runtime = _StubRuntime(inner_adapter)
    decorator = EventInterceptorDecorator(runtime)

    wf = MagicMock(spec=Workflow)
    adapter = decorator.get_internal_adapter(wf)

    assert adapter.run_id == "test-run"
    assert await adapter.get_now() == 1.0
    await adapter.close()
    assert inner_adapter.closed
