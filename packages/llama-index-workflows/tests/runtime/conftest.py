# SPDX-License-Identifier: MIT
# Copyright (c) 2026 LlamaIndex Inc.

"""Test fixtures and utilities for runtime tests."""

from __future__ import annotations

import asyncio
import time
from typing import TYPE_CHECKING, Any, AsyncGenerator

import pytest
import time_machine
from workflows.events import Event, StartEvent, StopEvent
from workflows.runtime.types.internal_state import BrokerConfig, BrokerState

if TYPE_CHECKING:
    from workflows.context.serializers import BaseSerializer
    from workflows.context.state_store import InMemoryStateStore
from workflows.plugins.basic import get_current_run_id
from workflows.runtime.types.plugin import (
    ExternalRunAdapter,
    InternalRunAdapter,
    RegisteredWorkflow,
    Runtime,
    SnapshottableAdapter,
    V2RuntimeCompatibilityShim,
    WaitResult,
    WaitResultTick,
    WaitResultTimeout,
)
from workflows.runtime.types.step_function import (
    as_step_worker_functions,
    create_workflow_run_function,
)
from workflows.runtime.types.ticks import WorkflowTick
from workflows.workflow import Workflow


class MockRuntime(Runtime):
    """Mock runtime that stores adapters for test access."""

    def __init__(self) -> None:
        self._adapters: dict[str, MockRunAdapter] = {}
        self._current_run_id: str | None = None

    def register(self, workflow: Workflow) -> RegisteredWorkflow:
        return RegisteredWorkflow(
            workflow=workflow,
            workflow_run_fn=create_workflow_run_function(workflow),
            steps=as_step_worker_functions(workflow),
        )

    def get_internal_adapter(self, workflow: Workflow) -> InternalRunAdapter:
        run_id = get_current_run_id() or self._current_run_id or "test"
        if run_id not in self._adapters:
            self._adapters[run_id] = MockRunAdapter(run_id)
        return self._adapters[run_id]

    def get_external_adapter(self, run_id: str) -> ExternalRunAdapter:
        if run_id not in self._adapters:
            self._adapters[run_id] = MockRunAdapter(run_id)
        return self._adapters[run_id]

    def run_workflow(
        self,
        run_id: str,
        workflow: Workflow,
        init_state: BrokerState,
        start_event: StartEvent | None = None,
        serialized_state: dict[str, Any] | None = None,
        serializer: "BaseSerializer | None" = None,
    ) -> ExternalRunAdapter:
        self._current_run_id = run_id
        return self.get_external_adapter(run_id)

    def set_adapter(self, run_id: str, adapter: "MockRunAdapter") -> None:
        """Set a specific adapter for a run_id (for test setup)."""
        self._adapters[run_id] = adapter


class MockRunAdapter(
    InternalRunAdapter,
    ExternalRunAdapter,
    SnapshottableAdapter,
    V2RuntimeCompatibilityShim,
):
    """Mock RunAdapter for testing control loops. Supports snapshot/replay."""

    def __init__(
        self, run_id: str, traveller: time_machine.Coordinates | None = None
    ) -> None:
        self._run_id = run_id
        # Queue for events sent from external sources (e.g., via send_event)
        self._external_queue: asyncio.Queue[WorkflowTick] = asyncio.Queue()
        # Queue for events published to the event stream (e.g., for UI/callbacks)
        self._event_stream: asyncio.Queue[Event] = asyncio.Queue()
        # Time-machine traveller for deterministic time control
        self._traveller = traveller
        # Current time in seconds, can be advanced manually for testing
        self._current_time: float = time.time()
        # Recorded ticks for snapshot/replay
        self._ticks: list[WorkflowTick] = []
        # State store for context
        self._state_store: "InMemoryStateStore[Any] | None" = None
        # Result tracking for get_result/cancel
        self._result: asyncio.Future[StopEvent] = asyncio.Future()
        self._cancelled: bool = False

    @property
    def run_id(self) -> str:
        return self._run_id

    @property
    def tags(self) -> dict[str, Any]:
        return {"llamaindex.run_id": self._run_id}

    @property
    def init_state(self) -> BrokerState:
        # Return a minimal BrokerState for testing
        return BrokerState(
            is_running=False,
            config=BrokerConfig(steps={}, timeout=None),
            workers={},
        )

    async def on_tick(self, tick: WorkflowTick) -> None:
        """Record a tick for replay."""
        self._ticks.append(tick)

    def replay(self) -> list[WorkflowTick]:
        """Return recorded ticks for replay."""
        return self._ticks

    async def close(self) -> None:
        """
        Close the adapter.
        """
        pass

    async def write_to_event_stream(self, event: Event) -> None:
        await self._event_stream.put(event)

    async def stream_published_events(self) -> AsyncGenerator[Event, None]:
        while True:
            item = await self._event_stream.get()
            yield item
            if isinstance(item, StopEvent):
                break

    async def send_event(self, tick: WorkflowTick) -> None:
        await self._external_queue.put(tick)

    async def get_now(self) -> float:
        if self._traveller is not None:
            return time.time()
        return self._current_time

    async def wait_receive(
        self,
        timeout_seconds: float | None = None,
    ) -> WaitResult:
        """Wait for tick with optional timeout.

        When a timeout occurs, advances mock time by the timeout duration
        to ensure scheduled ticks become due.
        """
        try:
            if timeout_seconds is None:
                tick = await self._external_queue.get()
            else:
                tick = await asyncio.wait_for(
                    self._external_queue.get(),
                    timeout=timeout_seconds,
                )
            return WaitResultTick(tick=tick)
        except asyncio.TimeoutError:
            # Advance mock time when timeout occurs
            if timeout_seconds is not None:
                self.advance_time(timeout_seconds)
            return WaitResultTimeout()

    def advance_time(self, seconds: float) -> None:
        if self._traveller is not None:
            self._traveller.shift(seconds)
        else:
            self._current_time += seconds

    async def get_stream_event(self, timeout: float = 1.0) -> Event:
        return await asyncio.wait_for(self._event_stream.get(), timeout=timeout)

    def has_stream_events(self) -> bool:
        return not self._event_stream.empty()

    def get_underlying_store(self) -> "InMemoryStateStore[Any] | None":
        return self._state_store

    def set_state_store(self, state_store: "InMemoryStateStore[Any]") -> None:
        self._state_store = state_store

    async def get_result(self) -> StopEvent:
        """Get the result of the workflow run."""
        return await self._result

    def get_result_or_none(self) -> StopEvent | None:
        """Get the result if completed, otherwise None."""
        if self._result.done() and not self._result.cancelled():
            return self._result.result()
        return None

    @property
    def is_running(self) -> bool:
        """Check if the workflow run is still running."""
        return not self._result.done() and not self._cancelled

    def abort(self) -> None:
        """Abort by cancelling the result future."""
        if not self._result.done():
            self._result.cancel()
            self._cancelled = True

    def set_result(self, result: StopEvent) -> None:
        """Set the result (for test setup)."""
        if not self._result.done():
            self._result.set_result(result)


@pytest.fixture
async def test_plugin() -> MockRunAdapter:
    return MockRunAdapter(run_id="test")


@pytest.fixture
async def test_plugin_with_time_machine() -> AsyncGenerator[
    tuple[MockRunAdapter, time_machine.Coordinates], None
]:
    """Adapter with time-machine at epoch 1000.0, tick=True."""
    with time_machine.travel("2026-01-07T12:27:00.000-08:00", tick=True) as traveller:
        yield MockRunAdapter(run_id="test", traveller=traveller), traveller
