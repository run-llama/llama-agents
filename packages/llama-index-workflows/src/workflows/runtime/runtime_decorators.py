# SPDX-License-Identifier: MIT
# Copyright (c) 2026 LlamaIndex Inc.
"""
Base decorator classes for Runtime, InternalRunAdapter, and ExternalRunAdapter.

These provide a simple forwarding pattern: accept an inner instance, delegate
every method to it. Subclasses override only the methods they need to customise.
"""

from __future__ import annotations

import logging
from contextlib import contextmanager
from typing import Any, AsyncGenerator, Generator

from workflows.context.serializers import BaseSerializer
from workflows.context.state_store import StateStore
from workflows.events import (
    Event,
    StartEvent,
    StopEvent,
)
from workflows.runtime.types.internal_state import BrokerState
from workflows.runtime.types.named_task import NamedTask, PendingStart
from workflows.runtime.types.plugin import (
    ExternalRunAdapter,
    InternalRunAdapter,
    RegisteredWorkflow,
    Runtime,
    WaitForNextTaskResult,
    WaitResult,
)
from workflows.runtime.types.ticks import WorkflowTick
from workflows.workflow import Workflow

logger = logging.getLogger(__name__)


class BaseRuntimeDecorator(Runtime):
    """Decorator base for :class:`Runtime`.

    Wraps an inner runtime and forwards every call to it.  Subclasses can
    override individual methods to add behaviour (logging, metrics, auth,
    etc.) without re-implementing the full interface.
    """

    def __init__(self, decorated: Runtime) -> None:
        super().__init__()
        self._decorated = decorated

    def register(self, workflow: Workflow) -> RegisteredWorkflow:
        return self._decorated.register(workflow)

    def run_workflow(
        self,
        run_id: str,
        workflow: Workflow,
        init_state: BrokerState,
        start_event: StartEvent | None = None,
        serialized_state: dict[str, Any] | None = None,
        serializer: BaseSerializer | None = None,
    ) -> ExternalRunAdapter:
        return self._decorated.run_workflow(
            run_id,
            workflow,
            init_state,
            start_event=start_event,
            serialized_state=serialized_state,
            serializer=serializer,
        )

    def get_internal_adapter(self, workflow: Workflow) -> InternalRunAdapter:
        return self._decorated.get_internal_adapter(workflow)

    def get_external_adapter(self, run_id: str) -> ExternalRunAdapter:
        return self._decorated.get_external_adapter(run_id)

    async def launch(self) -> None:
        await super().launch()
        await self._decorated.launch()

    @property
    def is_launched(self) -> bool:
        return self._decorated.is_launched

    async def destroy(self) -> None:
        await self._decorated.destroy()

    def track_workflow(self, workflow: Workflow) -> None:
        self._pending.add(workflow)
        self._decorated.track_workflow(workflow)

    def untrack_workflow(self, workflow: Workflow) -> None:
        self._pending.discard(workflow)
        self._decorated.untrack_workflow(workflow)

    def get_registered(self, workflow: Workflow) -> RegisteredWorkflow | None:
        return self._decorated.get_registered(workflow)

    @contextmanager
    def registering(self) -> Generator[Runtime, None, None]:
        with self._decorated.registering() as rt:
            yield rt


class BaseInternalRunAdapterDecorator(InternalRunAdapter):
    """Decorator base for :class:`InternalRunAdapter`.

    Wraps an inner adapter and forwards every call to it.  Subclasses can
    override individual methods to intercept or augment behaviour.
    """

    def __init__(self, decorated: InternalRunAdapter) -> None:
        self._decorated = decorated

    @property
    def run_id(self) -> str:
        return self._decorated.run_id

    async def write_to_event_stream(self, event: Event) -> None:
        await self._decorated.write_to_event_stream(event)

    async def get_now(self) -> float:
        return await self._decorated.get_now()

    async def send_event(self, tick: WorkflowTick) -> None:
        await self._decorated.send_event(tick)

    async def wait_receive(
        self,
        timeout_seconds: float | None = None,
    ) -> WaitResult:
        return await self._decorated.wait_receive(timeout_seconds)

    async def close(self) -> None:
        await self._decorated.close()

    def get_state_store(
        self, namespace: tuple[str, ...] = ()
    ) -> StateStore[Any] | None:
        return self._decorated.get_state_store(namespace)

    async def finalize_step(self) -> None:
        await self._decorated.finalize_step()

    def is_replaying(self) -> bool:
        return self._decorated.is_replaying()

    async def on_tick(self, tick: WorkflowTick) -> None:
        await self._decorated.on_tick(tick)

    async def after_tick(self, tick: WorkflowTick) -> None:
        await self._decorated.after_tick(tick)

    async def wait_for_next_task(
        self,
        running: list[NamedTask],
        pending: list[PendingStart],
        timeout: float | None = None,
    ) -> WaitForNextTaskResult:
        return await self._decorated.wait_for_next_task(running, pending, timeout)


class BaseExternalRunAdapterDecorator(ExternalRunAdapter):
    """Decorator base for :class:`ExternalRunAdapter`.

    Wraps an inner adapter and forwards every call to it.  Subclasses can
    override individual methods to intercept or augment behaviour.
    """

    def __init__(self, decorated: ExternalRunAdapter) -> None:
        self._decorated = decorated

    @property
    def run_id(self) -> str:
        return self._decorated.run_id

    async def send_event(self, tick: WorkflowTick) -> None:
        await self._decorated.send_event(tick)

    def stream_published_events(self) -> AsyncGenerator[Event, None]:
        return self._decorated.stream_published_events()

    async def close(self) -> None:
        await self._decorated.close()

    async def get_result(self) -> StopEvent:
        return await self._decorated.get_result()

    async def cancel(self) -> None:
        await self._decorated.cancel()

    def get_state_store(
        self, namespace: tuple[str, ...] = ()
    ) -> StateStore[Any] | None:
        return self._decorated.get_state_store(namespace)
