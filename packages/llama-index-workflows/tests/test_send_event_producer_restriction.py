# SPDX-License-Identifier: MIT
# Copyright (c) 2026 LlamaIndex Inc.
from __future__ import annotations

import pytest

from workflows import Context, Workflow, step
from workflows.errors import WorkflowValidationError
from workflows.events import Event, StartEvent, StopEvent


class WorkerEvent(Event):
    idx: int


async def test_send_event_allows_declared_event() -> None:
    class DeclaredWorkflow(Workflow):
        @step
        async def start(self, ctx: Context, ev: StartEvent) -> WorkerEvent | None:
            # WorkerEvent is declared in this step's return annotation, so
            # emitting it via send_event passes the producer-side check.
            ctx.send_event(WorkerEvent(idx=1))
            return None

        @step
        async def worker(self, ev: WorkerEvent) -> StopEvent:
            return StopEvent(result=ev.idx)

    result = await DeclaredWorkflow(disable_validation=True).run()
    assert result == 1


async def test_send_event_rejects_undeclared_event() -> None:
    # The graph is valid: WorkerEvent is produced by ``maker`` and consumed by
    # ``consumer``. But ``start`` declares only ``-> StopEvent`` and tries to
    # emit a WorkerEvent at runtime, which the producer-side check must reject.
    class UndeclaredWorkflow(Workflow):
        @step
        async def start(self, ctx: Context, ev: StartEvent) -> StopEvent:
            ctx.send_event(WorkerEvent(idx=1))
            return StopEvent(result="ok")

        @step
        async def maker(self, ev: StartEvent) -> WorkerEvent:
            return WorkerEvent(idx=0)

        @step
        async def consumer(self, ev: WorkerEvent) -> None:
            return None

    with pytest.raises(WorkflowValidationError) as exc_info:
        await UndeclaredWorkflow(disable_validation=True).run()

    message = str(exc_info.value)
    assert "start" in message
    assert "WorkerEvent" in message
    assert "StopEvent" in message
