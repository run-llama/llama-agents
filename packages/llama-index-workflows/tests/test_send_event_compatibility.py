# SPDX-License-Identifier: MIT
# Copyright (c) 2026 LlamaIndex Inc.
from __future__ import annotations

from workflows import Context, Workflow, step
from workflows.events import Event, StartEvent, StopEvent


class WorkerEvent(Event):
    idx: int


async def test_send_event_does_not_require_return_annotation() -> None:
    class CompatibilityWorkflow(Workflow):
        @step
        async def start(self, ctx: Context, ev: StartEvent) -> StopEvent:
            ctx.send_event(WorkerEvent(idx=1))
            return StopEvent(result="ok")

        @step
        async def worker(self, ev: WorkerEvent) -> None:
            return None

    result = await CompatibilityWorkflow(disable_validation=True).run()
    assert result == "ok"
