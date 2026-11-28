# SPDX-License-Identifier: MIT
# Copyright (c) 2026 LlamaIndex Inc.

from __future__ import annotations

import asyncio

from llama_agents.server import MemoryWorkflowStore, WorkflowServer
from workflows import Workflow, step
from workflows.events import StartEvent, StopEvent
from workflows.handler import WorkflowHandler
from workflows.plugins.basic import BasicRuntime


class QueuedCancellationWorkflow(Workflow):
    def __init__(self) -> None:
        super().__init__(timeout=None, num_concurrent_runs=1, runtime=BasicRuntime())
        self.entered = asyncio.Event()
        self.release = asyncio.Event()

    @step
    async def work(self, ev: StartEvent) -> StopEvent:
        self.entered.set()
        await self.release.wait()
        return StopEvent(result="completed")


async def test_cancel_queued_run_respects_grace_period() -> None:
    workflow = QueuedCancellationWorkflow()
    server = WorkflowServer(workflow_store=MemoryWorkflowStore())
    first = workflow.run()
    second: WorkflowHandler | None = None
    cancelling: asyncio.Task[None] | None = None
    try:
        await asyncio.wait_for(workflow.entered.wait(), timeout=2)
        second = workflow.run()
        cancelling = asyncio.create_task(server._service._cancel_run(second))
        # Observe the default five-second grace period without cancelling the call.
        done, _ = await asyncio.wait({cancelling}, timeout=6)
        assert cancelling in done, "server waited again after the grace period"
        await cancelling
        assert second.is_done()
        assert not first.is_done()
    finally:
        workflow.release.set()
        runs = [first]
        if second is not None:
            runs.append(second)
        await asyncio.gather(*runs, return_exceptions=True)
        if cancelling is not None:
            cancelling.cancel()
            await asyncio.gather(cancelling, return_exceptions=True)
