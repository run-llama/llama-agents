# SPDX-License-Identifier: MIT
# Copyright (c) 2026 LlamaIndex Inc.
"""Black box tests for idle release behavior over live HTTP."""

from __future__ import annotations

import pytest
from llama_agents.client.client import WorkflowClient
from llama_agents.server import MemoryWorkflowStore, WorkflowServer
from server_test_fixtures import (
    live_server,  # type: ignore[import]
    wait_for_passing,  # type: ignore[import]
)
from workflows import Context, Workflow, step
from workflows.events import Event, StartEvent, StopEvent, WorkflowIdleEvent


class WaitableExternalEvent(Event):
    response: str


class WaitingWorkflow(Workflow):
    @step
    async def start_and_wait(self, ctx: Context, ev: StartEvent) -> StopEvent:
        external = await ctx.wait_for_event(WaitableExternalEvent)
        return StopEvent(result=f"received: {external.response}")


@pytest.mark.asyncio
async def test_fast_idle_timeout_does_not_drop_valid_event() -> None:
    def make_server() -> WorkflowServer:
        server = WorkflowServer(
            workflow_store=MemoryWorkflowStore(),
        )
        server.add_workflow(
            "waiting",
            WaitingWorkflow(),
            additional_events=[WaitableExternalEvent],
        )
        return server

    async with live_server(make_server) as (base_url, _server):
        client = WorkflowClient(base_url=base_url)
        started = await client.run_workflow_nowait("waiting")
        handler_id = started.handler_id

        async for env in client.get_workflow_events(
            handler_id, include_internal_events=True
        ):
            if env.type != "WorkflowIdleEvent":
                continue
            event = env.load_event([WorkflowIdleEvent])
            if isinstance(event, WorkflowIdleEvent):
                send = await client.send_event(
                    handler_id, WaitableExternalEvent(response="hello")
                )
                assert send.status == "sent"
                break

        async def handler_completed() -> None:
            data = await client.get_handler(handler_id)
            assert data.status == "completed"
            assert data.result is not None
            assert data.result.value.get("result") == "received: hello"

        await wait_for_passing(handler_completed, max_duration=2.0, interval=0.01)
