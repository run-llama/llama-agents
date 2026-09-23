# SPDX-License-Identifier: MIT
# Copyright (c) 2026 LlamaIndex Inc.
"""End-to-end child-workflow durability over live HTTP with subprocess isolation.

Drives a parent+child HITL workflow through the full idle-release → resume
cycle: the child suspends on a human-input waiter, the run is released from
memory, and answering the child's concrete invocation path resumes it from the
DBOS journal. Also covers origin-namespace threading over SSE and the child's
elapsed-alive timeout budget surviving the release downtime.
"""

from __future__ import annotations

import asyncio
import subprocess
import sys
from pathlib import Path

import pytest
from llama_agents.client import WorkflowClient
from test_dbos_idle_release_e2e import (  # type: ignore[import]
    IDLE_TIMEOUT,
    REPLICA_SERVER_PATH,
    _stop_server,
    _wait_for_server,
    _wait_for_table_value,
)
from tests.fixtures.sample_workflows.child_hitl import UserInput
from workflows.events import WorkflowIdleEvent

CHILD_WORKFLOW_PATH = "tests.fixtures.sample_workflows.child_hitl:ChildHitlWorkflow"


def _start_child_server(
    port: int, db_url: str, idle_timeout: float
) -> subprocess.Popen[str]:
    cmd = [
        sys.executable,
        REPLICA_SERVER_PATH,
        "--workflow",
        CHILD_WORKFLOW_PATH,
        "--db-url",
        db_url,
        "--port",
        str(port),
        "--idle-timeout",
        str(idle_timeout),
    ]
    return subprocess.Popen(
        cmd,
        text=True,
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        start_new_session=True,
    )


async def _run_child_resume_cycle(port: int, db_url: str) -> None:
    proc = _start_child_server(port, db_url, IDLE_TIMEOUT)
    try:
        _wait_for_server(proc, port)
        client = WorkflowClient(base_url=f"http://localhost:{port}")

        handler = await client.run_workflow_nowait("test")
        handler_id = handler.handler_id
        run_id = handler.run_id or ""
        assert run_id, "Workflow should have a run_id"

        # Stream with children included: the child's InputRequired surfaces with
        # its concrete invocation path, and the run reaches idle.
        saw_child_ask = False
        child_origin: tuple[str, ...] = ()
        stream = client.get_workflow_events(
            handler_id, include_internal_events=True, include_children=True
        )
        async for env in stream:
            if "InputRequiredEvent" in ((env.types or []) + [env.type]):
                saw_child_ask = True
                child_origin = tuple(env.origin_namespace)
            if env.type == WorkflowIdleEvent.__name__:
                env.load_event([WorkflowIdleEvent])
                break
        assert saw_child_ask, "child InputRequiredEvent should surface with children"
        assert child_origin == ("child#0",), child_origin

        last_seq = stream.last_sequence

        # The run is released from memory; the child's alive-time budget forgives
        # this downtime rather than firing its timeout on resume.
        await _wait_for_table_value(
            db_url, "workflow_status", "status", "workflow_uuid", run_id, "SUCCESS"
        )
        await _wait_for_table_value(
            db_url, "run_lifecycle", "state", "run_id", run_id, "released"
        )

        h = await client.get_handler(handler_id)
        assert h.status == "running", f"Expected 'running', got '{h.status}'"

        # Answer the child's concrete invocation path; this resumes the released
        # run and reconstructs the suspended child waiter from the journal.
        send_resp = await client.send_event(
            handler_id, UserInput(response="world"), step="child#0/process"
        )
        assert send_resp.status == "sent"

        got_stop = False
        async for env in client.get_workflow_events(
            handler_id, after_sequence=last_seq
        ):
            if env.type == "StopEvent":
                got_stop = True
                break
        assert got_stop, "Should see StopEvent after resume"

        for _ in range(60):
            h = await client.get_handler(handler_id)
            if h.status == "completed":
                break
            await asyncio.sleep(0.25)
        assert h.status == "completed", f"Expected 'completed', got '{h.status}'"
        assert h.result is not None
        assert h.result.value.get("result", {}).get("response") == "world"
    finally:
        _stop_server(proc)


@pytest.mark.asyncio
@pytest.mark.timeout(60)
async def test_child_resume_across_idle_release_sqlite(tmp_path: Path) -> None:
    """A suspended child resumes after idle release, on SQLite."""
    db_path = tmp_path / "child_e2e.sqlite3"
    db_url = f"sqlite+pysqlite:///{db_path}?check_same_thread=false"
    await _run_child_resume_cycle(18040, db_url)
