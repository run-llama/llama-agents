# SPDX-License-Identifier: MIT
# Copyright (c) 2026 LlamaIndex Inc.
"""Subprocess runner for DBOS + WorkflowServer integration tests.

Runs a workflow through the full WorkflowServer decorator chain with the store
created by DBOSRuntime, enabling end-to-end event flow testing including
interrupt/resume scenarios.

Usage:
    python server_runner.py \
        --workflow "tests.fixtures.sample_workflows.chained:ChainedWorkflow" \
        --db-url "postgresql://user:pass@localhost/db" \
        --run-id "test-001" \
        --check-streams --check-events

    python server_runner.py \
        --workflow "tests.fixtures.sample_workflows.chained:ChainedWorkflow" \
        --db-url "postgresql://user:pass@localhost/db" \
        --run-id "test-001" \
        --interrupt-after StepTwoEvent

Flags:
    --check-streams: After workflow completes, query dbos.streams table
                     and print STREAMS_COUNT:<N>
    --check-events:  After workflow completes, query wf_events table
                     and print EVENTS_COUNT:<N> and EVENT_JSON:<json>
    --interrupt-after EVENT_NAME: Kill the process (os._exit) after seeing
                     EVENT_NAME or a StepStateChanged with matching output.
                     Simulates a crash for resume testing.
    --send-event JSON: Send an event with the given type and fields after startup.
"""

from __future__ import annotations

import argparse
import asyncio
import json
import os
import sys
from pathlib import Path

import asyncpg

# Add fixtures dir to find runner_common; safe now that fixtures/workflows
# was renamed to fixtures/sample_workflows to avoid shadowing the real package
sys.path.insert(0, str(Path(__file__).parent))

from llama_agents.server import WorkflowServer  # noqa: E402
from runner_common import (  # noqa: E402  # ty: ignore[unresolved-import]
    get_event_class_by_name,
    import_workflow,
    setup_dbos,
)


async def check_streams_count(db_url: str, run_id: str) -> None:
    """Query DBOS streams table and print count of published_events rows."""
    try:
        dsn = db_url
        if "+psycopg2" in dsn or "+psycopg" in dsn:
            dsn = "postgresql://" + dsn.split("://", 1)[1]

        conn = await asyncpg.connect(dsn)
        try:
            count = await conn.fetchval(
                "SELECT COUNT(*) FROM dbos.streams "
                "WHERE workflow_uuid = $1 AND key = 'published_events'",
                run_id,
            )
            print(f"STREAMS_COUNT:{count}", flush=True)
        finally:
            await conn.close()
    except Exception as e:
        print(f"ERROR:{type(e).__name__}:Failed to check streams: {e}", flush=True)


async def check_events(db_url: str, run_id: str, schema: str | None = None) -> None:
    """Query wf_events table and print all events."""
    try:
        dsn = db_url
        if "+psycopg2" in dsn or "+psycopg" in dsn:
            dsn = "postgresql://" + dsn.split("://", 1)[1]

        events_table = "wf_events" if schema is None else f"{schema}.wf_events"

        conn = await asyncpg.connect(dsn)
        try:
            rows = await conn.fetch(
                f"SELECT event_json FROM {events_table} "
                f"WHERE run_id = $1 ORDER BY sequence",
                run_id,
            )
            print(f"EVENTS_COUNT:{len(rows)}", flush=True)
            for row in rows:
                print(f"EVENT_JSON:{row['event_json']}", flush=True)
        finally:
            await conn.close()
    except Exception as e:
        print(f"ERROR:{type(e).__name__}:Failed to check events: {e}", flush=True)


async def run_workflow_with_server(
    workflow_path: str,
    db_url: str,
    run_id: str,
    do_check_streams: bool,
    do_check_events: bool,
    interrupt_after: str | None = None,
    send_event: str | None = None,
) -> None:
    """Run workflow through the full WorkflowServer + DBOS decorator chain."""
    workflow_class, _module = import_workflow(workflow_path)

    dbos_runtime = setup_dbos(db_url, app_name="test-server-workflow")

    wf = workflow_class(runtime=dbos_runtime)

    store = dbos_runtime.create_workflow_store()

    server_runtime = dbos_runtime.build_server_runtime()

    server = WorkflowServer(
        runtime=server_runtime,
        workflow_store=store,
        idle_timeout=60.0,
    )
    server.add_workflow("test", wf)

    try:
        async with server.contextmanager():
            wf_ref = server._service._runtime.get_workflow("test")
            assert wf_ref is not None

            # DBOS recovers pending runs at startup. Reattach to the original
            # handler instead of racing recovery by starting another run.
            handler_data = await server._service.load_handler(run_id)
            if handler_data is None:
                handler_data = await server._service.start_workflow(wf_ref, run_id)
            actual_run_id = handler_data.run_id
            assert actual_run_id is not None
            print(f"RUN_ID:{actual_run_id}", flush=True)

            if send_event is not None:
                event_config = json.loads(send_event)
                event_class = get_event_class_by_name(_module, event_config["type"])
                assert event_class is not None
                await server._service.send_event(
                    run_id, event_class(**event_config["fields"])
                )

            async for stored_event in store.subscribe_events(actual_run_id):
                event_type = stored_event.event.type
                print(f"EVENT:{event_type}", flush=True)

                # Check for interrupt: StepStateChanged events carry
                # output_event_name as a class repr string.
                if interrupt_after is not None:
                    data = stored_event.event.value or {}
                    output_name = data.get("output_event_name") or ""
                    if event_type == interrupt_after or interrupt_after in output_name:
                        print("INTERRUPTING", flush=True)
                        os._exit(0)

            print("SUCCESS", flush=True)

            if do_check_streams:
                await check_streams_count(db_url, actual_run_id)

            if do_check_events:
                await check_events(db_url, actual_run_id, dbos_runtime._schema)

    except Exception as e:
        print(f"ERROR:{type(e).__name__}:{e}", flush=True)
        raise
    finally:
        await dbos_runtime.destroy()


def main() -> None:
    """Entry point for the subprocess runner."""
    parser = argparse.ArgumentParser(
        description="Run workflows through WorkflowServer + DBOS for testing"
    )
    parser.add_argument("--workflow", required=True)
    parser.add_argument("--db-url", required=True)
    parser.add_argument("--run-id", required=True)
    parser.add_argument("--check-streams", action="store_true")
    parser.add_argument("--check-events", action="store_true")
    parser.add_argument("--send-event", help="JSON object with event type and fields")
    parser.add_argument(
        "--interrupt-after",
        default=None,
        help="Event name to interrupt after (simulates crash)",
    )

    args = parser.parse_args()

    asyncio.run(
        run_workflow_with_server(
            workflow_path=args.workflow,
            db_url=args.db_url,
            run_id=args.run_id,
            do_check_streams=args.check_streams,
            do_check_events=args.check_events,
            interrupt_after=args.interrupt_after,
            send_event=args.send_event,
        )
    )


if __name__ == "__main__":
    main()
