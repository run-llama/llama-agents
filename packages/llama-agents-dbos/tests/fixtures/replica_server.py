# SPDX-License-Identifier: MIT
# Copyright (c) 2026 LlamaIndex Inc.
"""Minimal WorkflowServer replica for cross-process integration tests.

Launches a WorkflowServer on the given port with a configurable workflow.
Prints READY to stdout once listening. The test process drives it via HTTP.

Usage:
    python replica_server.py --workflow module.path:ClassName --db-url DSN --port 18001
"""

from __future__ import annotations

import argparse
import asyncio
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent))

from dbos import DBOS, DBOSConfig  # noqa: E402
from llama_agents.dbos import DBOSRuntime  # noqa: E402
from llama_agents.server import WorkflowServer  # noqa: E402
from runner_common import import_workflow  # noqa: E402  # ty: ignore[unresolved-import]
from workflows.events import Event  # noqa: E402


async def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--workflow", required=True)
    parser.add_argument("--db-url", required=True)
    parser.add_argument("--port", type=int, required=True)
    parser.add_argument("--idle-timeout", type=float, default=None)
    parser.add_argument("--executor-id", default=None)
    args = parser.parse_args()

    workflow_class, _module = import_workflow(args.workflow)
    config: DBOSConfig = {
        "name": f"test-replica-{args.port}",
        "system_database_url": args.db_url,
        "run_admin_server": False,
        "notification_listener_polling_interval_sec": 0.01,
        "executor_id": args.executor_id or f"test-replica-{args.port}",
    }
    DBOS(config=config)
    dbos_runtime = DBOSRuntime(
        polling_interval_sec=0.01, queue_polling_interval_sec=1.0
    )

    wf = workflow_class(runtime=dbos_runtime)

    store = dbos_runtime.create_workflow_store()
    idle_kwargs = (
        {"idle_timeout": args.idle_timeout} if args.idle_timeout is not None else {}
    )
    server_runtime = dbos_runtime.build_server_runtime(**idle_kwargs)
    server = WorkflowServer(runtime=server_runtime, workflow_store=store)
    # Discover all Event subclasses in the workflow's module so that
    # wait_for_event types (not in step signatures) are in the registry.
    additional_events = [
        attr
        for name in dir(_module)
        if isinstance(attr := getattr(_module, name), type)
        and issubclass(attr, Event)
        and attr is not Event
    ]
    server.add_workflow("test", wf, additional_events=additional_events)

    # Bind the server runtime before DBOS launch can recover pending workflows.
    await server.start()
    try:
        print(f"READY:{args.port}", flush=True)
        await server.serve(host="0.0.0.0", port=args.port)
    finally:
        await server.stop()
        await dbos_runtime.destroy()


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        sys.exit(0)
