# LlamaAgents DBOS Runtime

DBOS durable runtime plugin for LlamaIndex Workflows.

## Installation

```bash
pip install llama-agents-dbos
```

## Usage

```python
import asyncio
from llama_agents.dbos import DBOSRuntime
from dbos import DBOS, DBOSConfig
from workflows import Workflow, step, StartEvent, StopEvent

# Configure DBOS
config: DBOSConfig = {
    "name": "my-app",
    "system_database_url": "postgresql://...",
}
DBOS(config=config)

# Create runtime and workflow
runtime = DBOSRuntime()

class MyWorkflow(Workflow):
    @step
    async def my_step(self, ev: StartEvent) -> StopEvent:
        return StopEvent(result="done")

workflow = MyWorkflow(runtime=runtime)

# launch_sync() works outside async contexts; use await runtime.launch() inside one
runtime.launch_sync()

async def main():
    result = await workflow.run()

asyncio.run(main())
```

## Workflow concurrency

The runtime submits every workflow run through a stable DBOS queue named
`_llamaindex_workflow_queue:<workflow_name>`. The runtime fixes that durable
name when the workflow first enters the runtime. WorkflowServer route aliases
do not replace the queue or the DBOS workflow registration. The default remains
unlimited:

```python
workflow = MyWorkflow(runtime=runtime, num_concurrent_runs=None)
```

The default workflow name includes the Python module and class name. Set an
explicit `workflow_name` when that code may be renamed and existing DBOS work
must survive the deployment:

```python
workflow = MyWorkflow(
    runtime=runtime,
    workflow_name="orders.v1",
    num_concurrent_runs=8,
)
```

Treat this name as a durable identifier. Changing it also changes DBOS's
control-loop and step registrations, so workers using the old name must drain
their work before they are removed.

Set `num_concurrent_runs` to a positive integer to limit active runs of that
workflow on each DBOS worker. Changing the value back to `None` removes the
limit without changing queues, so existing work remains available. Queue
admission normally takes about the configured
`DBOSRuntime(polling_interval_sec=...)`, but DBOS can back off longer while a
queue is idle.

Concurrency changes within the same DBOS application version are safe during a
rolling deployment. Workers can briefly use different limits while both
revisions are running, so total capacity is the sum of their per-worker limits.
The first deployment that moves existing direct-start runs onto queues can also
temporarily exceed the new limit while those earlier runs finish. If a
deployment changes the DBOS application version, keep workers for the old
version running until its queued and active workflows have drained.

Graceful cancellation remains event based. Cancelling an `ENQUEUED` run stores
the cancellation request, but the run must be admitted before it can publish a
`WorkflowCancelledEvent` and finish.

If the application calls `DBOS.listen_queues`, it must include every queue in
`runtime.workflow_queues`. DBOS does not expose its listener selection, so the
runtime cannot validate a restricted listener configuration. DBOS's default
listener discovers queues registered after launch, but an explicit listener
list does not. Applications using a restricted list must register workflows
and collect `runtime.workflow_queues` before launch.

## Features

- Durable workflow execution backed by DBOS
- Automatic step recording and replay
- Distributed workers and recovery support
- Per-worker workflow concurrency with an unlimited default
