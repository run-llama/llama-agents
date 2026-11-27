# LlamaAgents Server

HTTP server for deploying [LlamaIndex Workflows](https://pypi.org/project/llama-index-workflows/) as web services. Built on Starlette and Uvicorn.

## Installation

```bash
pip install llama-agents-server
```

## Quick Start

Create a server file (e.g., `my_server.py`):

```python
import asyncio
from workflows import Workflow, step
from workflows.context import Context
from workflows.events import Event, StartEvent, StopEvent
from llama_agents.server import WorkflowServer

class StreamEvent(Event):
    sequence: int

class GreetingWorkflow(Workflow):
    @step
    async def greet(self, ctx: Context, ev: StartEvent) -> StopEvent:
        for i in range(3):
            ctx.write_event_to_stream(StreamEvent(sequence=i))
        name = ev.get("name", "World")
        return StopEvent(result=f"Hello, {name}!")

server = WorkflowServer()
server.add_workflow("greet", GreetingWorkflow())

if __name__ == "__main__":
    asyncio.run(server.serve("0.0.0.0", 8080))
```

Or run it with the CLI:

```bash
llama-agents-server my_server.py
```

## Features

- REST API for running, streaming, and managing workflows
- Debugger UI automatically mounted at `/` for visualizing and debugging workflows
- Event streaming via newline-delimited JSON or Server-Sent Events
- Human-in-the-loop support for interactive workflows
- Persistence with built-in SQLite store (or bring your own via `AbstractWorkflowStore`)

## Client

Use [`llama-agents-client`](https://pypi.org/project/llama-agents-client/) to interact with deployed servers programmatically.

## Documentation

See the full [deployment guide](https://developers.llamaindex.ai/python/llamaagents/workflows/deployment/) for API details, persistence configuration, and more.

### PostgreSQL journal upgrades

The server keeps separate event and tick sequence numbers for each run. Journal
appends are serialized for the same run and stream, including across server
instances using the same PostgreSQL table. Distinct runs and streams remain
independent. Tick writes are acknowledged before the control loop applies the
corresponding commands; a journal failure stops the run and is reported as a
failure when the handler and event store remain writable. A failed write is not
retried automatically because the server may not know whether it committed.

The PostgreSQL migration checks each schema for event and tick sequence
constraints. If an older installation contains duplicate `(run_id, sequence)`
rows, migration stops without modifying the journal. Back up the database and
inspect the reported run and sequence before reconciling duplicates: replay
order is not recoverable from the cursor alone, and existing consumers may have
observed the duplicate cursor. Once the data is reconciled, restart the server
to retry the migration. The migration does not choose or delete rows for you.
