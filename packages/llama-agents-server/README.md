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

## Pressure Diagnostics

Pressure diagnostics are opt-in instrumentation for incident debugging. They
record workflow step intervals alongside event-loop lag and process RSS
threshold events, then emit structured log records through
`llama_agents.server.diagnostics.pressure`.

```python
from llama_agents.server import PressureDiagnosticsConfig, WorkflowServer

server = WorkflowServer(
    diagnostics=PressureDiagnosticsConfig(
        enabled=True,
        sample_interval=1.0,
        event_loop_lag_threshold_ms=250,
        memory_rss_threshold_mb=3000,
        memory_growth_threshold_mb=500,
        memory_growth_window_seconds=60,
        capture_lag_stacks=True,
    )
)
```

Example log payloads include `memory_rss_threshold_exceeded`,
`memory_growth_threshold_exceeded`, and
`event_loop_lag_threshold_exceeded`. Each pressure event includes the active or
overlapping workflow intervals at the time of the sample. Treat those intervals
as temporal context, not proof that a step caused memory growth or lag. Pair
these logs with process profilers such as py-spy or Memray when an incident
needs allocation or CPU attribution.

## Client

Use [`llama-agents-client`](https://pypi.org/project/llama-agents-client/) to interact with deployed servers programmatically.

## Documentation

See the full [deployment guide](https://developers.llamaindex.ai/python/llamaagents/workflows/deployment/) for API details, persistence configuration, and more.
