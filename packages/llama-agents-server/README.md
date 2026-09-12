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
