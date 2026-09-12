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

`Workflow(serializer=...)` configures internal state and event encoding for that
workflow. `WorkflowServer(serializer=...)` supplies the default for workflows
without an override. Both accept `BaseSerializer`, including `PickleSerializer`
and custom implementations of the string `serialize`/`deserialize` methods.
HTTP events and persisted handler results keep their JSON representation.

`WorkflowServer(json_serializer=...)` configures public JSON decoding separately.
By default the server decodes only the types it knows about: framework classes,
each workflow's events, its `Context[T]` state type, registered
`additional_events`, and anything passed as `extra_types`. Pydantic rebuilds the
fields of those models, so only a model stored inside an envelope of its own
needs listing in `extra_types`. Unknown names are rejected without importing
anything, and the default internal JSON serializer uses the same registration.

To decode by importing names, as earlier releases did, pass a serializer with
dynamic lookup:

```python
server = WorkflowServer(json_serializer=JsonSerializer())
```

These settings also survive appserver loading of a configured source server.

Custom workflow stores must accept keyword-only `result_decoder` on `query` and
`update_handler_status`. It maps a workflow name to a `JsonSerializer`; select it
before validating each stored handler. DBOS runtime chains can
receive the same callback through `build_server_runtime(result_decoder=...)`.
Without that callback, DBOS selects public JSON decoding from each tracked
workflow’s current runtime binding via `get_json_serializer(workflow)`.

`BaseSerializer.validation_context()` is a no-op by default. JSON serializers
use this synchronous scope to carry class lookup through metadata validation;
it does not encode or decode metadata with the application serializer.

Each `add_workflow` call replaces the default registration snapshot. New
executions and reads select the current decoder. Existing state stores and
captured decoders keep their original snapshot. Moving workflows to another
server does not revoke the source snapshot, so appserver can retain it during
transfer. No serializer is modified after construction.
