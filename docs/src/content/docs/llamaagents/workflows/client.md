---
sidebar:
  order: 19
title: Python Client
---

The `WorkflowClient` class provides a Python interface for interacting with a running `WorkflowServer`. It supports listing workflows, running them synchronously or asynchronously, streaming events, and sending events for human-in-the-loop workflows.

## Installation

The client is a separate package from the core `llama-index-workflows` library:

```bash
pip install llama-agents-client
```

## Setup

```python
from llama_agents.client import WorkflowClient

client = WorkflowClient(base_url="http://0.0.0.0:8080")
```

You can also pass a pre-configured `httpx.AsyncClient` instead of a `base_url`:

```python
import httpx

httpx_client = httpx.AsyncClient(base_url="http://0.0.0.0:8080", headers={"Authorization": "Bearer ..."})
client = WorkflowClient(httpx_client=httpx_client)
```

## Basic Usage

```python
from llama_agents.client import WorkflowClient
from workflows.events import StartEvent

async def main():
    client = WorkflowClient(base_url="http://0.0.0.0:8080")

    # Check server health
    await client.is_healthy()

    # List available workflows
    workflows = await client.list_workflows()
    print(workflows)

    # Run a workflow synchronously (blocks until completion)
    result = await client.run_workflow("greet", start_event=StartEvent(name="John"))
    print(result.result)
```

## Async Runs and Event Streaming

For long-running workflows, start the workflow asynchronously and stream events as they're produced:

```python
handler = await client.run_workflow_nowait("greet", start_event=StartEvent(name="John"))
handler_id = handler.handler_id

async for event in client.get_workflow_events(handler_id):
    print("Received:", event.type, event.value)

result = await client.get_handler(handler_id)
print(f"Final result: {result.result}")
```

### Cursor Behavior (`after_sequence`)

`get_workflow_events` accepts an `after_sequence` parameter that controls where in the event history the stream begins:

| Value | Behavior |
|---|---|
| `-1` (default) | Stream **all** events from the beginning, including any that were produced before you started listening. |
| `"now"` | Skip all existing events and only stream events produced **after** the request arrives. |
| Any integer `N` | Stream events with sequence number greater than `N`. |

This is useful for different use cases:

- **Full replay** (`-1`): You want to see every event from a run, even if it's already in progress or completed.
- **Live tail** (`"now"`): You started the workflow yourself and only care about new events going forward. If the workflow has already completed and all events have been produced, the server responds with HTTP 204 and the stream ends immediately.
- **Resume from checkpoint** (integer): You previously disconnected and want to resume from the last event you saw.

To access the stream position, use `get_workflow_events` which returns an `EventStream` object with a `last_sequence` property:

```python
stream = client.get_workflow_events(handler_id)
async for event in stream:
    print(event.type, "at sequence", stream.last_sequence)

# Save the position for later
saved_sequence = stream.last_sequence
```

You can then resume from a saved position:

```python
stream = client.get_workflow_events(handler_id, after_sequence=saved_sequence)
async for event in stream:
    print(event)
```

Or skip existing events and only receive new ones:

```python
stream = client.get_workflow_events(handler_id, after_sequence="now")
async for event in stream:
    print(event)
```

`get_workflow_events` automatically reconnects from the last received sequence on connection drops (up to `max_reconnect_attempts`, default 3).

## Human-in-the-Loop

For workflows that require external input, use event streaming combined with `send_event`:

```python
from workflows import Workflow, step
from workflows.context import Context
from workflows.events import (
    StartEvent,
    StopEvent,
    InputRequiredEvent,
    HumanResponseEvent,
)
from llama_agents.server import WorkflowServer

class RequestEvent(InputRequiredEvent):
    prompt: str

class ResponseEvent(HumanResponseEvent):
    response: str

class OutEvent(StopEvent):
    output: str

class HumanInTheLoopWorkflow(Workflow):
    @step
    async def prompt_human(self, ev: StartEvent, ctx: Context) -> RequestEvent:
        return RequestEvent(prompt="What is your name?")

    @step
    async def greet_human(self, ev: ResponseEvent) -> OutEvent:
        return OutEvent(output=f"Hello, {ev.response}")

server = WorkflowServer()
server.add_workflow("human", HumanInTheLoopWorkflow(timeout=1000))
await server.serve("0.0.0.0", "8080")
```

Then on the client side:

```python
from llama_agents.client import WorkflowClient

client = WorkflowClient(base_url="http://0.0.0.0:8080")
handler = await client.run_workflow_nowait("human")
handler_id = handler.handler_id

async for event in client.get_workflow_events(handler_id):
    # load_event() reconstructs the typed Event class by qualified name
    loaded = event.load_event()
    if isinstance(loaded, RequestEvent):
        print("Workflow is requiring human input:", loaded.prompt)
        name = input("Reply here: ")
        await client.send_event(
            handler_id=handler_id,
            event=ResponseEvent(response=name.capitalize().strip()),
        )

result = await client.get_handler(handler_id)
res = OutEvent.model_validate(result.result)
print("Received final message:", res.output)
```

`load_event()` works automatically when the event class is importable by its qualified name. You can also pass a `registry` list to resolve against: `event.load_event(registry=[RequestEvent, ResponseEvent])`. If you don't need typed events, the raw `event.type` and `event.value` dict are always available.
