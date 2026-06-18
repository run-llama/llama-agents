---
sidebar:
  order: 6
title: Managing State
---

Each workflow run has a `Context`, and each context has a state store. Use it for values that steps need to share during a run, or for values you intentionally carry into a later run by reusing or restoring the same context.

State is not a place for heavyweight clients, indexes, file handles, or other runtime dependencies. Put those in [resources](/python/llamaagents/workflows/resources). State should be data you are willing to serialize when you snapshot or resume a workflow.

By default, workflows initialize an untyped state store. You can read and write it through `ctx.store`:

```python
from workflows import Workflow, Context, step
from workflows.events import StartEvent, StopEvent


class MyWorkflow(Workflow):

    @step
    async def my_step(self, ctx: Context, ev: StartEvent) -> StopEvent:
        current_count = await ctx.store.get("count", default=0)
        current_count += 1
        await ctx.store.set("count", current_count)
        return StopEvent(result=current_count)
```

## Locking the State

There are cases where state can be manipulated by multiple steps running at the same time. In these cases, lock the state with `edit_state()` so the update is atomic:

```python
@step
async def my_step(self, ctx: Context, ev: StartEvent) -> StopEvent:
    async with ctx.store.edit_state() as state:
        current_count = state.get("count", 0)
        state["count"] = current_count + 1
        result = state["count"]
    return StopEvent(result=result)
```

No other step can edit the state while the block is running. Keep the block small: do the read-modify-write work inside it, and keep slow LLM or network calls outside it.

## Adding Typed State

Often, you'll have a known shape for your workflow state. Use a `Pydantic` model for that. This gives you:

- Get type hints for your state
- Get automatic validation of your state
- (Optionally) Have full control over the serialization and deserialization of your state using [validators](https://docs.pydantic.dev/latest/concepts/validators/) and [serializers](https://docs.pydantic.dev/latest/concepts/serialization/#custom-serializers)

**NOTE:** Use a Pydantic model with defaults for all fields. This lets the `Context` automatically initialize the state.

Here's a quick example of how you can leverage workflows + pydantic to take advantage of all these features:

```python
from pydantic import BaseModel, Field


class CounterState(BaseModel):
    count: int = Field(default=0)
```

Then, simply annotate your workflow state with the state model:

```python
from workflows import Workflow, Context, step
from workflows.events import (
    StartEvent,
    StopEvent,
)


class MyWorkflow(Workflow):
    @step
    async def start(
        self,
        ctx: Context[CounterState], ev: StartEvent
    ) -> StopEvent:
        # Allows for atomic state updates
        async with ctx.store.edit_state() as state:
            state.count += 1

        return StopEvent(result="Done!")
```

You can also work with typed state one field at a time:

```python
@step
async def start(self, ctx: Context[CounterState], ev: StartEvent) -> StopEvent:
    current = await ctx.store.get("count")
    await ctx.store.set("count", current + 1)
    state = await ctx.store.get_state()
    return StopEvent(result=state.count)
```

## Maintaining Context Across Runs

If you want to maintain state across multiple runs of a workflow, create a context and pass the same one into `.run()`:

```python
workflow = MyWorkflow()
ctx = Context(workflow)

handler = workflow.run(ctx=ctx)
result = await handler

# Optional: save the ctx somewhere and restore
# ctx_dict = ctx.to_dict()
# ctx = Context.from_dict(workflow, ctx_dict)

# continue with next run
handler = workflow.run(ctx=ctx)
result = await handler
```

If the context is still running, `run(ctx=ctx)` resumes that run and does not send a new `StartEvent`. If the previous run has completed, `run(ctx=ctx)` starts a new run with the same stored state.

## Serializable state

State you keep here is serialized when you snapshot a run to make it
[durable](/python/llamaagents/workflows/durable_workflows), so keep it to values a JSON serializer
can encode. Put clients and other non-serializable objects in
[resources](/python/llamaagents/workflows/resources) instead.

For custom values, either make them Pydantic-serializable or provide a custom serializer when calling `Context.to_dict()` and `Context.from_dict()`.
