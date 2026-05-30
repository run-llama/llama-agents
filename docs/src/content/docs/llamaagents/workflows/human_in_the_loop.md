---
sidebar:
  order: 6
title: Human in the Loop
---

Since workflows are so flexible, there are many possible ways to implement human-in-the-loop patterns.

The easiest way to implement a human-in-the-loop is to use the `InputRequiredEvent` and `HumanResponseEvent` events during event streaming.

```python
from workflows import Workflow, step
from workflows.events import StartEvent, StopEvent, InputRequiredEvent, HumanResponseEvent


class HumanInTheLoopWorkflow(Workflow):
    @step
    async def step1(self, ev: StartEvent) -> InputRequiredEvent:
        return InputRequiredEvent(prefix="Enter a number: ")

    @step
    async def step2(self, ev: HumanResponseEvent) -> StopEvent:
        return StopEvent(result=ev.response)


# workflow should work with streaming
workflow = HumanInTheLoopWorkflow()

handler = workflow.run()
async for event in handler.stream_events():
    if isinstance(event, InputRequiredEvent):
        # here, we can handle human input however you want
        # this means using input(), websockets, accessing async state, etc.
        # here, we just use input()
        response = input(event.prefix)
        handler.ctx.send_event(HumanResponseEvent(response=response))

final_result = await handler
```

Here, the workflow will wait until the `HumanResponseEvent` is emitted.

If needed, you can also subclass these two events to add custom payloads.

## Stopping/Resuming Between Human Responses

You can break out of the event loop and resume later. This is useful when you want to pause the workflow to wait for a human response asynchronously (e.g., from a web request).

```python
from workflows import Context

handler = workflow.run()
async for event in handler.stream_events():
    if isinstance(event, InputRequiredEvent):
        # Serialize the context, store it anywhere as a JSON blob
        ctx_dict = handler.ctx.to_dict()
        await handler.cancel_run()
        break

...

# now we handle the human response once it comes in
response = input(event.prefix)

restored_ctx = Context.from_dict(workflow, ctx_dict)
handler = workflow.run(ctx=restored_ctx)

# Send the event to resume the workflow
handler.ctx.send_event(HumanResponseEvent(response=response))

# now we resume the workflow streaming with our restored context
async for event in handler.stream_events():
    continue

final_result = await handler
```

## Using `wait_for_event`

An alternative approach is to use `ctx.wait_for_event()` to wait for input within a single step:

```python
@step
async def ask_user(self, ctx: Context, ev: StartEvent) -> StopEvent:
    response = await ctx.wait_for_event(
        HumanResponseEvent,
        waiter_event=InputRequiredEvent(prefix="Enter a number: "),
        waiter_id="get_number",
    )
    return StopEvent(result=response.response)
```

**Important**: `wait_for_event` replays all code preceding it whenever the step receives its triggering event _or_ a matching waiting event. The step always runs at least once up to the waiter, which then raises an internal exception to pause execution. Because of this, any code before the `wait_for_event` call must be idempotent (safe to repeat).

Due to this complexity, the event-based approach with separate steps is generally recommended.

See the [API reference](/python/workflows-api-reference/context/#workflows.context.Context.wait_for_event) for full details.
