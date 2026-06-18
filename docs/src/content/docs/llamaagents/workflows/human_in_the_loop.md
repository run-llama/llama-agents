---
sidebar:
  order: 12
title: Human in the Loop
---

Human-in-the-loop workflows need to pause, tell the caller what input is needed, and continue when the caller sends a response. Workflows support that with normal events.

The most direct pattern is a pair of steps: one returns `InputRequiredEvent`, and another consumes `HumanResponseEvent`. The caller watches the stream and sends the response back into the same handler.

```python
from workflows import Workflow, step
from workflows.events import StartEvent, StopEvent, InputRequiredEvent, HumanResponseEvent


class NumberWorkflow(Workflow):
    @step
    async def ask(self, ev: StartEvent) -> InputRequiredEvent:
        return InputRequiredEvent(prefix="Enter a number: ")

    @step
    async def answer(self, ev: HumanResponseEvent) -> StopEvent:
        return StopEvent(result=ev.response)


workflow = NumberWorkflow()

handler = workflow.run()
async for event in handler.stream_events():
    if isinstance(event, InputRequiredEvent):
        # This could be input(), a websocket reply, a web form submission, etc.
        response = input(event.prefix)
        await handler.send_event(HumanResponseEvent(response=response))

final_result = await handler
```

Here, the workflow waits until the `HumanResponseEvent` is emitted. You can subclass both events when the prompt or response needs more structure.

## Stopping/Resuming Between Human Responses

In a web app, the process that sees the prompt is often not the same request that receives the answer. Snapshot the context after the prompt, store it, and restore it when the response arrives.

```python
import json
from workflows import Context

handler = workflow.run()
async for event in handler.stream_events():
    if isinstance(event, InputRequiredEvent):
        await db.save("run-123", json.dumps(handler.ctx.to_dict()))
        await handler.cancel_run()
        break

# Later, when the human response arrives:
response = form_data["response"]
ctx_dict = json.loads(await db.load("run-123"))
restored_ctx = Context.from_dict(workflow, ctx_dict)
handler = workflow.run(ctx=restored_ctx)

await handler.send_event(HumanResponseEvent(response=response))
async for event in handler.stream_events():
    continue

final_result = await handler
```

Cancel the original handler after snapshotting if you are intentionally handing the run off to another request or process. That avoids leaving the in-memory run waiting for an answer that will be delivered to the restored run.

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

Use `waiter_id` when the same step may wait more than once. Use `requirements` when several waiters consume the same event type and you need to route the right response to the right waiter:

```python
response = await ctx.wait_for_event(
    HumanResponseEvent,
    waiter_event=InputRequiredEvent(prefix="Approve draft? "),
    waiter_id="approve-draft",
    requirements={"request_id": ev.request_id},
)
```

`wait_for_event` replays all code preceding it whenever the step receives its triggering event or a matching waiting event. The step always runs at least once up to the waiter, which then raises an internal exception to pause execution. Any code before the `wait_for_event` call must be safe to repeat.

Due to this complexity, the event-based approach with separate steps is generally recommended.

See the [API reference](/python/workflows-api-reference/context/#workflows.context.Context.wait_for_event) for full details.
