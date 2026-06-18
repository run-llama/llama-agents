---
sidebar:
  order: 13
title: Writing durable workflows
---

Workflows are ephemeral by default. Once `run()` returns, the state is gone, and the next `run()`
starts fresh. But a workflow is often where you've written an ad hoc concurrent process. You fan out
over a hundred documents, call an LLM per chunk, and collect the results. That is the kind of run you
don't want to start over from zero when the process is killed halfway through.

This page shows how to checkpoint that run and resume it after a restart.

## Snapshot the context

A run advances by reducing events in a controlled way, so its state is well defined at every step
boundary. That state is the events still in flight plus the
[state store](/python/llamaagents/workflows/managing_state). `Context.to_dict()` serializes it,
`Context.from_dict()` rebuilds it, and `run(ctx=...)` continues from there, even in a different
process:

```python
w = MyWorkflow()
handler = w.run()
result = await handler

# Persist the snapshot
db.save("my-run", json.dumps(handler.ctx.to_dict()))

# ...new process...
w = MyWorkflow()
ctx = Context.from_dict(w, json.loads(db.load("my-run")))
result = await w.run(ctx=ctx)   # continues with the restored state
```

This survives a restart, but not a crash during the run. You only have the state as of the last
`to_dict()`. To survive a crash, snapshot as the run progresses.

On resume, the restored run re-dispatches the events that were still pending and rebuilds the partial
fan-in buffers. Completed steps don't re-run, because their output is already in the restored state.
A step that was mid-execution when you captured the state is rewound and runs again from the top.
Resume is at-least-once, and step side effects need to be safe to repeat.

## The checkpoint loop

There is no built-in checkpointer to enable. The workflow stream emits an internal event every time
a step changes state, and you snapshot when you see one finish. `stream_events(expose_internal=True)`
surfaces those internal events, and `StepStateChanged` with `StepState.NOT_RUNNING` marks a step
finishing:

```python
from workflows.events import StepStateChanged, StepState

handler = w.run()
async for ev in handler.stream_events(expose_internal=True):
    if isinstance(ev, StepStateChanged) and ev.step_state == StepState.NOT_RUNNING:
        db.save("my-run", json.dumps(handler.ctx.to_dict()))
result = await handler
```

To resume after a crash, load the last snapshot and pass it to `run()`:

```python
w = MyWorkflow()
ctx = Context.from_dict(w, json.loads(db.load("my-run")))
result = await w.run(ctx=ctx)
```

In a busy run, you usually don't need to write on every boundary. Each snapshot is a serialize and a
write, so throttle it if the workflow is noisy. A crash then costs you at most that interval of
redone work.

The snapshot is the state at the moment you take it. Other workers may have advanced since the event
that triggered the write, but the state is still consistent and resumable.

## A concurrent fan-out that survives a restart

A workflow that fans out work, runs items concurrently, and collects the results. Checkpointed so a
kill mid-run doesn't redo completed items:

```python
import json, os
from typing import Annotated
from workflows import Context, Workflow, step
from workflows.events import Event, StartEvent, StepState, StepStateChanged, StopEvent
from workflows.resource import Resource


class WorkItem(Event):
    item_id: int

class WorkDone(Event):
    item_id: int
    result: str


# Heavy inputs come from a Resource (see below). They aren't serialized into
# the snapshot; they're re-created on resume.
def get_client() -> MyApiClient:
    return MyApiClient(...)


class EnrichBatch(Workflow):
    @step
    async def dispatch(self, ev: StartEvent) -> list[WorkItem]:
        return [WorkItem(item_id=item_id) for item_id in ev.item_ids]

    @step(num_workers=8)
    async def enrich(
        self,
        ev: WorkItem,
        client: Annotated[MyApiClient, Resource(get_client)],
    ) -> WorkDone:
        result = await client.enrich(ev.item_id)   # the expensive, repeatable work
        return WorkDone(item_id=ev.item_id, result=result)

    @step
    async def collect(self, events: list[WorkDone]) -> StopEvent:
        return StopEvent(result={ev.item_id: ev.result for ev in events})
```

Drive it with the checkpoint loop, and resume from the last snapshot if the process died:

```python
CKPT = "enrich-batch.json"

async def run_batch(item_ids):
    wf = EnrichBatch()
    if os.path.exists(CKPT):
        # Resume from the snapshot. The pending work and partial fan-in state
        # are already in the context, so don't send the StartEvent again.
        handler = wf.run(ctx=Context.from_dict(wf, json.load(open(CKPT))))
    else:
        handler = wf.run(item_ids=item_ids)

    async for ev in handler.stream_events(expose_internal=True):
        if isinstance(ev, StepStateChanged) and ev.step_state == StepState.NOT_RUNNING \
           and ev.name == "enrich":
            json.dump(handler.ctx.to_dict(), open(CKPT, "w"))
    return await handler
```

Say this is killed after 60 of 100 items. The finished `enrich` calls are part of the restored
workflow state, and the pending `WorkItem`s are still pending. On resume, `dispatch` does not re-run,
the completed items are not re-enriched, and the list fan-in fires once all 100 `WorkDone`s are
present across both runs.

The in-flight work does get repeated. At any moment up to `num_workers` `enrich` calls are running
but not finished, and each of those is rewound and re-enriched on resume. With `num_workers=8` that
is up to 8 items run again. It is wasted compute, not wrong output, and it is why `enrich` has to be
safe to repeat.

## Keeping snapshots cheap and correct

The snapshot is only as cheap and reliable as what you put in events and state.

Workflow instance attributes are not part of the snapshot. They can be useful for ordinary Python
objects that live as long as the workflow instance does, but they are not durable state. If a value
needs to survive restart, put it in an event or the state store.

Keep heavy or non-serializable inputs in a [Resource](/python/llamaagents/workflows/resources), not
in events or state. A `Resource` factory is resolved once, cached on the workflow, and never
serialized into the context. On resume it is re-created by calling the factory again. So put API
clients, model handles, and large reference data in resources, and let your events carry small
identifiers instead. This keeps the snapshot small, and keeps you clear of the serialization rule
below.

Everything on an in-flight event and in the state store has to be serializable. Snapshots use a JSON
serializer, Pydantic models included. If it hits a value it can't encode, `to_dict()` raises and the
whole snapshot fails, not just that field. So don't put raw bytes or open connections on events.

## Runtime plugins for durability

The checkpoint loop is durability you control. You decide when to snapshot, and resume is a couple of
lines. If you'd rather not manage that, use a runtime plugin that owns persistence and recovery.

For local apps, scripts, tests, and single-process workers that do not need an HTTP server,
`DurableWorkflowRuntime` gives you the managed version of the same pattern. It stores handler state,
events, state-store writes, and replay ticks in a `SqliteWorkflowStore`.

The durable handle is a stable `handler_id`. Start the run with that id, then use the same id after
restart to send events and read status from SQLite:

```python
import asyncio
from pathlib import Path

from llama_agents.server import (
    DurableWorkflowRuntime,
    HandlerQuery,
    SqliteWorkflowStore,
)
from workflows import Context, Workflow, step
from workflows.events import HumanResponseEvent, StartEvent, StopEvent


class Approval(HumanResponseEvent):
    response: str


class DraftWorkflow(Workflow):
    @step
    async def create_draft(self, ctx: Context, ev: StartEvent) -> None:
        await ctx.store.set("draft", f"Draft for {ev.get('topic')}")

    @step
    async def approve(self, ctx: Context, ev: Approval) -> StopEvent:
        draft = await ctx.store.get("draft")
        return StopEvent(result=f"{draft}: {ev.response}")


async def wait_until_complete(
    runtime: DurableWorkflowRuntime, handler_id: str
) -> StopEvent:
    while True:
        status = await runtime.get_handler_status(handler_id)
        if status.status == "completed":
            assert status.result is not None
            return status.result
        await asyncio.sleep(0.05)
```

Start the workflow against a SQLite file:

```python
db_path = Path("workflow-state.db")
handler_id = "draft-42"

runtime = DurableWorkflowRuntime(
    workflow_store=SqliteWorkflowStore(str(db_path)),
)
runtime.add_workflow("draft", DraftWorkflow())
await runtime.start()

handler_data = await runtime.run(
    "draft",
    handler_id=handler_id,
    topic="quarterly update",
)
print(handler_data.handler_id, handler_data.status)

# Simulate process shutdown. Active tasks are hard-aborted, but persisted
# ticks and state remain in SQLite for the next runtime instance.
await runtime.stop()
```

Then create a new runtime against the same SQLite file and continue by persistent id:

```python
runtime = DurableWorkflowRuntime(
    workflow_store=SqliteWorkflowStore(str(db_path)),
)
runtime.add_workflow("draft", DraftWorkflow())
await runtime.start()

await runtime.send_event(handler_id, Approval(response="approved"))
result = await wait_until_complete(runtime, handler_id)
print(result.result)

handlers = await runtime.query_handlers(HandlerQuery(handler_id_in=[handler_id]))
print(handlers[0].status)

await runtime.stop()
```

`run()` returns after the workflow has persisted enough state to be replayed, so immediate process
death after `run()` is recoverable. A loaded `WorkflowHandler` is only useful while the run is active
in the current process. Once the run only exists in SQLite, use the durable id API: `send_event`,
`get_handler_status`, and `query_handlers`.

If you already need HTTP, use `WorkflowServer` with a durable `workflow_store`. If you need
multi-process or distributed execution, use the [DBOS runtime](/python/llamaagents/workflows/dbos).
DBOS journals step transitions to a database, so a crashed workflow can resume without your
checkpoint loop, and `WorkflowServer` can run workflows with the same runtime plugin.
