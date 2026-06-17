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

## Three strategies, increasing durability

| Strategy | Persists over `run()` calls | Survives process restart | Survives a crash mid-run |
|---|---|---|---|
| Data on the workflow instance | ✅ | ❌ | ❌ |
| `Context.to_dict()` once at the end | ✅ | ✅ | ❌ |
| Checkpoint loop: snapshot at each step boundary | ✅ | ✅ | ✅ |

The first two are quick to show. The checkpoint loop is the one that survives a crash mid-run, and
it is most of this page.

### Data on the workflow instance

A workflow is a regular Python class, so you can keep data in instance variables and reuse it across
`run()` calls on the same instance:

```python
class DbWorkflow(Workflow):
    def __init__(self, db, *args, **kwargs):
        self.db = db
        super().__init__(*args, **kwargs)

    @step
    async def count(self, ev: StartEvent) -> StopEvent:
        return StopEvent(result=self.db.exec("select COUNT(*) from t;"))
```

This is shared state between runs. It survives neither a restart nor a crash.

### Snapshot the context

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

The snapshot is only as cheap and reliable as what you put in events and state. Two rules.

Keep heavy or non-serializable inputs in a [Resource](/python/llamaagents/workflows/resources), not
in events or state. A `Resource` factory is resolved once, cached on the workflow, and never
serialized into the context. On resume it is re-created by calling the factory again. So put API
clients, model handles, and large reference data in resources, and let your events carry small
identifiers instead. This keeps the snapshot small, and keeps you clear of the serialization rule
below.

Everything on an in-flight event and in the state store has to be serializable. Snapshots use a JSON
serializer, Pydantic models included. If it hits a value it can't encode, `to_dict()` raises and the
whole snapshot fails, not just that field. So don't put raw bytes or open connections on events.

## When you want durability handled for you

The checkpoint loop is durability you control. You decide when to snapshot, and resume is a couple of
lines. If you'd rather not manage that, the runtime can persist and restore state for you.

Running a workflow as a [server](/python/llamaagents/workflows/deployment) persists its state across
requests and restarts. The [DBOS runtime](/python/llamaagents/workflows/dbos) journals every step
transition to a database, so a crashed workflow resumes on its own. Both run the same workflow code
you have already written.
