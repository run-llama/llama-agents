---
sidebar:
  order: 13
title: Writing durable workflows
---

Workflows are ephemeral by default: once `run()` returns, the state is gone, and the next `run()`
starts fresh. But a workflow is often where you've expressed an ad-hoc, concurrent,
hard-to-reason-about process — fan out over a hundred documents, call an LLM per chunk, collect the
results. That's exactly the kind of run you don't want to start over from zero when the process is
killed halfway through.

This page shows how to make such a workflow **best-effort checkpoint and resume across a restart —
in a few lines, with no external durable runtime.** If you'd rather not write any checkpoint code
at all and can afford a backing database, skip to [DBOS](/python/llamaagents/workflows/dbos); this
page is the lightweight path.

## The mental model: events are the state

What makes checkpointing cheap here is that a workflow's runtime state *is* its in-flight events.
At any instant, "where the workflow is" comes down to: which events sit in step input queues, which
events a fan-in is still collecting, which steps are mid-execution, and the contents of the
[state store](/python/llamaagents/workflows/managing_state).

`Context.to_dict()` captures all of that as a single **point-in-time snapshot** — the pending event
queues, the `collect_events` buffers, any `wait_for_event` waiters, and the state store.
`Context.from_dict()` rebuilds a context from that snapshot, and passing it to `run()` resumes from
there. It's a snapshot, **not** a journal: it carries the events that are *currently relevant*, not
a log of everything that ever happened.

Resume has two behaviors worth internalizing, because together they make a snapshot safe to restart
from:

- **Completed steps don't re-run.** A step that already finished consumed its triggering event; that
  event is no longer in the snapshot, so it's never re-dispatched. Its output already lives
  downstream (in the next step's queue or a collect buffer), so the work is preserved.
- **In-flight steps are rewound and re-run.** A step that had consumed its event but hadn't finished
  when the snapshot was taken is rewound — its event goes back on the queue, and on resume the
  **entire step body executes again from the top.**

The consequence, and the one rule you must design around: **resume re-executes in-flight steps, so
steps must be safe to repeat.** This is at-least-once execution. If a step performs an external side
effect (a DB write, a payment, an email) before it returns, that effect can happen again on resume.
Make those effects idempotent, or guard them with a marker in the state store.

## Three strategies, increasing durability

| Strategy | Persists over `run()` calls | Survives process restart | Survives a crash mid-run |
|---|---|---|---|
| Data on the workflow instance | ✅ | ❌ | ❌ |
| `Context.to_dict()` once at the end | ✅ | ✅ | ❌ |
| Checkpoint loop: snapshot at each step boundary | ✅ | ✅ | ✅ |

The first two are below; the third — the one that actually survives a kill mid-run — is the rest of
this page.

### Data on the workflow instance

A workflow is a regular Python class, so you can stash data in instance variables and reuse it
across `run()` calls on the same instance:

```python
class DbWorkflow(Workflow):
    def __init__(self, db, *args, **kwargs):
        self.db = db
        super().__init__(*args, **kwargs)

    @step
    async def count(self, ev: StartEvent) -> StopEvent:
        return StopEvent(result=self.db.exec("select COUNT(*) from t;"))
```

This survives neither a process restart nor a crash — it's just shared state between runs.

### Snapshot the context

The context's [state store](/python/llamaagents/workflows/managing_state) is async-safe and, unlike
instance variables, **serializable**. Snapshot it after a run and restore it later — even in a
different process:

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

This survives a restart, but not a crash *during* the run — you only have the state as of the last
explicit `to_dict()`. To survive a crash, snapshot as the run progresses.

## The checkpoint loop

There's no built-in checkpointer to enable. Instead, the workflow stream emits an internal event
every time a step changes state, and you snapshot when you see one finish.
`stream_events(expose_internal=True)` surfaces these internal events; `StepStateChanged` with
`StepState.NOT_RUNNING` marks a step instance completing:

```python
from workflows.events import StepStateChanged, StepState

handler = w.run()
async for ev in handler.stream_events(expose_internal=True):
    if isinstance(ev, StepStateChanged) and ev.step_state == StepState.NOT_RUNNING:
        db.save("my-run", json.dumps(handler.ctx.to_dict()))
result = await handler
```

That's the whole mechanism: observe step boundaries, snapshot the context at each one. To resume
after a crash, load the last snapshot and pass it to `run()`:

```python
w = MyWorkflow()
ctx = Context.from_dict(w, json.loads(db.load("my-run")))
result = await w.run(ctx=ctx)
```

A few honest caveats about the snapshot point:

- `NOT_RUNNING` fires **per step execution**, not per logical step. A `@step(num_workers=N)` step or
  a fan-out emits one event per item, so you'll checkpoint many times in a concurrent run. `ev.name`
  and `ev.worker_id` tell you which.
- The snapshot you take when you observe the event is "the state right now," not a freeze of the
  instant that step finished — other concurrent workers may have advanced. That's fine: it's still a
  consistent, resumable checkpoint. It just isn't a deterministic per-step freeze unless the
  workflow is single-worker.
- Snapshotting on every boundary has a cost (serialize + write). For a long run you may snapshot
  only on the steps whose completion is expensive to lose, e.g. `if ev.name == "process_document"`.

## A concurrent fan-out that survives a restart

This is the case the page exists for: a workflow that fans out work, processes items concurrently,
and collects the results — checkpointed so a kill mid-run doesn't redo completed items.

```python
import asyncio, json, os
from typing import Annotated
from workflows import Workflow, Context, step
from workflows.events import Event, StartEvent, StopEvent, StepStateChanged, StepState
from workflows.resource import Resource


class WorkItem(Event):
    item_id: int

class WorkDone(Event):
    item_id: int
    result: str


# Heavy / non-serializable inputs come from a Resource (see next section), so
# they never land in the snapshot — they're re-created on resume.
def get_client() -> MyApiClient:
    return MyApiClient(...)


class EnrichBatch(Workflow):
    @step
    async def dispatch(self, ctx: Context, ev: StartEvent) -> WorkItem | None:
        await ctx.store.set("n", len(ev.item_ids))
        for item_id in ev.item_ids:
            ctx.send_event(WorkItem(item_id=item_id))

    @step(num_workers=8)
    async def enrich(
        self, ctx: Context, ev: WorkItem,
        client: Annotated[MyApiClient, Resource(get_client)],
    ) -> WorkDone:
        result = await client.enrich(ev.item_id)   # the expensive, repeatable work
        return WorkDone(item_id=ev.item_id, result=result)

    @step
    async def collect(self, ctx: Context, ev: WorkDone) -> StopEvent | None:
        n = await ctx.store.get("n")
        done = ctx.collect_events(ev, [WorkDone] * n)
        if done is None:
            return None
        return StopEvent(result={d.item_id: d.result for d in done})
```

Drive it with the checkpoint loop, and resume from the last snapshot if the process died:

```python
CKPT = "enrich-batch.json"

async def run_batch(item_ids):
    wf = EnrichBatch()
    if os.path.exists(CKPT):
        # Resume: completed items are already in the collect buffer and won't re-run.
        # Don't re-send the StartEvent — dispatch already ran in the previous process.
        handler = wf.run(ctx=Context.from_dict(wf, json.load(open(CKPT))))
    else:
        handler = wf.run(item_ids=item_ids)

    async for ev in handler.stream_events(expose_internal=True):
        if isinstance(ev, StepStateChanged) and ev.step_state == StepState.NOT_RUNNING \
           and ev.name == "enrich":
            json.dump(handler.ctx.to_dict(), open(CKPT, "w"))
    return await handler
```

When this is killed after, say, 60 of 100 items: the 60 finished `enrich` calls have their
`WorkDone` events sitting in the collect buffer, which is part of the snapshot. On resume, `dispatch`
does not re-run, the 60 done items are not re-enriched, and only the remaining `WorkItem` events —
still in the queue — are processed. The collect step fires once all 100 distinct `WorkDone`s are
present, across both runs, so the **final result is correct**.

What *does* get repeated is the in-flight work. At any instant up to `num_workers` `enrich`
executions are running but not yet complete; every one of those is rewound and re-enriched on
resume. So with `num_workers=8` you might re-run as many as 8 items — wasted compute, not wrong
output, and precisely why `enrich` must be safe to repeat. (If a step both completed *and* was
captured in the snapshot, it is not rewound — only genuinely in-flight executions are.)

## Keeping snapshots cheap and correct

The snapshot is only as cheap and reliable as what you put in events and state. Two rules:

**Heavy and non-serializable inputs belong in a [Resource](/python/llamaagents/workflows/resources),
not in events or state.** A `Resource` factory is resolved once, cached on the workflow, and is
**never serialized into the context** — on resume it's simply re-created by calling the factory
again. So API clients, model handles, large reference data, byte buffers: inject them as resources,
and let your events carry small identifiers (an id, a key, a plan) instead. This keeps the snapshot
small and keeps you clear of the serialization rule below.

**Everything on an in-flight event and in the state store must be serializable.** Snapshots use a
JSON serializer (Pydantic models included); if it hits a value it can't encode, `to_dict()` raises
and the *whole* snapshot fails — not just that field. So don't put raw bytes, open connections, or
arbitrary objects on events.

And the rule from the mental model, restated because it's the one that bites: **steps re-run on
resume if they were in flight, so make their side effects safe to repeat.**

## When to reach for DBOS instead

The checkpoint loop is best-effort durability you control: you choose when to snapshot, and resume
is a couple of lines. If you instead want **fully automatic, every-transition durability** — no
snapshot code, crash recovery that resumes exactly where it stopped — use the
[DBOS runtime](/python/llamaagents/workflows/dbos). It persists every step transition to a backing
database (SQLite by default), at the cost of running that database and runtime. Same workflow code;
different durability/effort trade-off.
