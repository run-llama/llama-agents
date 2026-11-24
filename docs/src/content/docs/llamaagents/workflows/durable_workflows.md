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

## Resume a runbook review

An incident workflow can search a shared runbook collection in
[Loom](https://github.com/ClickHouse/loom), then wait for an operator to confirm the
result. Keep connection settings in a `Resource` and put only the source URI in
the workflow's state. The approval step reads that URI after a restart.

Install `httpx` and `llama-index-workflows`. Set `LOOM_BASE_URL`, `LOOM_ORG`,
`LOOM_NAMESPACE`, and `LOOM_TOKEN` in the worker's environment. Use a viewer token
for the runbook namespace, and use HTTPS for a remote deployment.

Add a runbook with a known source URI to the namespace before starting the
example. Include the word `rollback` in its text to match the query below. The
approval prompt shows the source URI of the first matching runbook.

```python
import os
from dataclasses import dataclass, field
from typing import Annotated

import httpx
from workflows import Context, Workflow, step
from workflows.events import (
    HumanResponseEvent,
    InputRequiredEvent,
    StartEvent,
    StopEvent,
)
from workflows.resource import Resource


@dataclass(frozen=True)
class LoomConfig:
    base_url: str
    org: str
    namespace: str
    token: str = field(repr=False)


def loom_config() -> LoomConfig:
    return LoomConfig(
        base_url=os.environ["LOOM_BASE_URL"].rstrip("/"),
        org=os.environ["LOOM_ORG"],
        namespace=os.environ["LOOM_NAMESPACE"],
        token=os.environ["LOOM_TOKEN"],
    )


class SearchRequest(StartEvent):
    query: str


class RunbookReview(Workflow):
    @step
    async def find(
        self,
        ctx: Context,
        ev: SearchRequest,
        config: Annotated[LoomConfig, Resource(loom_config)],
    ) -> InputRequiredEvent | StopEvent:
        async with httpx.AsyncClient(timeout=10) as client:
            response = await client.post(
                f"{config.base_url}/v1/knowledge.search",
                headers={"Authorization": f"Bearer {config.token}"},
                json={
                    "org": config.org,
                    "namespace": config.namespace,
                    "namespace_scope": "this",
                    "query": ev.query,
                    "top_k": 1,
                    "search_mode": "lexical",
                    "rerank": "none",
                },
            )
            response.raise_for_status()

        hits = response.json()["results"]
        if not hits:
            return StopEvent(result=None)
        source_uri = hits[0]["source_uri"]
        await ctx.store.set("source_uri", source_uri)
        return InputRequiredEvent(prefix=f"Use this runbook? {source_uri}")

    @step
    async def review(self, ctx: Context, ev: HumanResponseEvent) -> StopEvent:
        source_uri = await ctx.store.get("source_uri")
        return StopEvent(result=source_uri if ev.response == "approve" else None)
```

Start the workflow with `RunbookReview().run(start_event=SearchRequest(query="rollback"))`.
Snapshot and cancel the run when it emits the `InputRequiredEvent`, then resume it
and send `HumanResponseEvent(response="approve")` using the
[human-in-the-loop handoff](/python/llamaagents/workflows/human_in_the_loop).
Protect snapshots as run data: they can contain the query and source URI.

## Runtime plugins for durability

The checkpoint loop is durability you control. You decide when to snapshot, and resume is a couple of
lines. If you'd rather not manage that, use a runtime plugin that owns persistence and recovery.

The default runtime runs workflows in-process. A plugin runtime can swap in a different execution
model without changing the workflow code. For example, the
[DBOS runtime](/python/llamaagents/workflows/dbos) journals step transitions to a database, so a
crashed workflow can resume without your checkpoint loop. `WorkflowServer` can run workflows with the
same runtime plugin, so served workflows get the same persistence and recovery behavior.
