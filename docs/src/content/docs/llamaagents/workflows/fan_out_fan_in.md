---
sidebar:
  order: 6
title: Fan-out and fan-in
---

A common shape: split work into many pieces, run them concurrently, then join the results back together. You can build this with [`ctx.send_event` and `ctx.collect_events`](/python/llamaagents/workflows/concurrent_execution), but those are dynamic. A step that fans out returns `-> None` and emits through a side channel, so its signature says nothing about what it sends, and the join threads its own cardinality through `ctx.store` to know when it's done.

The typed form puts the whole shape in the step signatures instead. A step that returns `list[E]` fans out. A step that takes `events: list[E]` fans in. The validator and the [workflow visualization](/python/llamaagents/workflows/drawing) read those annotations, so the graph shows what each step actually produces and consumes. No `-> None`, no manual counter.

## Fan-out: return a list

A step whose return type is `list[E]` emits each element as its own event. The five `Task`s below run concurrently under `work`, exactly as if you had called `ctx.send_event` five times, but the return annotation declares it:

```python
from workflows import Workflow, step
from workflows.events import Event, StartEvent, StopEvent


class Task(Event):
    n: int


class Done(Event):
    n: int


class FanOut(Workflow):
    @step
    async def fan_out(self, ev: StartEvent) -> list[Task]:
        return [Task(n=i) for i in range(5)]

    @step(num_workers=5)
    async def work(self, ev: Task) -> Done:
        return Done(n=ev.n)

    @step
    async def join(self, events: list[Done]) -> StopEvent:
        return StopEvent(result=sorted(e.n for e in events))
```

The list is one batch. Returning `[]` emits nothing and still completes the step; the batch closes immediately and the join fires once with an empty list.

## Fan-in: take a list

A step whose parameter is `events: list[E]` collects the batch and fires **once**, with every event in it. There is no per-arrival re-entry and no `None` sentinel to return while you wait:

```python
    @step
    async def join(self, events: list[Done]) -> StopEvent:
        return StopEvent(result=sorted(e.n for e in events))
```

If a worker between the fan-out and the join drops its branch (returns `None`), the join still fires once, with the surviving subset. The batch tracks which branches are still alive and closes when the last one resolves.

## Before and after

The same fan-out/fan-in with the dynamic API, then the typed one.

**Before** — fan-out is invisible in the signature (`-> None`), and the join counts arrivals by hand:

```python
from workflows import Context


class Concurrent(Workflow):
    @step
    async def fan_out(self, ctx: Context, ev: StartEvent) -> None:
        tasks = [Task(n=i) for i in range(5)]
        ctx.store.set("expected", len(tasks))
        for t in tasks:
            ctx.send_event(t)

    @step(num_workers=5)
    async def work(self, ev: Task) -> Done:
        return Done(n=ev.n)

    @step
    async def join(self, ctx: Context, ev: Done) -> StopEvent | None:
        expected = await ctx.store.get("expected")
        results = ctx.collect_events(ev, [Done] * expected)
        if results is None:
            return None
        return StopEvent(result=sorted(e.n for e in results))
```

**After** — the signatures carry the cardinality, so there is no `expected` counter, no `collect_events`, and no `None` sentinel:

```python
class Concurrent(Workflow):
    @step
    async def fan_out(self, ev: StartEvent) -> list[Task]:
        return [Task(n=i) for i in range(5)]

    @step(num_workers=5)
    async def work(self, ev: Task) -> Done:
        return Done(n=ev.n)

    @step
    async def join(self, events: list[Done]) -> StopEvent:
        return StopEvent(result=sorted(e.n for e in events))
```

`fan_out` now declares `-> list[Task]`, so the validator and the visualization know it emits `Task`. `join` declares `events: list[Done]`, so they know it consumes the whole `Done` batch. The control flow is in the type graph, not threaded through `ctx.store`.

## Releasing early

By default a `list[E]` join waits for the batch to close. Wrap the parameter in `Collect` to release sooner. `Take(n)` fires on the *n*-th arrival with the first `n` events, for quorum or first-wins patterns:

```python
from typing import Annotated
from workflows.collect import Collect, Take


class FastestWins(Workflow):
    @step
    async def fan_out(self, ev: StartEvent) -> list[Task]:
        return [Task(n=i) for i in range(5)]

    @step(num_workers=5)
    async def work(self, ev: Task) -> Done:
        return Done(n=ev.n)

    @step
    async def first(
        self, events: Annotated[list[Done], Collect(Take(1))]
    ) -> StopEvent:
        return StopEvent(result=events[0].n)
```

The siblings that lose the race keep running (there is no cancellation), they just never reach the join. A bare `list[Done]` parameter is exactly `Annotated[list[Done], Collect(All())]`, and `Collect()` with no argument is an explicit, greppable synonym for that default.

## Heterogeneous fan-in

A batch does not have to be one event type. `list[A | B]` collects a flat batch of both, and every member routes to the step:

```python
    @step
    async def join(self, events: list[Header | Footer]) -> StopEvent:
        ...
```

For a join that waits for *one of each* distinct type rather than a list, give the step several single-event parameters. It fires once when one of each has arrived, each parameter bound to its event:

```python
    @step
    async def assemble(self, h: Header, b: Body, f: Footer) -> StopEvent:
        ...
```

## Nesting

Fan-out composes. A step that fans out inside a fan-out produces a nested batch; the inner join fires once per outer member, then the outer join fires once with all the inner results:

```python
class Nested(Workflow):
    @step
    async def outer(self, ev: StartEvent) -> list[Task]:
        return [Task(n=o) for o in range(3)]

    @step
    async def inner(self, ev: Task) -> list[InnerTask]:
        return [InnerTask(outer=ev.n, inner=i) for i in range(2)]

    @step
    async def inner_work(self, ev: InnerTask) -> InnerDone:
        return InnerDone(outer=ev.outer, inner=ev.inner)

    @step
    async def per_inner(self, events: list[InnerDone]) -> InnerSummary:
        return InnerSummary(outer=events[0].outer, total=len(events))

    @step
    async def per_outer(self, events: list[InnerSummary]) -> StopEvent:
        return StopEvent(result=sorted((s.outer, s.total) for s in events))
```

Each join sees only its own level: `per_inner` runs three times (once per outer `Task`), and `per_outer` runs once with the three summaries.

## Notes

- Fan-out is **atomic**: the producer builds the whole `list[E]` and the batch is dispatched when the step returns. Streaming producers that emit elements one at a time (`AsyncIterator[E]`) are a planned follow-up and are rejected at decoration today. To stream events as you go, keep using `ctx.send_event`.
- The dynamic API still works. `ctx.send_event` and `ctx.collect_events` are unchanged, and the two styles interoperate. Reach for the typed `list[E]` form when the cardinality is known from the step's shape; reach for the dynamic API when emissions are conditional or open-ended.
