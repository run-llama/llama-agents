---
sidebar:
  order: 5
title: Concurrent execution of workflows
---

Workflows can run steps concurrently. When several steps are independent and spend their time `await`ing something slow, running them in parallel instead of one after another is a big win.

The usual shape is fan-out / fan-in: split the work into pieces, run them at once, then join the results back together. You write it directly in the step signatures. Return a `list` from a step and it fans out, one event per element. Take a `list` parameter and it fans in, firing once on the whole batch. The `@step` decorator reads those types, so the validator and the [visualization](/python/llamaagents/workflows/drawing) wire the producer to its consumers for free. When you need to emit events that don't follow from the signature, drop to `ctx.send_event`, covered as an [escape hatch](#the-dynamic-api) at the end.

## Fan-out: return a list

Return a `list` from a step and each element fires as its own event. Here five `Task`s run concurrently under `work`:

```python
import asyncio
import random
from workflows import Workflow, step
from workflows.events import Event, StartEvent, StopEvent


class Task(Event):
    n: int


class Done(Event):
    n: int


class ParallelFlow(Workflow):
    @step
    async def fan_out(self, ev: StartEvent) -> list[Task]:
        return [Task(n=i) for i in range(5)]

    @step(num_workers=5)
    async def work(self, ev: Task) -> Done:
        await asyncio.sleep(random.randint(0, 5))
        return Done(n=ev.n)
```

The whole list is one batch. `num_workers=5` lets up to five copies of `work` run at once. Return `[]` and nothing fires, but the step still completes and the batch closes right away.

## Fan-in: take a list

Take a `list` parameter instead of a single event and the step collects the whole batch, then fires **once** with everything in it:

```python
class ConcurrentFlow(Workflow):
    @step
    async def fan_out(self, ev: StartEvent) -> list[Task]:
        return [Task(n=i) for i in range(5)]

    @step(num_workers=5)
    async def work(self, ev: Task) -> Done:
        await asyncio.sleep(random.randint(1, 5))
        return Done(n=ev.n)

    @step
    async def join(self, events: list[Done]) -> StopEvent:
        return StopEvent(result=sorted(e.n for e in events))
```

`fan_out` returns `list[Task]` and `join` takes `list[Done]`, so the framework knows the batch from the types alone. No counter in `ctx.store`, no re-running `join` on every arrival; it fires once when the batch is done.

A worker can drop its own branch by returning `None`. The batch tracks which branches are still alive, so `join` still fires once, just with the survivors:

```python
    @step(num_workers=5)
    async def work(self, ev: Task) -> Done | None:
        if ev.n % 2 == 0:
            return None  # drop this branch
        return Done(n=ev.n)
```

Here the even-numbered tasks bow out and `join` sees only the odd `Done`s.

## Releasing early

By default a `list` join waits for the whole batch. To act on the first result instead, wrap the parameter in `Collect`. `Take(n)` fires on the *n*-th arrival with the first `n` events, which is what you want for quorum or first-wins:

```python
from typing import Annotated
from workflows.collect import Collect, Take


class FastestWins(Workflow):
    @step
    async def fan_out(self, ev: StartEvent) -> list[Task]:
        return [Task(n=i) for i in range(5)]

    @step(num_workers=5)
    async def work(self, ev: Task) -> Done:
        await asyncio.sleep(random.randint(1, 5))
        return Done(n=ev.n)

    @step
    async def first(
        self, events: Annotated[list[Done], Collect(Take(1))]
    ) -> StopEvent:
        return StopEvent(result=events[0].n)
```

`Take(1)` stops after whichever task finishes first. The losers keep running, there's no cancellation, they just never reach the join. A plain `list[Done]` parameter is the same as `Annotated[list[Done], Collect(All())]`, and `Collect()` with no argument is a more greppable way to spell that default.

## Heterogeneous fan-in

A batch doesn't have to be one event type. `list[A | B]` collects a flat batch of both, and either type routes to the step:

```python
    @step
    async def join(
        self, events: list[StepACompleteEvent | StepBCompleteEvent]
    ) -> StopEvent:
        ...
```

If you'd rather wait for *one of each* type than a list, give the step one parameter per event. It fires once when each parameter has its event, bound by type:

```python
import asyncio
from workflows import Workflow, step
from workflows.events import Event, StartEvent, StopEvent


class StepAEvent(Event):
    query: str


class StepBEvent(Event):
    query: str


class StepCEvent(Event):
    query: str


class StepACompleteEvent(Event):
    result: str


class StepBCompleteEvent(Event):
    result: str


class StepCCompleteEvent(Event):
    result: str


class ConcurrentFlow(Workflow):
    @step
    async def start(
        self, ev: StartEvent
    ) -> list[StepAEvent | StepBEvent | StepCEvent]:
        return [
            StepAEvent(query="Query 1"),
            StepBEvent(query="Query 2"),
            StepCEvent(query="Query 3"),
        ]

    @step
    async def step_a(self, ev: StepAEvent) -> StepACompleteEvent:
        return StepACompleteEvent(result=ev.query)

    @step
    async def step_b(self, ev: StepBEvent) -> StepBCompleteEvent:
        return StepBCompleteEvent(result=ev.query)

    @step
    async def step_c(self, ev: StepCEvent) -> StepCCompleteEvent:
        return StepCCompleteEvent(result=ev.query)

    @step
    async def assemble(
        self,
        a: StepACompleteEvent,
        b: StepBCompleteEvent,
        c: StepCCompleteEvent,
    ) -> StopEvent:
        return StopEvent(result=[a.result, b.result, c.result])
```

`start` fans out three distinct events and `assemble` takes one parameter each, so it fires once all three have landed. This one draws nicely:

![A concurrent workflow](./assets/different_events.png)

## Nesting

Fan-out composes. Fan out inside a fan-out and you get a nested batch: the inner join fires once per outer member, then the outer join fires once over all the inner results:

```python
class InnerTask(Event):
    outer: int
    inner: int


class InnerDone(Event):
    outer: int
    inner: int


class InnerSummary(Event):
    outer: int
    total: int


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

Each join stays at its own level. `per_inner` runs three times, once per outer `Task`, and `per_outer` runs once with the three summaries.

## The dynamic API

When the events you emit don't follow from the step's shape, send them yourself with `ctx.send_event` and collect them with `ctx.collect_events`. Reach for this when emission is conditional or open-ended, or when you want to emit one event at a time: a `list` return goes out as a single batch when the step returns, while `ctx.send_event` fires the moment you call it, so downstream steps can start before the producer is done.

`ctx.send_event` emits one event at a time:

```python
import asyncio
import random
from workflows import Workflow, Context, step
from workflows.events import Event, StartEvent, StopEvent


class StepTwoEvent(Event):
    query: str


class ParallelFlow(Workflow):
    @step
    async def start(self, ctx: Context, ev: StartEvent) -> StepTwoEvent | None:
        ctx.send_event(StepTwoEvent(query="Query 1"))
        ctx.send_event(StepTwoEvent(query="Query 2"))
        ctx.send_event(StepTwoEvent(query="Query 3"))

    @step(num_workers=4)
    async def step_two(self, ev: StepTwoEvent) -> StopEvent:
        print("Running slow query ", ev.query)
        await asyncio.sleep(random.randint(0, 5))
        return StopEvent(result=ev.query)
```

`start` emits through a side channel, so its return type is `StepTwoEvent | None` and the fan-out never shows up in the signature. The flip side: nothing links `start` to what it sends, so it draws as a disconnected node. To wait on several of these events before moving on, use `ctx.collect_events`:

```python
import asyncio
import random
from workflows import Workflow, Context, step
from workflows.events import Event, StartEvent, StopEvent


class StepTwoEvent(Event):
    query: str


class StepThreeEvent(Event):
    result: str


class ConcurrentFlow(Workflow):
    @step
    async def start(self, ctx: Context, ev: StartEvent) -> StepTwoEvent | None:
        ctx.send_event(StepTwoEvent(query="Query 1"))
        ctx.send_event(StepTwoEvent(query="Query 2"))
        ctx.send_event(StepTwoEvent(query="Query 3"))

    @step(num_workers=4)
    async def step_two(self, ctx: Context, ev: StepTwoEvent) -> StepThreeEvent:
        print("Running query ", ev.query)
        await asyncio.sleep(random.randint(1, 5))
        return StepThreeEvent(result=ev.query)

    @step
    async def step_three(
        self, ctx: Context, ev: StepThreeEvent
    ) -> StopEvent | None:
        # wait until we receive 3 events
        result = ctx.collect_events(ev, [StepThreeEvent] * 3)
        if result is None:
            return None

        # do something with all 3 results together
        print(result)
        return StopEvent(result="Done")
```

`ctx.collect_events` takes the triggering event and a list of types to wait for. `step_three` runs on every `StepThreeEvent`, but `collect_events` returns `None` until all three have arrived, then hands them back as a list in the order they came in. The count of expected events is on you to track, here the `3`.

You can wait on any mix of types, not just one repeated. The order you pass them is the order they come back, no matter when each arrived:

```python
    @step
    async def step_three(
        self,
        ctx: Context,
        ev: StepACompleteEvent | StepBCompleteEvent | StepCCompleteEvent,
    ) -> StopEvent | None:
        if (
            ctx.collect_events(
                ev,
                [StepCCompleteEvent, StepACompleteEvent, StepBCompleteEvent],
            )
            is None
        ):
            return None
        return StopEvent(result="Done")
```
