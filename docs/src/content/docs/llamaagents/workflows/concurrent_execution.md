---
sidebar:
  order: 3
title: Concurrent execution of workflows
---

Workflows can run steps at the same time. When several steps are independent and each one waits on something slow, running them in parallel is faster.

The usual pattern is fan-out and fan-in. You split the work into pieces, run them at the same time, then join the results back together. You write this directly in the step signatures. Return a `list` from a step and it fans out, with one event per element. Take a `list` parameter and it fans in, firing once on the whole batch. The `@step` decorator reads those types. The validator and the [visualization](/python/llamaagents/workflows/drawing) then connect each producer step to the steps that consume its events, with no extra work from you. When you need to emit events that do not follow from the signature, you can send them yourself with `ctx.send_event`. The [dynamic API](#the-dynamic-api) at the end of this page covers that.

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

`fan_out` returns `list[Task]` and `join` takes `list[Done]`, so the framework knows the batch from the types alone and fires `join` once it's complete.

The events in the list are in completion order, which is the order each worker finished. This is not the order `fan_out` sent them. If you need a fixed order, sort the list yourself, as `join` does here with `sorted`.

A worker can drop its own branch by returning `None`. The framework tracks which branches still return a value, so `join` still fires once, with only those branches:

```python
    @step(num_workers=5)
    async def work(self, ev: Task) -> Done | None:
        if ev.n % 2 == 0:
            return None  # drop this branch
        return Done(n=ev.n)
```

Here the even-numbered tasks return `None`, so `join` receives only the odd `Done` events.

## Releasing early

By default a `list` join waits for the whole batch. To act on the first result instead, wrap the parameter in `Collect`. `Take(n)` fires on the n-th arrival with the first `n` events. This is what you want when you only need the first few results, or the first one to finish:

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

`Take(1)` stops after whichever task finishes first. The other tasks keep running. Nothing cancels them. They just never reach the join. A plain `list[Done]` parameter is the same as `Annotated[list[Done], Collect(All())]`. `Collect()` with no argument writes that same default in a way that is easier to search for in code.

## Fan-in with mixed event types

A batch does not have to be one event type. `list[A | B]` collects a flat batch of both types, and the step receives either one:

```python
    @step
    async def join(
        self, events: list[StepACompleteEvent | StepBCompleteEvent]
    ) -> StopEvent:
        ...
```

If you want to wait for one of each type instead of a list, give the step one parameter per event. The step fires once each parameter has received its event. Each parameter is matched by its type:

```python
from workflows import Workflow, step
from workflows.events import Event, StartEvent, StopEvent


class StepACompleteEvent(Event):
    result: str


class StepBCompleteEvent(Event):
    result: str


class StepCCompleteEvent(Event):
    result: str


class ConcurrentFlow(Workflow):
    @step
    async def step_a(self, ev: StartEvent) -> StepACompleteEvent:
        return StepACompleteEvent(result="Query 1")

    @step
    async def step_b(self, ev: StartEvent) -> StepBCompleteEvent:
        return StepBCompleteEvent(result="Query 2")

    @step
    async def step_c(self, ev: StartEvent) -> StepCCompleteEvent:
        return StepCCompleteEvent(result="Query 3")

    @step
    async def assemble(
        self,
        a: StepACompleteEvent,
        b: StepBCompleteEvent,
        c: StepCCompleteEvent,
    ) -> StopEvent:
        return StopEvent(result=[a.result, b.result, c.result])
```

`step_a`, `step_b`, and `step_c` all run from the same `StartEvent`, so they run at the same time. `assemble` takes one parameter for each, so it fires once all three have arrived.

## Nesting

You can nest fan-out. Fan out inside another fan-out and you get a nested batch. The inner join fires once for each outer element. Then the outer join fires once over all the inner results:

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

When the events you emit do not follow from the step's signature, send them yourself with `ctx.send_event` and collect them with `ctx.collect_events`. Use this when you only emit some events depending on a condition, when you do not know in advance how many events you will emit, or when you want to emit them one at a time instead of in one batch.

There is a trade-off here. A `list` return is all or nothing. It goes out as one batch when the step returns, so if the step raises an error first, nothing is emitted. `ctx.send_event` fires the moment you call it. Downstream steps can start before the producer step is done. But anything you already sent stays out even if the step later fails.

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

`start` emits the events with `ctx.send_event` instead of returning them, so its return type is `StepTwoEvent | None` and the fan-out never appears in the signature. The downside is that nothing links `start` to the events it sends, so it appears as a disconnected node in the diagram. To wait for several of these events before moving on, use `ctx.collect_events`:

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

`ctx.collect_events` takes the triggering event and a list of the types to wait for. `step_three` runs on every `StepThreeEvent`, but `collect_events` returns `None` until all three have arrived. Then it returns them as a list, in the order they arrived. You have to track the number of events to expect yourself, which is the `3` here.

You can wait for any mix of types, not just one type repeated. The order you pass them in is the order they come back in, no matter when each one arrived:

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
