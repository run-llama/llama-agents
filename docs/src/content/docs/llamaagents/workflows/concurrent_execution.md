---
sidebar:
  order: 5
title: Concurrent execution of workflows
---

In addition to looping, branching, and streaming, workflows can run steps concurrently. This is useful when you have multiple steps that can be run independently of each other and they have time-consuming operations that they `await`, allowing other steps to run in parallel.

The common shape is fan-out / fan-in: split work into many pieces, run them concurrently, then join the results back together. The clearest way to express it is in the step signatures themselves. A step that returns `list[E]` fans out; a step that takes `events: list[E]` fans in. The `@step` decorator reads those annotations, so the validator and the [visualization](/python/llamaagents/workflows/drawing) connect the producer to its consumers. The dynamic alternative, `ctx.send_event`, returns `-> None` and emits through a side channel, so nothing in the signature links the producer to what it sends, and that step draws as a disconnected node. Prefer the typed form; reach for the dynamic API as an [escape hatch](#the-dynamic-api) when emission is conditional or open-ended.

## Fan-out: return a list

A step whose return type is `list[E]` emits each element as its own event. The five `Task`s below run concurrently under `work`:

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

The list is one batch. `work` is decorated with `num_workers=5`, which tells the workflow to run up to 5 instances of that step concurrently. Returning `[]` emits nothing and still completes the step; the batch closes immediately.

## Fan-in: take a list

A step whose parameter is `events: list[E]` collects the batch and fires **once**, with every event in it. There is no per-arrival re-entry and no counter to maintain:

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

The `fan_out` signature declares `-> list[Task]`, so the validator and the graph know it emits `Task`. The `join` signature declares `events: list[Done]`, so they know it consumes the whole `Done` batch and fires once. The cardinality lives in the type graph, not in `ctx.store`.

If a worker between the fan-out and the join drops its branch by returning `None`, the join still fires once, with the surviving subset:

```python
    @step(num_workers=5)
    async def work(self, ev: Task) -> Done | None:
        if ev.n % 2 == 0:
            return None  # drop this branch
        return Done(n=ev.n)
```

Here the batch tracks which branches are still alive and closes when the last one resolves, so `join` sees only the odd-numbered `Done`s.

## Releasing early

By default a `list[E]` join waits for the whole batch to close. To act on the first result instead of all of them, wrap the parameter in `Collect`. `Take(n)` fires on the *n*-th arrival with the first `n` events, for quorum or first-wins patterns:

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

`Take(1)` is the deliberate "stop after whichever query finishes first" behavior: the siblings that lose the race keep running (there is no cancellation), they just never reach the join. A bare `list[Done]` parameter is exactly `Annotated[list[Done], Collect(All())]`, and `Collect()` with no argument is an explicit, greppable synonym for that default.

## Heterogeneous fan-in

A batch does not have to be one event type. `list[A | B]` collects a flat batch of both, and every member type routes to the step:

```python
    @step
    async def join(
        self, events: list[StepACompleteEvent | StepBCompleteEvent]
    ) -> StopEvent:
        ...
```

For a join that waits for *one of each* distinct type rather than a list, give the step several single-event parameters. It fires once when one of each has arrived, each parameter bound to its event:

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

The `start` step fans out three distinct event types, and `assemble` declares one parameter per type, so it fires once when one of each has arrived. The visualization of this workflow is quite pleasing:

![A concurrent workflow](./assets/different_events.png)

## Nesting

Fan-out composes. A step that fans out inside a fan-out produces a nested batch; the inner join fires once per outer member, then the outer join fires once with all the inner results:

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

Each join sees only its own level: `per_inner` runs three times (once per outer `Task`), and `per_outer` runs once with the three summaries.

## The dynamic API

When the events to emit are not known from the step's shape, emit them yourself with `ctx.send_event` and collect them with `ctx.collect_events`. Use this when emission is conditional or open-ended, or when you want to emit incrementally: a `list` return is dispatched as one batch when the step returns, whereas `ctx.send_event` emits each event the moment you call it, so downstream steps can start before the producer finishes. To stream events out as you generate them, `ctx.send_event` is the way to do it.

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

Because `start` emits through a side channel, its return type is `StepTwoEvent | None` and the fan-out is not visible in the signature. To wait for several of these events before moving on, use `ctx.collect_events`:

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

`ctx.collect_events` lives on the `Context` and takes the event that triggered the step and an array of event types to wait for. `step_three` fires every time a `StepThreeEvent` is received, but `collect_events` returns `None` until all 3 have arrived, at which point it returns them as an array in the order they were received. You maintain the count of expected events yourself (here, `3`).

You can wait for any combination of event types, not just one repeated type. The order of the types passed to `collect_events` is the order the returned events come back in, regardless of when they arrived:

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
