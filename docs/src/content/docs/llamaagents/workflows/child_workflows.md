---
sidebar:
  order: 8
title: Child workflows
---

As a workflow grows, you often find a chunk of it that stands on its own. A document pipeline has an extraction phase, a research agent has a single-question loop, a report builder has a section it runs once per heading. You could paste those steps into the parent, but then you maintain two copies and they drift. A child workflow lets you keep that piece as its own workflow and drop it into a bigger one as a single unit.

The child runs as part of the parent, but it keeps its own steps, its own state, and its own event routing. You can also run it on its own, unchanged. Nothing about being a child changes how the workflow behaves when you run it directly.

## Declaring a child

A parent marks a child field with `Annotated[Child, ChildWorkflow]`. Import `Annotated` from `typing` and `ChildWorkflow` from `workflows`. The field name is up to you, and the type is the child workflow class. A plain `Workflow` annotation is an ordinary field, not a child declaration:

```python
from typing import Annotated

from workflows import ChildWorkflow, Workflow, step
from workflows.events import StartEvent, StopEvent


class SummarizeStart(StartEvent):
    text: str = ""


class SummarizeStop(StopEvent):
    summary: str = ""


class Summarize(Workflow):
    @step
    async def run(self, ev: SummarizeStart) -> SummarizeStop:
        return SummarizeStop(summary=ev.text[:100])


class Report(Workflow):
    summarize: Annotated[Summarize, ChildWorkflow]

    @step
    async def start(self, ev: StartEvent) -> SummarizeStart:
        return SummarizeStart(text="a long document...")

    @step
    async def finish(self, ev: SummarizeStop) -> StopEvent:
        return StopEvent(result=ev.summary)
```

You pass the child in when you construct the parent:

```python
report = Report(summarize=Summarize())
result = await report.run()
```

The marked field lets `Report(summarize=Summarize())` pass the child to a generated constructor alongside base config arguments like `timeout`. If you would rather build the child yourself, write your own `__init__` and assign `self.summarize = Summarize()` after calling `super().__init__()`. Either way the child is wired in by the time construction finishes. A plain `Workflow` annotation can still describe a helper, but it does not create a child slot or constructor argument.

## The start and stop boundary

A child used inside a parent must define its own `StartEvent` subclass and `StopEvent` subclass. That is what makes it addressable. The parent starts the child by emitting the child's `StartEvent`, and the child hands control back by emitting its `StopEvent`, which the parent receives as an ordinary event.

In the example above, `start` returns a `SummarizeStart`, which runs the child. The child finishes with a `SummarizeStop`, and the parent's `finish` step picks it up like any other event. The child's stop does not end the run. Only the root workflow's `StopEvent` does that, so the parent stays in control of when the whole thing is done.

A child that uses the bare `StartEvent` or `StopEvent` is rejected when you construct the parent, because there would be no distinct event type to route to it.

Sibling children need distinct, unrelated `StartEvent` classes. If one child's start class inherits from another's, the parent rejects the pair because a single event could otherwise start both children. Parent steps can still consume the child's `StopEvent`.

## Nesting

A child can have children of its own. The boundary rule is the same at every level: each workflow defines its own start and stop events, the parent emits the start, and the stop comes back as a routable event one level up.

```python
top = Top(mid=Mid(grand=Grand()))
```

## Isolation

Each workflow in the tree runs in its own namespace, so a child and its parent stay out of each other's way in two places.

State is separate. A child's `ctx.store` is its own. Keys the parent writes are not visible inside the child, and keys the child writes do not leak back into the parent. The handler's store, after the run, holds only the root workflow's state.

Event routing stays within a namespace. If the same event type is used in both the parent and a child, the parent's copy routes to the parent's steps and the child's copy routes to the child's steps. They never cross.

```python
class SharedMid(Event):
    tag: str = ""

# The parent emits SharedMid(tag="parent") and the child emits
# SharedMid(tag="child"). Each one is only seen by steps in the workflow
# that produced it.
```

This is what lets you compose two workflows that were written separately without auditing them for event-name collisions.

## Streaming child events

By default, a parent's `stream_events()` shows only the parent's own streamed events. Events written from inside a child are hidden, so a consumer that already streams a parent keeps seeing the same thing after you add a child to it.

To see the child's events too, pass `include_children=True`. The child's events come through tagged with the namespace path of the child that produced them, which you can read with `get_event_origin_namespace`:

```python
from workflows.events import get_event_origin_namespace

handler = report.run()
async for ev in handler.stream_events(include_children=True):
    namespace = get_event_origin_namespace(ev)  # () for the parent, ("summarize",) for the child
    print(namespace, ev)
await handler
```

## Configuration

A `timeout` set on a child limits that child's known alive time. The runtime adds elapsed time when it processes journal ticks. Time while the workflow is stopped before a resume does not count, nor does the interval between the last processed tick and a crash. A child without an explicit timeout is bounded by its parent. Other run-level settings like `verbose` only apply to the workflow you actually run, so setting them on a nested child does nothing. Attaching a child that carries one of those dead settings emits a warning that names it, so it does not silently get ignored.
