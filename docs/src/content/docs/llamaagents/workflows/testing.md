---
sidebar:
  order: 16
title: Testing Workflows
---

Workflows are easiest to test as event-driven systems. Run the workflow, collect the events it streamed, assert the final result, and inspect the context state when state matters.

The `workflows.testing` module includes a small runner for this:

```python
from workflows.testing import WorkflowTestRunner
```

## End-to-end tests

`WorkflowTestRunner` starts the workflow, drains its event stream, awaits the final result, and returns everything in one object:

```python
import pytest

from workflows import Context, Workflow, step
from workflows.events import Event, StartEvent, StopEvent
from workflows.testing import WorkflowTestRunner


class Progress(Event):
    message: str


class Done(Event):
    value: str


class ExampleWorkflow(Workflow):
    @step
    async def start(self, ctx: Context, ev: StartEvent) -> Done:
        ctx.write_event_to_stream(Progress(message="started"))
        return Done(value=ev.topic.upper())

    @step
    async def finish(self, ev: Done) -> StopEvent:
        return StopEvent(result=ev.value)


@pytest.mark.asyncio
async def test_workflow_streams_progress_and_returns_result() -> None:
    result = await WorkflowTestRunner(ExampleWorkflow()).run(
        start_event=StartEvent(topic="docs")
    )

    assert result.result == "DOCS"
    assert result.event_types[Progress] == 1
    assert any(
        isinstance(ev, Progress) and ev.message == "started"
        for ev in result.collected
    )
```

The returned object has:

| Field | Meaning |
|---|---|
| `result` | The final value returned by awaiting the workflow handler. |
| `collected` | Every streamed event that was not excluded. |
| `event_types` | A count of collected events by event class. |
| `ctx` | The final `Context`, useful for state assertions or snapshots. |

## Internal events

By default the runner exposes internal events, including `StepStateChanged`. That is useful when you want to assert execution shape:

```python
from workflows.events import StepStateChanged


@pytest.mark.asyncio
async def test_step_state_events_are_emitted() -> None:
    result = await WorkflowTestRunner(ExampleWorkflow()).run(
        start_event=StartEvent(topic="docs")
    )

    assert result.event_types[StepStateChanged] > 0
```

If a test only cares about user events, turn internal events off:

```python
result = await WorkflowTestRunner(ExampleWorkflow()).run(
    start_event=StartEvent(topic="docs"),
    expose_internal=False,
)
```

Or keep internal events available but exclude the noisy ones from the collected list:

```python
result = await WorkflowTestRunner(ExampleWorkflow()).run(
    start_event=StartEvent(topic="docs"),
    exclude_events=[StepStateChanged]
)
```

## State assertions

Use the returned context when a workflow writes to `ctx.store`:

```python
from pydantic import BaseModel, Field


class CounterState(BaseModel):
    count: int = Field(default=0)


class CounterWorkflow(Workflow):
    @step
    async def count(self, ctx: Context[CounterState], ev: StartEvent) -> StopEvent:
        async with ctx.store.edit_state() as state:
            state.count += 1
        return StopEvent(result="done")


@pytest.mark.asyncio
async def test_workflow_updates_state() -> None:
    result = await WorkflowTestRunner(CounterWorkflow()).run()

    state = await result.ctx.store.get_state()
    assert state.count == 1
```

For durable workflow code, prefer asserting behavior after a real snapshot and restore:

```python
workflow = CounterWorkflow()
first = await WorkflowTestRunner(workflow).run()

ctx_dict = first.ctx.to_dict()
restored_workflow = CounterWorkflow()
restored = Context.from_dict(restored_workflow, ctx_dict)

resumed = await WorkflowTestRunner(restored_workflow).run(ctx=restored)
```

That catches the mistakes unit tests often miss: state that cannot serialize, events that are not importable when restored, and side effects that are not safe to repeat.
