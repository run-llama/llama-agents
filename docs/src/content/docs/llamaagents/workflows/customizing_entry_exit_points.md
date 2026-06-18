---
sidebar:
  order: 7
title: Custom start and stop events
---

Most workflows can use the default `StartEvent` and `StopEvent` from the [Getting Started](/python/llamaagents/workflows/) section. Define custom start and stop events when the workflow boundary itself has a useful schema: a typed request object at the beginning, or a typed result object at the end.

## Using a custom `StartEvent`

When you call `run()` with keyword arguments, Workflows builds the workflow's start event from those arguments. With the default `StartEvent`, any extra field is accepted:

```python
result = await workflow.run(topic="pirates")
```

That is convenient for small inputs. For production code, a custom `StartEvent` gives the entry point a real schema and lets Pydantic validate missing or malformed input before the first step runs.

Create a custom class that inherits from `StartEvent`:

```python
from workflows.events import StartEvent


class JokeStartEvent(StartEvent):
    topic: str
    tone: str = "funny"
```

Then use that event type in the step that starts the workflow:

```python
class JokeFlow(Workflow):
    @step
    async def generate_joke(self, ev: JokeStartEvent) -> JokeEvent:
        prompt = f"Write a {ev.tone} joke about {ev.topic}."
        response = await self.llm.acomplete(prompt)
        return JokeEvent(joke=str(response))
```

You can still pass the fields as keyword arguments:

```python
w = JokeFlow(timeout=60)
result = await w.run(topic="pirates", tone="dry")
```

For a larger input object, pass the event instance through `start_event`:

```python
start_event = JokeStartEvent(topic="pirates", tone="dry")
w = JokeFlow(timeout=60)
result = await w.run(start_event=start_event)
```

Use events for serializable data. If the workflow needs an LLM client, an index, a database connection, or a file handle, inject it as a [resource](/python/llamaagents/workflows/resources) instead of putting it on the start event. Start events can be serialized when you snapshot or serve workflows, and heavyweight runtime objects usually cannot.

## Using a custom `StopEvent`

The built-in `StopEvent` returns whatever you put in `result`:

```python
return StopEvent(result={"critique": critique, "score": score})
```

That is fine for quick workflows, but the result is typed as `Any`. A custom stop event makes the output shape explicit.

Create a subclass of `StopEvent`:

```python
from workflows.events import StopEvent


class JokeResult(StopEvent):
    joke: str
    critique: str
```

We can now replace `StopEvent` with `JokeResult` in our workflow:

```python
class JokeFlow(Workflow):
    ...

    @step
    async def critique_joke(self, ev: JokeEvent) -> JokeResult:
        prompt = f"Give a thorough analysis and critique of the following joke: {ev.joke}"
        response = await self.llm.acomplete(prompt)
        return JokeResult(joke=ev.joke, critique=str(response))
```

When a step returns the base `StopEvent`, `await workflow.run(...)` returns `stop_event.result`. When a step returns a custom `StopEvent` subclass, `await workflow.run(...)` returns the event instance:

```python
w = JokeFlow(timeout=60)
result = await w.run(topic="pirates")
print(result.joke)
print(result.critique)
```

That makes the result friendly to type checkers, editor autocomplete, and callers that introspect workflow schemas.
