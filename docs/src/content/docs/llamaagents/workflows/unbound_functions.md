---
sidebar:
  order: 10
title: Workflows from unbound functions
---

Most examples define steps as methods on a `Workflow` subclass. You can also attach standalone functions to a workflow class with `@step(workflow=...)`. This is useful when a workflow is assembled from reusable functions, or when an integration wants to add a step without subclassing the workflow.

First we create an empty class to hold the steps:

```python
from workflows import Workflow


class TestWorkflow(Workflow):
    pass
```

Now we can add steps to the workflow by defining functions and decorating them with the `@step()` decorator:

```python
from workflows import step
from workflows.events import StartEvent, StopEvent


@step(workflow=TestWorkflow)
async def some_step(ev: StartEvent) -> StopEvent:
    return StopEvent()
```

In this example, the decorator registers `some_step` on the `TestWorkflow` class. The function signature is the same as a method step, except there is no `self` parameter.

The registration happens at import time. Make sure the module that defines the function is imported before you instantiate or validate the workflow. Step names still have to be unique: registering a free function with the same name as an existing method or free-function step raises a validation error.
