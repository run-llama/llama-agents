---
sidebar:
  order: 15
title: Drawing a Workflow
---

Workflows can be visualized from the same type annotations the runtime uses for validation. That makes diagrams useful in two different moments: before a run, to see every possible path, and after a run, to see what actually happened.

There are two main ways to visualize a workflow.

## Generate diagram files

First install:

```bash
pip install llama-index-utils-workflow
```

Then import and use:

```python
from llama_index.utils.workflow import (
    draw_all_possible_flows,
    draw_all_possible_flows_mermaid,
    draw_most_recent_execution,
    draw_most_recent_execution_mermaid,
)

# Draw every statically possible path as HTML
draw_all_possible_flows(MyWorkflow, filename="all_paths.html")

# Draw the same static graph as Mermaid
mermaid = draw_all_possible_flows_mermaid(MyWorkflow)

# Draw one completed execution as HTML
w = MyWorkflow()
handler = w.run(topic="Pirates")
await handler
draw_most_recent_execution(handler, filename="most_recent.html")

# Draw the same completed execution as Mermaid
execution_mermaid = draw_most_recent_execution_mermaid(handler)
```

`draw_all_possible_flows` accepts either a workflow class or a workflow instance. The execution functions take a `WorkflowHandler`, so you need to run the workflow first.

Long event names can make diagrams hard to read. Pass `max_label_length=...` to truncate node labels:

```python
draw_all_possible_flows(MyWorkflow, filename="all_paths.html", max_label_length=24)
```

By default, static diagrams include child workflows. Pass `include_child_workflows=False` if you want only the parent workflow graph.

## Use the debugger UI

The [`WorkflowServer`](/python/llamaagents/workflows/deployment) serves a debugger UI at `/`. Use it when you want to run a workflow, inspect events, and send human-in-the-loop responses from a browser.

Using this server app, you can visualize and run your workflows:

![workflow debugger](./assets/ui_sample.png)

Setting up the server is straightforward:

```python
import asyncio
from workflows import Workflow, step
from workflows.events import StartEvent, StopEvent
from llama_agents.server import WorkflowServer


class MyWorkflow(Workflow):
    @step
    async def my_step(self, ev: StartEvent) -> StopEvent:
        return StopEvent(result="Done!")


async def main():
    server = WorkflowServer()
    server.add_workflow("my_workflow", MyWorkflow())
    await server.serve("0.0.0.0", 8080)

if __name__ == "__main__":
    asyncio.run(main())
```
