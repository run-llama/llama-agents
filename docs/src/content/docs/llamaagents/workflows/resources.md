---
sidebar:
  order: 9
title: Resource Objects
---

Resources are external dependencies you can inject into the steps of a workflow.

Use resources for objects that should be created by the runtime instead of passed around as events or stored in `ctx.store`: LLM clients, retrievers, indexes, database handles, model configuration, and other dependencies that are expensive, stateful, or not JSON-serializable.

As a simple example, look at `Memory` from LlamaIndex in the following workflow:

```python
from typing import Annotated

from workflows import Workflow, step
from workflows.events import Event, StartEvent, StopEvent
from workflows.resource import Resource
from llama_index.core.llms import ChatMessage
from llama_index.core.memory import Memory


def get_memory() -> Memory:
    return Memory.from_defaults("user_id_123", token_limit=60000)


class SecondEvent(Event):
    msg: str


class WorkflowWithResource(Workflow):
    @step
    async def first_step(
        self,
        ev: StartEvent,
        memory: Annotated[Memory, Resource(get_memory)],
    ) -> SecondEvent:
        print("Memory before step 1", memory)
        await memory.aput(
            ChatMessage(role="user", content="This is the first step")
        )
        print("Memory after step 1", memory)
        return SecondEvent(msg="This is an input for step 2")

    @step
    async def second_step(
        self, ev: SecondEvent, memory: Annotated[Memory, Resource(get_memory)]
    ) -> StopEvent:
        print("Memory before step 2", memory)
        await memory.aput(ChatMessage(role="user", content=ev.msg))
        print("Memory after step 2", memory)
        return StopEvent(result="Messages put into memory")
```

To inject a resource, add a parameter to the step signature, wrap the type in `Annotated`, and pass a factory to `Resource()`:

```python
memory: Annotated[Memory, Resource(get_memory)]
```

The factory return type should match the annotated parameter type. By default, the resource is cached for the workflow run, so both steps receive the same `Memory` object and the factory is called once. If a step needs a fresh object each time, pass `cache=False`:

```python
memory: Annotated[Memory, Resource(get_memory, cache=False)]
```

Factories can be synchronous or async.

## Config-backed Resources

For configuration data stored in JSON files, use `ResourceConfig` instead of `Resource`. It loads the JSON file and parses it into a Pydantic model.

```python
from typing import Annotated
from pydantic import BaseModel
from workflows import Workflow, step
from workflows.events import StartEvent, StopEvent
from workflows.resource import ResourceConfig


class ClassifierConfig(BaseModel):
    categories: list[str]
    threshold: float


class DocumentClassifier(Workflow):
    @step
    async def classify(
        self,
        ev: StartEvent,
        config: Annotated[
            ClassifierConfig,
            ResourceConfig(config_file="classifier.json"),
        ],
    ) -> StopEvent:
        # config is loaded from classifier.json and validated as ClassifierConfig
        return StopEvent(result=f"Using threshold: {config.threshold}")
```

### Parameters

- `config_file`: Path to the JSON file containing the configuration.
- `path_selector`: Optional "."-delimited JSON path to extract a nested value from the JSON file (for example, `"settings.classifier"`).
- `label`: Optional display name for workflow visualizations.
- `description`: Optional description for workflow visualizations.

### Selecting nested values

If your JSON file contains multiple configs, use `path_selector` to extract a specific section:

```python
# Given config.json: {"classifier": {"categories": [...], "threshold": 0.8}, "other": {...}}
config: Annotated[
    ClassifierConfig,
    ResourceConfig(config_file="config.json", path_selector="classifier"),
]
```

### Labels and descriptions in visualizations

When viewing workflows in the debugger or other visualization tools, `label` and `description` help identify configs:

```python
config: Annotated[
    ClassifierConfig,
    ResourceConfig(
        config_file="classifier.json",
        label="Document Classifier",
        description="Categories and confidence threshold for classification",
    ),
]
```

If no label is provided, the Pydantic model's type name is used (e.g., "ClassifierConfig").

## Chaining Resources

Resources and ResourceConfigs can be chained together. A `Resource` factory function can declare dependencies on other resources using the same `Annotated` pattern:

```python
from typing import Annotated
from pydantic import BaseModel
from workflows import Workflow, step
from workflows.events import StartEvent, StopEvent
from workflows.resource import Resource, ResourceConfig
from llama_index.llms.anthropic import Anthropic


class LLMConfig(BaseModel):
    model: str
    temperature: float
    max_tokens: int


def get_llm(
    config: Annotated[LLMConfig, ResourceConfig(config_file="llm.json")],
) -> Anthropic:
    return Anthropic(
        model=config.model,
        temperature=config.temperature,
        max_tokens=config.max_tokens,
    )


class MyWorkflow(Workflow):
    @step
    async def generate(
        self,
        ev: StartEvent,
        llm: Annotated[Anthropic, Resource(get_llm)],
    ) -> StopEvent:
        response = await llm.acomplete(ev.input)
        return StopEvent(result=response.text)
```

The dependency chain is resolved automatically. In this example, when the workflow runs:
1. `llm.json` is loaded and parsed into `LLMConfig`
2. `get_llm` is called with that config to create the LLM client
3. The resulting client is passed to the step

This pattern works with any combination of `Resource` and `ResourceConfig` dependencies.
