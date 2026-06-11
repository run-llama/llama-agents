# llama-index-workflows

## 2.21.0

### Minor Changes

- 9a4dd16: Add optional `allowed_types` and `allow_unknown_types` parameters to `JsonSerializer` to support type allowlisting during deserialization
- db1258b: Support opt-in subclass event routing via the step decorator parameter `accept_event_subclasses`

## 2.20.0

### Minor Changes

- 9bf247a: Add replay_ticks_stream and ReplayResult to surface the last exit-indicating command from tick replay
- 2cc9fae: Add `@catch_error` handler (supports `for_steps=[...]` and `max_recoveries`) and `Context.retry_info()` for handling exhausted step retries inline. `retry_info().last_exception` and `StepFailedEvent.exception` are live Python exceptions.

## 2.19.1

### Patch Changes

- f7e037e: Stream ticks during resume so peak memory is bounded by batch size rather than total tick history.

## 2.19.0

### Minor Changes

- 2592c80: Add composable retry primitives (`retry_policy(retry=..., wait=..., stop=...)`, `retry_if_exception_type`, `wait_exponential`, `stop_after_attempt`, etc.).

## 2.18.0

### Minor Changes

- 43ff242: Add `Context.get_step_context()` static method to retrieve the step context without a `ctx` parameter in the step signature

## 2.17.3

### Patch Changes

- b8c7c7e: fix memory leak where asyncio timers could capture a Workflow reference via RunContext

## 2.17.2

### Patch Changes

- 7e06f87: Make retry jitter deterministic for journaled replay support

## 2.17.1

### Patch Changes

- 979d68b: Fix draw_all_possible_flows to accept workflow classes
- 983f6f6: feat: Enhance VerboseDecorator with tick-level logging

## 2.17.0

### Minor Changes

- 7fc1aae: feat: add graph structure validation (reachability, terminal events) with opt-out
- b32ec53: Drop python 3.9 support

## 2.16.1

### Patch Changes

- c7bbedb: Fix wait_for_event timeout not being enforced
- 703ec92: Internal support for post tick processed callbacks

## 2.16.0

### Minor Changes

- 5e7f9e5: Add event input/output summaries to step spans and rehydrate span context across serialization boundaries. Log instead of fail cancelled steps from cancelled workflows. Do not fail from wait_for_event exceptions.

### Patch Changes

- 9f26314: feat: add ExponentialBackoffRetryPolicy for retry steps

## 2.15.1

### Patch Changes

- 6ec262c: Reduce noisy errors during shutdown

## 2.15.0

### Minor Changes

- 3c22216: Make WorkflowTick serializable, and support switching workflow name and runtime before launch

### Patch Changes

- 77a3f9c: Add workflow release for idle DBOS workflows (with replica support)
- 707a254: Fix `Workflow(verbose=True)` being a no-op by adding a `VerboseDecorator` that intercepts `StepStateChanged` events to print step starts and completions
- 05f5f4e: Fix idle detection only working for wait_for_event, not for steps waiting on InputRequiredEvent
- 96e437e: Move task execution into the runtime, for maximal control of specific runtime semantics around determinism

## 2.15.0-rc.1

### Patch Changes

- 3720c61: Add workflow release for idle DBOS workflows (with replica support)
- a2aad32: Move task execution into the runtime, for maximal control of specific runtime semantics around determinism

## 2.15.0-rc.0

### Minor Changes

- b515a46: Make WorkflowTick serializable, and support switching workflow name and runtime before launch

### Patch Changes

- e981f73: Fix idle detection only working for wait_for_event, not for steps waiting on InputRequiredEvent
- 7433d4c: Add fix for double send when waiter event and accepted event match

## 2.14.2

### Patch Changes

- 3590913: Fix span tracking in observability tooling
- 7433d4c: Add fix for double send when waiter event and accepted event match

## 2.14.1

### Patch Changes

- 6ece797: Fix concurrent step cancellation regression where StopEvent no longer cancelled as quickly as previously

## 2.14.0

### Minor Changes

- 73c1254: refactor: expand runtime plugin architecture

  - Refactoring to better support alternate distributed backends
  - Some `Context` methods may now raise errors if used in an unexpected context
  - `WorkflowHandler` is no longer a future. Retains compatibility methods for main use cases (exception, cancel, etc)

- 45e7614: Replace InMemoryStateStore types with a corresponding StateStore protocol
- 2900f58: Support state type inheritance in workflows

### Patch Changes

- 45e7614: Refact: make control loop more deterministic

  - Switches out the asyncio delay mechanism for a pull-with-timeout that is more deterministic friendly
  - Adds a priority queue of delayed tasks
  - Switches out the misc firing /spawning of async tasks to a more rigorous pattern where tasks are only created in the main loop, and gathered in one location. This makes the concurrency more straightforward to reason about

- 6fdc45c: Update debugger assets

  - JavaScript: https://cdn.jsdelivr.net/npm/@llamaindex/workflow-debugger@0.2.15/dist/app.js
  - CSS: https://cdn.jsdelivr.net/npm/@llamaindex/workflow-debugger@0.2.15/dist/app.css

## 2.13.1

### Patch Changes

- e958ed2: fix: ResourceConfig was loading config file eagerly
- ebaf212: Add resource validation to workflow.validate()

## 2.13.0

### Minor Changes

- 6dd7fc0: Add resource config node support to workflow representation
- be19869: Add support for injecting resources more flexibly

  - Add support for injecting Resources recursively, so a Resource can depend on another Resource or ResourceConfig
  - Add support for injecting ResourceConfig directly into steps
  - Fix issues with resolving from String quoted types

## 2.12.2

### Patch Changes

- bfbfba4: Return an empty list for empty target events, rather than None
- 85f948e: fix: rebuild_state_from_ticks clears in_progress before replaying

  Fixed ctx.to_dict() failing with "Worker X not found in in_progress" when checkpointing resumed workflows. The function now also rewinds in progress when recreating from ticks, to match the actual behavior when resuming a workflow.

## 2.12.1

### Patch Changes

- 40be1c7: add workflow class name to WorkflowGraph representation

## 2.12.0

### Minor Changes

- e53c654: Add further detail to workflow graph, mainly adding `Resource` nodes to workflow graph and visualizations
- 2ff316d: Updates workflow server with functionality to drop and restore idle workflow handlers that are waiting on external input.
- 0d72b4d: reorganize workflow graph representation types
- f96faa2: Add dedicated StopEvent subclasses for workflow termination (timeout, cancellation, failure)

### Patch Changes

- 3b043b8: Track when workflows are idle (waiting on external input)
- 7a85c96: Add ResourceConfig for resource-level configuration injection

## 2.11.7

### Patch Changes

- 6c35e4d: Update debugger assets

  - JavaScript: https://cdn.jsdelivr.net/npm/@llamaindex/workflow-debugger@0.2.12/dist/app.js
  - CSS: https://cdn.jsdelivr.net/npm/@llamaindex/workflow-debugger@0.2.12/dist/app.css

- f58537a: Update debugger assets

  - JavaScript: https://cdn.jsdelivr.net/npm/@llamaindex/workflow-debugger@0.2.14/dist/app.js
  - CSS: https://cdn.jsdelivr.net/npm/@llamaindex/workflow-debugger@0.2.14/dist/app.css

## 2.11.6

### Patch Changes

- 94fa8ce: Fix infinite retries with no delay
- f8fa366: Update debugger assets

  - JavaScript: https://cdn.jsdelivr.net/npm/@llamaindex/workflow-debugger@0.2.10/dist/app.js
  - CSS: https://cdn.jsdelivr.net/npm/@llamaindex/workflow-debugger@0.2.10/dist/app.css

## 2.11.5

### Patch Changes

- 27a4cf0: Update debugger assets

  - JavaScript: https://cdn.jsdelivr.net/npm/@llamaindex/workflow-debugger@0.2.2/dist/app.js
  - CSS: https://cdn.jsdelivr.net/npm/@llamaindex/workflow-debugger@0.2.2/dist/app.css

## 2.11.4

### Patch Changes

- 95abac0: Update debugger assets

  - JavaScript: https://cdn.jsdelivr.net/npm/@llamaindex/workflow-debugger@0.2.0/dist/app.js
  - CSS: https://cdn.jsdelivr.net/npm/@llamaindex/workflow-debugger@0.2.0/dist/app.css

- 8f344bd: Fix resuming from serialized context for workflows that uses typed events

## 2.11.3

### Patch Changes

- f307253: Update typechecking to support ty
- 91159d7: Moving `_extract_workflow_structure` to its own module in workflow core
- 300fd05: Add stricter ruff formatting checks
- 32ae78a: Switch build backend to uv

## 2.11.2

### Patch Changes

- ee56c97: Fix remove task functionality on \_execute_task, specially when the task has gone missing
