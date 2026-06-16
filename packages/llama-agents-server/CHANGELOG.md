# llama-agents-server

## 0.6.1

### Patch Changes

- 71a1aac: Fix startup resume incorrectly failing newly-created handlers before their first persisted tick
- Updated dependencies [34e166c]
- Updated dependencies [cb89120]
- Updated dependencies [fd223e8]
- Updated dependencies [5724404]
- Updated dependencies [58e0174]
- Updated dependencies [aee5fda]
  - llama-index-workflows@2.22.0
  - llama-agents-client@0.3.9

## 0.6.0

### Minor Changes

- 070fc70: Decode workflow state by payload shape instead of persisted type metadata, and make state-store runtime handoff explicit.

### Patch Changes

- 41e354a: Seed retry jitter with the run id during snapshot tick replay so rebuilt snapshots match the live run, and consume old-format delayed-retry journal entries instead of duplicating them
- 5fd64dc: Update debugger assets

  - JavaScript: https://cdn.jsdelivr.net/npm/@llamaindex/workflow-debugger@0.2.40/dist/app.js
  - CSS: https://cdn.jsdelivr.net/npm/@llamaindex/workflow-debugger@0.2.40/dist/app.css

- Updated dependencies [9a4dd16]
- Updated dependencies [db1258b]
- Updated dependencies [070fc70]
- Updated dependencies [41e354a]
- Updated dependencies [41e354a]
- Updated dependencies [070fc70]
  - llama-index-workflows@2.21.0
  - llama-agents-client@0.3.8

## 0.5.0

### Minor Changes

- 95d8c2b: Share a single asyncpg pool across DBOSRuntime, PostgresWorkflowStore, and ExecutorLeaseManager instead of each opening their own. Pool size is configurable via `DBOSRuntimeConfig.pool_size`. Also adds LISTEN reconnect with backoff to PostgresWorkflowStore.

## 0.4.7

### Patch Changes

- 9bf247a: Classify zombie handlers on resume and collapse duplicate handler rows on write.
- Updated dependencies [9bf247a]
- Updated dependencies [2cc9fae]
  - llama-index-workflows@2.20.0
  - llama-agents-client@0.3.7

## 0.4.6

### Patch Changes

- f7e037e: Stream ticks during resume so peak memory is bounded by batch size rather than total tick history.
- 60cd349: Update debugger assets

  - JavaScript: https://cdn.jsdelivr.net/npm/@llamaindex/workflow-debugger@0.2.39/dist/app.js
  - CSS: https://cdn.jsdelivr.net/npm/@llamaindex/workflow-debugger@0.2.39/dist/app.css

- Updated dependencies [f7e037e]
  - llama-index-workflows@2.19.1
  - llama-agents-client@0.3.6

## 0.4.5

### Patch Changes

- Updated dependencies [2592c80]
  - llama-index-workflows@2.19.0
  - llama-agents-client@0.3.5

## 0.4.4

### Patch Changes

- Updated dependencies [43ff242]
  - llama-index-workflows@2.18.0
  - llama-agents-client@0.3.4

## 0.4.3

### Patch Changes

- 12bda18: Ensure single_connection=True prevents locking from happening

## 0.4.2

### Patch Changes

- 3850844: Support single connection mode for sqlite

## 0.4.1

### Patch Changes

- 9f52f40: Make the sqlite db more resliant to locking

## 0.4.0

### Minor Changes

- 391f287: Make the context API opt-in via `accept_context_api=True` on `WorkflowServer`.

### Patch Changes

- 3432a83: Update debugger assets

  - JavaScript: https://cdn.jsdelivr.net/npm/@llamaindex/workflow-debugger@0.2.36/dist/app.js
  - CSS: https://cdn.jsdelivr.net/npm/@llamaindex/workflow-debugger@0.2.36/dist/app.css

- Updated dependencies [b8c7c7e]
  - llama-index-workflows@2.17.3
  - llama-agents-client@0.3.3

## 0.3.3

### Patch Changes

- Updated dependencies [7e06f87]
  - llama-index-workflows@2.17.2
  - llama-agents-client@0.3.2

## 0.3.2

### Patch Changes

- e7da58b: Log workflow failures and timeouts

## 0.3.1

### Patch Changes

- 5776bd1: Update debugger assets

  - JavaScript: https://cdn.jsdelivr.net/npm/@llamaindex/workflow-debugger@0.2.35/dist/app.js
  - CSS: https://cdn.jsdelivr.net/npm/@llamaindex/workflow-debugger@0.2.35/dist/app.css

- Updated dependencies [979d68b]
- Updated dependencies [983f6f6]
  - llama-index-workflows@2.17.1
  - llama-agents-client@0.3.1

## 0.3.0

### Minor Changes

- b32ec53: Drop python 3.9 support

### Patch Changes

- 7049e23: Add SSE heartbeat to prevent idle closed connections
- Updated dependencies [7fc1aae]
- Updated dependencies [b32ec53]
  - llama-index-workflows@2.17.0
  - llama-agents-client@0.3.0

## 0.2.3

### Patch Changes

- 703ec92: Improve agent data store step and state performance
- ccd0db6: Update debugger assets

  - JavaScript: https://cdn.jsdelivr.net/npm/@llamaindex/workflow-debugger@0.2.34/dist/app.js
  - CSS: https://cdn.jsdelivr.net/npm/@llamaindex/workflow-debugger@0.2.34/dist/app.css

- Updated dependencies [c7bbedb]
- Updated dependencies [703ec92]
  - llama-index-workflows@2.16.1
  - llama-agents-client@0.2.3

## 0.2.2

### Patch Changes

- 9c0b4a0: Fix performance issues during heavy streaming from excessive chatter
- 5e7f9e5: Namespace handler_id instrument tag as `llamaindex.handler_id`.
- Updated dependencies [5e7f9e5]
- Updated dependencies [9f26314]
  - llama-index-workflows@2.16.0
  - llama-agents-client@0.2.2

## 0.2.1

### Patch Changes

- 6605457: Bump dependency requirements
- Updated dependencies [6605457]
- Updated dependencies [6ec262c]
  - llama-agents-client@0.2.1
  - llama-index-workflows@2.15.1

## 0.2.0

### Minor Changes

- d61646f: Add max_completed history cap to MemoryWorkflowStore in order to control memory consumption
- 18a5d68: Refactor server internals from monolithic handler to composable runtime decorators (ServerRuntimeDecorator, PersistenceDecorator, IdleReleaseDecorator) enabling pluggable server runtimes
- 6bccda7: Add AgentDataStore backed by LlamaCloud Agent Data API
- 4ba29dc: Add tick storage, event storage with SSE subscription, per-run state stores, and centralized handler status transitions to AbstractWorkflowStore and SQLite/memory implementations

### Patch Changes

- 77a3f9c: Add workflow release for idle DBOS workflows (with replica support)
- 96e437e: Move task execution into the runtime, for maximal control of specific runtime semantics around determinism
- 23385c7: Add better 500 error logging and structured responses
- Updated dependencies [4ba29dc]
- Updated dependencies [77a3f9c]
- Updated dependencies [62ffc15]
- Updated dependencies [707a254]
- Updated dependencies [05f5f4e]
- Updated dependencies [3c22216]
- Updated dependencies [96e437e]
- Updated dependencies [23385c7]
  - llama-agents-client@0.2.0
  - llama-index-workflows@2.15.0

## 0.2.0-rc.3

### Minor Changes

- c1fbb8f: Add max_completed history cap to MemoryWorkflowStore in order to control memory consumption
- 281d441: Add AgentDataStore backed by LlamaCloud Agent Data API

### Patch Changes

- 3720c61: Add workflow release for idle DBOS workflows (with replica support)
- a2aad32: Move task execution into the runtime, for maximal control of specific runtime semantics around determinism
- Updated dependencies [3720c61]
- Updated dependencies [8762129]
- Updated dependencies [a2aad32]
  - llama-index-workflows@2.15.0-rc.1
  - llama-agents-client@0.2.0-rc.1

## 0.2.0-rc.2

### Minor Changes

- 6ccdebd: Refactor server internals from monolithic handler to composable runtime decorators (ServerRuntimeDecorator, PersistenceDecorator, IdleReleaseDecorator) enabling pluggable server runtimes

## 0.2.0-rc.1

### Minor Changes

- 528d562: Add tick storage, event storage with SSE subscription, per-run state stores, and centralized handler status transitions to AbstractWorkflowStore and SQLite/memory implementations

### Patch Changes

- Updated dependencies [528d562]
- Updated dependencies [e981f73]
- Updated dependencies [b515a46]
- Updated dependencies [7433d4c]
  - llama-agents-client@0.2.0-rc.0
  - llama-index-workflows@2.15.0-rc.0

## 0.2.0-rc.0

### Minor Changes

- 06cca76: Test pre-release functioning

## 0.1.3

### Patch Changes

- Updated dependencies [3590913]
- Updated dependencies [7433d4c]
  - llama-index-workflows@2.14.2
  - llama-agents-client@0.1.3

## 0.1.2

### Patch Changes

- ef7f808: Fix OpenAPI schema version to use current server package, not workflows core
- Updated dependencies [6ece797]
  - llama-index-workflows@2.14.1
  - llama-agents-client@0.1.2

## 0.1.1

### Patch Changes

- db90f89: Separate server/client to their own packages under a llama_agents namespace
- 45e7614: Refact: make control loop more deterministic

  - Switches out the asyncio delay mechanism for a pull-with-timeout that is more deterministic friendly
  - Adds a priority queue of delayed tasks
  - Switches out the misc firing /spawning of async tasks to a more rigorous pattern where tasks are only created in the main loop, and gathered in one location. This makes the concurrency more straightforward to reason about

- Updated dependencies [db90f89]
- Updated dependencies [73c1254]
- Updated dependencies [45e7614]
- Updated dependencies [45e7614]
- Updated dependencies [2900f58]
- Updated dependencies [6fdc45c]
  - llama-agents-client@0.1.1
  - llama-index-workflows@2.14.0
