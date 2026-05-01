# llama-agents-dbos

## 0.3.1

### Patch Changes

- 83d5f9f: Use DBOS async send for internal workflow ticks.

## 0.3.0

### Minor Changes

- 56701a9: Add `max_recovery_attempts` to `DBOSRuntimeConfig`. When set, it is forwarded to the `@DBOS.workflow` decorator wrapping the runtime's control loop.

## 0.2.3

### Patch Changes

- 95d8c2b: Share a single asyncpg pool across DBOSRuntime, PostgresWorkflowStore, and ExecutorLeaseManager instead of each opening their own. Pool size is configurable via `DBOSRuntimeConfig.pool_size`. Also adds LISTEN reconnect with backoff to PostgresWorkflowStore.

## 0.2.2

### Patch Changes

- f7e037e: Stream ticks during resume so peak memory is bounded by batch size rather than total tick history.

## 0.2.1

### Patch Changes

- 85c78a2: Fix crash recovery determinism errors by trimming DBOS operation rows that ran ahead of the workflow journal

## 0.2.0

### Minor Changes

- b32ec53: Drop python 3.9 support

### Patch Changes

- 2535e1f: fix dbos launch running multiple loops

## 0.1.2

### Patch Changes

- 5e7f9e5: Add event input/output summaries to step spans and rehydrate span context across serialization boundaries. Log instead of fail cancelled steps from cancelled workflows. Do not fail from wait_for_event exceptions.

## 0.1.1

### Patch Changes

- 6605457: Bump dependency requirements
- 6ec262c: Fix graceful teardown leading to poisoned DBOS workflow

## 0.1.0

### Minor Changes

- d56be47: Add postgres and DBOS support to the workflow server
- 57902d5: Add alternate DBOS runtime plugin for running workflows against a DBOS backend

### Patch Changes

- 77a3f9c: Add workflow release for idle DBOS workflows (with replica support)
- 96e437e: Move task execution into the runtime, for maximal control of specific runtime semantics around determinism

## 0.1.0-rc.1

### Patch Changes

- 3720c61: Add workflow release for idle DBOS workflows (with replica support)
- a2aad32: Move task execution into the runtime, for maximal control of specific runtime semantics around determinism

## 0.1.0-rc.0

### Minor Changes

- c2e7f17: Add postgres and DBOS support to the workflow server
- 79159f0: Add alternate DBOS runtime plugin for running workflows against a DBOS backend
