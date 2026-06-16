# Control Loop Architecture

The control loop is the core execution engine for workflows. It follows a **reducer pattern** — pure state transitions with side effects expressed as commands:

```
State + Tick --> (NewState, Commands)
```

[`control_loop/`](../packages/llama-index-workflows/src/workflows/runtime/control_loop) — split along the reducer seam into `runner.py` (async runtime: tasks, the wakeup heap, command execution), `reduce.py` (the pure reducer: `_reduce_tick` and the per-tick processors), and `streams.py` (collection-stream accounting). The package `__init__` re-exports the surface so `workflows.runtime.control_loop` stays a single import target.

## Main Loop

```mermaid
flowchart TD
    A[Initialize: queue StartEvent, schedule timeout] --> B[Drain tick buffer]
    B --> C{Buffer empty?}
    C -- No --> D[Reduce tick --> state + commands]
    D --> E[Execute commands]
    E --> B
    C -- Yes --> F[Wait for next completion]
    F --> G{What completed?}
    G -- Timeout --> H[Pop due scheduled ticks into buffer]
    G -- External tick --> I[Add to buffer]
    G -- Worker result --> J[Add TickStepResult to buffer]
    H --> B
    I --> B
    J --> B
```

1. **Initialize** — Queue `StartEvent`, schedule workflow timeout, rewind any in-progress work from a prior run.
2. **Drain tick buffer** — Process all queued ticks synchronously. Each tick runs through the reducer and its commands execute before the next tick.
3. **Wait for next completion** — Build a task set (worker tasks + one pull task), then wait for the first to complete. Workers have priority over pull tasks.
4. **Process completed task** — Route the result back into the tick buffer and loop.

## Ticks and Commands

**Ticks** are inputs to the reducer. They represent things that happen: events arriving, steps completing, cancellation requests, timeouts, and publish requests from steps. Each tick type dispatches to a dedicated reducer function.

[`types/ticks.py`](../packages/llama-index-workflows/src/workflows/runtime/types/ticks.py) — all tick types

**Commands** are outputs from the reducer — the side effects the loop executes. They represent actions to take: spawning step workers, queuing events (with optional delays), completing or failing the run, and publishing events to the external stream.

[`types/commands.py`](../packages/llama-index-workflows/src/workflows/runtime/types/commands.py) — all command types

## Runtime Integration

The control loop is runtime-agnostic. It talks to the outside world exclusively through `InternalRunAdapter` (see [core-overview.md — Runtime and Adapters](./core-overview.md#runtime-and-adapters)). This is the extension point — runtime decorators wrap the adapter to add behavior like tick persistence, idle detection, or event recording.

```mermaid
sequenceDiagram
    participant CL as Control Loop
    participant A as InternalRunAdapter
    participant Ext as External (handler/client)

    Note over CL: Main loop iteration
    CL->>A: wait_receive() [pull task]
    Ext-->>A: send_event() delivers tick
    A-->>CL: WaitResultTick

    CL->>CL: reduce tick --> (state, commands)
    CL->>A: on_tick(tick) [journaling hook]

    Note over CL: Execute commands
    CL->>A: write_to_event_stream(event)
    CL->>CL: spawn worker task

    CL->>A: wait_for_next_task(task_set, timeout)
    A-->>CL: completed task (worker or pull)
```

[`plugin.py`](../packages/llama-index-workflows/src/workflows/runtime/types/plugin.py) — full adapter interface

## Key Design Decisions

- **Deterministic replay** — The reducer is pure. Adapters can record ticks and replay them to reconstruct state, and override time functions for deterministic timestamps.
- **Priority ordering** — Worker tasks complete before pull tasks, ensuring in-flight work finishes before accepting new external events.
- **Optimistic execution with retry** — Workers receive a snapshot of collected events. If new events arrive during execution, the worker re-runs with the updated snapshot.
- **State rehydration** — On resume, in-progress events move back to the queue and worker IDs reset, allowing clean restart from stored ticks.
- **Idle detection** — When all steps are waiting on external input, the loop publishes `WorkflowIdleEvent`. Runtime decorators can use this signal to release idle workflows from memory.
- **Retry-exhaustion hook** — `_schedule_retry_or_route_failure` (the `StepWorkerFailed` path of `_process_step_result_tick`) routes a `StepFailedEvent` to a registered `@catch_error` handler. Handlers can be scoped (`@catch_error(for_steps=[...])`) or wildcard, with a per-handler `max_recoveries` budget tracked per event lineage in `recovery_counts: dict[str, int]` on `EventAttempt` / `TickAddEvent` / `CommandQueueEvent`. Routing consults `BrokerConfig.handler_for_step` and `BrokerConfig.catch_error_handlers`; when the count exceeds `max_recoveries` or no handler owns the step, the loop publishes a `WorkflowFailedEvent` carrying the live exception and fails the run. The live `Exception` rides on `EventAttempt` / `TickAddEvent` / `CommandQueueEvent` between retries — annotated with `SerializableException` where it crosses a pydantic serialization boundary — and is exposed to step bodies via `Context.retry_info()`.
