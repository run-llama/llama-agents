---
sidebar:
  order: 10
title: Error handling
---

A step that fails its execution might result in the failure of the entire workflow, but oftentimes errors are
expected and the execution can be safely retried. Think of a HTTP request that times out because of a transient
congestion of the network, or an external API call that hits a rate limiter.

For all those situations where you want the step to try again, you can use a **retry policy**. A retry policy
instructs the workflow to execute a step multiple times, controlling how long to wait before each new attempt,
which errors are retryable, and when to give up.

The retry module is built from three families of composable building blocks:

- **Retry conditions** decide whether an exception is retryable.
- **Wait strategies** decide how long to sleep before the next attempt.
- **Stop conditions** decide when the workflow should give up.

Compose them into a policy with `retry_policy(retry=..., wait=..., stop=...)` and
pass the result to the `@step` decorator. Retry conditions support `|` and `&`,
wait strategies support `+`, and stop conditions support `|` and `&`.

## Basic usage

```python
from workflows import Workflow, Context, step
from workflows.events import StartEvent, StopEvent
from workflows.retry_policy import (
    retry_policy,
    stop_after_attempt,
    wait_fixed,
)


class MyWorkflow(Workflow):
    @step(
        retry_policy=retry_policy(
            wait=wait_fixed(5),
            stop=stop_after_attempt(10),
        )
    )
    async def flaky_step(self, ctx: Context, ev: StartEvent) -> StopEvent:
        result = flaky_call()  # this might raise
        return StopEvent(result=result)
```

With no arguments, `retry_policy()` retries all exceptions up to 3 attempts
with a 5-second fixed delay between each. Pass `retry=`, `wait=`, or `stop=`
to customize any component.

## Filtering exceptions

Use retry conditions to control which exceptions are retried:

```python
from workflows import Workflow, step
from workflows.events import StartEvent, StopEvent
from workflows.retry_policy import (
    retry_policy,
    retry_if_exception_message,
    retry_if_exception_type,
    stop_after_attempt,
    stop_before_delay,
    wait_fixed,
    wait_random,
)


class MyWorkflow(Workflow):
    @step(
        retry_policy=retry_policy(
            retry=retry_if_exception_type((TimeoutError, ConnectionError))
            | retry_if_exception_message(match="rate limit|temporarily unavailable"),
            wait=wait_fixed(1) + wait_random(0, 1),
            stop=stop_after_attempt(5) | stop_before_delay(30),
        )
    )
    async def call_provider(self, ev: StartEvent) -> StopEvent:
        result = await flaky_call()
        return StopEvent(result=result)
```

The named combinators are equivalent if you prefer a more explicit style:

```python
from workflows.retry_policy import (
    retry_policy,
    retry_any,
    retry_if_exception_message,
    retry_if_exception_type,
    stop_any,
    stop_after_attempt,
    stop_before_delay,
    wait_combine,
    wait_fixed,
    wait_random,
)

policy = retry_policy(
    retry=retry_any(
        retry_if_exception_type((TimeoutError, ConnectionError)),
        retry_if_exception_message(match="rate limit|temporarily unavailable"),
    ),
    wait=wait_combine(wait_fixed(1), wait_random(0, 1)),
    stop=stop_any(stop_after_attempt(5), stop_before_delay(30)),
)
```

## Exponential backoff

For steps that call LLM providers or rate-limited APIs, exponential backoff avoids
thundering-herd effects:

```python
from workflows import Workflow, Context, step
from workflows.events import StartEvent, StopEvent
from workflows.retry_policy import (
    retry_policy,
    stop_after_attempt,
    wait_exponential_jitter,
)


class MyWorkflow(Workflow):
    @step(
        retry_policy=retry_policy(
            wait=wait_exponential_jitter(initial=1, exp_base=2, max=30, jitter=1),
            stop=stop_after_attempt(5),
        )
    )
    async def call_llm(self, ctx: Context, ev: StartEvent) -> StopEvent:
        result = await llm_call()  # this might raise on rate-limit
        return StopEvent(result=result)
```

## Full API reference

The module exposes many more retry conditions, wait strategies, and stop conditions
than shown here. See the [API reference](/python/workflows-api-reference/retry_policy/)
for the complete list of building blocks and their parameters.

## Custom retry policies

If the composable API doesn't cover your use case, you can write a custom policy.
The only requirement is a class with a `next` method matching the `RetryPolicy`
protocol:

```python
def next(
    self, elapsed_time: float, attempts: int, error: Exception
) -> Optional[float]:
    ...
```

Return the number of seconds to wait before retrying, or `None` to stop.

For example, this policy only retries on Fridays:

```python
from datetime import datetime
from typing import Optional


class RetryOnFridayPolicy:
    def next(
        self, elapsed_time: float, attempts: int, error: Exception
    ) -> Optional[float]:
        if datetime.today().strftime("%A") == "Friday":
            return 5  # retry in 5 seconds
        return None  # don't retry
```

## Deprecated convenience constructors

:::caution
`ConstantDelayRetryPolicy` and `ExponentialBackoffRetryPolicy` predate the
composable API and are kept for backwards compatibility only. Prefer
`retry_policy(...)` with explicit retry, wait, and stop arguments.
:::

```python
from workflows.retry_policy import ConstantDelayRetryPolicy

# Deprecated — equivalent to:
#   retry_policy(wait=wait_fixed(5), stop=stop_after_attempt(10))
policy = ConstantDelayRetryPolicy(delay=5, maximum_attempts=10)
```

```python
from workflows.retry_policy import ExponentialBackoffRetryPolicy

# Deprecated — equivalent to:
#   retry_policy(wait=wait_random_exponential(multiplier=1, exp_base=2, max=30),
#                stop=stop_after_attempt(5))
policy = ExponentialBackoffRetryPolicy(
    initial_delay=1, multiplier=2, max_delay=30, maximum_attempts=5,
)
```

## Inspecting retry state inside a step

`Context.retry_info()` returns a `RetryInfo` describing the current attempt.
Use it to adapt behavior between retries — widen a search, shorten a timeout,
log a warning:

```python
from workflows import Context, Workflow, step
from workflows.events import StartEvent, StopEvent
from workflows.retry_policy import retry_policy, stop_after_attempt


class MyWorkflow(Workflow):
    @step(retry_policy=retry_policy(stop=stop_after_attempt(3)))
    async def flaky(self, ctx: Context, ev: StartEvent) -> StopEvent:
        info = ctx.retry_info()
        if info.last_exception is not None:
            logger.warning(
                "retry %d after %s", info.retry_number, str(info.last_exception)
            )
        ...
```

`retry_number` is `0` on the first run. `last_exception` and `last_failed_at`
are `None` until the first failure, then describe the most recent prior failure.

## Recovering when retries are exhausted

When a policy gives up, the exception propagates and fails the workflow — unless
a `@catch_error` handler is declared. A handler is a step that accepts
`StepFailedEvent`; it can return a `StopEvent` to end the run gracefully, return
some other event to re-route the workflow, or raise to fail with a new
exception. Inspect `ev.step_name` and `ev.exception` to decide; see the
[`StepFailedEvent` API reference](/python/workflows-api-reference/events/#workflows.events.StepFailedEvent)
for the full set of fields.

```python
from workflows import Context, Workflow, catch_error, step
from workflows.events import StartEvent, StepFailedEvent, StopEvent
from workflows.retry_policy import retry_policy, stop_after_attempt, wait_fixed


class Pipeline(Workflow):
    @step(retry_policy=retry_policy(wait=wait_fixed(1), stop=stop_after_attempt(3)))
    async def fetch(self, ev: StartEvent) -> StopEvent:
        return StopEvent(result=await call_api())

    @catch_error
    async def on_failure(
        self, ctx: Context, ev: StepFailedEvent
    ) -> StopEvent:
        return StopEvent(
            result={"failed_step": ev.step_name, "error": str(ev.exception)}
        )
```

A bare `@catch_error` is a **wildcard** — it catches any step whose retries are
exhausted and isn't claimed by a more specific handler. Only one wildcard is
allowed per workflow.

### Scoping to specific steps

Pass `for_steps=[...]` to limit which steps a handler covers. A step name may
appear in at most one scoped handler; unknown names are rejected at
construction time.

```python
class Pipeline(Workflow):
    @step(retry_policy=...)
    async def fetch(self, ev: StartEvent) -> FetchedEvent: ...

    @step(retry_policy=...)
    async def parse(self, ev: FetchedEvent) -> StopEvent: ...

    @catch_error(for_steps=["fetch"])
    async def on_fetch_failure(
        self, ctx: Context, ev: StepFailedEvent
    ) -> StopEvent:
        return StopEvent(result={"fallback": True})

    @catch_error  # wildcard — covers `parse` and anything else
    async def on_any_failure(
        self, ctx: Context, ev: StepFailedEvent
    ) -> StopEvent:
        return StopEvent(result={"aborted": ev.step_name})
```

### Recovery budget

Handlers can themselves emit events that flow back into the graph, so a lineage
may re-enter the same handler. `max_recoveries` (default `1`) caps how many
times that can happen per event lineage before the workflow fails instead of
re-entering. Set it higher for handlers that participate in a bounded retry
loop; keep it at `1` for terminal handlers.

```python
@catch_error(for_steps=["fetch", "retry_fetch"], max_recoveries=2)
async def on_failure(self, ctx: Context, ev: StepFailedEvent) -> RetryFetch:
    return RetryFetch()
```
