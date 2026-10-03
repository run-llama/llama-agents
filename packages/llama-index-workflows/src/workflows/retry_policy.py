# SPDX-License-Identifier: MIT
# Copyright (c) 2026 LlamaIndex Inc.

from __future__ import annotations

import random
import re
import warnings
from collections.abc import Callable
from dataclasses import dataclass
from datetime import datetime, timedelta
from typing import Protocol, cast, runtime_checkable

time_unit_type = int | float | timedelta


def _to_seconds(value: time_unit_type) -> float:
    return float(value.total_seconds() if isinstance(value, timedelta) else value)


@dataclass(frozen=True)
class RetryInfo:
    """Snapshot of the currently-executing step's retry state.

    Returned by ``Context.retry_info()``. On the first attempt ``retry_number``
    is 0, ``elapsed_seconds`` is 0.0, and both ``last_exception`` and
    ``last_failed_at`` are ``None``. On subsequent retries they describe the
    most recent prior failure.

    Attributes:
        retry_number: 0 on the first run, 1 on the first retry, and so on.
        elapsed_seconds: Seconds since the first attempt began.
        last_exception: The most recent prior exception, or ``None``.
            ``__traceback__`` is available in-process but is lost after a
            replay from persisted state.
        last_failed_at: Timezone-aware UTC datetime of the most recent prior
            failure, or ``None``.
    """

    retry_number: int
    elapsed_seconds: float
    last_exception: Exception | None
    last_failed_at: datetime | None


@runtime_checkable
class RetryPolicy(Protocol):
    """
    Structural interface for step retry policies.

    Any object with a compatible ``next`` method satisfies this protocol,
    including policies built with `retry_policy()`, `ConstantDelayRetryPolicy`,
    `ExponentialBackoffRetryPolicy`, and user-defined policies.

    Most users do not implement this protocol directly. Instead, construct a
    policy with ``retry_policy(retry=..., wait=..., stop=...)`` and combine
    retry conditions, wait strategies, and stop conditions with the operators
    supported by this module.

    Examples:
        ```python
        from workflows.retry_policy import (
            retry_policy,
            retry_if_exception_type,
            stop_after_attempt,
            wait_exponential,
        )

        policy = retry_policy(
            retry=retry_if_exception_type((TimeoutError, ConnectionError)),
            wait=wait_exponential(multiplier=1, exp_base=2, max=30),
            stop=stop_after_attempt(5),
        )
        ```

    See Also:
        - [step][workflows.decorators.step]
    """

    def next(
        self,
        elapsed_time: float,
        attempts: int,
        error: Exception,
        *,
        seed: int | None = None,
    ) -> float | None:
        """
        Decide if another retry should occur and the delay before it.

        Args:
            elapsed_time: Seconds since the first failure.
            attempts: Number of failures so far, starting at 1 for the
                first failure.
            error: The last exception encountered.
            seed: Optional RNG seed for deterministic jitter (DBOS replay).

        Returns:
            Seconds to wait before retrying, or ``None`` to stop.
        """


class RetryCondition(Protocol):
    """Predicate that decides whether an exception is retryable."""

    def __call__(self, error: BaseException) -> bool: ...


class WaitStrategy(Protocol):
    """Compute the delay in seconds before the next retry attempt.

    ``attempts`` starts at 0 for the first retry. ``RetryPolicy.next`` counts
    failures from 1 and subtracts one before calling the wait strategy.
    """

    def __call__(self, attempts: int, *, seed: int | None = None) -> float: ...


class StopCondition(Protocol):
    """Predicate that decides whether retries should stop."""

    def __call__(
        self, attempts: int, elapsed_time: float, *, upcoming_sleep: float = 0.0
    ) -> bool: ...


class _RetryConditionBase:
    """Base class for retry predicates that support tenacity-style composition."""

    def __call__(self, error: BaseException) -> bool:
        raise NotImplementedError

    def __and__(self, other: RetryCondition) -> retry_all:
        return retry_all(self, other)

    def __rand__(self, other: RetryCondition) -> retry_all:
        return retry_all(other, self)

    def __or__(self, other: RetryCondition) -> retry_any:
        return retry_any(self, other)

    def __ror__(self, other: RetryCondition) -> retry_any:
        return retry_any(other, self)


class _WaitStrategyBase:
    """Base class for wait strategies that support addition and ``sum()``."""

    def __call__(self, attempts: int, *, seed: int | None = None) -> float:
        raise NotImplementedError

    def __add__(self, other: WaitStrategy) -> wait_combine:
        return wait_combine(self, other)

    def __radd__(self, other: object) -> object:
        if other == 0:
            return self
        if callable(other):
            return self.__add__(cast(WaitStrategy, other))
        return NotImplemented


class _StopConditionBase:
    """Base class for stop conditions that support ``&`` and ``|`` operators."""

    def __call__(
        self, attempts: int, elapsed_time: float, *, upcoming_sleep: float = 0.0
    ) -> bool:
        raise NotImplementedError

    def __and__(self, other: StopCondition) -> stop_all:
        return stop_all(self, other)

    def __rand__(self, other: StopCondition) -> stop_all:
        return stop_all(other, self)

    def __or__(self, other: StopCondition) -> stop_any:
        return stop_any(self, other)

    def __ror__(self, other: StopCondition) -> stop_any:
        return stop_any(other, self)


def _compile_pattern(match: str | re.Pattern[str] | None) -> re.Pattern[str] | None:
    if match is None:
        return None
    if isinstance(match, str):
        return re.compile(match)
    return match


class retry_if_exception(_RetryConditionBase):
    """
    Retry when the raised exception satisfies a custom predicate.

    Use this when your retry decision depends on exception details that are not
    covered by the built-in helpers.

    Examples:
        ```python
        retry_if_exception(lambda error: "rate limit" in str(error).lower())
        ```
    """

    def __init__(self, predicate: Callable[[BaseException], bool]) -> None:
        self.predicate = predicate

    def __call__(self, error: BaseException) -> bool:
        return self.predicate(error)


class retry_if_exception_type(retry_if_exception):
    """
    Retry only when the exception is an instance of one of the given types.

    This is the most common retry predicate for transient network and provider
    failures.

    Examples:
        ```python
        retry_if_exception_type((TimeoutError, ConnectionError))
        ```
    """

    def __init__(
        self,
        exception_types: type[BaseException]
        | tuple[type[BaseException], ...] = Exception,
    ) -> None:
        self.exception_types = exception_types
        super().__init__(lambda error: isinstance(error, exception_types))


class retry_if_not_exception_type(retry_if_exception):
    """
    Retry unless the exception is an instance of one of the given types.

    This is useful when most failures are retryable except for a small set of
    known permanent errors.

    Examples:
        ```python
        retry_if_not_exception_type((ValueError, PermissionError))
        ```
    """

    def __init__(
        self,
        exception_types: type[BaseException]
        | tuple[type[BaseException], ...] = Exception,
    ) -> None:
        self.exception_types = exception_types
        super().__init__(lambda error: not isinstance(error, exception_types))


class retry_unless_exception_type(retry_if_not_exception_type):
    """
    Retry unless the exception is an instance of one of the given types.

    Tenacity-style alias for `retry_if_not_exception_type`.

    Examples:
        ```python
        retry_unless_exception_type(AuthenticationError)
        ```
    """

    pass


class retry_if_exception_message(_RetryConditionBase):
    """
    Retry when the exception message matches an exact string or regex pattern.

    Pass either ``message`` for an exact string match or ``match`` for a regular
    expression. Passing both is an error.

    Examples:
        ```python
        retry_if_exception_message(message="please retry")
        retry_if_exception_message(match=r"HTTP 5\\d\\d")
        ```
    """

    def __init__(
        self,
        message: str | None = None,
        match: str | re.Pattern[str] | None = None,
    ) -> None:
        if message is None and match is None:
            raise TypeError(
                "retry_if_exception_message() missing 1 required argument "
                "'message' or 'match'"
            )
        if message is not None and match is not None:
            raise TypeError(
                "retry_if_exception_message() takes either 'message' or 'match', not both"
            )
        self.message = message
        self.match = match
        self._pattern = _compile_pattern(match)

    def __call__(self, error: BaseException) -> bool:
        error_message = str(error)
        if self.message is not None:
            return error_message == self.message
        return bool(self._pattern and self._pattern.search(error_message))


class retry_if_not_exception_message(retry_if_exception_message):
    """
    Retry when the exception message does not match the given string or regex.

    This is useful when a provider uses specific messages to signal permanent
    failures that should stop retries.

    Examples:
        ```python
        retry_if_not_exception_message(match="invalid_api_key|permission denied")
        ```
    """

    def __call__(self, error: BaseException) -> bool:
        return not super().__call__(error)


class retry_if_exception_cause_type(_RetryConditionBase):
    """
    Retry when any exception in the ``__cause__`` chain matches the given type.

    Only explicit exception chaining (``raise X from Y``) is followed. Implicit
    chaining via ``__context__`` is **not** inspected, matching tenacity's
    behavior. If you need to match implicitly chained exceptions, use
    `retry_if_exception` with a custom predicate that walks ``__context__``.

    Examples:
        ```python
        retry_if_exception_cause_type(ConnectionError)
        ```
    """

    def __init__(
        self,
        exception_types: type[BaseException]
        | tuple[type[BaseException], ...] = Exception,
    ) -> None:
        self.exception_types = exception_types

    def __call__(self, error: BaseException) -> bool:
        current: BaseException | None = error
        while current is not None:
            cause = current.__cause__
            if isinstance(cause, self.exception_types):
                return True
            current = cause
        return False


class retry_any(_RetryConditionBase):
    """
    Retry if any of the provided retry predicates match.

    Equivalent to combining retry predicates with ``|``.

    Examples:
        ```python
        retry_any(
            retry_if_exception_type(ConnectionError),
            retry_if_exception_message(match="rate limit"),
        )
        ```
    """

    def __init__(self, *retries: RetryCondition) -> None:
        self.retries = retries

    def __call__(self, error: BaseException) -> bool:
        return any(retry(error) for retry in self.retries)


class retry_all(_RetryConditionBase):
    """
    Retry if all of the provided retry predicates match.

    Equivalent to combining retry predicates with ``&``.

    Examples:
        ```python
        retry_all(
            retry_if_exception_type(RuntimeError),
            retry_if_exception_message(match="temporary"),
        )
        ```
    """

    def __init__(self, *retries: RetryCondition) -> None:
        self.retries = retries

    def __call__(self, error: BaseException) -> bool:
        return all(retry(error) for retry in self.retries)


class retry_always(_RetryConditionBase):
    """
    Retry condition that always retries.

    This is mainly useful when you want to be explicit in a composed policy.

    Examples:
        ```python
        retry_policy(retry=retry_always(), stop=stop_after_attempt(3))
        ```
    """

    def __call__(self, error: BaseException) -> bool:
        return True


class retry_never(_RetryConditionBase):
    """
    Retry condition that never retries.

    This can be useful in tests or to disable one branch of a composed retry
    expression.

    Examples:
        ```python
        retry_never() | retry_if_exception_type(ConnectionError)
        ```
    """

    def __call__(self, error: BaseException) -> bool:
        return False


class wait_fixed(_WaitStrategyBase):
    """
    Wait a fixed number of seconds between attempts.

    Examples:
        ```python
        wait_fixed(5)
        ```
    """

    def __init__(self, wait: time_unit_type) -> None:
        self.wait = _to_seconds(wait)

    def __call__(self, attempts: int, *, seed: int | None = None) -> float:
        return self.wait


class wait_none(wait_fixed):
    """
    Wait strategy that does not delay retries.

    Examples:
        ```python
        wait_none()
        ```
    """

    def __init__(self) -> None:
        super().__init__(0)


class wait_exponential(_WaitStrategyBase):
    """
    Wait with exponentially increasing delays, clamped between ``min`` and ``max``.

    The delay for retry ``n`` is ``multiplier * exp_base**n`` before clamping,
    with ``n=0`` for the first retry.

    Examples:
        ```python
        wait_exponential(multiplier=1, exp_base=2, max=60)
        ```
    """

    def __init__(
        self,
        multiplier: int | float = 1.0,
        exp_base: int | float = 2.0,
        max: time_unit_type = 60.0,
        min: time_unit_type = 0.0,
    ) -> None:
        self.multiplier = float(multiplier)
        self.exp_base = float(exp_base)
        self.max = _to_seconds(max)
        self.min = _to_seconds(min)

    def __call__(self, attempts: int, *, seed: int | None = None) -> float:
        try:
            delay = self.multiplier * self.exp_base**attempts
        except OverflowError:
            delay = self.max
        return max(
            max(0.0, self.min),
            min(delay, self.max),
        )


class wait_incrementing(_WaitStrategyBase):
    """
    Wait an incrementally larger amount after each attempt.

    The delay is ``start`` for the first retry and increases by ``increment``
    on each subsequent retry, capped by ``max`` and never going below zero.

    Examples:
        ```python
        wait_incrementing(start=1, increment=2, max=10)
        ```
    """

    def __init__(
        self,
        start: time_unit_type = 0.0,
        increment: time_unit_type = 100.0,
        max: time_unit_type = float("inf"),
    ) -> None:
        self.start = _to_seconds(start)
        self.increment = _to_seconds(increment)
        self.max = _to_seconds(max)

    def __call__(self, attempts: int, *, seed: int | None = None) -> float:
        result = self.start + (self.increment * attempts)
        return max(0.0, min(result, self.max))


class wait_random(_WaitStrategyBase):
    """
    Wait a random duration uniformly sampled from ``[min, max]``.

    When the workflow runtime provides a ``seed``, the sampled value is
    deterministic across replayed runs.

    Examples:
        ```python
        wait_random(min=0.5, max=1.5)
        ```
    """

    def __init__(self, min: time_unit_type = 0.0, max: time_unit_type = 1.0) -> None:
        self.min = _to_seconds(min)
        self.max = _to_seconds(max)

    def __call__(self, attempts: int, *, seed: int | None = None) -> float:
        rng = random.Random(seed) if seed is not None else random
        return rng.uniform(self.min, self.max)


class wait_exponential_jitter(_WaitStrategyBase):
    """
    Exponential backoff with additive random jitter.

    The deterministic base delay grows exponentially and a random value in
    ``[0, jitter]`` is added on top.

    Examples:
        ```python
        wait_exponential_jitter(initial=1, exp_base=2, max=60, jitter=1)
        ```
    """

    def __init__(
        self,
        initial: float = 1.0,
        exp_base: float = 2.0,
        max: float = 60.0,
        jitter: float = 1.0,
    ) -> None:
        self.initial = initial
        self.exp_base = exp_base
        self.max = max
        self.jitter = jitter

    def __call__(self, attempts: int, *, seed: int | None = None) -> float:
        try:
            base = min(self.initial * self.exp_base**attempts, self.max)
        except OverflowError:
            base = self.max
        rng = random.Random(seed) if seed is not None else random
        return min(base + rng.uniform(0, self.jitter), self.max)


class wait_random_exponential(_WaitStrategyBase):
    """
    Exponential backoff with full jitter.

    A random delay is sampled between ``min`` and the exponential upper bound
    for the current attempt.

    Examples:
        ```python
        wait_random_exponential(multiplier=1, exp_base=2, max=60)
        ```
    """

    def __init__(
        self,
        multiplier: int | float = 1.0,
        exp_base: int | float = 2.0,
        max: time_unit_type = 60.0,
        min: time_unit_type = 0.0,
    ) -> None:
        self.multiplier = float(multiplier)
        self.exp_base = float(exp_base)
        self.max = _to_seconds(max)
        self.min = _to_seconds(min)

    def __call__(self, attempts: int, *, seed: int | None = None) -> float:
        rng = random.Random(seed) if seed is not None else random
        try:
            delay = self.multiplier * self.exp_base**attempts
        except OverflowError:
            delay = self.max
        upper = max(
            max(0.0, self.min),
            min(delay, self.max),
        )
        return rng.uniform(self.min, upper)


def wait_full_jitter(
    multiplier: int | float = 1.0,
    exp_base: int | float = 2.0,
    max: time_unit_type = 60.0,
    min: time_unit_type = 0.0,
) -> wait_random_exponential:
    """
    Alias for `wait_random_exponential`.

    Examples:
        ```python
        wait_full_jitter(multiplier=1, exp_base=2, max=60)
        ```
    """

    return wait_random_exponential(
        multiplier=multiplier,
        exp_base=exp_base,
        max=max,
        min=min,
    )


class wait_chain(_WaitStrategyBase):
    """
    Use a different wait strategy for each retry in order, starting with the
    first.

    After the provided strategies are exhausted, the last strategy is reused
    for all subsequent retries.

    Examples:
        ```python
        wait_chain(wait_fixed(1), wait_fixed(2), wait_fixed(5))
        ```
    """

    def __init__(self, *strategies: WaitStrategy) -> None:
        if not strategies:
            raise ValueError("wait_chain requires at least one strategy")
        self.strategies = strategies

    def __call__(self, attempts: int, *, seed: int | None = None) -> float:
        idx = min(attempts, len(self.strategies) - 1)
        return self.strategies[idx](attempts, seed=seed)


class wait_combine(_WaitStrategyBase):
    """
    Combine multiple wait strategies by summing their delays.

    Equivalent to combining waits with ``+``.

    Examples:
        ```python
        wait_combine(wait_fixed(1), wait_random(0, 1))
        ```
    """

    def __init__(self, *strategies: WaitStrategy) -> None:
        self.strategies = strategies

    def __call__(self, attempts: int, *, seed: int | None = None) -> float:
        return sum(strategy(attempts, seed=seed) for strategy in self.strategies)


class stop_after_attempt(_StopConditionBase):
    """
    Stop after a fixed number of attempts.

    Examples:
        ```python
        stop_after_attempt(5)
        ```
    """

    def __init__(self, max_attempt_number: int) -> None:
        self.max_attempt_number = max_attempt_number

    def __call__(
        self, attempts: int, elapsed_time: float, *, upcoming_sleep: float = 0.0
    ) -> bool:
        return attempts >= self.max_attempt_number


class stop_after_delay(_StopConditionBase):
    """
    Stop after a maximum elapsed time in seconds.

    Examples:
        ```python
        stop_after_delay(30)
        ```
    """

    def __init__(self, max_delay: time_unit_type) -> None:
        self.max_delay = _to_seconds(max_delay)

    def __call__(
        self, attempts: int, elapsed_time: float, *, upcoming_sleep: float = 0.0
    ) -> bool:
        return elapsed_time >= self.max_delay


class stop_before_delay(_StopConditionBase):
    """
    Stop if the next sleep would move the retry past the configured limit.

    Unlike `stop_after_delay`, this condition considers the ``upcoming_sleep``
    value produced by the wait strategy.

    Examples:
        ```python
        stop_before_delay(30)
        ```
    """

    def __init__(self, max_delay: time_unit_type) -> None:
        self.max_delay = _to_seconds(max_delay)

    def __call__(
        self, attempts: int, elapsed_time: float, *, upcoming_sleep: float = 0.0
    ) -> bool:
        return elapsed_time + upcoming_sleep >= self.max_delay


class stop_any(_StopConditionBase):
    """
    Stop if any of the provided stop predicates match.

    Equivalent to combining stop conditions with ``|``.

    Examples:
        ```python
        stop_any(stop_after_attempt(5), stop_after_delay(30))
        ```
    """

    def __init__(self, *stops: StopCondition) -> None:
        self.stops = stops

    def __call__(
        self, attempts: int, elapsed_time: float, *, upcoming_sleep: float = 0.0
    ) -> bool:
        return any(
            stop(attempts, elapsed_time, upcoming_sleep=upcoming_sleep)
            for stop in self.stops
        )


class stop_all(_StopConditionBase):
    """
    Stop if all of the provided stop predicates match.

    Equivalent to combining stop conditions with ``&``.

    Examples:
        ```python
        stop_all(stop_after_attempt(5), stop_after_delay(30))
        ```
    """

    def __init__(self, *stops: StopCondition) -> None:
        self.stops = stops

    def __call__(
        self, attempts: int, elapsed_time: float, *, upcoming_sleep: float = 0.0
    ) -> bool:
        return all(
            stop(attempts, elapsed_time, upcoming_sleep=upcoming_sleep)
            for stop in self.stops
        )


class stop_never(_StopConditionBase):
    """
    Stop condition that never stops.

    This is typically paired with a retry predicate or workflow timeout that
    provides the real upper bound.

    Examples:
        ```python
        stop_never()
        ```
    """

    def __call__(
        self, attempts: int, elapsed_time: float, *, upcoming_sleep: float = 0.0
    ) -> bool:
        return False


class _ComposableRetryPolicy:
    """
    Composable retry policy built from retry conditions, wait strategies, and stop conditions.

    Decomposes retry behavior into three orthogonal concerns:

    - **retry**: Should we retry this error? (default: retry any exception)
    - **wait**: How long to wait before the next attempt?
    - **stop**: When to give up?

    Users typically construct this through ``retry_policy(...)`` rather than by
    referencing this internal class directly.
    """

    def __init__(
        self,
        retry: RetryCondition | None = None,
        wait: WaitStrategy = wait_fixed(5),
        stop: StopCondition = stop_after_attempt(3),
    ) -> None:
        self.retry = retry
        self.wait = wait
        self.stop = stop

    def next(
        self,
        elapsed_time: float,
        attempts: int,
        error: Exception,
        *,
        seed: int | None = None,
    ) -> float | None:
        if self.retry is not None and not self.retry(error):
            return None

        # `attempts` counts failures from 1 (see runtime/control_loop/
        # reduce.py: `failures = this_execution.attempts + 1`) while wait
        # strategies count retries from 0, so subtract one. The clamp keeps
        # a direct caller passing 0 on the first delay instead of index -1,
        # which wait_chain would map to its last strategy.
        delay = self.wait(max(attempts - 1, 0), seed=seed)
        if self.stop(attempts, elapsed_time, upcoming_sleep=delay):
            return None
        return delay


def retry_policy(
    retry: RetryCondition | None = None,
    wait: WaitStrategy = wait_fixed(5),
    stop: StopCondition = stop_after_attempt(3),
) -> RetryPolicy:
    """
    Construct a composable retry policy from retry, wait, and stop components.

    This is the primary way to create retry policies. Combine retry conditions,
    wait strategies, and stop conditions using operators (``|``, ``&``, ``+``)
    or the named combinators.

    Examples:
        ```python
        from workflows.retry_policy import (
            retry_policy,
            retry_if_exception_type,
            stop_after_attempt,
            wait_exponential,
        )

        policy = retry_policy(
            retry=retry_if_exception_type((TimeoutError, ConnectionError)),
            wait=wait_exponential(multiplier=1, exp_base=2, max=30),
            stop=stop_after_attempt(5),
        )
        ```

    With no arguments, ``retry_policy()`` retries all exceptions up to 3
    attempts with a 5-second fixed delay between each.

    Args:
        retry: Predicate that decides whether an exception is retryable.
            When ``None``, all exceptions are retried.
        wait: Strategy that computes the delay before the next attempt.
            Defaults to ``wait_fixed(5)`` (5 seconds).
        stop: Predicate that decides when to give up.
            Defaults to ``stop_after_attempt(3)``.

    Returns:
        A `RetryPolicy` implementation.
    """
    return _ComposableRetryPolicy(retry=retry, wait=wait, stop=stop)


def ConstantDelayRetryPolicy(
    maximum_attempts: int = 3,
    delay: float = 5,
) -> RetryPolicy:
    """
    Retry at a fixed interval up to a maximum number of attempts.

    Deprecated: use ``retry_policy(wait=wait_fixed(delay), stop=stop_after_attempt(n))`` instead.

    Examples:
        ```python
        ConstantDelayRetryPolicy(delay=5, maximum_attempts=10)
        ```
    """
    warnings.warn(
        "ConstantDelayRetryPolicy is deprecated, use "
        "retry_policy(wait=wait_fixed(delay), stop=stop_after_attempt(n)) instead",
        DeprecationWarning,
        stacklevel=2,
    )

    return _ComposableRetryPolicy(
        wait=wait_fixed(delay),
        stop=stop_after_attempt(maximum_attempts),
    )


def ExponentialBackoffRetryPolicy(
    maximum_attempts: int = 5,
    initial_delay: float = 1.0,
    multiplier: float = 2.0,
    max_delay: float = 60.0,
    jitter: bool = True,
) -> RetryPolicy:
    """
    Retry with exponentially increasing delays, optional jitter, and a cap.

    Deprecated: use ``retry_policy(wait=wait_exponential(...), stop=stop_after_attempt(n))`` instead.
    For jitter, use ``wait_random_exponential`` or ``wait_exponential_jitter``.

    Examples:
        ```python
        ExponentialBackoffRetryPolicy(
            initial_delay=1,
            multiplier=2,
            max_delay=30,
            maximum_attempts=5,
            jitter=True,
        )
        ```
    """
    warnings.warn(
        "ExponentialBackoffRetryPolicy is deprecated, use "
        "retry_policy(wait=wait_exponential(...), stop=stop_after_attempt(n)) instead",
        DeprecationWarning,
        stacklevel=2,
    )

    wait: WaitStrategy
    if jitter:
        wait = wait_random_exponential(
            multiplier=initial_delay,
            exp_base=multiplier,
            max=max_delay,
        )
    else:
        wait = wait_exponential(
            multiplier=initial_delay,
            exp_base=multiplier,
            max=max_delay,
        )

    return _ComposableRetryPolicy(
        wait=wait,
        stop=stop_after_attempt(maximum_attempts),
    )
