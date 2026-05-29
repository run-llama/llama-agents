# SPDX-License-Identifier: MIT
# Copyright (c) 2026 LlamaIndex Inc.

from __future__ import annotations

import hashlib
import re
from typing import Any, cast

import pytest
from workflows import retry_policy as retry_policy_module
from workflows.context import Context
from workflows.decorators import step
from workflows.events import Event, StartEvent, StopEvent
from workflows.retry_policy import (
    ConstantDelayRetryPolicy,
    ExponentialBackoffRetryPolicy,
    retry_all,
    retry_always,
    retry_any,
    retry_if_exception,
    retry_if_exception_cause_type,
    retry_if_exception_message,
    retry_if_exception_type,
    retry_if_not_exception_message,
    retry_if_not_exception_type,
    retry_never,
    retry_policy,
    retry_unless_exception_type,
    stop_after_attempt,
    stop_after_delay,
    stop_all,
    stop_any,
    stop_before_delay,
    stop_never,
    wait_chain,
    wait_combine,
    wait_exponential,
    wait_exponential_jitter,
    wait_fixed,
    wait_full_jitter,
    wait_incrementing,
    wait_none,
    wait_random,
    wait_random_exponential,
)
from workflows.testing import WorkflowTestRunner
from workflows.workflow import Workflow

# ---------------------------------------------------------------------------
# Retry conditions
# ---------------------------------------------------------------------------


def test_retry_if_exception_type_matches() -> None:
    cond = retry_if_exception_type(exception_types=(ValueError, TypeError))
    assert cond(ValueError("bad")) is True
    assert cond(TypeError("bad")) is True
    assert cond(RuntimeError("bad")) is False


def test_retry_if_exception_type_subclass() -> None:
    cond = retry_if_exception_type(exception_types=OSError)
    assert cond(ConnectionError("refused")) is True


def test_retry_if_not_exception_type() -> None:
    cond = retry_if_not_exception_type(exception_types=(ValueError, KeyError))
    assert cond(ValueError("bad")) is False
    assert cond(KeyError("bad")) is False
    assert cond(RuntimeError("bad")) is True


def test_retry_if_exception_uses_predicate() -> None:
    cond = retry_if_exception(lambda error: "retryable" in str(error))
    assert cond(RuntimeError("retryable failure")) is True
    assert cond(RuntimeError("permanent failure")) is False


def test_retry_if_exception_message_exact_match() -> None:
    cond = retry_if_exception_message(message="rate limit exceeded")
    assert cond(Exception("rate limit exceeded")) is True
    assert cond(Exception("rate limit exceeded immediately")) is False
    assert cond(Exception("invalid input")) is False


def test_retry_if_exception_message_regex() -> None:
    cond = retry_if_exception_message(match=r"code=\d{3}")
    assert cond(Exception("code=429")) is True
    assert cond(Exception("code=abc")) is False


def test_retry_if_exception_message_accepts_compiled_pattern() -> None:
    cond = retry_if_exception_message(match=re.compile(r"retry-\d+"))
    assert cond(Exception("retry-42")) is True
    assert cond(Exception("retry-later")) is False


def test_retry_if_exception_message_requires_exactly_one_matcher() -> None:
    with pytest.raises(TypeError, match="message' or 'match"):
        retry_if_exception_message()

    with pytest.raises(TypeError, match="either 'message' or 'match'"):
        retry_if_exception_message(message="rate limited", match="rate")


def test_retry_if_not_exception_message() -> None:
    cond = retry_if_not_exception_message(message="do not retry")
    assert cond(Exception("transient")) is True
    assert cond(Exception("do not retry")) is False


def test_retry_if_exception_cause_type() -> None:
    root = ValueError("root")
    middle = RuntimeError("middle")
    middle.__cause__ = root
    top = Exception("top")
    top.__cause__ = middle
    cond = retry_if_exception_cause_type(exception_types=ValueError)
    assert cond(top) is True
    assert cond(RuntimeError("no cause")) is False


def test_retry_unless_exception_type() -> None:
    cond = retry_unless_exception_type(exception_types=(ValueError, KeyError))
    assert cond(RuntimeError("retry")) is True
    assert cond(ValueError("stop")) is False


def test_retry_named_combinators() -> None:
    combined_any = retry_any(
        retry_if_exception_type(ValueError),
        retry_if_exception_message(message="retry me"),
    )
    combined_all = retry_all(
        retry_if_exception_type(ValueError),
        retry_if_exception_message(match="retry"),
    )

    assert combined_any(ValueError("other")) is True
    assert combined_any(RuntimeError("retry me")) is True
    assert combined_any(RuntimeError("stop")) is False
    assert combined_all(ValueError("retry now")) is True
    assert combined_all(ValueError("stop")) is False


def test_retry_operator_sugar_matches_named_combinators() -> None:
    by_type = retry_if_exception_type(ValueError)
    by_message = retry_if_exception_message(message="retry me")

    via_operator = by_type | by_message
    via_named = retry_any(by_type, by_message)
    assert via_operator(ValueError("other")) == via_named(ValueError("other"))
    assert via_operator(RuntimeError("retry me")) == via_named(RuntimeError("retry me"))
    assert via_operator(RuntimeError("stop")) == via_named(RuntimeError("stop"))

    via_operator_all = by_type & retry_if_exception_message(match="retry")
    via_named_all = retry_all(by_type, retry_if_exception_message(match="retry"))
    assert via_operator_all(ValueError("retry")) == via_named_all(ValueError("retry"))
    assert via_operator_all(ValueError("stop")) == via_named_all(ValueError("stop"))


def test_retry_operator_grouping_is_left_associative() -> None:
    combined = (
        retry_if_exception_type(ValueError)
        | retry_if_exception_message(message="retry")
        | retry_if_exception_cause_type(TypeError)
    )

    caused = RuntimeError("outer")
    caused.__cause__ = TypeError("root")

    assert combined(ValueError("other")) is True
    assert combined(RuntimeError("retry")) is True
    assert combined(caused) is True
    assert combined(RuntimeError("stop")) is False


def test_retry_always_and_retry_never() -> None:
    always = retry_always()
    never = retry_never()
    assert always(Exception("anything")) is True
    assert never(Exception("anything")) is False
    assert (always & retry_if_exception_type(Exception))(Exception("anything")) is True
    assert (never | retry_if_exception_type(ValueError))(ValueError("bad")) is True


# ---------------------------------------------------------------------------
# Wait strategies
# ---------------------------------------------------------------------------


def test_wait_fixed() -> None:
    w = wait_fixed(wait=3.5)
    assert w(0) == 3.5
    assert w(5) == 3.5


def test_wait_none() -> None:
    assert wait_none()(0) == 0


def test_wait_exponential() -> None:
    w = wait_exponential(multiplier=1.0, exp_base=2.0, max=100.0, min=0.0)
    assert w(0) == 1.0
    assert w(1) == 2.0
    assert w(2) == 4.0
    assert w(3) == 8.0


def test_wait_exponential_floor() -> None:
    w = wait_exponential(multiplier=0.5, exp_base=2.0, max=100.0, min=2.0)
    assert w(0) == 2.0
    assert w(1) == 2.0


def test_wait_exponential_cap() -> None:
    w = wait_exponential(multiplier=1.0, exp_base=10.0, max=50.0, min=0.0)
    assert w(2) == 50.0


def test_wait_incrementing() -> None:
    w = wait_incrementing(start=1.0, increment=0.5, max=2.0)
    assert w(0) == 1.0
    assert w(1) == 1.5
    assert w(2) == 2.0
    assert w(10) == 2.0


def test_wait_incrementing_never_returns_negative_delay() -> None:
    w = wait_incrementing(start=-5.0, increment=-1.0, max=10.0)
    assert w(0) == 0.0
    assert w(3) == 0.0


def test_wait_random_range() -> None:
    w = wait_random(min=1.0, max=5.0)
    for _ in range(20):
        val = w(0)
        assert 1.0 <= val <= 5.0


def test_wait_random_deterministic_with_seed() -> None:
    w = wait_random(min=0.0, max=10.0)
    assert w(0, seed=42) == w(0, seed=42)


def test_wait_exponential_jitter_base() -> None:
    w = wait_exponential_jitter(initial=1.0, exp_base=2.0, max=100.0, jitter=0.0)
    assert w(0) == 1.0
    assert w(1) == 2.0
    assert w(2) == 4.0


def test_wait_exponential_jitter_adds_jitter() -> None:
    w = wait_exponential_jitter(initial=1.0, exp_base=2.0, max=100.0, jitter=1.0)
    for attempt in range(5):
        base = min(1.0 * 2.0**attempt, 100.0)
        val = w(attempt, seed=attempt)
        assert base <= val <= base + 1.0


def test_wait_exponential_jitter_deterministic() -> None:
    w = wait_exponential_jitter(initial=1.0, exp_base=2.0, max=60.0, jitter=1.0)
    assert w(3, seed=99) == w(3, seed=99)


def test_wait_exponential_jitter_respects_max_after_jitter() -> None:
    w = wait_exponential_jitter(initial=10.0, exp_base=2.0, max=10.0, jitter=5.0)
    for seed in range(10):
        assert w(3, seed=seed) <= 10.0


def test_wait_random_exponential_range() -> None:
    w = wait_random_exponential(multiplier=1.0, exp_base=2.0, max=100.0, min=0.0)
    for attempt in range(6):
        val = w(attempt, seed=attempt)
        assert 0.0 <= val <= min(1.0 * 2.0**attempt, 100.0)


def test_wait_random_exponential_deterministic_with_seed() -> None:
    w = wait_random_exponential(multiplier=0.5, exp_base=3.0, max=50.0, min=0.0)
    assert w(4, seed=17) == w(4, seed=17)


def test_wait_random_exponential_respects_minimum() -> None:
    w = wait_random_exponential(multiplier=0.5, exp_base=2.0, max=100.0, min=2.0)
    assert w(0, seed=11) == 2.0
    assert 2.0 <= w(4, seed=12) <= 8.0


def test_wait_chain_sequence() -> None:
    w = wait_chain(wait_fixed(1), wait_fixed(2), wait_fixed(5))
    assert w(0) == 1.0
    assert w(1) == 2.0
    assert w(2) == 5.0


def test_wait_chain_repeats_last() -> None:
    w = wait_chain(wait_fixed(1), wait_fixed(10))
    assert w(0) == 1.0
    assert w(1) == 10.0
    assert w(2) == 10.0
    assert w(99) == 10.0


def test_wait_chain_requires_strategies() -> None:
    with pytest.raises(ValueError, match="at least one"):
        wait_chain()


def test_wait_combine_adds_delays() -> None:
    combined = wait_combine(
        wait_fixed(1.5), wait_incrementing(start=1.0, increment=1.0)
    )
    assert combined(0) == 2.5
    assert combined(2) == 4.5


def test_wait_operator_sugar_matches_wait_combine() -> None:
    left = wait_fixed(1.0)
    right = wait_random(min=0.0, max=1.0)
    via_operator = left + right
    via_named = wait_combine(left, right)
    assert via_operator(3, seed=42) == via_named(3, seed=42)


def test_wait_sum_support() -> None:
    combined = cast(Any, sum([wait_fixed(1.0), wait_fixed(2.0), wait_none()]))
    assert combined(0) == 3.0


def test_wait_combine_preserves_seeded_determinism() -> None:
    combined = wait_random(min=0.0, max=1.0) + wait_random_exponential(
        multiplier=0.5,
        exp_base=2.0,
        max=10.0,
        min=0.0,
    )
    assert combined(3, seed=123) == combined(3, seed=123)


def test_wait_full_jitter_alias() -> None:
    alias = wait_full_jitter(multiplier=1.0, exp_base=2.0, max=20.0, min=0.0)
    direct = wait_random_exponential(multiplier=1.0, exp_base=2.0, max=20.0, min=0.0)
    assert alias(4, seed=99) == direct(4, seed=99)


# ---------------------------------------------------------------------------
# Stop conditions
# ---------------------------------------------------------------------------


def test_stop_after_attempt() -> None:
    s = stop_after_attempt(max_attempt_number=3)
    assert s(2, 0.0) is False
    assert s(3, 0.0) is True
    assert s(4, 0.0) is True


def test_stop_after_delay() -> None:
    s = stop_after_delay(10.0)
    assert s(1, 9.9) is False
    assert s(1, 10.0) is True
    assert s(1, 15.0) is True


def test_stop_any_and_stop_all() -> None:
    any_stop = stop_any(stop_after_attempt(3), stop_after_delay(10.0))
    all_stop = stop_all(stop_after_attempt(3), stop_after_delay(10.0))

    assert any_stop(3, 1.0) is True
    assert any_stop(1, 10.0) is True
    assert any_stop(1, 1.0) is False

    assert all_stop(3, 10.0) is True
    assert all_stop(3, 1.0) is False


def test_stop_operator_sugar_matches_named_combinators() -> None:
    by_attempt = stop_after_attempt(3)
    by_delay = stop_after_delay(10.0)
    assert (by_attempt | by_delay)(3, 1.0) == stop_any(by_attempt, by_delay)(3, 1.0)
    assert (by_attempt & by_delay)(3, 10.0) == stop_all(by_attempt, by_delay)(3, 10.0)


def test_stop_never() -> None:
    assert stop_never()(999, 999.0) is False


def test_stop_before_delay_uses_upcoming_sleep_inside_retry_policy() -> None:
    policy = retry_policy(
        wait=wait_fixed(0.5),
        stop=stop_before_delay(5.0),
    )

    assert policy.next(4.4, 1, Exception("retry")) == 0.5
    assert policy.next(4.5, 1, Exception("retry")) is None


# ---------------------------------------------------------------------------
# RetryPolicy composition
# ---------------------------------------------------------------------------


def test_retry_policy_defaults() -> None:
    p = retry_policy()
    err = Exception("fail")
    assert type(p).__name__ == "_ComposableRetryPolicy"
    assert p.next(0.0, 0, err) == 5.0
    assert p.next(0.0, 1, err) == 5.0
    assert p.next(0.0, 2, err) == 5.0
    assert p.next(0.0, 3, err) is None


def test_retry_policy_constructor() -> None:
    p = retry_policy(
        wait=wait_fixed(wait=1),
        stop=stop_after_attempt(max_attempt_number=3),
    )
    err = Exception("fail")
    assert p.next(0.0, 0, err) == 1.0
    assert p.next(0.0, 2, err) == 1.0
    assert p.next(0.0, 3, err) is None


def test_retry_policy_with_retry_condition() -> None:
    p = retry_policy(
        retry=retry_if_exception_type(exception_types=ValueError),
        stop=stop_after_attempt(max_attempt_number=5),
    )
    assert p.next(0.0, 0, ValueError("bad")) == 5.0
    assert p.next(0.0, 0, RuntimeError("bad")) is None


def test_retry_policy_with_wait_strategy() -> None:
    p = retry_policy(
        wait=wait_exponential(multiplier=1.0, exp_base=2.0, max=100.0, min=0.0),
        stop=stop_after_attempt(max_attempt_number=5),
    )
    assert p.next(0.0, 0, Exception()) == 1.0
    assert p.next(0.0, 1, Exception()) == 2.0
    assert p.next(0.0, 2, Exception()) == 4.0


def test_retry_policy_with_random_exponential_wait() -> None:
    p = retry_policy(
        wait=wait_random_exponential(multiplier=1.0, exp_base=2.0, max=100.0, min=0.0),
        stop=stop_after_attempt(max_attempt_number=5),
    )
    a = p.next(0.0, 2, Exception(), seed=42)
    b = p.next(0.0, 2, Exception(), seed=42)
    assert a == b
    assert a is not None
    assert 0.0 <= a <= 4.0


def test_retry_policy_stop_after_delay() -> None:
    p = retry_policy(
        wait=wait_fixed(1),
        stop=stop_after_delay(10),
    )
    assert p.next(9.0, 100, Exception()) == 1.0
    assert p.next(10.0, 100, Exception()) is None


def test_retry_policy_stop_before_delay_stops_using_next_sleep() -> None:
    p = retry_policy(
        wait=wait_incrementing(start=1.0, increment=1.0, max=10.0),
        stop=stop_before_delay(5.0),
    )
    assert p.next(2.0, 1, Exception("retry")) == 2.0
    assert p.next(2.0, 2, Exception("retry")) is None


def test_retry_policy_seed_forwarded() -> None:
    p = retry_policy(
        wait=wait_random(min=0, max=10),
        stop=stop_after_attempt(max_attempt_number=5),
    )
    a = p.next(0.0, 0, Exception(), seed=42)
    b = p.next(0.0, 0, Exception(), seed=42)
    assert a == b


def test_retry_policy_retry_none_retries_all() -> None:
    p = retry_policy(retry=None, stop=stop_after_attempt(max_attempt_number=2))
    assert p.next(0.0, 0, ValueError("a")) == 5.0
    assert p.next(0.0, 0, RuntimeError("b")) == 5.0


def test_retry_policy_all_three_composed() -> None:
    p = retry_policy(
        retry=retry_if_exception_type(exception_types=ConnectionError),
        wait=wait_exponential(multiplier=0.5, exp_base=3.0, max=50.0, min=0.0),
        stop=stop_after_attempt(max_attempt_number=3),
    )
    err = ConnectionError("refused")
    assert p.next(0.0, 0, err) == 0.5
    assert p.next(0.0, 1, err) == 1.5
    assert p.next(0.0, 2, err) == 4.5
    assert p.next(0.0, 3, err) is None
    assert p.next(0.0, 0, ValueError("bad")) is None


def test_retry_policy_with_operator_composition() -> None:
    retry = retry_if_exception_type(ValueError) | retry_if_exception_message(
        message="retry"
    )
    wait = wait_fixed(1.0) + wait_none()
    stop = stop_after_attempt(3) | stop_before_delay(10.0)
    policy = retry_policy(retry=retry, wait=wait, stop=stop)

    assert policy.next(0.0, 1, ValueError("other")) == 1.0
    assert policy.next(0.0, 1, RuntimeError("retry")) == 1.0
    assert policy.next(0.0, 1, RuntimeError("stop")) is None


def test_composable_retry_policy_is_no_longer_public() -> None:
    assert not hasattr(retry_policy_module, "ComposableRetryPolicy")


# ---------------------------------------------------------------------------
# Legacy policies — deprecation warnings
# ---------------------------------------------------------------------------


def test_ConstantDelayRetryPolicy_emits_deprecation_warning() -> None:
    with pytest.warns(
        DeprecationWarning, match="ConstantDelayRetryPolicy is deprecated"
    ):
        ConstantDelayRetryPolicy()


def test_ExponentialBackoffRetryPolicy_emits_deprecation_warning() -> None:
    with pytest.warns(
        DeprecationWarning, match="ExponentialBackoffRetryPolicy is deprecated"
    ):
        ExponentialBackoffRetryPolicy()


# ---------------------------------------------------------------------------
# Legacy policies — factory function behavior
# ---------------------------------------------------------------------------


@pytest.mark.filterwarnings("ignore::DeprecationWarning")
def test_ConstantDelayRetryPolicy_next() -> None:
    delay = 4.2
    p = ConstantDelayRetryPolicy(maximum_attempts=5, delay=delay)
    assert type(p).__name__ == "_ComposableRetryPolicy"
    assert p.next(elapsed_time=0.0, attempts=4, error=Exception()) == delay
    assert p.next(elapsed_time=0.0, attempts=5, error=Exception()) is None
    assert p.next(elapsed_time=0.0, attempts=999, error=Exception()) is None


@pytest.mark.filterwarnings("ignore::DeprecationWarning")
def test_ExponentialBackoffRetryPolicy_next_basic() -> None:
    p = ExponentialBackoffRetryPolicy(
        initial_delay=1.0, multiplier=2.0, max_delay=100.0, jitter=False
    )
    assert type(p).__name__ == "_ComposableRetryPolicy"
    err = Exception()
    assert p.next(elapsed_time=0.0, attempts=0, error=err) == 1.0
    assert p.next(elapsed_time=0.0, attempts=1, error=err) == 2.0
    assert p.next(elapsed_time=0.0, attempts=2, error=err) == 4.0
    assert p.next(elapsed_time=0.0, attempts=3, error=err) == 8.0


@pytest.mark.filterwarnings("ignore::DeprecationWarning")
def test_ExponentialBackoffRetryPolicy_max_delay_cap() -> None:
    p = ExponentialBackoffRetryPolicy(
        initial_delay=1.0, multiplier=10.0, max_delay=50.0, jitter=False
    )
    assert p.next(elapsed_time=0.0, attempts=2, error=Exception()) == 50.0
    assert p.next(elapsed_time=0.0, attempts=5, error=Exception()) is None


@pytest.mark.filterwarnings("ignore::DeprecationWarning")
def test_ExponentialBackoffRetryPolicy_gives_up() -> None:
    p = ExponentialBackoffRetryPolicy(maximum_attempts=3, jitter=False)
    err = Exception()
    assert p.next(elapsed_time=0.0, attempts=2, error=err) is not None
    assert p.next(elapsed_time=0.0, attempts=3, error=err) is None
    assert p.next(elapsed_time=0.0, attempts=999, error=err) is None


@pytest.mark.filterwarnings("ignore::DeprecationWarning")
def test_ExponentialBackoffRetryPolicy_jitter() -> None:
    p = ExponentialBackoffRetryPolicy(
        initial_delay=1.0, multiplier=2.0, max_delay=100.0, jitter=True
    )
    err = Exception()
    for attempt in range(5):
        computed = min(1.0 * 2.0**attempt, 100.0)
        delay = p.next(elapsed_time=0.0, attempts=attempt, error=err, seed=attempt)
        assert delay is not None
        assert 0 <= delay <= computed


@pytest.mark.filterwarnings("ignore::DeprecationWarning")
def test_ExponentialBackoffRetryPolicy_jitter_deterministic() -> None:
    """Same seed must produce the same delay on every call (DBOS replay determinism)."""
    p = ExponentialBackoffRetryPolicy(
        initial_delay=1.0, multiplier=2.0, max_delay=100.0, jitter=True
    )
    err = Exception()
    for attempt in range(5):
        seed = (
            int(
                hashlib.sha256(f"run-abc:my_step:{attempt + 1}".encode()).hexdigest(),
                16,
            )
            & 0xFFFF_FFFF
        )
        first = p.next(elapsed_time=0.0, attempts=attempt, error=err, seed=seed)
        second = p.next(elapsed_time=0.0, attempts=attempt, error=err, seed=seed)
        assert first == second


@pytest.mark.filterwarnings("ignore::DeprecationWarning")
def test_ExponentialBackoffRetryPolicy_no_jitter() -> None:
    p = ExponentialBackoffRetryPolicy(
        initial_delay=0.5, multiplier=3.0, max_delay=100.0, jitter=False
    )
    err = Exception()
    assert p.next(elapsed_time=0.0, attempts=0, error=err) == 0.5
    assert p.next(elapsed_time=0.0, attempts=1, error=err) == 1.5
    assert p.next(elapsed_time=0.0, attempts=2, error=err) == 4.5
    assert p.next(elapsed_time=0.0, attempts=3, error=err) == 13.5


# ---------------------------------------------------------------------------
# E2E — retry policies still work through the full workflow
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
@pytest.mark.filterwarnings("ignore::DeprecationWarning")
async def test_retry_e2e() -> None:
    class CountEvent(Event):
        """Empty event to signal a step to increment a counter in the Context."""

    class DummyWorkflow(Workflow):
        @step(retry_policy=ConstantDelayRetryPolicy(delay=0.2, maximum_attempts=4))
        async def flaky_step(self, ctx: Context, ev: StartEvent) -> StopEvent:
            count = await ctx.store.get("counter", default=0)
            ctx.send_event(CountEvent())
            if count < 3:
                raise ValueError("Something bad happened!")
            return StopEvent(result="All good!")

        @step
        async def counter(self, ctx: Context, ev: CountEvent) -> None:
            count = await ctx.store.get("counter", default=0)
            await ctx.store.set("counter", count + 1)

    res = await WorkflowTestRunner(DummyWorkflow(disable_validation=True)).run()
    assert res.result == "All good!"


@pytest.mark.asyncio
@pytest.mark.filterwarnings("ignore::DeprecationWarning")
async def test_retry_e2e_exponential() -> None:
    class CountEvent(Event):
        """Event to increment a counter in the Context."""

    class DummyWorkflow(Workflow):
        @step(
            retry_policy=ExponentialBackoffRetryPolicy(
                initial_delay=0.05, multiplier=2.0, maximum_attempts=4, jitter=False
            )
        )
        async def flaky_step(self, ctx: Context, ev: StartEvent) -> StopEvent:
            count = await ctx.store.get("counter", default=0)
            ctx.send_event(CountEvent())
            if count < 3:
                raise ValueError("Something bad happened!")
            return StopEvent(result="All good!")

        @step
        async def counter(self, ctx: Context, ev: CountEvent) -> None:
            count = await ctx.store.get("counter", default=0)
            await ctx.store.set("counter", count + 1)

    res = await WorkflowTestRunner(DummyWorkflow(disable_validation=True)).run()
    assert res.result == "All good!"


# ---------------------------------------------------------------------------
# E2E — RetryPolicy through the full workflow
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_retry_e2e_retry_policy() -> None:
    class CountEvent(Event):
        pass

    class DummyWorkflow(Workflow):
        @step(
            retry_policy=retry_policy(
                retry=retry_if_exception_type(exception_types=ValueError),
                wait=wait_fixed(wait=0.1),
                stop=stop_after_attempt(max_attempt_number=4),
            )
        )
        async def flaky_step(self, ctx: Context, ev: StartEvent) -> StopEvent:
            count = await ctx.store.get("counter", default=0)
            ctx.send_event(CountEvent())
            if count < 3:
                raise ValueError("transient failure")
            return StopEvent(result="recovered")

        @step
        async def counter(self, ctx: Context, ev: CountEvent) -> None:
            count = await ctx.store.get("counter", default=0)
            await ctx.store.set("counter", count + 1)

    res = await WorkflowTestRunner(DummyWorkflow(disable_validation=True)).run()
    assert res.result == "recovered"


@pytest.mark.asyncio
async def test_retry_e2e_retry_policy_non_retryable() -> None:
    class DummyWorkflow(Workflow):
        @step(
            retry_policy=retry_policy(
                retry=retry_if_exception_type(exception_types=ConnectionError),
                wait=wait_fixed(wait=0.1),
                stop=stop_after_attempt(max_attempt_number=5),
            )
        )
        async def always_fails(self, ev: StartEvent) -> StopEvent:
            raise ValueError("permanent failure")

    with pytest.raises(ValueError, match="permanent failure"):
        await WorkflowTestRunner(DummyWorkflow(disable_validation=True)).run()


@pytest.mark.asyncio
async def test_retry_e2e_composed_policy() -> None:
    class DummyWorkflow(Workflow):
        attempts = 0

        @step(
            retry_policy=retry_policy(
                retry=retry_if_exception_type(ValueError)
                | retry_if_exception_message(message="retry me"),
                wait=wait_fixed(0.01) + wait_none(),
                stop=stop_after_attempt(4) | stop_never(),
            )
        )
        async def flaky_step(self, ev: StartEvent) -> StopEvent:
            self.attempts += 1
            if self.attempts < 4:
                raise ValueError("retry me")
            return StopEvent(result="recovered")

    res = await WorkflowTestRunner(DummyWorkflow(disable_validation=True)).run()
    assert res.result == "recovered"
