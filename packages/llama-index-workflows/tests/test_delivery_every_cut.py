# SPDX-License-Identifier: MIT
# Copyright (c) 2026 LlamaIndex Inc.
"""A snapshot or journal cut at any position restores and resumes correctly.

These tests record real runs on the in-memory runtime, fold every prefix of the
recorded tick journal, resume each fold through ``Context.from_dict``, and check
the run's result and how many times each event was consumed.
"""

from __future__ import annotations

import asyncio
from collections.abc import Awaitable, Callable
from typing import Any

import pytest
from workflows import Context, Workflow
from workflows.context.serializers import (
    BaseSerializer,
    JsonSerializer,
    PickleSerializer,
)
from workflows.runtime.types.internal_state import BrokerState
from workflows.runtime.types.ticks import WorkflowTick

from tests.runtime.delivery_fixtures import (
    EVERY_CUT_DETERMINISTIC,
    LEDGER,
    PICKLED_DETERMINISTIC,
    EveryCutWorkflow,
    PickledWorkflow,
    SendThenWaitWorkflow,
    assert_settled,
    finished,
    fold_snapshot,
    label,
    prefix_consumption,
    record_once,
    record_with_resume,
)

Recording = tuple[BrokerState, list[WorkflowTick], Any]


async def _record_every_cut() -> tuple[Workflow, BaseSerializer, Recording]:
    wf = EveryCutWorkflow(timeout=30)
    return wf, JsonSerializer(), await record_with_resume(wf)


async def _record_pickled() -> tuple[Workflow, BaseSerializer, Recording]:
    serializer = PickleSerializer()
    wf = PickledWorkflow(timeout=30, serializer=serializer)
    return wf, serializer, await record_once(wf, serializer)


@pytest.mark.parametrize(
    ("record", "deterministic"),
    [
        pytest.param(_record_every_cut, EVERY_CUT_DETERMINISTIC, id="json-resume"),
        pytest.param(_record_pickled, PICKLED_DETERMINISTIC, id="pickle-uncomparable"),
    ],
)
async def test_every_journal_cut_resumes_with_each_output_consumed_once(
    record: Callable[[], Awaitable[tuple[Workflow, BaseSerializer, Recording]]],
    deterministic: set[tuple[str, str]],
) -> None:
    """Fold every prefix of a recorded journal, resume it, and count consumers.

    ``json-resume`` records a run with sends, returns, a fan-out,
    collect_events, a retry and a cancel and resume. ``pickle-uncomparable``
    persists its journal with the pickle serializer, carries a payload JSON
    cannot encode, and uses events whose ``__eq__`` raises.

    A cut that leaves a producer in progress after its send was routed reruns
    that producer as a new invocation, which sends a new value. So sent values
    carry a per-execution nonce: every value is consumed at most once, every
    deterministic output exactly once, and every delivery the snapshot owes is
    consumed by the resumed run.
    """
    wf, serializer, (init, ticks, expected) = await record()

    for k in range(len(ticks) + 1):
        prefix = ticks[:k]
        state, data = fold_snapshot(init, prefix, serializer)
        counts = prefix_consumption(prefix)
        owed = {label(d.event) for d in state.deliveries.values()}
        LEDGER.clear()
        if finished(prefix):
            assert_settled(data)
        else:
            handler = wf.run(ctx=Context.from_dict(wf, data, serializer=serializer))
            assert await asyncio.wait_for(handler, timeout=5) == expected, k
            assert_settled(handler.ctx.to_dict(serializer))
        undelivered = owed - {consumed for _, consumed in LEDGER}
        assert not undelivered, (k, undelivered)
        counts.update(LEDGER)

        duplicated = {key: n for key, n in counts.items() if n > 1}
        assert not duplicated, (k, duplicated)
        missing = {key for key in deterministic if counts[key] != 1}
        assert not missing, (k, missing)


async def test_send_then_wait_in_the_same_step_still_works() -> None:
    # Answer reaches ``ask`` only through its waiter, which graph validation
    # cannot see.
    wf = SendThenWaitWorkflow(timeout=10, disable_validation=True)
    result = await asyncio.wait_for(wf.run(), timeout=5)

    assert result == "answered"
