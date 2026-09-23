# SPDX-License-Identifier: MIT
# Copyright (c) 2026 LlamaIndex Inc.
"""Child workflows on the DBOS durable runtime.

Children run inside the parent's DBOS workflow (one control loop, one journal);
their steps are DBOS-registered under the parent by static slot path, and each
namespace persists to its own durable state row.
"""

from __future__ import annotations

from collections.abc import Generator

import pytest
from dbos import DBOS, DBOSConfig
from llama_agents.dbos import DBOSRuntime
from workflows import Context, Workflow
from workflows.decorators import step
from workflows.events import StartEvent, StopEvent
from workflows.testing import WorkflowTestRunner


@pytest.fixture(scope="module")
def dbos_config(tmp_path_factory: pytest.TempPathFactory) -> DBOSConfig:
    db_file = tmp_path_factory.mktemp("dbos") / "dbos_child_test.sqlite3"
    system_db_url = f"sqlite+pysqlite:///{db_file}?check_same_thread=false"
    return {
        "name": "workflows-dbos-child",
        "system_database_url": system_db_url,
        "run_admin_server": False,
    }  # type: ignore[return-value]


@pytest.fixture(scope="module")
def dbos_runtime(dbos_config: DBOSConfig) -> Generator[DBOSRuntime, None, None]:
    DBOS(config=dbos_config)
    runtime = DBOSRuntime(polling_interval_sec=0.01)
    try:
        yield runtime
    finally:
        runtime.destroy_sync()


class ChildStart(StartEvent):
    pass


class ChildStop(StopEvent):
    val: str = ""


class StateChild(Workflow):
    @step
    async def run_child(self, ctx: Context, ev: ChildStart) -> ChildStop:
        # Parent state must not be visible inside the child's own store.
        assert await ctx.store.get("parent_key", None) is None
        await ctx.store.set("child_key", "child-value")
        return ChildStop(val="ok")


class StateParent(Workflow):
    child: StateChild

    @step
    async def start(self, ctx: Context, ev: StartEvent) -> ChildStart:
        await ctx.store.set("parent_key", "parent-value")
        return ChildStart()

    @step
    async def finish(self, ctx: Context, ev: ChildStop) -> StopEvent:
        # Child's writes must not leak into the parent's store.
        assert await ctx.store.get("child_key", None) is None
        return StopEvent(result=await ctx.store.get("parent_key"))


@pytest.mark.asyncio
async def test_dbos_child_state_isolated_per_namespace(
    dbos_runtime: DBOSRuntime,
) -> None:
    """A parent and its child run in one DBOS workflow with isolated state."""
    with dbos_runtime.registering():
        parent = StateParent(child=StateChild())
    await dbos_runtime.launch()

    result = await WorkflowTestRunner(parent).run()
    assert result.result == "parent-value"

    # Child steps are DBOS-registered under the parent, not as a separate
    # top-level workflow.
    child = parent.child
    assert dbos_runtime.get_registered(child) is None


class GrandStart(StartEvent):
    pass


class GrandStop(StopEvent):
    val: str = ""


class StateGrandchild(Workflow):
    @step
    async def run_grand(self, ctx: Context, ev: GrandStart) -> GrandStop:
        await ctx.store.set("grand_key", "grand-value")
        return GrandStop(val="ok")


class MidChild(Workflow):
    grand: StateGrandchild

    @step
    async def run_mid(self, ctx: Context, ev: ChildStart) -> GrandStart:
        await ctx.store.set("mid_key", "mid-value")
        return GrandStart()

    @step
    async def finish_mid(self, ctx: Context, ev: GrandStop) -> ChildStop:
        assert await ctx.store.get("grand_key", None) is None
        return ChildStop(val=await ctx.store.get("mid_key"))


class GrandParent(Workflow):
    child: MidChild

    @step
    async def start(self, ctx: Context, ev: StartEvent) -> ChildStart:
        await ctx.store.set("root_key", "root-value")
        return ChildStart()

    @step
    async def finish(self, ctx: Context, ev: ChildStop) -> StopEvent:
        assert await ctx.store.get("mid_key", None) is None
        return StopEvent(result=f"{await ctx.store.get('root_key')}|{ev.val}")


@pytest.mark.asyncio
async def test_dbos_grandchild_state_isolated(dbos_runtime: DBOSRuntime) -> None:
    """Three nesting levels each persist to their own durable state row."""
    with dbos_runtime.registering():
        parent = GrandParent(child=MidChild(grand=StateGrandchild()))
    await dbos_runtime.launch()

    result = await WorkflowTestRunner(parent).run()
    assert result.result == "root-value|mid-value"

    assert dbos_runtime.get_registered(parent.child) is None
    assert dbos_runtime.get_registered(parent.child.grand) is None
