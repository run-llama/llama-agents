# SPDX-License-Identifier: MIT
# Copyright (c) 2026 LlamaIndex Inc.
"""Runtime propagation into child workflows under the server.

``add_workflow`` switches the parent onto the server runtime; the switch must
propagate into attached children (and grandchildren) so their steps resolve the
durable server adapter rather than the in-memory basic runtime they adopted at
construction. Children are *not* registered as named, routable workflows.
"""

from __future__ import annotations

from llama_agents.server import SqliteWorkflowStore, WorkflowServer
from workflows import Workflow, step
from workflows.events import StartEvent, StopEvent


class GrandStart(StartEvent):
    pass


class GrandStop(StopEvent):
    pass


class MidStart(StartEvent):
    pass


class MidStop(StopEvent):
    pass


class Grandchild(Workflow):
    @step
    async def run_grand(self, ev: GrandStart) -> GrandStop:
        return GrandStop()


class Mid(Workflow):
    grand: Grandchild

    @step
    async def begin(self, ev: MidStart) -> GrandStart:
        return GrandStart()

    @step
    async def finish(self, ev: GrandStop) -> MidStop:
        return MidStop()


class Top(Workflow):
    mid: Mid

    @step
    async def begin(self, ev: StartEvent) -> MidStart:
        return MidStart()

    @step
    async def finish(self, ev: MidStop) -> StopEvent:
        return StopEvent(result="ok")


def test_add_workflow_propagates_runtime_into_child_tree(
    sqlite_store: SqliteWorkflowStore,
) -> None:
    server = WorkflowServer(workflow_store=sqlite_store)
    top = Top(mid=Mid(grand=Grandchild()))
    server.add_workflow("top", top)

    runtime = server._service._runtime
    # Parent and every descendant report the parent's (server) runtime, not the
    # inner basic runtime they were constructed under.
    assert top.runtime is runtime
    assert top.mid.runtime is runtime
    assert top.mid.grand.runtime is runtime


def test_children_are_not_registered_as_named_workflows(
    sqlite_store: SqliteWorkflowStore,
) -> None:
    server = WorkflowServer(workflow_store=sqlite_store)
    server.add_workflow("top", Top(mid=Mid(grand=Grandchild())))

    service = server._service
    # Only the explicitly added name is routable; children/grandchildren are
    # sub-graphs, never exposed as independent workflows.
    assert service.get_workflow_names() == ["top"]
    assert service.get_workflow("Mid") is None
    assert service.get_workflow("Grandchild") is None
