# SPDX-License-Identifier: MIT
# Copyright (c) 2026 LlamaIndex Inc.

from __future__ import annotations

import asyncio
import gc
import threading
import weakref
from pathlib import Path
from typing import Any

import pytest
from dbos import DBOS, DBOSConfig, Queue
from llama_agents.dbos import DBOSRuntime
from llama_agents.server import WorkflowServer
from workflows.decorators import step
from workflows.errors import WorkflowCancelledByUser
from workflows.events import StartEvent, StopEvent, WorkflowCancelledEvent
from workflows.workflow import Workflow


class _AdmissionGate:
    def __init__(self) -> None:
        self._condition = threading.Condition()
        self._release = threading.Event()
        self.started: list[str] = []

    async def enter(self, run: str) -> None:
        with self._condition:
            self.started.append(run)
            self._condition.notify_all()
        await asyncio.to_thread(self._release.wait)

    def wait_for_started(self, count: int, timeout: float = 5.0) -> bool:
        with self._condition:
            return self._condition.wait_for(
                lambda: len(self.started) >= count,
                timeout=timeout,
            )

    def release(self) -> None:
        self._release.set()


def _dbos_config(db_path: Path, name: str) -> DBOSConfig:
    return {
        "name": name,
        "system_database_url": (
            f"sqlite+pysqlite:///{db_path}?check_same_thread=false"
        ),
        "run_admin_server": False,
    }  # type: ignore[return-value]


def _blocking_workflow_type(
    gate: _AdmissionGate,
    *,
    class_name: str,
) -> type[Workflow]:
    class BlockingWorkflow(Workflow):
        @step
        async def block(self, ev: StartEvent) -> StopEvent:
            run = ev.get("run")
            assert isinstance(run, str)
            await gate.enter(run)
            return StopEvent(result=run)

    BlockingWorkflow.__name__ = class_name
    BlockingWorkflow.__qualname__ = class_name
    return BlockingWorkflow


def test_runtime_creates_stable_unlimited_workflow_queue() -> None:
    runtime = DBOSRuntime(polling_interval_sec=0.125)
    workflow = _blocking_workflow_type(
        _AdmissionGate(), class_name="UnlimitedQueueWorkflow"
    )(
        runtime=runtime,
        workflow_name="tests.dbos.unlimited-queue",
    )

    assert len(runtime.workflow_queues) == 1
    queue = runtime.workflow_queues[0]
    assert queue.name == ("_llamaindex_workflow_queue:tests.dbos.unlimited-queue")
    assert queue.worker_concurrency is None
    assert queue.polling_interval_sec == 0.125

    runtime.track_workflow(workflow)
    assert runtime.workflow_queues == (queue,)


def test_runtime_creates_stable_limited_workflow_queue() -> None:
    runtime = DBOSRuntime(polling_interval_sec=0.25)
    _blocking_workflow_type(_AdmissionGate(), class_name="LimitedQueueWorkflow")(
        runtime=runtime,
        workflow_name="tests.dbos.limited-queue",
        num_concurrent_runs=3,
    )

    assert len(runtime.workflow_queues) == 1
    queue = runtime.workflow_queues[0]
    assert queue.name == "_llamaindex_workflow_queue:tests.dbos.limited-queue"
    assert queue.worker_concurrency == 3
    assert queue.concurrency is None
    assert queue.polling_interval_sec == 0.25


def test_untrack_releases_exclusive_prelaunch_queue() -> None:
    runtime = DBOSRuntime()
    workflow = _blocking_workflow_type(
        _AdmissionGate(), class_name="UntrackedQueueWorkflow"
    )(
        runtime=runtime,
        workflow_name="tests.dbos.untracked-queue",
    )
    queue = runtime.workflow_queues[0]

    runtime.untrack_workflow(workflow)

    assert runtime.workflow_queues == ()
    replacement_runtime = DBOSRuntime()
    _blocking_workflow_type(
        _AdmissionGate(), class_name="ReplacementUntrackedQueueWorkflow"
    )(
        runtime=replacement_runtime,
        workflow_name="tests.dbos.untracked-queue",
        num_concurrent_runs=1,
    )
    assert replacement_runtime.workflow_queues == (queue,)
    assert queue.worker_concurrency == 1
    asyncio.run(replacement_runtime.destroy())


def test_workflow_alias_does_not_change_declared_queue() -> None:
    runtime = DBOSRuntime()
    workflow = _blocking_workflow_type(
        _AdmissionGate(), class_name="RenamedQueueWorkflow"
    )(
        runtime=runtime,
        workflow_name="tests.dbos.rename-original",
    )
    original_queue = runtime.workflow_queues[0]
    workflow._switch_workflow_name("tests.dbos.rename-collision")
    assert runtime.workflow_queues == (original_queue,)
    assert original_queue.name == (
        "_llamaindex_workflow_queue:tests.dbos.rename-original"
    )


def test_server_alias_before_launch_keeps_dbos_queue_name() -> None:
    runtime = DBOSRuntime()
    workflow = _blocking_workflow_type(
        _AdmissionGate(), class_name="PrelaunchServerAliasWorkflow"
    )(
        runtime=runtime,
        workflow_name="tests.dbos.stable-registration-name",
    )
    server = WorkflowServer(
        runtime=runtime.build_server_runtime(),
        workflow_store=runtime.create_workflow_store(),
    )

    server.add_workflow("route-alias", workflow)

    assert workflow.workflow_name == "route-alias"
    assert runtime.workflow_queues[0].name == (
        "_llamaindex_workflow_queue:tests.dbos.stable-registration-name"
    )


def test_runtime_rejects_queue_declared_by_application() -> None:
    queue_name = "_llamaindex_workflow_queue:tests.dbos.application-queue"
    Queue(queue_name)
    runtime = DBOSRuntime()
    _blocking_workflow_type(_AdmissionGate(), class_name="ApplicationQueueWorkflow")(
        runtime=runtime,
        workflow_name="tests.dbos.application-queue",
    )

    with pytest.raises(RuntimeError, match="DBOS rejected workflow queue"):
        runtime.workflow_queues


def test_runtime_rejects_duplicate_name_with_conflicting_limits() -> None:
    runtime = DBOSRuntime()
    workflow_type = _blocking_workflow_type(
        _AdmissionGate(), class_name="DuplicateNameWorkflow"
    )
    workflow_type(
        runtime=runtime,
        workflow_name="tests.dbos.duplicate-name",
    )

    with pytest.raises(RuntimeError, match="conflicting num_concurrent_runs"):
        workflow_type(
            runtime=runtime,
            workflow_name="tests.dbos.duplicate-name",
            num_concurrent_runs=2,
        )


def test_equivalent_workflows_share_queue_with_instance_registrations() -> None:
    runtime = DBOSRuntime()
    workflow_type = _blocking_workflow_type(
        _AdmissionGate(), class_name="SharedNameWorkflow"
    )
    first = workflow_type(
        runtime=runtime,
        workflow_name="tests.dbos.shared-name",
        num_concurrent_runs=2,
    )
    second = workflow_type(
        runtime=runtime,
        workflow_name="tests.dbos.shared-name",
        num_concurrent_runs=2,
    )

    assert len(runtime.workflow_queues) == 1
    assert runtime.register(first) is not runtime.register(second)


def test_destroy_reuses_queue_across_limit_transitions(tmp_path: Path) -> None:
    workflow_type = _blocking_workflow_type(
        _AdmissionGate(), class_name="ReusedQueueWorkflow"
    )
    workflow_name = "tests.dbos.reused-queue"
    config = _dbos_config(
        tmp_path / "reused-queue.sqlite3",
        "dbos-reused-queue-test",
    )
    DBOS(config=config)
    first_runtime = DBOSRuntime()
    workflow_type(runtime=first_runtime, workflow_name=workflow_name)
    queue = first_runtime.workflow_queues[0]
    asyncio.run(first_runtime.destroy())

    DBOS(config=config)
    second_runtime = DBOSRuntime()
    workflow_type(
        runtime=second_runtime,
        workflow_name=workflow_name,
        num_concurrent_runs=1,
    )
    assert second_runtime.workflow_queues == (queue,)
    assert queue.worker_concurrency == 1
    asyncio.run(second_runtime.destroy())

    DBOS(config=config)
    third_runtime = DBOSRuntime()
    workflow_type(runtime=third_runtime, workflow_name=workflow_name)
    assert third_runtime.workflow_queues == (queue,)
    assert queue.worker_concurrency is None
    asyncio.run(third_runtime.destroy())


def test_destroy_without_dbos_retains_queue_ownership() -> None:
    workflow_type = _blocking_workflow_type(
        _AdmissionGate(), class_name="ExternallyOwnedQueueWorkflow"
    )
    workflow_name = "tests.dbos.externally-owned-queue"
    owner_runtime = DBOSRuntime()
    workflow_type(runtime=owner_runtime, workflow_name=workflow_name)
    owner_runtime.workflow_queues

    asyncio.run(owner_runtime.destroy(destroy_dbos=False))

    other_runtime = DBOSRuntime()
    workflow_type(
        runtime=other_runtime,
        workflow_name=workflow_name,
        num_concurrent_runs=1,
    )
    with pytest.raises(RuntimeError, match="already declared"):
        other_runtime.workflow_queues
    asyncio.run(owner_runtime.destroy())


def test_destroyed_queue_does_not_retain_runtime() -> None:
    runtime = DBOSRuntime()
    workflow_type = _blocking_workflow_type(
        _AdmissionGate(), class_name="ReleasedOwnerQueueWorkflow"
    )
    workflow_type(
        runtime=runtime,
        workflow_name="tests.dbos.released-owner-queue",
    )
    runtime_ref = weakref.ref(runtime)

    asyncio.run(runtime.destroy())
    del runtime
    gc.collect()

    assert runtime_ref() is None


def test_launch_accepts_runtime_workflow_queues(tmp_path: Path) -> None:
    DBOS(
        config=_dbos_config(
            tmp_path / "accepted-listener.sqlite3",
            "dbos-accepted-listener-test",
        )
    )
    runtime = DBOSRuntime(polling_interval_sec=0.01)
    workflow = _blocking_workflow_type(
        _AdmissionGate(), class_name="AcceptedListenerWorkflow"
    )(
        runtime=runtime,
        workflow_name="tests.dbos.accepted-listener",
    )
    DBOS.listen_queues(list(runtime.workflow_queues))

    try:
        runtime.launch_sync()
        assert not workflow._runtime_locked
    finally:
        asyncio.run(runtime.destroy())


def test_late_registration_joins_default_queue_listener(tmp_path: Path) -> None:
    DBOS(
        config=_dbos_config(
            tmp_path / "late-listener.sqlite3",
            "dbos-late-listener-test",
        )
    )
    runtime = DBOSRuntime(polling_interval_sec=0.01)
    first_type = _blocking_workflow_type(
        _AdmissionGate(), class_name="InitialListenerWorkflow"
    )
    first_type(runtime=runtime, workflow_name="tests.dbos.initial-listener")
    try:
        runtime.launch_sync()
        late_type = _blocking_workflow_type(
            _AdmissionGate(), class_name="LateListenerWorkflow"
        )
        late_workflow = late_type(
            runtime=runtime,
            workflow_name="tests.dbos.late-listener",
        )
        assert runtime.get_registered(late_workflow) is not None
        assert any(
            queue.name == "_llamaindex_workflow_queue:tests.dbos.late-listener"
            for queue in runtime.workflow_queues
        )
    finally:
        asyncio.run(runtime.destroy())


async def _run_server_rename_after_launch(tmp_path: Path) -> None:
    DBOS(
        config=_dbos_config(
            tmp_path / "server-rename.sqlite3",
            "dbos-server-rename-test",
        )
    )
    runtime = DBOSRuntime(polling_interval_sec=0.01)
    workflow_type = _blocking_workflow_type(
        _AdmissionGate(), class_name="ServerRenamedWorkflow"
    )
    workflow = workflow_type(runtime=runtime)
    original_queue = runtime.workflow_queues[0]

    try:
        await runtime.launch()
        server = WorkflowServer(
            runtime=runtime.build_server_runtime(),
            workflow_store=runtime.create_workflow_store(),
        )
        server.add_workflow("server-alias", workflow)

        assert workflow.workflow_name == "server-alias"
        assert runtime.workflow_queues == (original_queue,)
        assert runtime.get_registered(workflow) is not None
    finally:
        await runtime.destroy()


def test_server_can_rename_workflow_after_dbos_launch(tmp_path: Path) -> None:
    asyncio.run(_run_server_rename_after_launch(tmp_path))


async def _run_admission_cases(
    tmp_path: Path,
) -> None:
    name = "workflow-admission"
    DBOS(config=_dbos_config(tmp_path / f"{name}.sqlite3", name))
    runtime = DBOSRuntime(polling_interval_sec=0.01)
    limited_gate = _AdmissionGate()
    unlimited_gate = _AdmissionGate()
    limited_type = _blocking_workflow_type(
        limited_gate,
        class_name="LimitedAdmissionWorkflow",
    )
    unlimited_type = _blocking_workflow_type(
        unlimited_gate,
        class_name="UnlimitedAdmissionWorkflow",
    )
    limited_workflow = limited_type(
        runtime=runtime,
        workflow_name="tests.dbos.limited-admission",
        num_concurrent_runs=1,
        timeout=10,
    )
    unlimited_workflow = unlimited_type(
        runtime=runtime,
        workflow_name="tests.dbos.unlimited-admission",
        timeout=10,
    )
    handlers: list[Any] = []

    try:
        await runtime.launch()
        handlers = [
            limited_workflow.run(run="limited-first", run_id="limited-first"),
            limited_workflow.run(run="limited-second", run_id="limited-second"),
            unlimited_workflow.run(
                run="unlimited-first",
                run_id="unlimited-first",
            ),
            unlimited_workflow.run(
                run="unlimited-second",
                run_id="unlimited-second",
            ),
        ]

        limited_admitted = await asyncio.to_thread(
            limited_gate.wait_for_started,
            1,
        )
        unlimited_admitted = await asyncio.to_thread(
            unlimited_gate.wait_for_started,
            2,
        )
        assert limited_admitted
        assert unlimited_admitted
        assert len(limited_gate.started) == 1
        assert limited_gate.started[0] in {"limited-first", "limited-second"}
        assert set(unlimited_gate.started) == {
            "unlimited-first",
            "unlimited-second",
        }

        unstarted_run = (
            "limited-second"
            if limited_gate.started == ["limited-first"]
            else "limited-first"
        )
        status = await DBOS.get_workflow_status_async(unstarted_run)
        assert status is not None
        assert status.status == "ENQUEUED"

        limited_gate.release()
        unlimited_gate.release()
        results = await asyncio.wait_for(
            asyncio.gather(*handlers),
            timeout=10,
        )
        assert results == [
            "limited-first",
            "limited-second",
            "unlimited-first",
            "unlimited-second",
        ]
    finally:
        limited_gate.release()
        unlimited_gate.release()
        if handlers:
            await asyncio.gather(*handlers, return_exceptions=True)
        await runtime.destroy()


def test_limited_and_unlimited_worker_admission(
    tmp_path: Path,
) -> None:
    asyncio.run(_run_admission_cases(tmp_path))


async def _run_queued_cancellation_case(tmp_path: Path) -> None:
    DBOS(
        config=_dbos_config(
            tmp_path / "queued-cancellation.sqlite3",
            "dbos-queued-cancellation-test",
        )
    )
    runtime = DBOSRuntime(polling_interval_sec=0.01)
    gate = _AdmissionGate()

    class QueuedCancellationWorkflow(Workflow):
        @step
        async def block(self, ev: StartEvent) -> StopEvent:
            run = ev.get("run")
            assert isinstance(run, str)
            await gate.enter(run)
            # Keep the admitted run active while the control loop reduces the
            # cancellation tick that was stored before admission.
            await asyncio.sleep(0.2)
            return StopEvent(result=run)

    workflow = QueuedCancellationWorkflow(
        runtime=runtime,
        workflow_name="tests.dbos.queued-cancellation",
        num_concurrent_runs=1,
        timeout=10,
    )
    handlers: dict[str, Any] = {}

    try:
        await runtime.launch()
        handlers = {
            run_id: workflow.run(run=run_id, run_id=run_id)
            for run_id in ("cancel-first", "cancel-second")
        }
        admitted = await asyncio.to_thread(gate.wait_for_started, 1)
        assert admitted
        queued_run = (
            "cancel-second" if gate.started == ["cancel-first"] else "cancel-first"
        )
        queued_handler = handlers[queued_run]

        await queued_handler._external_adapter.cancel()

        status = await DBOS.get_workflow_status_async(queued_run)
        assert status is not None
        assert status.status == "ENQUEUED"
        gate.release()
        await handlers[gate.started[0]]
        with pytest.raises(WorkflowCancelledByUser):
            await queued_handler
        assert isinstance(queued_handler.get_stop_event(), WorkflowCancelledEvent)
    finally:
        gate.release()
        if handlers:
            await asyncio.gather(*handlers.values(), return_exceptions=True)
        await runtime.destroy()


def test_enqueued_cancellation_waits_for_admission(tmp_path: Path) -> None:
    asyncio.run(_run_queued_cancellation_case(tmp_path))
