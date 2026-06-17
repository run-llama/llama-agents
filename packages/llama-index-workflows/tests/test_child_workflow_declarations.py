# SPDX-License-Identifier: MIT
# Copyright (c) 2026 LlamaIndex Inc.

from __future__ import annotations

import inspect
import pickle
from typing import Any, cast

import pytest
from workflows import Workflow, step
from workflows.errors import WorkflowValidationError
from workflows.events import StartEvent, StopEvent
from workflows.runtime.types.internal_state import BrokerState
from workflows.runtime.types.step_id import StepId


class BareChild(Workflow):
    @step
    async def run_child(self, ev: StartEvent) -> StopEvent:
        return StopEvent(result="bare")


class LegacyManualParent(Workflow):
    child: BareChild

    def __init__(self) -> None:
        super().__init__()
        self.child = BareChild()

    @step
    async def run_parent(self, ev: StartEvent) -> StopEvent:
        return StopEvent(result="parent")


def test_legacy_annotated_manual_composition_is_not_auto_attached() -> None:
    parent = LegacyManualParent()

    assert parent.child_workflows == {}
    assert isinstance(parent.child, BareChild)


class InheritedLegacyManualParent(LegacyManualParent):
    pass


def test_inherited_legacy_manual_composition_is_not_auto_attached() -> None:
    parent = _construct_without_static_signature(InheritedLegacyManualParent)

    assert parent.child_workflows == {}
    assert isinstance(parent.child, BareChild)


class ChildStart(StartEvent):
    pass


class ChildStop(StopEvent):
    pass


class TypedChild(Workflow):
    @step
    async def run_child(self, ev: ChildStart) -> ChildStop:
        return ChildStop()


class TypedParent(Workflow):
    child: TypedChild

    @step
    async def start(self, ev: StartEvent) -> ChildStart:
        return ChildStart()

    @step
    async def finish(self, ev: ChildStop) -> StopEvent:
        return StopEvent(result="done")


def test_typed_child_annotation_still_synthesizes_constructor_slot() -> None:
    parent = TypedParent(child=TypedChild())

    assert isinstance(parent.child_workflows["child"], TypedChild)


def test_synthesized_constructor_signature_includes_child_and_config() -> None:
    signature = inspect.signature(TypedParent)

    assert "child" in signature.parameters
    assert "timeout" in signature.parameters
    assert "runtime" in signature.parameters


def _construct_without_static_signature(cls: type[Any]) -> Any:
    return cls()


def test_missing_synthesized_child_slot_fails_at_construction() -> None:
    with pytest.raises(WorkflowValidationError, match="Missing child workflow"):
        _construct_without_static_signature(TypedParent)


class CustomInitTypedParent(Workflow):
    child: TypedChild

    def __init__(self) -> None:
        super().__init__()

    @step
    async def start(self, ev: StartEvent) -> ChildStart:
        return ChildStart()

    @step
    async def finish(self, ev: ChildStop) -> StopEvent:
        return StopEvent(result="done")


def test_missing_custom_init_typed_child_slot_fails_at_construction() -> None:
    with pytest.raises(WorkflowValidationError, match="Missing child workflow"):
        CustomInitTypedParent()


def test_class_body_child_instance_is_rejected() -> None:
    class SharedDefaultParent(Workflow):
        child: TypedChild = TypedChild()

        @step
        async def start(self, ev: StartEvent) -> ChildStart:
            return ChildStart()

        @step
        async def finish(self, ev: ChildStop) -> StopEvent:
            return StopEvent(result="done")

    with pytest.raises(WorkflowValidationError, match="shared class-body"):
        SharedDefaultParent()


class WithPlainAnnotation(Workflow):
    retries: int = 3

    @step
    async def run_step(self, ev: StartEvent) -> StopEvent:
        return StopEvent(result="done")


def test_non_child_annotations_are_not_constructor_fields() -> None:
    with pytest.raises(TypeError):
        WithPlainAnnotation(retries=5)  # type: ignore[call-arg]


def test_pickled_broker_state_with_legacy_worker_keys_normalizes_on_load() -> None:
    state = BrokerState.from_workflow(WithPlainAnnotation())
    state.workers = cast(
        Any, {str(step_id): worker for step_id, worker in state.workers.items()}
    )

    recovered = pickle.loads(pickle.dumps(state))

    assert set(recovered.workers) == {StepId.root("run_step")}


def test_pickled_broker_state_with_legacy_config_keys_normalizes_on_load() -> None:
    state = BrokerState.from_workflow(WithPlainAnnotation())
    state.workers = cast(
        Any, {str(step_id): worker for step_id, worker in state.workers.items()}
    )
    object.__setattr__(
        state.config,
        "steps",
        cast(
            Any,
            {str(step_id): config for step_id, config in state.config.steps.items()},
        ),
    )

    recovered = pickle.loads(pickle.dumps(state))

    assert set(recovered.workers) == {StepId.root("run_step")}
    assert set(recovered.config.steps) == {StepId.root("run_step")}
    assert set(recovered.deepcopy().config.steps) == {StepId.root("run_step")}


def test_old_pickled_broker_state_without_namespace_started_recovers() -> None:
    state = BrokerState.from_workflow(WithPlainAnnotation())
    state.__dict__.pop("namespace_started")

    recovered = pickle.loads(pickle.dumps(state))

    assert recovered.namespace_started == {}
    assert recovered.deepcopy().namespace_started == {}


def test_broker_state_string_worker_keys_parse_namespaces() -> None:
    state = BrokerState.from_workflow(WithPlainAnnotation())
    worker = next(iter(state.workers.values()))

    recovered = BrokerState(
        is_running=True,
        config=state.config,
        workers=cast(Any, {"child/run_step": worker}),
    )

    assert set(recovered.workers) == {StepId(("child",), "run_step")}
