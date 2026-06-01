# SPDX-License-Identifier: MIT
# Copyright (c) 2026 LlamaIndex Inc.

import inspect
from typing import (
    Any,
    AsyncGenerator,
    AsyncIterator,
    List,
    get_type_hints,
)

import pytest
from workflows.context import Context
from workflows.decorators import step
from workflows.errors import WorkflowValidationError
from workflows.events import Event, StartEvent, StopEvent
from workflows.utils import (
    StepSignatureSpec,
    _event_list_element_types,
    _flatten_return_annotation,
    _get_param_types,
    _get_return_types,
    get_steps_from_class,
    get_steps_from_instance,
    inspect_signature,
    is_free_function,
    validate_step_signature,
)

from .conftest import (  # type: ignore[import]
    AnotherTestEvent,
    OneTestEvent,
)


def test_validate_step_signature_of_method() -> None:
    def f(self, ev: OneTestEvent) -> OneTestEvent:  # noqa: ANN001
        return OneTestEvent()

    validate_step_signature(inspect_signature(f))


def test_validate_step_signature_of_free_function() -> None:
    def f(ev: OneTestEvent) -> OneTestEvent:
        return OneTestEvent()

    validate_step_signature(inspect_signature(f))


def test_validate_step_signature_union() -> None:
    def f(ev: OneTestEvent | AnotherTestEvent) -> OneTestEvent:
        return OneTestEvent()

    validate_step_signature(inspect_signature(f))


def test_validate_step_signature_of_free_function_with_context() -> None:
    def f(ctx: Context, ev: OneTestEvent) -> OneTestEvent:
        return OneTestEvent()

    validate_step_signature(inspect_signature(f))


def test_validate_step_signature_union_invalid() -> None:
    def f(ev: OneTestEvent | str) -> None:
        pass

    with pytest.raises(
        WorkflowValidationError,
        match="Step signature must have at least one parameter annotated as type Event",
    ):
        validate_step_signature(inspect_signature(f))


def test_validate_step_signature_no_params() -> None:
    def f() -> None:
        pass

    with pytest.raises(
        WorkflowValidationError, match="Step signature must have at least one parameter"
    ):
        validate_step_signature(inspect_signature(f))


def test_validate_step_signature_no_annotations() -> None:
    def f(self, ev) -> None:  # noqa: ANN001
        pass

    with pytest.raises(
        WorkflowValidationError,
        match="Step signature must have at least one parameter annotated as type Event",
    ):
        validate_step_signature(inspect_signature(f))


def test_validate_step_signature_wrong_annotations() -> None:
    def f(self, ev: str) -> None:  # noqa: ANN001
        pass

    with pytest.raises(
        WorkflowValidationError,
        match="Step signature must have at least one parameter annotated as type Event",
    ):
        validate_step_signature(inspect_signature(f))


def test_validate_step_signature_no_return_annotations() -> None:
    def f(self, ev: OneTestEvent):  # noqa: ANN001
        pass

    with pytest.raises(
        WorkflowValidationError,
        match="Return types of workflows step functions must be annotated with their type",
    ):
        validate_step_signature(inspect_signature(f))


def test_validate_step_signature_no_events() -> None:
    def f(self, ctx: Context) -> None:  # noqa: ANN001
        pass

    with pytest.raises(
        WorkflowValidationError,
        match="Step signature must have at least one parameter annotated as type Event",
    ):
        validate_step_signature(inspect_signature(f))


def test_validate_step_signature_multiple_single_event_params_is_collect_mode() -> None:
    # More than one single-event parameter is now a valid heterogeneous fan-in
    # (collect-mode) signature, not a validation error.
    def f1(self, ev: OneTestEvent, foo: AnotherTestEvent) -> StopEvent:  # noqa: ANN001
        return StopEvent()

    def f2(ev: OneTestEvent, foo: AnotherTestEvent) -> StopEvent:
        return StopEvent()

    # Neither should raise.
    validate_step_signature(inspect_signature(f1))
    validate_step_signature(inspect_signature(f2))


def test_validate_step_signature_union_collect_param_rejected() -> None:
    # A union-typed parameter alongside another event parameter is rejected:
    # collect-mode parameters must each name a single event type.
    def f(ev: OneTestEvent, foo: AnotherTestEvent | OneTestEvent) -> StopEvent:
        return StopEvent()

    with pytest.raises(WorkflowValidationError, match="single event type"):
        validate_step_signature(inspect_signature(f))


def test_get_steps_from() -> None:
    class Test:
        @step
        def start(self, start: StartEvent) -> OneTestEvent:
            return OneTestEvent()

        @step
        def my_method(self, event: OneTestEvent) -> StopEvent:
            return StopEvent()

        def not_a_step(self) -> None:
            pass

    steps = get_steps_from_class(Test)
    assert len(steps)
    assert "my_method" in steps

    steps = get_steps_from_instance(Test())
    assert len(steps)
    assert "my_method" in steps


def test_get_param_types() -> None:
    def f(foo: str) -> None:
        pass

    sig = inspect.signature(f)
    type_hints = get_type_hints(f)
    res = _get_param_types(sig.parameters["foo"], type_hints)
    assert len(res) == 1
    assert res[0] is str


def test_get_param_types_no_annotations() -> None:
    def f(foo) -> None:  # noqa: ANN001
        pass

    sig = inspect.signature(f)
    type_hints = get_type_hints(f)
    res = _get_param_types(sig.parameters["foo"], type_hints)
    assert len(res) == 1
    assert res[0] is Any


def test_get_param_types_union() -> None:
    def f(foo: str | int) -> None:
        pass

    sig = inspect.signature(f)
    type_hints = get_type_hints(f)
    res = _get_param_types(sig.parameters["foo"], type_hints)
    assert len(res) == 2
    assert res == [str, int]


def test_get_return_types() -> None:
    def f(foo: int) -> str:
        return ""

    assert _get_return_types(f) == [str]


def test_get_return_types_union() -> None:
    def f(foo: int) -> str | int:
        return ""

    assert _get_return_types(f) == [str, int]


def test_get_return_types_optional() -> None:
    def f(foo: int) -> str | None:
        return ""

    assert _get_return_types(f) == [str]


def test_get_return_types_list() -> None:
    # list[E] is flattened to its element type for workflow validation and
    # graph representation.
    def f(foo: int) -> list[str]:
        return [""]

    assert _get_return_types(f) == [str]


def test_is_free_function() -> None:
    assert is_free_function("my_function") is True
    assert is_free_function("MyClass.my_method") is False
    assert is_free_function("some_function.<locals>.my_function") is True
    assert is_free_function("some_function.<locals>.MyClass.my_function") is False
    with pytest.raises(ValueError):
        is_free_function("")


def test_inspect_signature_raises_if_not_callable() -> None:
    with pytest.raises(TypeError, match="Expected a callable object, got str"):
        inspect_signature("foo")  # type: ignore


class _EventA(Event):
    pass


class _EventB(Event):
    pass


def test_return_type_list_is_flattened() -> None:
    def f(ev: StartEvent) -> list[_EventA]:
        return [_EventA()]

    spec = inspect_signature(f)
    assert spec.return_types == [_EventA]


def test_async_iterator_return_is_rejected() -> None:
    async def f(ev: StartEvent) -> AsyncIterator[_EventA]:
        yield _EventA()

    with pytest.raises(WorkflowValidationError, match="Async-iterator fan-out"):
        inspect_signature(f)


def test_async_generator_return_is_rejected() -> None:
    async def f(ev: StartEvent) -> AsyncGenerator[_EventA, None]:
        yield _EventA()

    with pytest.raises(WorkflowValidationError, match="Async-iterator fan-out"):
        inspect_signature(f)


def test_return_type_list_of_union_is_flattened() -> None:
    def f(ev: StartEvent) -> list[_EventA | _EventB]:
        return [_EventA()]

    spec = inspect_signature(f)
    assert spec.return_types == [_EventA, _EventB]


def test_return_type_optional_list_strips_none() -> None:
    def f(ev: StartEvent) -> list[_EventA] | None:
        return None

    spec = inspect_signature(f)
    assert spec.return_types == [_EventA]


def test_return_type_bare_event_unchanged() -> None:
    def f(ev: StartEvent) -> _EventA:
        return _EventA()

    spec = inspect_signature(f)
    assert spec.return_types == [_EventA]


def test_return_type_bare_none_reports_nonetype() -> None:
    def f(ev: StartEvent) -> None:
        return None

    spec = inspect_signature(f)
    assert spec.return_types == [type(None)]


def test_validate_step_signature_accepts_list_return() -> None:
    def f(ev: StartEvent) -> list[_EventA]:
        return [_EventA()]

    spec = inspect_signature(f)
    # Should not raise: list[E] flattens to a real event return type.
    validate_step_signature(spec)


def test_validate_step_signature_rejects_stream_param_with_other_events() -> None:
    # A list[E] collection collect parameter cannot be combined with additional event
    # params.
    spec = StepSignatureSpec(
        accepted_events={"a": [StartEvent], "b": [StopEvent]},
        return_types=[StopEvent],
        context_parameter=None,
        context_state_type=None,
        resources=[],
        collection_collect_param=("a", (StartEvent,)),
        collection_collect=None,
        is_fan_out=False,
    )
    with pytest.raises(WorkflowValidationError, match="cannot be combined with other"):
        validate_step_signature(spec)


def test_event_list_element_types_union_without_event_members_returns_none() -> None:
    # A list[Union[...]] whose members are not Event subclasses is not an event-list.
    assert _event_list_element_types(list[int | str]) is None


def test_event_list_element_types_mixed_members_returns_none() -> None:
    # A list[Union[Event, non-event]] is rejected: not all members are events.
    assert _event_list_element_types(list[StartEvent | int]) is None


def test_event_list_element_types_pure_event_list_is_recognized() -> None:
    assert _event_list_element_types(list[StartEvent]) == [StartEvent]


def test_flatten_return_annotation_unparameterized_collection_returns_empty() -> None:
    # A collection origin with no type args (bare ``typing.List``) flattens to no types.
    assert _flatten_return_annotation(List) == []
