# SPDX-License-Identifier: MIT
# Copyright (c) 2026 LlamaIndex Inc.

from __future__ import annotations

import collections.abc as cabc
import inspect
import secrets
import string
from typing import (
    TYPE_CHECKING,
    Annotated,
    Any,
    Callable,
    Optional,
    cast,
    get_args,
    get_origin,
    get_type_hints,
)

if TYPE_CHECKING:
    from workflows.decorators import StepFunction

from types import UnionType
from typing import Union

from pydantic import BaseModel

from .collect import All, Collect, Take
from .errors import WorkflowValidationError
from .events import Event, EventType
from .resource import ResourceDefinition, ResourceDescriptor

BUSY_WAIT_DELAY = 0.01


class StepSignatureSpec(BaseModel):
    """A Pydantic model representing the signature of a step function or method."""

    accepted_events: dict[str, list[EventType]]
    return_types: list[Any]
    context_parameter: str | None
    context_state_type: Any | None
    resources: list[Any]
    # Collection-stream fan-in: ``(parameter_name, element_event_types)`` when
    # the step declares a single ``list[E]`` collect parameter, else None. The
    # element types are a tuple — ``list[Done]`` -> ``(Done,)``; a union flat list
    # ``list[A | B]`` -> ``(A, B)``.
    collection_param: tuple[str, tuple[Any, ...]] | None = None
    # The resolved ``Collect`` marker for the collection parameter. A bare
    # ``list[E]`` parameter resolves to ``Collect()`` (``All`` cardinality).
    # None for steps without a collection parameter.
    collection_policy: Any | None = None
    # Fan-out producer: True when the return annotation is ``list[E]``.
    is_fan_out: bool = False
    # Non-list members of a fan-out return union (``-> list[A] | B`` -> [B]).
    # A bare return of one of these types is ordinary dispatch; any other bare
    # event under a list-returning annotation is a runtime error.
    bare_return_types: list[Any] = []


def inspect_signature(
    fn: Callable, localns: dict[str, Any] | None = None
) -> StepSignatureSpec:
    """
    Given a function, ensure the signature is compatible with a workflow step.

    Args:
        fn (Callable): The function to inspect.

    Returns:
        StepSignatureSpec: A specification object containing:
            - accepted_events: Dictionary mapping parameter names to their event types
            - return_types: List of return type annotations
            - context_parameter: Name of the context parameter if present

    Raises:
        TypeError: If fn is not a callable object

    """
    if not callable(fn):
        raise TypeError(f"Expected a callable object, got {type(fn).__name__}")

    sig = inspect.signature(fn)
    type_hints = _resolve_type_hints(fn, include_extras=True, localns=localns)
    _reject_async_iterator_return(fn, type_hints.get("return"))

    accepted_events: dict[str, list[EventType]] = {}
    context_parameter = None
    context_state_type = None
    resources = []
    collection_param: tuple[str, tuple[Any, ...]] | None = None
    collection_policy: Collect | None = None

    # Inspect function parameters
    for name, t in sig.parameters.items():
        # Ignore self and cls
        if name in ("self", "cls"):
            continue

        annotation = type_hints.get(name, t.annotation)

        # Handle Context[StateType] annotations
        if get_origin(annotation) is not None:
            origin = get_origin(annotation)
            args = get_args(annotation)

            # Check if this is Context[StateType]
            if hasattr(origin, "__name__") and origin.__name__ == "Context":
                context_parameter = name
                # Extract state type from generic parameter
                if args:
                    context_state_type = args[0]
                continue

        # Handle Annotated types: resource descriptors and Collect markers. A
        # ``Collect`` marker unwraps to the inner ``list[E]`` annotation and is
        # carried forward to the collection-collect handling below; everything else
        # (unknown metadata) is ignored.
        collect_marker: Collect | None = None
        if get_origin(annotation) is Annotated:
            args = get_args(annotation)
            type_annotation = args[0] if args else None
            descriptor = next(
                (a for a in args[1:] if isinstance(a, ResourceDescriptor)), None
            )
            if descriptor is not None:
                # Pass localns to resource for nested annotation resolution
                descriptor.set_localns(localns)
                resources.append(
                    ResourceDefinition(
                        name=name, resource=descriptor, type_annotation=type_annotation
                    )
                )
                continue
            collect_marker = next((a for a in args[1:] if isinstance(a, Collect)), None)
            if collect_marker is None:
                # Unknown Annotated metadata — ignore as before.
                continue
            annotation = type_annotation

        # Get name and type of the Context param (without state type)
        if getattr(annotation, "__name__", None) == "Context":
            context_parameter = name
            continue

        # Collection-stream fan-in: a ``list[E]`` parameter (e.g.
        # ``events: list[Done]``) is a collect step. It buffers incoming ``E`` by
        # stream id and releases per its ``Collect`` cardinality. ``E`` may be a
        # union (``list[A | B]``) — every member type routes to the step. A bare
        # ``list[E]`` is exactly ``Annotated[list[E], Collect(All())]``.
        element_types = _event_list_element_types(annotation)
        if element_types is not None:
            marker = collect_marker if collect_marker is not None else Collect()
            _validate_collect_marker(marker, name)
            if collection_param is not None:
                msg = (
                    "A step may declare at most one list[E] collection parameter, "
                    f"but found both {collection_param[0]!r} and {name!r}. "
                    "Multi-slot collects are not supported."
                )
                raise WorkflowValidationError(msg)
            collection_param = (name, tuple(element_types))
            collection_policy = marker
            accepted_events[name] = list(element_types)
            continue
        if collect_marker is not None:
            msg = (
                f"Parameter {name!r} carries a Collect marker but is not annotated "
                "as list[E] of an Event subclass. Collect markers apply only to "
                "collection fan-in (list[Event]) parameters."
            )
            raise WorkflowValidationError(msg)

        # Collect name and types of the event param
        param_types = _get_param_types(t, type_hints)
        if all(
            param_t == Event
            or (inspect.isclass(param_t) and issubclass(param_t, Event))
            for param_t in param_types
        ):
            accepted_events[name] = param_types
            continue

    return StepSignatureSpec(
        accepted_events=accepted_events,
        return_types=_get_return_types(fn, localns=localns),
        context_parameter=context_parameter,
        context_state_type=context_state_type,
        resources=resources,
        collection_param=collection_param,
        collection_policy=collection_policy,
        is_fan_out=_is_fan_out_return(fn, localns=localns),
        bare_return_types=_bare_return_types(fn, localns=localns),
    )


def _event_list_element_types(annotation: Any) -> list[Any] | None:
    """Element event types of a ``list[E]`` annotation, or None if not one.

    ``list[Done]`` -> ``[Done]``; ``list[A | B]`` -> ``[A, B]`` (the single arg is
    itself a union). ``None`` is dropped. Returns None when ``annotation`` is not
    a ``list`` of ``Event`` subclasses (so the caller falls through to other
    parameter handling). Order is preserved; duplicates are removed.

    ``Optional[list[E]]`` / ``list[E] | None`` unwraps to the inner ``list[E]``,
    mirroring the fan-out *return* side (which already unwraps Optional) so a
    collect parameter declared optional is recognized rather than rejected with a
    misleading "no Event parameter" error.
    """
    if get_origin(annotation) in (Union, UnionType):
        members = [a for a in get_args(annotation) if a is not type(None)]
        list_members = [a for a in members if get_origin(a) is list]
        # Only unwrap the unambiguous ``list[E] | None`` shape: exactly one list
        # member and no other non-None members.
        if len(list_members) == 1 and len(members) == 1:
            annotation = list_members[0]
    if get_origin(annotation) is not list:
        return None

    element_types: list[Any] = []
    for arg in get_args(annotation):
        if get_origin(arg) in (Union, UnionType):
            members = [a for a in get_args(arg) if a is not type(None)]
        else:
            members = [arg]
        for member in members:
            if member not in element_types:
                element_types.append(member)
    if not element_types:
        return None
    if all(
        et is Event or (inspect.isclass(et) and issubclass(et, Event))
        for et in element_types
    ):
        return element_types
    return None


def _validate_collect_marker(marker: Collect, name: str) -> None:
    """Reject Collect cardinalities outside the supported contract."""
    if not isinstance(marker.cardinality, (All, Take)):
        raise WorkflowValidationError(
            f"Collect cardinality on {name!r} must be All() or Take(n). "
            "Other cardinalities are not supported."
        )


def validate_step_signature(spec: StepSignatureSpec) -> None:
    """
    Validate that a step signature specification meets workflow requirements.

    Args:
        spec (StepSignatureSpec): The signature specification to validate.

    Raises:
        WorkflowValidationError: If the signature is invalid for a workflow step.

    """
    num_of_events = len(spec.accepted_events)
    if num_of_events == 0:
        msg = "Step signature must have at least one parameter annotated as type Event"
        raise WorkflowValidationError(msg)
    if spec.collection_param is not None and num_of_events > 1:
        # A list[E] collection parameter alongside other event params is a
        # multi-slot collect, which this contract does not support.
        msg = (
            "A list[E] collection parameter cannot be combined with other "
            "event parameters. Use a single list[E] parameter, or multiple "
            "single-event parameters for a multi-slot join."
        )
        raise WorkflowValidationError(msg)
    if num_of_events > 1:
        # Collect-mode (multi-slot fan-in): a step with more than one
        # event-shaped parameter fires once every declared slot is filled. Each
        # parameter must name exactly one concrete event type; union-typed
        # collect parameters (e.g. ``x: A | B``) are rejected.
        for name, param_types in spec.accepted_events.items():
            if len(param_types) != 1:
                msg = (
                    "Collect-mode steps (more than one event parameter) require "
                    f"each event parameter to declare a single event type, but "
                    f"parameter {name!r} declares {len(param_types)}. Union-typed "
                    "collect parameters are not supported."
                )
                raise WorkflowValidationError(msg)

    if not spec.return_types:
        msg = "Return types of workflows step functions must be annotated with their type."
        raise WorkflowValidationError(msg)


def get_steps_from_class(_class: object) -> dict[str, StepFunction]:
    """
    Given a class, return the list of its methods that were defined as steps.

    Args:
        _class (object): The class to inspect for step methods.

    Returns:
        dict[str, Callable]: A dictionary mapping step names to their corresponding methods.

    """
    from workflows.decorators import StepFunction

    step_methods: dict[str, StepFunction] = {}
    all_methods = inspect.getmembers(_class, predicate=inspect.isfunction)

    for name, method in all_methods:
        if hasattr(method, "_step_config"):
            step_methods[name] = cast(StepFunction, method)

    return step_methods


def get_steps_from_instance(workflow: object) -> dict[str, StepFunction]:
    """
    Given a workflow instance, return the list of its methods that were defined as steps.

    Args:
        workflow (object): The workflow instance to inspect.

    Returns:
        dict[str, Callable]: A dictionary mapping step names to their corresponding methods.

    """
    from workflows.decorators import StepFunction

    step_methods: dict[str, StepFunction] = {}
    all_methods = inspect.getmembers(workflow, predicate=inspect.ismethod)

    for name, method in all_methods:
        if hasattr(method, "_step_config"):
            step_methods[name] = cast(StepFunction, method)

    return step_methods


def _get_param_types(param: inspect.Parameter, type_hints: dict) -> list[Any]:
    """
    Extract and process the types of a parameter.

    This helper function handles Union and Optional types, returning a list of the actual types.
    For Union[A, None] (Optional[A]), it returns [A].

    Args:
        param (inspect.Parameter): The parameter to analyze.
        type_hints (dict): The resolved type hints for the function.

    Returns:
        list[Any]: A list of extracted types, excluding None from Unions/Optionals.

    """
    typ = type_hints.get(param.name, param.annotation)
    if typ is inspect.Parameter.empty:
        return [Any]
    if get_origin(typ) in (Union, Optional, UnionType):
        return [t for t in get_args(typ) if t is not type(None)]
    return [typ]


# Return-annotation origins whose single element type describes emitted events.
# A step that returns ``list[E]`` declares produced events for validation and
# graph representation. Async-iterator returns are rejected at decoration.
_COLLECTION_RETURN_ORIGINS = (list,)

# Async-iterator return origins, rejected at decoration with a pointer to
# ``list[E]``.
_ASYNC_ITERATOR_RETURN_ORIGINS = (
    cabc.AsyncIterator,
    cabc.AsyncIterable,
    cabc.AsyncGenerator,
)


def _flatten_return_annotation(hint: Any) -> list[Any]:
    """Flatten a return annotation into the set of produced event types.

    Unwraps unions, ``Optional`` (``None`` is dropped), and the
    emission-collection wrappers in ``_COLLECTION_RETURN_ORIGINS`` (``list[E]``).
    The element ``E`` may itself be a union, which is flattened recursively.
    ``NoneType`` never appears in the result; order is preserved and duplicates
    are removed.
    """
    if hint is type(None):
        return []

    origin = get_origin(hint)
    if origin in (Union, UnionType):
        # Optional is Union[type, None] so it's covered here.
        result: list[Any] = []
        for arg in get_args(hint):
            for t in _flatten_return_annotation(arg):
                if t not in result:
                    result.append(t)
        return result

    if origin in _COLLECTION_RETURN_ORIGINS:
        args = get_args(hint)
        if not args:
            return []
        # list[E] -> E
        return _flatten_return_annotation(args[0])

    return [hint]


def _get_return_types(
    func: Callable, localns: dict[str, Any] | None = None
) -> list[Any]:
    """
    Extract the return type hints from a function.

    Handles Union, Optional, and the ``list[E]`` emission collection, which is
    unwrapped to the event types it emits for validation and graph
    representation.
    """
    type_hints = _resolve_type_hints(func, localns=localns)
    return_hint = type_hints.get("return")
    if return_hint is None:
        return []

    flattened = _flatten_return_annotation(return_hint)
    # Preserve the historical contract that a bare ``-> None`` reports
    # ``[NoneType]`` (a truthy, annotated return) rather than an empty list,
    # which validation treats as a missing annotation.
    if not flattened:
        return [type(None)]
    return flattened


def _reject_async_iterator_return(func: Callable, return_hint: Any) -> None:
    """Reject async-iterator fan-out returns.

    A step that is an async generator, or annotates its return as
    ``AsyncIterator[E]`` / ``AsyncIterable[E]`` / ``AsyncGenerator[E, None]``,
    The supported fan-out contract is a finite ``list[E]`` return. Async
    iterators imply streaming member delivery, which the runtime does not model.
    Producers should return ``list[E]`` or use ``ctx.send_event`` for ordinary
    dispatch.
    """
    is_async_gen = inspect.isasyncgenfunction(
        getattr(func, "__func__", func)
    ) or inspect.isasyncgenfunction(func)
    origin = get_origin(return_hint)
    if is_async_gen or origin in _ASYNC_ITERATOR_RETURN_ORIGINS:
        raise WorkflowValidationError(
            "Async-iterator fan-out (AsyncIterator[E] / AsyncGenerator[E, None] / "
            "async generator steps) is not supported. Return list[E] for a "
            "static collection, or emit incrementally with ctx.send_event."
        )


def _is_fan_out_return(func: Callable, localns: dict[str, Any] | None = None) -> bool:
    """True if the return annotation emits a finite collection stream.

    A fan-out producer returns ``list[E]``. ``Optional[list[E]]``
    (``list[E] | None``) also counts — it may emit a stream or nothing. A plain
    single-event or union-of-single-events return is NOT fan-out.
    """
    type_hints = _resolve_type_hints(func, localns=localns)
    return_hint = type_hints.get("return")
    if return_hint is None:
        return False
    return _return_hint_is_fan_out(return_hint)


def _return_hint_is_fan_out(hint: Any) -> bool:
    if hint is type(None):
        return False
    origin = get_origin(hint)
    if origin in (Union, UnionType):
        # Optional / unions: fan-out if any non-None member is a collection.
        return any(
            _return_hint_is_fan_out(arg)
            for arg in get_args(hint)
            if arg is not type(None)
        )
    return origin in _COLLECTION_RETURN_ORIGINS


def _bare_return_types(
    func: Callable, localns: dict[str, Any] | None = None
) -> list[Any]:
    """Non-list members of the return annotation.

    For ``-> list[A] | B`` this is ``[B]``: a bare ``B`` return from a fan-out
    step is ordinary dispatch rather than stream emission. A pure ``-> list[A]``
    has none, so any bare event return is a runtime error there. ``NoneType``
    is dropped (a ``None`` return is the no-emission path).
    """
    type_hints = _resolve_type_hints(func, localns=localns)
    return_hint = type_hints.get("return")
    if return_hint is None:
        return []
    return _flatten_bare_members(return_hint)


def _flatten_bare_members(hint: Any) -> list[Any]:
    if hint is type(None):
        return []
    origin = get_origin(hint)
    if origin in (Union, UnionType):
        result: list[Any] = []
        for arg in get_args(hint):
            for t in _flatten_bare_members(arg):
                if t not in result:
                    result.append(t)
        return result
    if origin in _COLLECTION_RETURN_ORIGINS:
        return []
    return [hint]


def _resolve_type_hints(
    func: Callable,
    *,
    include_extras: bool = False,
    localns: dict[str, Any] | None = None,
) -> dict[str, Any]:
    try:
        return get_type_hints(func, include_extras=include_extras, localns=localns)
    except NameError as exc:
        missing_name = getattr(exc, "name", None)
        missing_msg = f" Missing name: {missing_name}." if missing_name else ""
        func_name = getattr(func, "__qualname__", type(func).__name__)
        msg = (
            "Failed to resolve type annotations for "
            f"{func_name}.{missing_msg} "
            "If you are using 'from __future__ import annotations' or string "
            "annotations, ensure referenced names are available in module scope "
            "or in the scope where the @step decorator is applied."
        )
        raise WorkflowValidationError(msg) from exc


def is_free_function(qualname: str) -> bool:
    """
    Determines whether a certain qualified name points to a free function.

    A free function is either a module-level function or a nested function.
    This implementation follows PEP-3155 for handling nested function detection.

    Args:
        qualname (str): The qualified name to analyze.

    Returns:
        bool: True if the name represents a free function, False otherwise.

    Raises:
        ValueError: If the qualified name is empty.

    """
    if not qualname:
        msg = "The qualified name cannot be empty"
        raise ValueError(msg)

    toks = qualname.split(".")
    if len(toks) == 1:
        # e.g. `my_function`
        return True
    elif "<locals>" not in toks:
        # e.g. `MyClass.my_method`
        return False
    else:
        return toks[-2] == "<locals>"


_alphabet = string.ascii_letters + string.digits  # A-Z, a-z, 0-9


def _nanoid(size: int = 10) -> str:
    """Returns a unique identifier with the format 'kY2xP9hTnQ'."""
    return "".join(secrets.choice(_alphabet) for _ in range(size))
