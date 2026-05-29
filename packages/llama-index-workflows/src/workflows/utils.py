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

    accepted_events: dict[str, list[EventType]] = {}
    context_parameter = None
    context_state_type = None
    resources = []

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

        # Handle Annotated types for resources
        if get_origin(annotation) is Annotated:
            args = get_args(annotation)
            type_annotation = args[0] if args else None
            descriptor = args[1] if len(args) > 1 else None
            if descriptor is not None and isinstance(descriptor, ResourceDescriptor):
                # Pass localns to resource for nested annotation resolution
                descriptor.set_localns(localns)
                resources.append(
                    ResourceDefinition(
                        name=name, resource=descriptor, type_annotation=type_annotation
                    )
                )
            continue

        # Get name and type of the Context param (without state type)
        if hasattr(annotation, "__name__") and annotation.__name__ == "Context":
            context_parameter = name
            continue

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
    elif num_of_events > 1:
        msg = f"Step signature must contain exactly one parameter of type Event but found {num_of_events}."
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
# A step that returns these directly declares produced events as ``list[E]`` /
# ``AsyncIterator[E]`` etc. for validation and graph representation.
_COLLECTION_RETURN_ORIGINS = (
    list,
    cabc.AsyncIterator,
    cabc.AsyncIterable,
    cabc.AsyncGenerator,
)


def _flatten_return_annotation(hint: Any) -> list[Any]:
    """Flatten a return annotation into the set of produced event types.

    Unwraps unions, ``Optional`` (``None`` is dropped), and the
    emission-collection wrappers in ``_COLLECTION_RETURN_ORIGINS`` (``list[E]``,
    ``AsyncIterator[E]``, ``AsyncGenerator[E, None]``). The element ``E`` may
    itself be a union, which is flattened recursively. ``NoneType`` never appears
    in the result; order is preserved and duplicates are removed.
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
        # list[E] -> E; AsyncGenerator[E, None] -> E (first arg is the yield type)
        return _flatten_return_annotation(args[0])

    return [hint]


def _get_return_types(
    func: Callable, localns: dict[str, Any] | None = None
) -> list[Any]:
    """
    Extract the return type hints from a function.

    Handles Union, Optional, and emission-collection types (``list[E]``,
    ``AsyncIterator[E]``, ``AsyncGenerator[E, None]``). Collection wrappers are
    unwrapped to the event types they emit for validation and graph
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
