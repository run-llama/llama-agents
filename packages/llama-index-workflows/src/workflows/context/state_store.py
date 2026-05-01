# SPDX-License-Identifier: MIT
# Copyright (c) 2026 LlamaIndex Inc.

from __future__ import annotations

import asyncio
import functools
import warnings
from contextlib import asynccontextmanager
from typing import (
    TYPE_CHECKING,
    Any,
    AsyncContextManager,
    AsyncGenerator,
    Generic,
    Literal,
    Protocol,
    runtime_checkable,
)

from pydantic import BaseModel, ValidationError, model_validator
from typing_extensions import TypeVar

from workflows.decorators import StepConfig
from workflows.events import DictLikeModel

from .serializers import BaseSerializer

if TYPE_CHECKING:
    from workflows.workflow import Workflow

MAX_DEPTH = 1000

# Keys set by pre-built workflows that are known to be unserializable in some cases.
KNOWN_UNSERIALIZABLE_KEYS: tuple[str, ...] = ("memory",)


class InMemorySerializedState(BaseModel):
    """Serialized state containing actual data (from InMemoryStateStore)."""

    store_type: Literal["in_memory"] = "in_memory"
    state_type: str = "DictState"
    state_module: str = "workflows.context.state_store"
    state_data: Any = (
        None  # {"_data": {...}} for DictState, serialized string for typed
    )

    @model_validator(mode="before")
    @classmethod
    def default_store_type(cls, data: dict[str, Any]) -> dict[str, Any]:
        """Default missing store_type to 'in_memory' for backwards compatibility."""
        if isinstance(data, dict) and "store_type" not in data:
            data = {**data, "store_type": "in_memory"}
        return data


def parse_in_memory_state(
    data: dict[str, Any],
) -> InMemorySerializedState:
    """Parse raw dict into InMemorySerializedState.

    Args:
        data: Serialized state payload from InMemoryStateStore.to_dict().

    Returns:
        InMemorySerializedState if the format is recognized.

    Raises:
        ValueError: If store_type is not 'in_memory' or missing.
    """
    store_type = data.get("store_type")

    if store_type == "in_memory" or store_type is None:
        # Backwards compat: missing store_type = InMemory
        return InMemorySerializedState.model_validate(data)
    else:
        raise ValueError(
            f"Cannot parse store_type '{store_type}' as InMemorySerializedState. "
            "Use the appropriate store's from_dict() method."
        )


def serialize_dict_state_data(
    state: DictState,
    serializer: BaseSerializer,
    known_unserializable_keys: tuple[str, ...] = KNOWN_UNSERIALIZABLE_KEYS,
) -> dict[str, Any]:
    """Serialize DictState items to {"_data": {...}} format.

    Args:
        state: The DictState to serialize.
        serializer: Strategy for encoding values.
        known_unserializable_keys: Keys to skip with warning if they fail to serialize.

    Returns:
        Dict with {"_data": {...}} structure containing serialized values.

    Raises:
        ValueError: If serialization fails for a non-known-unserializable key.
    """
    serialized_data = {}
    for key, value in state.items():
        try:
            serialized_data[key] = serializer.serialize(value)
        except Exception as e:
            if key in known_unserializable_keys:
                warnings.warn(
                    f"Skipping serialization of known unserializable key: {key} -- "
                    "This is expected but will require this item to be set manually after deserialization.",
                    category=UnserializableKeyWarning,
                )
                continue
            raise ValueError(f"Failed to serialize state value for key {key}: {e}")
    return {"_data": serialized_data}


def create_in_memory_payload(
    state: BaseModel,
    serializer: BaseSerializer,
    known_unserializable_keys: tuple[str, ...] = KNOWN_UNSERIALIZABLE_KEYS,
) -> InMemorySerializedState:
    """Create InMemorySerializedState from any state model.

    Args:
        state: The Pydantic model to serialize (DictState or typed model).
        serializer: Strategy for encoding values.
        known_unserializable_keys: Keys to skip with warning (DictState only).

    Returns:
        InMemorySerializedState containing the serialized data.
    """
    if isinstance(state, DictState):
        state_data = serialize_dict_state_data(
            state, serializer, known_unserializable_keys
        )
    else:
        state_data = serializer.serialize(state)

    return InMemorySerializedState(
        state_type=type(state).__name__,
        state_module=type(state).__module__,
        state_data=state_data,
    )


def traverse_path_step(obj: Any, segment: str) -> Any:
    """Follow one segment into obj (dict key, list index, or attribute).

    Args:
        obj: The object to traverse into.
        segment: The path segment (dict key, list index, or attribute name).

    Returns:
        The value at the given segment.

    Raises:
        KeyError, IndexError, AttributeError: If the segment doesn't exist.
    """
    if isinstance(obj, dict):
        return obj[segment]

    # Attempt list/tuple index
    try:
        idx = int(segment)
        return obj[idx]
    except (ValueError, TypeError, IndexError):
        pass

    # Fallback to attribute access (Pydantic models, normal objects)
    return getattr(obj, segment)


def assign_path_step(obj: Any, segment: str, value: Any) -> None:
    """Assign value to segment of obj (dict key, list index, or attribute).

    Args:
        obj: The object to assign into.
        segment: The path segment (dict key, list index, or attribute name).
        value: The value to assign.
    """
    if isinstance(obj, dict):
        obj[segment] = value
        return

    # Attempt list/tuple index assignment
    try:
        idx = int(segment)
        obj[idx] = value
        return
    except (ValueError, TypeError, IndexError):
        pass

    # Fallback to attribute assignment
    setattr(obj, segment, value)


def get_by_path(state: Any, path: str, default: Any = Ellipsis) -> Any:
    """Get a nested value from state using a dot-separated path.

    Args:
        state: The root state object.
        path: Dot-separated path, e.g. "user.profile.name".
        default: If provided, return this when the path does not exist;
            otherwise, raise ValueError.

    Returns:
        The resolved value.

    Raises:
        ValueError: If the path is invalid and no default is provided,
            or if path depth exceeds MAX_DEPTH.
    """
    segments = path.split(".") if path else []
    if len(segments) > MAX_DEPTH:
        raise ValueError(f"Path length exceeds {MAX_DEPTH} segments")

    try:
        value: Any = state
        for segment in segments:
            value = traverse_path_step(value, segment)
    except Exception:
        if default is not Ellipsis:
            return default
        raise ValueError(f"Path '{path}' not found in state")
    return value


def set_by_path(state: Any, path: str, value: Any) -> None:
    """Set a nested value on state using a dot-separated path.

    Intermediate dicts are created as needed.

    Args:
        state: The root state object (mutated in place).
        path: Dot-separated path to write.
        value: Value to assign.

    Raises:
        ValueError: If the path is empty or exceeds MAX_DEPTH.
    """
    if not path:
        raise ValueError("Path cannot be empty")

    segments = path.split(".")
    if len(segments) > MAX_DEPTH:
        raise ValueError(f"Path length exceeds {MAX_DEPTH} segments")

    current = state
    for segment in segments[:-1]:
        try:
            current = traverse_path_step(current, segment)
        except (KeyError, AttributeError, IndexError, TypeError):
            intermediate: Any = {}
            assign_path_step(current, segment, intermediate)
            current = intermediate

    assign_path_step(current, segments[-1], value)


def merge_state(current_state: MODEL_T, incoming: BaseModel) -> MODEL_T:
    """Replace or merge incoming state onto current state.

    If incoming is the same type (or subclass) of current, it replaces directly.
    If current's type is a subclass of incoming's type (parent provided),
    fields are merged preserving child-specific fields.

    Args:
        current_state: The existing state.
        incoming: The new state to apply.

    Returns:
        The resulting state after merge/replace.

    Raises:
        ValueError: If the types are not compatible.
    """
    current_type = type(current_state)
    new_type = type(incoming)

    if isinstance(incoming, current_type):
        return incoming  # type: ignore[return-value]
    elif issubclass(current_type, new_type):
        parent_data = incoming.model_dump()
        return current_type.model_validate(
            {**current_state.model_dump(), **parent_data}
        )
    else:
        raise ValueError(
            f"State must be of type {current_type.__name__} or a parent type, "
            f"got {new_type.__name__}"
        )


def create_cleared_state(state_type: type[MODEL_T]) -> MODEL_T:
    """Create a default instance of the state type, wrapping ValidationError.

    Args:
        state_type: The state model class to instantiate.

    Returns:
        A new default instance.

    Raises:
        ValueError: If the model cannot be instantiated from defaults.
    """
    try:
        return state_type()
    except ValidationError:
        raise ValueError("State must have defaults for all fields")


# Only warn once about unserializable keys
class UnserializableKeyWarning(Warning):
    pass


warnings.simplefilter("once", UnserializableKeyWarning)


class DictState(DictLikeModel):
    """
    Dynamic, dict-like Pydantic model for workflow state.

    Used as the default state model when no typed state is provided. Behaves
    like a mapping while retaining Pydantic validation and serialization.

    Examples:
        ```python
        from workflows.context.state_store import DictState

        state = DictState()
        state["foo"] = 1
        state.bar = 2  # attribute-style access works for nested structures
        ```

    See Also:
        - [InMemoryStateStore][workflows.context.state_store.InMemoryStateStore]
    """

    def __init__(self, **params: Any):
        super().__init__(**params)


# Default state type is DictState for the generic type
MODEL_T = TypeVar("MODEL_T", bound=BaseModel, default=DictState)  # type: ignore[reportGeneralTypeIssues]


@runtime_checkable
class StateStore(Protocol[MODEL_T]):
    """Protocol defining the async state store interface.

    State stores hold a single Pydantic model instance representing global
    workflow state. Implementations must be async-safe and support both
    atomic operations and transactional edits.

    This protocol enables runtime plugins to provide custom state store
    implementations (e.g., backed by databases, Redis, or external services)
    while maintaining API compatibility with the default
    [InMemoryStateStore][workflows.context.state_store.InMemoryStateStore].

    For remote state stores, `to_dict`/`from_dict` should serialize non-secret
    connection info (e.g., URL, table name) rather than the full state contents,
    since the actual state lives in the external service.

    See Also:
        - [InMemoryStateStore][workflows.context.state_store.InMemoryStateStore]
        - [Context.store][workflows.context.context.Context.store]
    """

    state_type: type[MODEL_T]

    async def get_state(self) -> MODEL_T:
        """Return a copy of the current state model."""
        ...

    async def set_state(self, state: MODEL_T) -> None:
        """Replace or merge into the current state model."""
        ...

    async def get(self, path: str, default: Any = ...) -> Any:
        """Get a nested value using dot-separated paths."""
        ...

    async def set(self, path: str, value: Any) -> None:
        """Set a nested value using dot-separated paths."""
        ...

    async def clear(self) -> None:
        """Reset the state to its type defaults."""
        ...

    def edit_state(self) -> AsyncContextManager[MODEL_T]:
        """Edit state transactionally under a lock."""
        ...

    def to_dict(self, serializer: "BaseSerializer") -> dict[str, Any]:
        """Serialize state for persistence."""
        ...


class InMemoryStateStore(Generic[MODEL_T]):
    """
    Default in-memory implementation of the [StateStore][workflows.context.state_store.StateStore] protocol.

    Holds a single Pydantic model instance representing global workflow state.
    When the generic parameter is omitted, it defaults to
    [DictState][workflows.context.state_store.DictState] for flexible,
    dictionary-like usage.

    Thread-safety is ensured with an internal `asyncio.Lock`. Consumers can
    either perform atomic reads/writes via `get_state` and `set_state`, or make
    in-place, transactional edits via the `edit_state` context manager.

    Examples:
        Typed state model:

        ```python
        from pydantic import BaseModel
        from workflows.context.state_store import InMemoryStateStore

        class MyState(BaseModel):
            count: int = 0

        store = InMemoryStateStore(MyState())
        async with store.edit_state() as state:
            state.count += 1
        ```

        Dynamic state with `DictState`:

        ```python
        from workflows.context.state_store import InMemoryStateStore, DictState

        store = InMemoryStateStore(DictState())
        await store.set("user.profile.name", "Ada")
        name = await store.get("user.profile.name")
        ```

    See Also:
        - [Context.store][workflows.context.context.Context.store]
    """

    state_type: type[MODEL_T]

    def __init__(self, initial_state: MODEL_T):
        self._state = initial_state
        self.state_type = type(initial_state)

    @functools.cached_property
    def _lock(self) -> asyncio.Lock:
        """Lazy lock initialization for Python 3.14+ compatibility.

        asyncio.Lock() requires a running event loop in Python 3.14+.
        Using cached_property defers creation to first use in async context.
        """
        return asyncio.Lock()

    async def get_state(self) -> MODEL_T:
        """Return a shallow copy of the current state model.

        Returns:
            MODEL_T: A `.model_copy()` of the internal Pydantic model.
        """
        return self._state.model_copy()

    async def set_state(self, state: MODEL_T) -> None:
        """Replace or merge into the current state model.

        If the provided state is the exact type of the current state, it replaces
        the state entirely. If the provided state is a parent type (i.e., the
        current state type is a subclass of the provided state type), the fields
        from the parent are merged onto the current state, preserving any child
        fields that aren't present in the parent.

        This enables workflow inheritance where a base workflow step can call
        set_state with a base state type without obliterating child state fields.

        Args:
            state (MODEL_T): New state, either the same type or a parent type.

        Raises:
            ValueError: If the types are not compatible (neither same nor parent).
        """
        async with self._lock:
            self._state = merge_state(self._state, state)

    def to_dict(self, serializer: "BaseSerializer") -> dict[str, Any]:
        """Serialize the state and model metadata for persistence.

        For `DictState`, each individual item is serialized using the provided
        serializer since values can be arbitrary Python objects. For other
        Pydantic models, defers to the serializer (e.g. JSON) which can leverage
        model-aware encoding.

        Args:
            serializer (BaseSerializer): Strategy used to encode values.

        Returns:
            dict[str, Any]: A payload suitable for
            [from_dict][workflows.context.state_store.InMemoryStateStore.from_dict].
        """
        payload = create_in_memory_payload(self._state, serializer)
        return payload.model_dump()

    @classmethod
    def from_dict(
        cls, serialized_state: dict[str, Any], serializer: "BaseSerializer"
    ) -> "InMemoryStateStore[MODEL_T]":
        """Restore a state store from a serialized payload.

        Args:
            serialized_state (dict[str, Any]): The payload produced by
                [to_dict][workflows.context.state_store.InMemoryStateStore.to_dict].
            serializer (BaseSerializer): Strategy to decode stored values.

        Returns:
            InMemoryStateStore[MODEL_T]: A store with the reconstructed model.

        Raises:
            ValueError: If the payload is not in_memory format.
        """
        if not serialized_state:
            return cls(DictState())  # type: ignore[arg-type]

        # Validate it's in_memory format (raises ValueError if not)
        parse_in_memory_state(serialized_state)

        state_instance = deserialize_state_from_dict(serialized_state, serializer)
        return cls(state_instance)  # type: ignore[arg-type]

    @asynccontextmanager
    async def edit_state(self) -> AsyncGenerator[MODEL_T, None]:
        """Edit state transactionally under a lock.

        Yields the mutable model and writes it back on exit. This pattern avoids
        read-modify-write races and keeps updates atomic.

        Yields:
            MODEL_T: The current state model for in-place mutation.
        """
        async with self._lock:
            state = self._state

            yield state

            self._state = state

    async def get(self, path: str, default: Any = Ellipsis) -> Any:
        """Get a nested value using dot-separated paths.

        Args:
            path (str): Dot-separated path, e.g. "user.profile.name".
            default (Any): If provided, return this when the path does not
                exist; otherwise, raise `ValueError`.

        Returns:
            Any: The resolved value.

        Raises:
            ValueError: If the path is invalid and no default is provided or if
                the path depth exceeds limits.
        """
        async with self._lock:
            return get_by_path(self._state, path, default)

    async def set(self, path: str, value: Any) -> None:
        """Set a nested value using dot-separated paths.

        Args:
            path (str): Dot-separated path to write.
            value (Any): Value to assign.

        Raises:
            ValueError: If the path is empty or exceeds the maximum depth.
        """
        async with self._lock:
            set_by_path(self._state, path, value)

    async def clear(self) -> None:
        """Reset the state to its type defaults.

        Raises:
            ValueError: If the model type cannot be instantiated from defaults
                (i.e., fields missing default values).
        """
        await self.set_state(create_cleared_state(self._state.__class__))


def deserialize_dict_state_data(
    data: dict[str, Any],
    serializer: BaseSerializer,
) -> DictState:
    """Deserialize DictState from {"_data": {...}} format.

    Args:
        data: Dict with {"_data": {...}} structure containing serialized values.
        serializer: Strategy for decoding values.

    Returns:
        DictState with deserialized values.

    Raises:
        ValueError: If deserialization fails for any key.
    """
    _data_serialized = data.get("_data", {})
    deserialized_data = {}
    for key, value in _data_serialized.items():
        try:
            deserialized_data[key] = serializer.deserialize(value)
        except Exception as e:
            raise ValueError(f"Failed to deserialize state value for key {key}: {e}")
    return DictState(_data=deserialized_data)


def deserialize_state_from_dict(
    serialized_state: dict[str, Any],
    serializer: "BaseSerializer",
    state_type: type[BaseModel] | None = None,
) -> BaseModel:
    """Deserialize state from a serialized payload.

    This is the inverse of InMemoryStateStore.to_dict(). It handles both
    DictState (with per-key serialization) and typed Pydantic models.

    Args:
        serialized_state: The payload from to_dict(), containing state_data,
            state_type, and state_module.
        serializer: Strategy to decode stored values.
        state_type: Optional explicit state type. When provided, uses
            issubclass to determine if it's DictState. When omitted, falls
            back to reading state_type from the dict.

    Returns:
        The deserialized state model instance.

    Raises:
        ValueError: If deserialization fails for any key.
    """
    state_data = serialized_state.get("state_data", {})
    state_type_name = serialized_state.get("state_type", "DictState")

    if state_type_name == "DictState":
        _data_serialized = state_data.get("_data", {})
        deserialized_data = {}
        for key, value in _data_serialized.items():
            try:
                deserialized_data[key] = serializer.deserialize(value)
            except Exception as e:
                raise ValueError(
                    f"Failed to deserialize state value for key {key}: {e}"
                )
        return DictState(_data=deserialized_data)
    else:
        return serializer.deserialize(state_data)


def infer_state_type(workflow: "Workflow") -> type[BaseModel]:
    """Infer the state type from workflow step configs.

    Looks at Context[T] annotations in step functions to determine
    the expected state type. Returns DictState if no typed state is found.

    Args:
        workflow: The workflow to inspect for state type annotations.

    Returns:
        The inferred state type, or DictState if none found.

    Raises:
        ValueError: If multiple different state types are found.
    """
    state_types: set[type[BaseModel]] = set()
    for _, step_func in workflow._get_steps().items():
        step_config: StepConfig = step_func._step_config
        if (
            step_config.context_state_type is not None
            and step_config.context_state_type != DictState
            and issubclass(step_config.context_state_type, BaseModel)
        ):
            state_types.add(step_config.context_state_type)

    state_type: type[BaseModel]
    if state_types:
        state_type = _find_most_derived_state_type(state_types)
    else:
        state_type = DictState

    return state_type


def _find_most_derived_state_type(state_types: set[type[BaseModel]]) -> type[BaseModel]:
    """Find the most derived (most specific) state type from a set of types.

    All types must be in a single inheritance chain, i.e., one type must be
    a subclass of all other types (the most derived type).

    Args:
        state_types: Set of state types to analyze.

    Returns:
        The most derived type in the inheritance hierarchy.

    Raises:
        ValueError: If types are not in a compatible inheritance hierarchy.
    """
    type_list = list(state_types)

    if len(type_list) == 1:
        return type_list[0]

    # Find the most derived type - it should be a subclass of all others
    most_derived: type[BaseModel] | None = None

    for candidate in type_list:
        is_most_derived = True
        for other in type_list:
            if other is candidate:
                continue
            # candidate must be a subclass of other (or equal to it)
            if not issubclass(candidate, other):
                is_most_derived = False
                break
        if is_most_derived:
            most_derived = candidate
            break

    if most_derived is None:
        # No single type is a subclass of all others - incompatible hierarchy
        raise ValueError(
            "Multiple state types are not in a compatible inheritance hierarchy. "
            "All state types must share a common inheritance chain. Found: "
            + ", ".join([st.__name__ for st in state_types])
        )

    return most_derived
