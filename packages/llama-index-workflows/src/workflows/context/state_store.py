# SPDX-License-Identifier: MIT
# Copyright (c) 2026 LlamaIndex Inc.

from __future__ import annotations

import asyncio
import functools
import json
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
    cast,
    runtime_checkable,
)

from pydantic import BaseModel, ValidationError, model_validator
from typing_extensions import TypeVar

from workflows.decorators import StepConfig
from workflows.events import DictLikeModel

from .serializers import BaseSerializer, JsonSerializer

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


class _StateRecord(BaseModel):
    """Raw state record loaded and saved by a storage backend."""

    data: Any
    state_type: str | None = None
    state_module: str | None = None


@runtime_checkable
class _StateStorage(Protocol):
    """Internal persistence boundary for workflow state."""

    async def load(self) -> _StateRecord | None:
        """Load a raw state record, or None when no state exists yet."""
        ...

    async def save(self, record: _StateRecord) -> None:
        """Persist a raw state record."""
        ...


@runtime_checkable
class _DurableStateStorage(_StateStorage, Protocol):
    """Storage whose state outlives the process and can emit a reconnect handle."""

    def to_handle(self) -> dict[str, Any]:
        """Return backend-specific reconnect metadata."""
        ...


@runtime_checkable
class _SeededStateStorage(Protocol):
    """Internal storage lifecycle hook for lazy seed materialization."""

    async def ensure_seeded(self) -> None:
        """Materialize any deferred seed before observable storage operations."""
        ...


def is_durable_serialized_state(data: dict[str, Any] | None) -> bool:
    """Return whether a serialized state payload is a durable provider handle."""
    if not data:
        return False
    return data.get("store_type", "in_memory") != "in_memory"


def _record_from_state(
    state: BaseModel,
    serializer: BaseSerializer,
    known_unserializable_keys: tuple[str, ...] = KNOWN_UNSERIALIZABLE_KEYS,
) -> _StateRecord:
    """Encode a state model into a raw storage record."""
    state_data, state_type_name, state_module = encode_state(
        state, serializer, known_unserializable_keys
    )
    return _StateRecord(
        data=state_data,
        state_type=state_type_name,
        state_module=state_module,
    )


def _string_record_from_state(
    state: BaseModel,
    serializer: BaseSerializer,
    known_unserializable_keys: tuple[str, ...] = KNOWN_UNSERIALIZABLE_KEYS,
) -> _StateRecord:
    """Encode a state model into a storage record with string data."""
    state_data, state_type_name, state_module = encode_state_to_str(
        state, serializer, known_unserializable_keys
    )
    return _StateRecord(
        data=state_data,
        state_type=state_type_name,
        state_module=state_module,
    )


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


def encode_state(
    state: BaseModel,
    serializer: BaseSerializer,
    known_unserializable_keys: tuple[str, ...] = KNOWN_UNSERIALIZABLE_KEYS,
) -> tuple[Any, str, str]:
    """Encode a state model and its self-describing metadata."""
    if isinstance(state, DictState):
        state_data = serialize_dict_state_data(
            state, serializer, known_unserializable_keys
        )
    else:
        state_data = serializer.serialize(state)

    return state_data, type(state).__name__, type(state).__module__


def encode_state_to_str(
    state: BaseModel,
    serializer: BaseSerializer,
    known_unserializable_keys: tuple[str, ...] = KNOWN_UNSERIALIZABLE_KEYS,
) -> tuple[str, str, str]:
    """Encode state for durable stores that persist a single string column."""
    state_data, state_type_name, state_module = encode_state(
        state, serializer, known_unserializable_keys
    )
    if not isinstance(state_data, str):
        state_data = json.dumps(state_data)
    return state_data, state_type_name, state_module


def decode_state(
    state_data: Any,
    serializer: BaseSerializer,
) -> BaseModel:
    """Decode a persisted state payload by dispatching on its shape.

    Persisted type metadata is intentionally not consulted, so rows cannot
    drive module imports. Typed payloads self-describe through the serializer
    (which validates the embedded qualified name before importing anything);
    DictState payloads are recognized by their ``{"_data": ...}`` wrapper.
    """
    if isinstance(state_data, BaseModel):
        # Live model from an in-process handoff.
        return state_data

    if isinstance(state_data, str):
        try:
            parsed: Any = json.loads(state_data)
        except ValueError:
            parsed = None
        if isinstance(parsed, dict) and "_data" in parsed:
            state_data = parsed
        else:
            # Non-JSON strings (e.g. pickled payloads) and JSON-encoded typed
            # payloads both reconstruct through the serializer, which fails
            # closed on disallowed embedded types.
            deserialized = serializer.deserialize(state_data)
            if isinstance(deserialized, BaseModel):
                return deserialized
            state_data = deserialized

    if isinstance(state_data, dict) and "_data" not in state_data:
        # Already-parsed typed payload. JsonSerializer-style serializers can
        # reconstruct it from the embedded self-description.
        deserialize_value = getattr(serializer, "deserialize_value", None)
        if callable(deserialize_value):
            value = deserialize_value(state_data)
            if isinstance(value, BaseModel):
                return value

    if not isinstance(state_data, dict):
        state_data = {}
    return deserialize_dict_state_data(state_data, serializer)


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
    state_data, state_type_name, state_module = encode_state(
        state, serializer, known_unserializable_keys
    )

    return InMemorySerializedState(
        state_type=state_type_name,
        state_module=state_module,
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
    """Protocol defining the public async state store interface.

    State stores hold a single Pydantic model instance representing global
    workflow state. Implementations must be async-safe and support both
    atomic operations and transactional edits.

    Runtime plugins can provide custom implementations while maintaining API
    compatibility with the default
    [InMemoryStateStore][workflows.context.state_store.InMemoryStateStore].

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
        """Serialize state for legacy persistence.

        Runtime integrations should prefer
        `workflows.context.state_store_integration.state_store_handoff`.
        """
        ...


class _TypedStateStore(Generic[MODEL_T]):
    """Typed StateStore facade over raw storage.

    Concurrency contract: reads (`get_state`, `get`, `snapshot`) take no
    lock; writers (`set_state`, `set`, `clear`, `edit_state`) take the
    internal lock, which serializes read-modify-write cycles within one
    process *through the same facade instance*. Calling a writer inside
    an `edit_state` block deadlocks and is unsupported. Workflow stores
    memoize one facade per run so in-process writers share that lock.
    Writers in other processes or replicas are not serialized;
    cross-replica consistency requires backend-level atomicity.
    """

    state_type: type[MODEL_T]

    def __init__(
        self,
        storage: _StateStorage,
        state_type: type[MODEL_T],
        serializer: BaseSerializer,
        *,
        to_dict_mode: Literal["snapshot", "handle"] = "snapshot",
    ) -> None:
        self._storage = storage
        self.state_type = state_type
        self._serializer = serializer
        self._to_dict_mode = to_dict_mode

    @functools.cached_property
    def _lock(self) -> asyncio.Lock:
        """Lazy lock initialization for Python 3.14+ compatibility."""
        return asyncio.Lock()

    def _create_default_state(self) -> MODEL_T:
        return create_cleared_state(self.state_type)

    async def ensure_seeded(self) -> None:
        """Materialize any deferred storage seed."""
        if isinstance(self._storage, _SeededStateStorage):
            await self._storage.ensure_seeded()

    async def _load_state_or_none(self) -> MODEL_T | None:
        await self.ensure_seeded()
        record = await self._storage.load()
        if record is None:
            return None
        return cast(MODEL_T, decode_state(record.data, self._serializer))

    async def _load_state(self) -> MODEL_T:
        state = await self._load_state_or_none()
        if state is not None:
            return state
        state = self._create_default_state()
        await self._save_state(state)
        return state

    async def _save_state(self, state: BaseModel) -> None:
        await self.ensure_seeded()
        await self._storage.save(_record_from_state(state, self._serializer))

    async def get_state(self) -> MODEL_T:
        """Return a copy of the current state model."""
        state = await self._load_state()
        return state.model_copy()

    async def set_state(self, state: MODEL_T) -> None:
        """Replace or merge into the current state model."""
        async with self._lock:
            current = await self._load_state_or_none()
            merged: BaseModel = (
                state if current is None else merge_state(current, state)
            )
            await self._save_state(merged)

    async def get(self, path: str, default: Any = Ellipsis) -> Any:
        """Get a nested value using dot-separated paths.

        Lockless read, like `get_state`, so it stays safe inside
        `edit_state`.
        """
        return get_by_path(await self._load_state(), path, default)

    async def set(self, path: str, value: Any) -> None:
        """Set a nested value using dot-separated paths."""
        async with self.edit_state() as state:
            set_by_path(state, path, value)

    async def clear(self) -> None:
        """Reset the state to its type defaults.

        Clear is a reset, not a merge: the stored state is replaced with a
        default instance of its *current* type, so subclass fields are reset
        too. Falls back to the construction-time `state_type` when storage
        is empty.
        """
        async with self._lock:
            current = await self._load_state_or_none()
            target = type(current) if current is not None else self.state_type
            await self._save_state(create_cleared_state(target))

    @asynccontextmanager
    async def edit_state(self) -> AsyncGenerator[MODEL_T, None]:
        """Edit state transactionally under a lock.

        Reads (`get_state`, `get`, `snapshot`) are safe inside the block;
        calling a writer (`set_state`, `set`, `clear`, or a nested
        `edit_state`) deadlocks and is unsupported.
        """
        async with self._lock:
            state = await self._load_state()
            yield state
            await self._save_state(state)

    async def snapshot(self, serializer: BaseSerializer) -> dict[str, Any]:
        """Serialize portable state data."""
        state = await self._load_state()
        return create_in_memory_payload(state, serializer).model_dump()

    async def serialize_for_handoff(self, serializer: BaseSerializer) -> dict[str, Any]:
        """Serialize this store for runtime handoff.

        Durable stores return a reconnect handle (the state stays in the
        backend); in-memory stores return a portable, serializer-encoded
        snapshot that round-trips through ``from_dict``.
        """
        await self.ensure_seeded()
        if self._to_dict_mode == "handle":
            return self._durable_storage().to_handle()
        return await self.snapshot(serializer)

    def _durable_storage(self) -> _DurableStateStorage:
        """Constructor invariant: handle mode implies durable storage."""
        if not isinstance(self._storage, _DurableStateStorage):
            raise TypeError(
                "to_dict_mode='handle' requires storage implementing to_handle()"
            )
        return self._storage

    def to_dict(self, serializer: BaseSerializer) -> dict[str, Any]:
        """Serialize state for legacy callers."""
        if self._to_dict_mode == "handle":
            return self._durable_storage().to_handle()
        record = self._sync_snapshot_record()
        return InMemorySerializedState(
            state_type=record.state_type or "DictState",
            state_module=record.state_module or "workflows.context.state_store",
            state_data=record.data,
        ).model_dump()

    def _sync_snapshot_record(self) -> _StateRecord:
        raise NotImplementedError("Use await snapshot(serializer) for async storage")


class _InMemoryStateStorage:
    """Raw in-process storage for workflow state."""

    def __init__(self, record: _StateRecord | None = None) -> None:
        self._record = record

    async def load(self) -> _StateRecord | None:
        return self._record.model_copy() if self._record is not None else None

    async def save(self, record: _StateRecord) -> None:
        self._record = record.model_copy()

    def load_sync(self) -> _StateRecord | None:
        return self._record.model_copy() if self._record is not None else None


class InMemoryStateStore(_TypedStateStore[MODEL_T]):
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
        self._memory_storage = _InMemoryStateStorage(
            _StateRecord(
                data=initial_state,
                state_type=type(initial_state).__name__,
                state_module=type(initial_state).__module__,
            )
        )
        super().__init__(
            self._memory_storage,
            type(initial_state),
            JsonSerializer(),
            to_dict_mode="snapshot",
        )

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
        record = self._sync_snapshot_record()
        state = decode_state(record.data, JsonSerializer())
        payload = create_in_memory_payload(state, serializer)
        return payload.model_dump()

    def _sync_snapshot_record(self) -> _StateRecord:
        record = self._memory_storage.load_sync()
        if record is None:
            state = self.state_type()
            record = _StateRecord(
                data=state,
                state_type=type(state).__name__,
                state_module=type(state).__module__,
            )
            self._memory_storage._record = record
        return record

    async def _save_state(self, state: BaseModel) -> None:
        await self._memory_storage.save(
            _StateRecord(
                data=state,
                state_type=type(state).__name__,
                state_module=type(state).__module__,
            )
        )

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

        state_instance = _decode_seed_state(serialized_state, serializer)
        return cls(state_instance)  # type: ignore[arg-type]


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
        state_type: Deprecated and ignored. Decoding dispatches on the
            payload shape; the kwarg is kept so released callers
            (llama-agents-dbos <= 0.3.x) don't break.

    Returns:
        The deserialized state model instance.

    Raises:
        ValueError: If deserialization fails for any key.
    """
    return decode_state(serialized_state.get("state_data", {}), serializer)


def _decode_seed_state(
    serialized_state: dict[str, Any],
    serializer: "BaseSerializer",
) -> BaseModel:
    """Validate and decode an in-memory serialized state seed."""
    parse_in_memory_state(serialized_state)
    return deserialize_state_from_dict(serialized_state, serializer)


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
