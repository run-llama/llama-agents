# SPDX-License-Identifier: MIT
# Copyright (c) 2026 LlamaIndex Inc.

from __future__ import annotations

import asyncio
import functools
import json
import uuid
import warnings
from contextlib import asynccontextmanager
from typing import (
    TYPE_CHECKING,
    Any,
    AsyncContextManager,
    AsyncGenerator,
    Callable,
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
from workflows.runtime.types.invocation import slot_namespace

from .serializers import BaseSerializer, JsonSerializer

if TYPE_CHECKING:
    from workflows.workflow import Workflow

MAX_DEPTH = 1000

# Keys set by pre-built workflows that are known to be unserializable in some cases.
KNOWN_UNSERIALIZABLE_KEYS: tuple[str, ...] = ("memory",)

# Portable childful snapshots nest per-namespace state data under this key.
CHILD_STATES_KEY = "children"


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


class StateRecord(BaseModel):
    """Raw state record loaded and saved by a storage backend.

    ``state_type``/``state_module`` are written for debugging only; decoding
    dispatches on the payload shape, so loads may leave them ``None``.
    """

    data: Any
    state_type: str | None = None
    state_module: str | None = None


@runtime_checkable
class _StateStorage(Protocol):
    """Minimal persistence boundary for workflow state: load and save."""

    async def load(self) -> StateRecord | None:
        """Load a raw state record, or None when no state exists yet."""
        ...

    async def save(self, record: StateRecord) -> None:
        """Persist a raw state record."""
        ...

    def session(self) -> AsyncContextManager[_StateStorage]:
        """Scope a load+save pair to one backend connection.

        Yields a *separate* storage value bound to that connection; the
        receiver storage (and concurrent readers going through it) stays
        untouched. Backends without per-call connections yield themselves.
        """
        ...


@runtime_checkable
class StateStorage(_StateStorage, Protocol):
    """Durable storage: state outlives the process and supports reconnect handles.

    Backends implement raw I/O only; the seed lifecycle, locking, and
    save-path encoding live in [StateStoreFacade][workflows.context.state_store.StateStoreFacade].
    """

    @property
    def run_id(self) -> str:
        """Run id identifying this storage's target."""
        ...

    def to_handle(self) -> dict[str, Any]:
        """Return backend-specific reconnect metadata."""
        ...

    def parse_own_handle(self, payload: dict[str, Any]) -> Any | None:
        """Parse a serialized payload into this backend's handle, or None.

        Must be sync and pure (no I/O). Returns None when the payload is not
        a handle this backend produced.
        """
        ...

    async def copy_from_handle(self, handle: Any) -> None:
        """Copy state from another target of this backend into this one."""
        ...


def is_durable_serialized_state(data: dict[str, Any] | None) -> bool:
    """Return whether a serialized state payload is a durable provider handle."""
    if not data:
        return False
    return data.get("store_type", "in_memory") != "in_memory"


def restored_run_id(run_id: str | None, payload: dict[str, Any]) -> str:
    """Resolve the target run id for a ``from_dict`` restore.

    Explicit caller run id wins, then the payload's own run id, then a
    fresh id for portable snapshots that carry none.
    """
    return run_id or payload.get("run_id") or str(uuid.uuid4())


def _record_from_state(
    state: BaseModel,
    serializer: BaseSerializer,
    known_unserializable_keys: tuple[str, ...] = KNOWN_UNSERIALIZABLE_KEYS,
) -> StateRecord:
    """Encode a state model into a raw storage record."""
    state_data, state_type_name, state_module = encode_state(
        state, serializer, known_unserializable_keys
    )
    return StateRecord(
        data=state_data,
        state_type=state_type_name,
        state_module=state_module,
    )


def string_record_from_state(
    state: BaseModel,
    serializer: BaseSerializer,
    known_unserializable_keys: tuple[str, ...] = KNOWN_UNSERIALIZABLE_KEYS,
) -> StateRecord:
    """Encode a state model into a storage record with string data.

    For durable stores that persist a single string column.
    """
    record = _record_from_state(state, serializer, known_unserializable_keys)
    if not isinstance(record.data, str):
        record.data = json.dumps(record.data)
    return record


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


def decode_state(
    state_data: Any,
    serializer: BaseSerializer,
) -> BaseModel:
    """Decode a persisted state payload by dispatching on its shape.

    Persisted type metadata is intentionally not consulted, so rows cannot
    drive module imports. Typed payloads self-describe through the serializer
    (which validates the embedded qualified name before importing anything);
    DictState payloads are recognized by their ``{"_data": ...}`` wrapper.

    Decoding is strict: ``None`` means a blank store (default empty state);
    any other unrecognized shape raises ``ValueError`` instead of silently
    degrading to an empty state.
    """
    if isinstance(state_data, BaseModel):
        # Live model from an in-process handoff.
        return state_data

    if state_data is None:
        # Blank-store handoffs serialize as state_data=None.
        return DictState()

    if isinstance(state_data, str):
        try:
            parsed: Any = json.loads(state_data)
        except ValueError:
            parsed = None
        if isinstance(parsed, dict) and "_data" in parsed:
            return deserialize_dict_state_data(parsed, serializer)
        # Non-JSON strings (e.g. pickled payloads) and JSON-encoded typed
        # payloads both reconstruct through the serializer, which fails
        # closed on disallowed embedded types.
        deserialized = serializer.deserialize(state_data)
        if isinstance(deserialized, BaseModel):
            return deserialized
        raise ValueError(
            "Unrecognized state payload: string decoding to "
            f"{type(deserialized).__name__}; expected a typed model payload "
            "or a {'_data': ...} DictState wrapper"
        )

    if isinstance(state_data, dict):
        if "_data" in state_data:
            return deserialize_dict_state_data(state_data, serializer)
        # Already-parsed typed payload. JsonSerializer-style serializers can
        # reconstruct it from the embedded self-description.
        deserialize_value = getattr(serializer, "deserialize_value", None)
        if callable(deserialize_value):
            value = deserialize_value(state_data)
            if isinstance(value, BaseModel):
                return value
        raise ValueError(
            "Unrecognized state payload: dict without a '_data' wrapper that "
            "the serializer could not reconstruct into a model"
        )

    raise ValueError(
        f"Unrecognized state payload of type {type(state_data).__name__}; "
        "refusing to decode"
    )


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


def is_declared_model_path_segment(obj: DictLikeModel, segment: str) -> bool:
    """Return whether a path segment names a declared model attribute."""
    cls = type(obj)
    return (
        segment in cls.model_fields
        or segment in cls.model_computed_fields
        or isinstance(getattr(cls, segment, None), property)
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

    # DictLikeModel dynamic keys live in _data and are shadowed by mapping
    # methods (items/keys/values/get, ...) under plain getattr, so check _data
    # first and never resolve methods as values.
    if isinstance(obj, DictLikeModel):
        if segment in obj:  # __contains__ checks _data
            return obj[segment]
        if is_declared_model_path_segment(obj, segment):
            return getattr(obj, segment)
        raise KeyError(segment)

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

    # DictLikeModel: __setattr__ routes declared field names to the field and
    # everything else into _data. Handling it before the int-index attempt
    # keeps numeric segments stored under string keys, matching reads.
    if isinstance(obj, DictLikeModel):
        setattr(obj, segment, value)
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


class _WriterLock:
    """asyncio.Lock that records its holder task.

    Writers acquire exclusively and fail loudly when the current task
    already holds the lock (a nested writer inside `edit_state`). Reads
    never take this lock: they return committed state.
    """

    def __init__(self) -> None:
        self._lock = asyncio.Lock()
        self._holder: asyncio.Task[Any] | None = None

    def _held_by_current_task(self) -> bool:
        return self._holder is not None and self._holder is asyncio.current_task()

    @asynccontextmanager
    async def acquire_write(self) -> AsyncGenerator[None, None]:
        if self._held_by_current_task():
            raise RuntimeError(
                "writer called inside edit_state: mutate the yielded state instead"
            )
        async with self._lock:
            self._holder = asyncio.current_task()
            try:
                yield
            finally:
                self._holder = None


class _CopySeed:
    """Pending seed: copy state from another target of the same backend."""

    def __init__(self, handle: Any) -> None:
        self.handle = handle


class _PayloadSeed:
    """Pending seed: portable in-memory payload to decode and save."""

    def __init__(self, payload: dict[str, Any], serializer: BaseSerializer) -> None:
        self.payload = payload
        self.serializer = serializer


class StateStoreFacade(Generic[MODEL_T]):
    """Typed StateStore facade over raw storage.

    Concurrency contract: Writers serialize on the per-run lock; reads
    are lockless and read-committed on every backend — durable reads see
    the backend's committed row, in-memory reads see the committed record.
    An in-flight `edit_state` block works on an isolated copy, so reads
    (including reads inside the block) return the pre-edit state until the
    block commits on exit. Nested writers raise.

    Workflow stores memoize one facade per run so in-process writers share
    that lock. Writers in other processes or replicas are not serialized;
    cross-replica consistency requires backend-level atomicity.

    Reads are pure: an empty storage yields a default state instance
    without persisting it; only writers (and seed materialization) save.

    The facade also owns the seed lifecycle: `add_seed` validates a
    serialized-state payload eagerly (sync, pure) and `ensure_seeded`
    materializes it lazily on first async access or handoff.
    """

    state_type: type[MODEL_T]

    def __init__(
        self,
        storage: _StateStorage,
        state_type: type[MODEL_T] | None = None,
        serializer: BaseSerializer | None = None,
    ) -> None:
        self._storage = storage
        # The durability decision is made exactly once, here; everything
        # downstream branches on this typed field.
        self._durable: StateStorage | None = (
            storage if isinstance(storage, StateStorage) else None
        )
        self.state_type = state_type or DictState  # type: ignore[assignment]  # ty: ignore[invalid-assignment]
        self._serializer = serializer or JsonSerializer()
        self._pending_seed: _CopySeed | _PayloadSeed | None = None

    @property
    def run_id(self) -> str:
        """Run id of the durable storage target."""
        if self._durable is not None:
            return self._durable.run_id
        raise AttributeError("run_id is only available on durable state stores")

    @functools.cached_property
    def _lock(self) -> _WriterLock:
        """Lazy lock initialization for Python 3.14+ compatibility."""
        return _WriterLock()

    @functools.cached_property
    def _seed_lock(self) -> asyncio.Lock:
        """Lazy lock initialization for Python 3.14+ compatibility."""
        return asyncio.Lock()

    def _create_default_state(self) -> MODEL_T:
        return create_cleared_state(self.state_type)

    def add_seed(self, payload: dict[str, Any], serializer: BaseSerializer) -> None:
        """Stage a serialized-state seed for lazy materialization.

        Validation is eager, sync, and pure: empty payloads and durable
        handles foreign to this storage raise immediately. I/O stays lazy —
        the seed materializes on the first async access via `ensure_seeded`.
        A pending seed wins over whatever already exists in storage.
        """
        if not payload:
            raise ValueError("Cannot seed state store from an empty payload")
        if self._durable is not None:
            handle = self._durable.parse_own_handle(payload)
            if handle is not None:
                own = self._durable.parse_own_handle(self._durable.to_handle())
                # Same target: state already lives in the backend, nothing
                # to copy (and any previously staged seed is superseded).
                self._pending_seed = None if handle == own else _CopySeed(handle)
                return
        if is_durable_serialized_state(payload):
            raise ValueError(
                "Cannot seed this state store from a durable handle with "
                f"store_type '{payload.get('store_type')}'"
            )
        parse_in_memory_state(payload)  # eager shape validation, raises
        self._pending_seed = _PayloadSeed(payload, serializer)

    async def ensure_seeded(self) -> None:
        """Materialize any pending seed exactly once (double-checked)."""
        if self._pending_seed is None:
            return
        async with self._seed_lock:
            seed = self._pending_seed
            if seed is None:
                return
            if isinstance(seed, _CopySeed):
                # add_seed only stages _CopySeed on durable storage.
                assert self._durable is not None
                await self._durable.copy_from_handle(seed.handle)
            else:
                # Bypass _save_state: its ensure_seeded re-entry would
                # deadlock on the seed lock we hold. A namespace facade only
                # ever seeds its own state; the router distributes children.
                state = decode_seed_state(seed.payload, seed.serializer)
                await self._write_state(state)
            # Popped only after success so a failed materialization stays
            # pending; concurrent readers block on the seed lock above
            # until the seed is committed. Identity-checked: sync add_seed
            # may have staged a fresh seed during the awaits above, which
            # must survive for the next ensure_seeded call.
            if self._pending_seed is seed:
                self._pending_seed = None

    async def _load_state_or_none(
        self, storage: _StateStorage | None = None
    ) -> MODEL_T | None:
        await self.ensure_seeded()
        record = await (storage or self._storage).load()
        if record is None:
            return None
        return cast(MODEL_T, decode_state(record.data, self._serializer))

    async def _load_state(self, storage: _StateStorage | None = None) -> MODEL_T:
        state = await self._load_state_or_none(storage)
        if state is not None:
            return state
        # Reads are pure: return a default without persisting it.
        return self._create_default_state()

    async def _save_state(
        self, state: BaseModel, storage: _StateStorage | None = None
    ) -> None:
        await self.ensure_seeded()
        await self._write_state(state, storage)

    async def _write_state(
        self, state: BaseModel, storage: _StateStorage | None = None
    ) -> None:
        """Single save chokepoint: persists string records.

        Non-durable facades override this (`InMemoryStateStore` saves
        live-model records instead).
        """
        await (storage or self._storage).save(
            string_record_from_state(state, self._serializer)
        )

    async def _write_record(
        self, record: StateRecord, storage: _StateStorage | None = None
    ) -> None:
        """Persist a pre-encoded raw state record."""
        await (storage or self._storage).save(record)

    async def _load_state_for_edit(
        self, storage: _StateStorage | None = None
    ) -> MODEL_T:
        """State instance handed to `edit_state`.

        Must be isolated from concurrent readers: mutations inside the
        block may not become observable until the block commits. Durable
        backends get this for free (each load decodes a fresh instance
        from the committed row); in-memory storage overrides this to edit
        a copy of its live record.
        """
        return await self._load_state(storage)

    async def get_state(self) -> MODEL_T:
        """Return a copy of the current state model.

        Reads are lockless and read-committed: an in-flight `edit_state`
        block is not observable until it commits.
        """
        state = await self._load_state()
        return state.model_copy()

    async def set_state(self, state: MODEL_T) -> None:
        """Replace or merge into the current state model."""
        async with self._lock.acquire_write():
            async with self._storage.session() as storage:
                current = await self._load_state_or_none(storage)
                merged: BaseModel = (
                    state if current is None else merge_state(current, state)
                )
                await self._save_state(merged, storage)

    async def get(self, path: str, default: Any = Ellipsis) -> Any:
        """Get a nested value using dot-separated paths.

        Reads are lockless and read-committed (see `get_state`).
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
        async with self._lock.acquire_write():
            async with self._storage.session() as storage:
                current = await self._load_state_or_none(storage)
                target = type(current) if current is not None else self.state_type
                await self._save_state(create_cleared_state(target), storage)

    @asynccontextmanager
    async def edit_state(self) -> AsyncGenerator[MODEL_T, None]:
        """Edit an isolated copy of the state, committed on block exit.

        Reads (`get_state`, `get`, `snapshot`) never block on the writer
        lock; inside or concurrent with the block they return the
        committed pre-edit state. Calling a writer (`set_state`, `set`,
        `clear`, or a nested `edit_state`) inside the block raises
        RuntimeError.
        """
        async with self._lock.acquire_write():
            async with self._storage.session() as storage:
                state = await self._load_state_for_edit(storage)
                yield state
                await self._save_state(state, storage)

    async def snapshot(self, serializer: BaseSerializer) -> dict[str, Any]:
        """Serialize portable state data."""
        state = await self._load_state()
        return create_in_memory_payload(state, serializer).model_dump()

    async def serialize_for_handoff(self, serializer: BaseSerializer) -> dict[str, Any]:
        """Serialize this store for runtime handoff.

        Durable stores return a reconnect handle (the state stays in the
        backend); in-memory stores return a portable, serializer-encoded
        snapshot that round-trips through ``from_dict``. Any pending seed
        is materialized first so handles never point at unmaterialized
        seeds.
        """
        await self.ensure_seeded()
        if self._durable is not None:
            return self._durable.to_handle()
        return await self.snapshot(serializer)

    def to_dict(self, serializer: BaseSerializer) -> dict[str, Any]:
        """Serialize state for legacy callers.

        Durable stores return a reconnect handle. Non-durable storage has no
        sync snapshot here; `InMemoryStateStore` overrides this.
        """
        if self._durable is not None:
            return self._durable.to_handle()
        raise NotImplementedError("Use await snapshot(serializer) for async storage")


def _live_record(state: BaseModel) -> StateRecord:
    """Record wrapping a live model: in-memory state keeps value identity."""
    return StateRecord(
        data=state,
        state_type=type(state).__name__,
        state_module=type(state).__module__,
    )


class _InMemoryStateStorage:
    """Raw in-process storage for workflow state."""

    def __init__(self, record: StateRecord | None = None) -> None:
        self._record = record

    async def load(self) -> StateRecord | None:
        return self._record.model_copy() if self._record is not None else None

    async def save(self, record: StateRecord) -> None:
        self._record = record.model_copy()

    @asynccontextmanager
    async def session(self) -> AsyncGenerator[_InMemoryStateStorage, None]:
        # No per-call connections: the storage scopes itself.
        yield self

    def load_sync(self) -> StateRecord | None:
        return self._record.model_copy() if self._record is not None else None

    def save_sync(self, record: StateRecord) -> None:
        self._record = record.model_copy()


class InMemoryStateStore(StateStoreFacade[MODEL_T]):
    """
    Default in-memory implementation of the [StateStore][workflows.context.state_store.StateStore] protocol.

    Holds a single Pydantic model instance representing global workflow state.
    When the generic parameter is omitted, it defaults to
    [DictState][workflows.context.state_store.DictState] for flexible,
    dictionary-like usage.

    Writers serialize on an internal `asyncio.Lock`; reads are lockless and
    read-committed. Consumers can either perform atomic reads/writes via
    `get_state` and `set_state`, or make transactional edits via the
    `edit_state` context manager — the block edits an isolated copy that is
    committed when the block exits.

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
        self._memory_storage = _InMemoryStateStorage(_live_record(initial_state))
        super().__init__(
            self._memory_storage,
            type(initial_state),
            JsonSerializer(),
        )

    def add_seed(self, payload: dict[str, Any], serializer: BaseSerializer) -> None:
        """Apply a seed eagerly — in-memory storage has no I/O to defer.

        Lazy seeding (the base implementation) exists to keep durable backends
        from materializing on construction. In-process state has nothing to
        defer, and sync readers (``to_dict``) would otherwise miss a seed that
        no async access ever triggered. Decode and commit it now.
        """
        if not payload:
            raise ValueError("Cannot seed state store from an empty payload")
        if is_durable_serialized_state(payload):
            raise ValueError(
                "Cannot seed this state store from a durable handle with "
                f"store_type '{payload.get('store_type')}'"
            )
        state = decode_seed_state(payload, serializer)
        self._memory_storage.save_sync(_live_record(state))

    async def _load_state_for_edit(
        self, storage: _StateStorage | None = None
    ) -> MODEL_T:
        # The live record IS the committed state. Hand `edit_state` a deep
        # copy so lockless readers keep seeing the committed state until
        # the block commits (matching durable backends, where each load
        # decodes a fresh instance from the committed row).
        state = await self._load_state(storage)
        return state.model_copy(deep=True)

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
        record = self._memory_storage.load_sync()
        # Records always hold a live model here (see _write_state); when the
        # storage is empty, snapshot a default without persisting it.
        if record is None:
            state = self.state_type()
        elif isinstance(record.data, BaseModel):
            state = cast(MODEL_T, record.data)
        else:
            state = cast(MODEL_T, decode_state(record.data, serializer))
        return create_in_memory_payload(state, serializer).model_dump()

    async def _write_state(
        self, state: BaseModel, storage: _StateStorage | None = None
    ) -> None:
        # Live-model record: no encoding, value identity preserved. The
        # in-memory session yields the storage itself, so writes always
        # land in self._memory_storage.
        await self._memory_storage.save(_live_record(state))

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

        # Childful trees are distributed per namespace by the router; a single
        # InMemoryStateStore only ever holds one namespace's state.
        state_instance = decode_seed_state(serialized_state, serializer)
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
    # Absent/None state data decodes to a default empty state.
    return decode_state(serialized_state.get("state_data"), serializer)


def decode_seed_state(
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


def _namespace_key(namespace: tuple[str, ...]) -> str:
    return "/".join(namespace)


def _namespace_from_key(key: str) -> tuple[str, ...]:
    """Inverse of ``_namespace_key``: ``""`` -> ``()``, ``"child"`` -> ``("child",)``."""
    return tuple(key.split("/")) if key else ()


def _in_memory_seed_payload(
    state_data: Any, state_type: type[BaseModel]
) -> dict[str, Any]:
    """Wrap raw namespace ``state_data`` as an in-memory seed payload."""
    return {
        "store_type": "in_memory",
        "state_type": state_type.__name__,
        "state_module": state_type.__module__,
        "state_data": state_data,
    }


def namespaced_seed_payloads(
    serialized_state: dict[str, Any] | None,
    state_types: dict[tuple[str, ...], type[BaseModel]],
) -> dict[tuple[str, ...], dict[str, Any]] | None:
    """Split a portable snapshot into one in-memory seed per namespace.

    Returns ``namespace -> in_memory payload`` for the root (the snapshot
    minus ``children``) and each ``children[ns]`` entry, or ``None`` when there
    is nothing to distribute: an empty payload, or a durable handle (seeded by
    copy, not by per-namespace payloads).
    """
    if not serialized_state:
        return None
    if serialized_state.get("store_type", "in_memory") != "in_memory":
        return None
    root_payload = {
        key: value for key, value in serialized_state.items() if key != CHILD_STATES_KEY
    }
    seeds: dict[tuple[str, ...], dict[str, Any]] = {(): root_payload}
    children = serialized_state.get(CHILD_STATES_KEY) or {}
    for namespace_key, state_data in children.items():
        namespace = _namespace_from_key(namespace_key)
        seeds[namespace] = _in_memory_seed_payload(
            state_data, state_types.get(slot_namespace(namespace), DictState)
        )
    return seeds


NamespaceFactory = Callable[[tuple[str, ...]], StateStoreFacade[Any]]


class NamespacedStateStores:
    """Routes each namespace to its own ``StateStoreFacade``.

    A namespace is just a run with an extra key component: every namespace owns
    its own backing record, writer lock, and seed lifecycle. There is no shared
    record and no cross-namespace lock — two namespaces never read-modify-write
    the same physical record.
    """

    def __init__(
        self,
        factory: NamespaceFactory,
        serializer: BaseSerializer,
        state_types: dict[tuple[str, ...], type[BaseModel]],
    ) -> None:
        self._factory = factory
        self.serializer = serializer
        self.state_types = state_types
        self._views: dict[tuple[str, ...], StateStoreFacade[Any]] = {}

    @property
    def is_single_namespace(self) -> bool:
        return len(self.state_types) <= 1

    def _view_facade(self, namespace: tuple[str, ...]) -> StateStoreFacade[Any]:
        view = self._views.get(namespace)
        if view is None:
            view = self._factory(namespace)
            self._views[namespace] = view
        return view

    def view(self, namespace: tuple[str, ...]) -> StateStore[Any]:
        return self._view_facade(namespace)

    def _child_namespaces(self) -> list[tuple[str, ...]]:
        # Topology is the source of truth for which namespaces exist; union in
        # any extra materialized views so nothing accessed is silently dropped.
        namespaces = set(self.state_types) | set(self._views)
        return sorted(ns for ns in namespaces if ns != ())

    def serialize_tree(
        self,
        serializer: BaseSerializer,
        child_namespaces: set[tuple[str, ...]] | None = None,
    ) -> dict[str, Any]:
        """Portable snapshot: the root payload with ``children[ns]`` nested.

        Childless runs (a single namespace) produce exactly the flat root
        snapshot, with no ``children`` key.

        Durable roots hand back a reconnect handle, not inline state: the
        handle already spans every namespace row for the run (``copy_from_handle``
        copies all rows under the ``run_id``), so resume reconnects the whole
        tree from it. There is nothing to nest, and the durable child facades
        have no sync ``state_data`` to gather — return the handle as-is.
        """
        result = self.view(()).to_dict(serializer)
        if is_durable_serialized_state(result):
            return result
        children: dict[str, Any] = {}
        namespaces = (
            sorted(child_namespaces)
            if child_namespaces is not None
            else self._child_namespaces()
        )
        for namespace in namespaces:
            if namespace == ():
                continue
            children[_namespace_key(namespace)] = self.view(namespace).to_dict(
                serializer
            )["state_data"]
        if children:
            result = {**result, CHILD_STATES_KEY: children}
        return result

    def add_seed_tree(
        self, serialized_state: dict[str, Any] | None, serializer: BaseSerializer
    ) -> None:
        """Distribute a portable tree snapshot across per-namespace facades.

        The root payload seeds the root facade; each ``children[ns]`` seeds its
        own namespace facade via ``add_seed``. There is no single bundle seed.
        """
        seeds = namespaced_seed_payloads(serialized_state, self.state_types)
        if seeds is None:
            return
        for namespace, payload in seeds.items():
            self._view_facade(namespace).add_seed(payload, serializer)


def in_memory_namespace_factory(
    state_types: dict[tuple[str, ...], type[BaseModel]],
) -> NamespaceFactory:
    """A namespace factory that mints a fresh in-memory facade per namespace."""

    def factory(namespace: tuple[str, ...]) -> StateStoreFacade[Any]:
        state_type = state_types.get(slot_namespace(namespace), DictState)
        return InMemoryStateStore(create_cleared_state(state_type))

    return factory


def namespaced_state_types(
    root_workflow: "Workflow",
) -> dict[tuple[str, ...], type[BaseModel]]:
    return {
        namespace: infer_state_type(instance)
        for namespace, instance in root_workflow._namespace_instances().items()
    }


def build_namespaced_state(
    root_workflow: "Workflow",
    factory: NamespaceFactory,
    serializer: BaseSerializer,
) -> NamespacedStateStores:
    """Build a per-namespace router for *root_workflow*.

    *factory* mints a ``StateStoreFacade`` for a given namespace (durable:
    ``create_state_store(run_id, namespace, ...)``; in-memory:
    ``in_memory_namespace_factory``).
    """
    return NamespacedStateStores(
        factory=factory,
        serializer=serializer,
        state_types=namespaced_state_types(root_workflow),
    )
