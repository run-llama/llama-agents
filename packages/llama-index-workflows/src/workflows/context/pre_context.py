"""Pre-run context - configuration face before workflow execution."""

from __future__ import annotations

from typing import TYPE_CHECKING, Any, Generic, cast

from pydantic import ValidationError

from workflows.context.context_types import MODEL_T, SerializedContext
from workflows.context.serializers import BaseSerializer, JsonSerializer
from workflows.context.state_store import (
    InMemoryStateStore,
    StateStore,
    infer_state_type,
    is_durable_serialized_state,
)
from workflows.errors import ContextSerdeError
from workflows.runtime.types.internal_state import BrokerState

if TYPE_CHECKING:
    from workflows.workflow import Workflow


class PreContext(Generic[MODEL_T]):
    """Context state before workflow starts.

    Provides access to workflow configuration and serialization
    for persistence/restoration. A staging store is lazily created
    on first `.store` access and carried into the runtime when the
    workflow starts.
    """

    _init_snapshot: SerializedContext
    _serializer: BaseSerializer
    _workflow: "Workflow"
    _store: InMemoryStateStore[MODEL_T] | None

    def __init__(
        self,
        workflow: "Workflow",
        previous_context: dict[str, Any] | None = None,
        serializer: BaseSerializer | None = None,
    ) -> None:
        self._serializer = serializer or JsonSerializer()
        self._workflow = workflow
        self._store = None

        # Parse the serialized context
        if previous_context is not None:
            try:
                # Auto-detect and convert V0 to V1 if needed
                previous_context_parsed = SerializedContext.from_dict_auto(
                    previous_context
                )
                # Validate it fully parses synchronously to avoid delayed validation errors
                BrokerState.from_serialized(
                    previous_context_parsed, workflow, self._serializer
                )
            except (ValidationError, ValueError) as e:
                raise ContextSerdeError(
                    f"Context dict specified in an invalid format: {e}"
                ) from e
        else:
            previous_context_parsed = SerializedContext()

        self._init_snapshot = previous_context_parsed

    @property
    def store(self) -> StateStore[MODEL_T]:
        """Lazily-created staging store for pre-run state access.

        For fresh contexts, the state type is inferred from workflow step
        annotations. For deserialized contexts, the store is restored from
        the serialized state data.
        """
        if self._store is None:
            serialized_state = self._init_snapshot.state
            if serialized_state:
                if is_durable_serialized_state(serialized_state):
                    store_type = serialized_state.get("store_type")
                    raise ContextSerdeError(
                        "Context.store cannot be accessed before workflow start "
                        f"for durable state store '{store_type}'. Pass the context "
                        "to workflow.run() so the runtime can restore it."
                    )
                self._store = cast(
                    InMemoryStateStore[MODEL_T],
                    InMemoryStateStore.from_dict(serialized_state, self._serializer),
                )
            else:
                state_type = infer_state_type(self._workflow)
                self._store = cast(
                    InMemoryStateStore[MODEL_T],
                    InMemoryStateStore(state_type()),
                )
        return self._store

    @property
    def serialized_state(self) -> dict[str, Any] | None:
        """Return the serialized state for handoff to the runtime.

        If the staging store was accessed, its current contents are
        serialized.  Otherwise the snapshot's original state is returned
        unchanged, avoiding unnecessary work.
        """
        if self._store is not None:
            return self._store.to_dict(self._serializer)
        return self._init_snapshot.state

    @property
    def is_running(self) -> bool:
        """Whether the workflow is currently running.

        Returns the is_running state from the init snapshot, which may be True
        if restoring a context that was previously mid-run.
        """
        return self._init_snapshot.is_running

    @property
    def init_snapshot(self) -> SerializedContext:
        """The initial serialized context snapshot."""
        return self._init_snapshot

    @property
    def serializer(self) -> BaseSerializer:
        """The serializer used for this context."""
        return self._serializer
