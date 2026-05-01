import json
from typing import Any

from pydantic import BaseModel, ConfigDict, Field
from pydantic.functional_validators import model_validator
from typing_extensions import TypeVar

from workflows.context.state_store import DictState
from workflows.events import SerializableOptionalException

MODEL_T = TypeVar("MODEL_T", bound=BaseModel, default=DictState)  # type: ignore[reportGeneralTypeIssues]


class SerializedContextV0(BaseModel):
    """
    Legacy format for serialized context (V0). Supported for backwards compatibility, but does not
    include all currently required runtime state.
    """

    # Serialized state store payload produced by InMemoryStateStore.to_dict(serializer).
    # Shape:
    #   {
    #     "state_type": str,            # class name of the model (e.g. "DictState" or custom model)
    #     "state_module": str,          # module path of the model
    #     "state_data": ...             # see below
    #   }
    # For DictState: state_data = {"_data": {key: serialized_value_str}}, where each value is the
    # serializer-encoded string (e.g. JSON string from JsonSerializer.serialize).
    # For typed Pydantic models: state_data is a serializer-encoded string containing JSON for a dict with
    # discriminator fields (e.g. {"__is_pydantic": true, "value": <model_dump>, "qualified_name": <module.Class>}).
    state: dict[str, Any] = Field(default_factory=dict)

    # Streaming queue contents used by the event stream. This is a JSON string representing a list
    # of serializer-encoded events (each element is a string as returned by BaseSerializer.serialize).
    # Example: '["<serialized_event>", "<serialized_event>"]'.
    streaming_queue: str = Field(default="[]")

    # Per-step (and waiter) inbound event queues. Maps queue name -> JSON string representing a list
    # of serializer-encoded events (same format as streaming_queue).
    queues: dict[str, str] = Field(default_factory=dict)

    # Buffered events used by Context.collect_events. Maps buffer_id -> { fully.qualified.EventType: [serialized_event_str, ...] }.
    # Each inner list element is a serializer-encoded string for an Event.
    event_buffers: dict[str, dict[str, list[str]]] = Field(default_factory=dict)

    # Events that were in-flight for each step at serialization time. Maps step_name -> [serialized_event_str, ...].
    in_progress: dict[str, list[str]] = Field(default_factory=dict)

    # Pairs recorded when a step produced an output event: (step_name, input_event_class_name).
    # Note: stored as Python tuples here; if JSON-encoded externally they become 2-element lists.
    accepted_events: list[tuple[str, str]] = Field(default_factory=list)

    # Broker log of all dispatched events in order, as serializer-encoded strings.
    broker_log: list[str] = Field(default_factory=list)

    # Whether the workflow was running when serialized.
    is_running: bool = Field(default=False)

    # IDs currently waiting in wait_for_event to suppress duplicate waiter events. These IDs may appear
    # as keys in `queues` (they are used as queue names for waiter-specific queues).
    waiting_ids: list[str] = Field(default_factory=list)


class SerializedEventAttempt(BaseModel):
    """Serialized representation of an EventAttempt with retry information."""

    model_config = ConfigDict(arbitrary_types_allowed=True)

    # The event being processed (as serializer-encoded string)
    event: str
    # Number of times this event has been attempted (0 for first attempt)
    attempts: int = 0
    # Unix timestamp of first attempt, or None if not yet attempted
    first_attempt_at: float | None = None
    # Most recent exception when this event is scheduled for retry, if any.
    last_exception: SerializableOptionalException = None
    # Unix timestamp of the most recent failure, or None.
    last_failed_at: float | None = None
    # Per-handler recovery counts on this event's lineage. Maps catch_error
    # handler step name -> invocations so far. Empty on the main graph.
    recovery_counts: dict[str, int] = Field(default_factory=dict)


class SerializedWaiter(BaseModel):
    """Serialized representation of a waiter created by wait_for_event."""

    # Unique waiter ID
    waiter_id: str
    # The original event that triggered the wait (serialized)
    event: str
    # Fully qualified name of the event type being waited for (e.g. "mymodule.MyEvent")
    waiting_for_event: str
    # Requirements dict for matching the waited-for event
    has_requirements: bool = Field(default=False)
    # Resolved event if available (serialized), None otherwise
    resolved_event: str | None = None

    @model_validator(mode="before")
    @classmethod
    def deserialize_requirements(cls, v: dict[str, Any]) -> dict[str, Any]:
        # handle old requirements object
        if (
            "requirements" in v
            and isinstance(v["requirements"], dict)
            and len(v["requirements"]) > 0
        ):
            v["has_requirements"] = True
        return v


class SerializedStepWorkerState(BaseModel):
    """Serialized representation of a step worker's state."""

    # Queue of events waiting to be processed (with retry info)
    queue: list[SerializedEventAttempt] = Field(default_factory=list)
    # Events currently being processed (no retry info needed, will be re-queued on failure)
    in_progress: list[str] = Field(default_factory=list)
    # Collected events for ctx.collect_events(), keyed by buffer_id -> [event, ...]
    # Events are serialized strings
    collected_events: dict[str, list[str]] = Field(default_factory=dict)
    # Active waiters created by ctx.wait_for_event()
    collected_waiters: list[SerializedWaiter] = Field(default_factory=list)


class SerializedContext(BaseModel):
    """
    Current format for serialized context. Uses proper JSON structures instead of nested JSON strings.
    This format better represents BrokerState needs including retry information and waiter state.
    """

    # Version marker to distinguish from V0
    version: int = Field(default=1)

    # Serialized state store payload (same format as V0)
    state: dict[str, Any] = Field(default_factory=dict)

    # Whether the workflow was running when serialized
    is_running: bool = Field(default=False)

    # Per-step worker state with queues, in-progress events, collected events, and waiters
    # Maps step_name -> SerializedStepWorkerState
    workers: dict[str, SerializedStepWorkerState] = Field(default_factory=dict)

    @staticmethod
    def from_v0(v0: SerializedContextV0) -> "SerializedContext":
        """Convert V0 format to current format.

        Note: V0 doesn't store retry information or waiter state, so these will be lost.
        V0 also doesn't properly separate collected_events by buffer_id per step.
        """
        workers: dict[str, SerializedStepWorkerState] = {}

        # Convert queues and in_progress per step
        all_step_names = (
            set(v0.queues.keys())
            | set(v0.in_progress.keys())
            | set(v0.event_buffers.keys())
        )

        for step_name in all_step_names:
            # Skip waiter-specific queues (identified by waiter IDs)
            if step_name in v0.waiting_ids:
                continue

            queue_events: list[SerializedEventAttempt] = []

            # Convert in_progress events to queue entries with no retry info
            if step_name in v0.in_progress:
                for event_str in v0.in_progress[step_name]:
                    queue_events.append(
                        SerializedEventAttempt(
                            event=event_str, attempts=0, first_attempt_at=None
                        )
                    )

            # Convert queued events
            if step_name in v0.queues:
                queue_str = v0.queues[step_name]
                queue_list = json.loads(queue_str)
                for event_str in queue_list:
                    queue_events.append(
                        SerializedEventAttempt(
                            event=event_str, attempts=0, first_attempt_at=None
                        )
                    )

            # Convert collected events (V0 doesn't track buffer_id properly, so we use "default")
            collected: dict[str, list[str]] = {}
            if step_name in v0.event_buffers:
                # V0 format: step_name -> { event_type -> [event_str, ...] }
                # We flatten this into a single "default" buffer
                all_events = []
                for event_list in v0.event_buffers[step_name].values():
                    all_events.extend(event_list)
                if all_events:
                    collected["default"] = all_events

            workers[step_name] = SerializedStepWorkerState(
                queue=queue_events,
                in_progress=[],  # V0 in_progress are moved to queue
                collected_events=collected,
                collected_waiters=[],  # V0 doesn't store waiter state
            )

        return SerializedContext(
            version=1,
            state=v0.state,
            is_running=v0.is_running,
            workers=workers,
        )

    @staticmethod
    def from_dict_auto(data: dict[str, Any]) -> "SerializedContext":
        """Parse a dict as either V0 or V1 format and return V1."""
        # Check if it has version field
        if "version" in data and data["version"] == 1:
            return SerializedContext.model_validate(data)
        else:
            # Assume V0 format
            v0 = SerializedContextV0.model_validate(data)
            return SerializedContext.from_v0(v0)
