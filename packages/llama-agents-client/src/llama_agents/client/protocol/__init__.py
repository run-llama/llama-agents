from __future__ import annotations

from typing import Any, Literal

from pydantic import BaseModel
from workflows.representation import WorkflowGraph

from .serializable_events import EventEnvelopeWithMetadata

# Shared protocol types between client and server

# Mirrors server.store Status
Status = Literal["running", "completed", "failed", "cancelled"]


def is_status_completed(status: Status) -> bool:
    return status in {"completed", "failed", "cancelled"}


class HandlerData(BaseModel):
    handler_id: str
    workflow_name: str
    run_id: str | None
    error: str | None
    result: EventEnvelopeWithMetadata | None
    result_unreadable: bool = False
    status: Status
    started_at: str
    updated_at: str | None
    completed_at: str | None


class HandlersListResponse(BaseModel):
    handlers: list[HandlerData]


class HealthResponse(BaseModel):
    status: Literal["healthy", "unhealthy"]


class WorkflowsListResponse(BaseModel):
    workflows: list[str]


class SendEventResponse(BaseModel):
    status: Literal["sent"]


class CancelHandlerResponse(BaseModel):
    status: Literal["deleted", "cancelled"]


class WorkflowSchemaResponse(BaseModel):
    start: dict[str, Any]
    stop: dict[str, Any]


class WorkflowEventsListResponse(BaseModel):
    events: list[dict[str, Any]]


class WorkflowGraphResponse(BaseModel):
    graph: WorkflowGraph


__all__ = [
    "Status",
    "is_status_completed",
    "HandlerData",
    "HandlersListResponse",
    "HealthResponse",
    "WorkflowsListResponse",
    "SendEventResponse",
    "CancelHandlerResponse",
    "WorkflowSchemaResponse",
    "WorkflowEventsListResponse",
    "WorkflowGraphResponse",
]
