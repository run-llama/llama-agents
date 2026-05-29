# SPDX-License-Identifier: MIT
# Copyright (c) 2026 LlamaIndex Inc.

"""Typed step identity for the workflow control loop.

A :class:`StepId` is the key under which a step's worker pool, queue, and
collected-event/waiter buffers live in :class:`~workflows.runtime.types.internal_state.BrokerState`.
It bundles a *namespace path* with the step's bare name so that child-workflow
steps can live, namespaced, inside the parent's single broker state without
overloading a bare string at every use site.

Root (parent) steps live at namespace ``()``; a child declared on field
``child`` namespaces its steps under ``("child",)``; a grandchild under
``("child", "grandchild")``. The namespace is a flat tuple, never a nested
structure.

The serialized form is a ``/``-joined string (``"child/grandchild/step"``);
the root form is just the bare name (``"step"``). Because step names and child
field names are Python identifiers — they cannot contain ``/`` — this
projection is unambiguous, and the root form round-trips identically to the
pre-StepId ``str`` wire format, so existing journals deserialize unchanged.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any

from pydantic import GetCoreSchemaHandler
from pydantic_core import core_schema

_NAMESPACE_SEP = "/"


@dataclass(frozen=True)
class StepId:
    """The namespaced identity of a workflow step.

    Attributes:
        namespace: The child-workflow path this step lives under. Empty for a
            root (parent) step.
        name: The bare step name (the method/function name dispatched against
            the owning workflow instance).
    """

    namespace: tuple[str, ...]
    name: str

    @property
    def is_root(self) -> bool:
        """Whether this step belongs to the root (parent) workflow."""
        return not self.namespace

    @classmethod
    def root(cls, name: str) -> StepId:
        """Construct a root-namespace step id."""
        return cls((), name)

    def __str__(self) -> str:
        return _NAMESPACE_SEP.join((*self.namespace, self.name))

    @classmethod
    def from_str(cls, value: str) -> StepId:
        """Parse the ``/``-joined serialized form back into a :class:`StepId`."""
        parts = value.split(_NAMESPACE_SEP)
        return cls(tuple(parts[:-1]), parts[-1])

    @classmethod
    def __get_pydantic_core_schema__(
        cls, source: Any, handler: GetCoreSchemaHandler
    ) -> core_schema.CoreSchema:
        """Serialize as the ``/``-joined string; validate from string or StepId.

        Accepting the bare-string form is what keeps pre-StepId journals
        (``{"step_name": "foo"}``) decodable: ``"foo"`` parses to a root
        ``StepId``.
        """

        def _validate(value: Any) -> StepId:
            if isinstance(value, StepId):
                return value
            if isinstance(value, str):
                return cls.from_str(value)
            raise TypeError(
                f"Cannot parse StepId from {type(value).__name__!r}; "
                "expected str or StepId."
            )

        return core_schema.no_info_plain_validator_function(
            _validate,
            serialization=core_schema.plain_serializer_function_ser_schema(
                str, return_schema=core_schema.str_schema()
            ),
        )
