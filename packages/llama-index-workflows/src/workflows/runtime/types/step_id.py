# SPDX-License-Identifier: MIT
# Copyright (c) 2026 LlamaIndex Inc.

from __future__ import annotations

from dataclasses import dataclass
from typing import Any

from pydantic_core import core_schema


@dataclass(frozen=True)
class StepId:
    """Internal workflow runtime step identity."""

    namespace: tuple[str, ...]
    name: str

    def __post_init__(self) -> None:
        if not self.name:
            raise ValueError("StepId name cannot be empty")
        if any(not part for part in self.namespace):
            raise ValueError("StepId namespace parts cannot be empty")

    @classmethod
    def root(cls, name: str) -> StepId:
        return cls(namespace=(), name=name)

    @classmethod
    def from_str(cls, value: str) -> StepId:
        parts = tuple(value.split("/"))
        if len(parts) == 1:
            return cls.root(value)
        return cls(namespace=parts[:-1], name=parts[-1])

    def __str__(self) -> str:
        if not self.namespace:
            return self.name
        return "/".join((*self.namespace, self.name))

    @classmethod
    def _validate(cls, value: Any) -> StepId:
        if isinstance(value, StepId):
            return value
        if isinstance(value, str):
            return cls.from_str(value)
        raise TypeError(f"Expected StepId or str, got {type(value).__name__}")

    @classmethod
    def __get_pydantic_core_schema__(
        cls,
        source_type: Any,
        handler: Any,
    ) -> core_schema.CoreSchema:
        return core_schema.no_info_after_validator_function(
            cls._validate,
            core_schema.union_schema(
                [core_schema.is_instance_schema(cls), core_schema.str_schema()]
            ),
            serialization=core_schema.plain_serializer_function_ser_schema(str),
        )
