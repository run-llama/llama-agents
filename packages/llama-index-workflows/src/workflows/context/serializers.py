# SPDX-License-Identifier: MIT
# Copyright (c) 2026 LlamaIndex Inc.

from __future__ import annotations

import base64
import json
import pickle
from abc import ABC, abstractmethod
from collections.abc import Iterable
from typing import Any

from pydantic import BaseModel

from .utils import get_qualified_name, import_module_from_qualified_name


class BaseSerializer(ABC):
    """
    Interface for value serialization used by the workflow context and state store.

    Implementations must encode arbitrary Python values into a string and be able
    to reconstruct the original values from that string.

    See Also:
        - [JsonSerializer][workflows.context.serializers.JsonSerializer]
        - [PickleSerializer][workflows.context.serializers.PickleSerializer]
    """

    @abstractmethod
    def serialize(self, value: Any) -> str: ...

    @abstractmethod
    def deserialize(self, value: str) -> Any: ...


class JsonSerializer(BaseSerializer):
    """
    JSON-first serializer that understands Pydantic models and LlamaIndex components.

    Behavior:
    - Pydantic models are encoded as JSON with their qualified class name so they
      can be faithfully reconstructed.
    - LlamaIndex components (objects exposing `class_name` and `to_dict`) are
      serialized to their dict form alongside the qualified class name.
    - Dicts and lists are handled recursively.

    Fallback for unsupported objects is to attempt JSON encoding directly; if it
    fails, a `ValueError` is raised.

    Examples:
        ```python
        s = JsonSerializer()
        payload = s.serialize({"x": 1, "y": [2, 3]})
        data = s.deserialize(payload)
        assert data == {"x": 1, "y": [2, 3]}
        ```

    See Also:
        - [BaseSerializer][workflows.context.serializers.BaseSerializer]
        - [PickleSerializer][workflows.context.serializers.PickleSerializer]
    """

    def __init__(
        self,
        *,
        allowed_types: Iterable[type[Any] | str] | None = None,
    ) -> None:
        if allowed_types is None:
            self._allowed_type_names: frozenset[str] | None = None
        else:
            self._allowed_type_names = frozenset(
                t if isinstance(t, str) else f"{t.__module__}.{t.__qualname__}"
                for t in allowed_types
            )

    def _validate_qualified_name(self, qualified_name: str) -> None:
        if self._allowed_type_names is None:
            return
        if qualified_name not in self._allowed_type_names:
            raise ValueError(
                f"Refusing to import disallowed workflow state type: {qualified_name}. "
                "Pass it via allowed_types to the JsonSerializer constructor."
            )

    def serialize_value(self, value: Any) -> Any:
        """
        Events with a wrapper type that includes type metadata, so that they can be reserialized into the original Event type.
        Traverses dicts and lists recursively.

        Args:
            value (Any): The value to serialize.

        Returns:
            Any: The serialized value. A dict, list, string, number, or boolean.
        """
        # This has something to do with BaseComponent from llama_index.core. Is it still needed?
        if hasattr(value, "class_name"):
            retval = {
                "__is_component": True,
                "value": value.to_dict(),
                "qualified_name": get_qualified_name(value),
            }
            return retval

        if isinstance(value, BaseModel):
            return {
                "__is_pydantic": True,
                "value": value.model_dump(mode="json"),
                "qualified_name": get_qualified_name(value),
            }

        if isinstance(value, dict):
            return {k: self.serialize_value(v) for k, v in value.items()}

        if isinstance(value, list):
            return [self.serialize_value(item) for item in value]

        return value

    def serialize(self, value: Any) -> str:
        """Serialize an arbitrary value to a JSON string.

        Args:
            value (Any): The value to encode.

        Returns:
            str: JSON string.

        Raises:
            ValueError: If the value cannot be encoded to JSON.
        """
        try:
            serialized_value = self.serialize_value(value)
            return json.dumps(serialized_value)
        except Exception:
            raise ValueError(f"Failed to serialize value: {type(value)}: {value!s}")

    def deserialize_value(self, data: Any) -> Any:
        """Helper to deserialize a single dict or other json value from its discriminator fields back into a python class.

        Args:
            data (Any): a dict, list, string, number, or boolean

        Returns:
            Any: The deserialized value.
        """
        if isinstance(data, dict):
            if data.get("__is_pydantic") and data.get("qualified_name"):
                self._validate_qualified_name(data["qualified_name"])
                module_class = import_module_from_qualified_name(data["qualified_name"])
                return module_class.model_validate(data["value"])
            elif data.get("__is_component") and data.get("qualified_name"):
                self._validate_qualified_name(data["qualified_name"])
                module_class = import_module_from_qualified_name(data["qualified_name"])
                return module_class.from_dict(data["value"])
            return {k: self.deserialize_value(v) for k, v in data.items()}
        elif isinstance(data, list):
            return [self.deserialize_value(item) for item in data]
        return data

    def deserialize(self, value: str) -> Any:
        """Deserialize a JSON string into Python objects.

        Args:
            value (str): JSON string.

        Returns:
            Any: The reconstructed value.
        """
        data = json.loads(value)
        return self.deserialize_value(data)


class PickleSerializer(JsonSerializer):
    """
    Hybrid serializer: JSON when possible, Pickle as a safe fallback.

    This serializer attempts JSON first for readability and portability, and
    transparently falls back to Pickle for objects that cannot be represented in
    JSON. Deserialization prioritizes Pickle and falls back to JSON.

    Warning:
        Pickle can execute arbitrary code during deserialization. Only
        deserialize trusted payloads.

    Note: Used to be called `JsonPickleSerializer` but it was renamed to `PickleSerializer`.

    Examples:
        ```python
        s = PickleSerializer()
        class Foo:
            def __init__(self, x):
                self.x = x
        payload = s.serialize(Foo(1))  # will likely use Pickle
        obj = s.deserialize(payload)
        assert isinstance(obj, Foo)
        ```
    """

    def serialize(self, value: Any) -> str:
        """Serialize with JSON preference and Pickle fallback.

        Args:
            value (Any): The value to encode.

        Returns:
            str: Encoded string (JSON or base64-encoded Pickle bytes).
        """
        try:
            return super().serialize(value)
        except Exception:
            return base64.b64encode(pickle.dumps(value)).decode("utf-8")

    def deserialize(self, value: str) -> Any:
        """Deserialize with Pickle preference and JSON fallback.

        Args:
            value (str): Encoded string.

        Returns:
            Any: The reconstructed value.

        Notes:
            Use only with trusted payloads due to Pickle security implications.
        """
        try:
            return pickle.loads(base64.b64decode(value))
        except Exception:
            return super().deserialize(value)


JsonPickleSerializer = PickleSerializer
