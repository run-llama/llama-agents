# SPDX-License-Identifier: MIT
# Copyright (c) 2026 LlamaIndex Inc.

"""Regression for PickleSerializer mistaking some JSON strings for pickle."""

from __future__ import annotations

import base64
import pickle

from workflows.context.serializers import JsonSerializer, PickleSerializer


def test_pickle_serializer_roundtrips_json_string_that_looks_like_base64() -> None:
    """JSON ``\"SwEu\"`` must not deserialize as the integer pickle of ``1``."""
    serializer = PickleSerializer()
    payload = serializer.serialize("SwEu")
    # Prefer JSON when possible
    assert payload == JsonSerializer().serialize("SwEu")
    assert payload == '"SwEu"'
    assert serializer.deserialize(payload) == "SwEu"


def test_pickle_serializer_still_roundtrips_unjsonable_objects() -> None:
    serializer = PickleSerializer()
    value = {1, 2, 3}  # set is not JSON-serializable
    payload = serializer.serialize(value)
    assert base64.b64decode(payload, validate=True)
    assert serializer.deserialize(payload) == value


def test_legacy_base64_pickle_payload_still_loads() -> None:
    """Existing pickle payloads (no JSON wrapper) keep working."""
    payload = base64.b64encode(pickle.dumps({"k": "v"})).decode("utf-8")
    assert PickleSerializer().deserialize(payload) == {"k": "v"}
