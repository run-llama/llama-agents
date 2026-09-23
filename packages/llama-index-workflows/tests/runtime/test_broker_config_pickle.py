# SPDX-License-Identifier: MIT
# Copyright (c) 2026 LlamaIndex Inc.

from __future__ import annotations

import base64
import pickle

from workflows.runtime.types.internal_state import BrokerConfig, CollectionStreamInstance
from workflows.runtime.types.step_id import StepId


# Pickled with origin/main's BrokerConfig and CollectionBinding, using string step names.
_LEGACY_CONFIG = (
    "gASViAEAAAAAAACMJndvcmtmbG93cy5ydW50aW1lLnR5cGVzLmludGVybmFsX3N0YXRllIwM"
    "QnJva2VyQ29uZmlnlJOUKYGUfZQojAVzdGVwc5R9lCiMBnNvdXJjZZROjAZ0YXJnZXSUTnWM"
    "B3RpbWVvdXSUTowUY2F0Y2hfZXJyb3JfaGFuZGxlcnOUfZSMEGhhbmRsZXJfZm9yX3N0ZXCU"
    "fZSME2NvbGxlY3Rpb25fYmluZGluZ3OUfZSMDXNvdXJjZTp0YXJnZXSUaACMEUNvbGxlY3Rp"
    "b25CaW5kaW5nlJOUKYGUfZQojAJpZJRoEIwLc291cmNlX3N0ZXCUaAeMC3RhcmdldF9zdGVw"
    "lGgIjAppdGVtX3R5cGVzlIwQd29ya2Zsb3dzLmV2ZW50c5SMClN0YXJ0RXZlbnSUk5SFlIwG"
    "cG9saWN5lIwRd29ya2Zsb3dzLmNvbGxlY3SUjAdDb2xsZWN0lJOUKYGUfZSMC2NhcmRpbmFs"
    "aXR5lGgejANBbGyUk5QpgZRzYnVic3ViLg=="
)


def test_legacy_pickle_restores_collection_binding_step_ids() -> None:
    config = pickle.loads(base64.b64decode(_LEGACY_CONFIG))
    assert isinstance(config, BrokerConfig)

    source = StepId.root("source")
    target = StepId.root("target")
    binding = config.collection_bindings["source:target"]
    stream = CollectionStreamInstance(
        stream_id="stream",
        source_step=source,
        scope_path=(),
        accepting_binding_ids=(binding.id,),
    )

    assert set(config.steps) == {source, target}
    assert config.bindings_for_source(source) == (binding,)
    assert config.binding_for_target("stream", target, {"stream": stream}) == binding
