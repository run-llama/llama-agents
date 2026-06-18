# SPDX-License-Identifier: MIT
# Copyright (c) 2026 LlamaIndex Inc.
"""Re-export server components from the optional llama-agents-server package."""

import warnings

warnings.warn(
    "Importing from 'workflows.server' is deprecated. "
    "Install 'llama-agents-server' and use "
    "'from llama_agents.server import ...' instead.",
    DeprecationWarning,
    stacklevel=2,
)

try:
    from llama_agents.server import (
        AbstractWorkflowStore,
        HandlerQuery,
        PersistentHandler,
        SqliteWorkflowStore,
        WorkflowServer,
    )
except ImportError as e:
    raise ImportError(
        "workflows.server requires the 'server' extra. "
        "Install with: pip install 'llama-index-workflows[server]'"
    ) from e

__all__ = [
    "WorkflowServer",
    "AbstractWorkflowStore",
    "HandlerQuery",
    "PersistentHandler",
    "SqliteWorkflowStore",
]
