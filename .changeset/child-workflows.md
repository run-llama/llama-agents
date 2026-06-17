---
"llama-index-workflows": minor
"llama-agents-agentcore": patch
"llama-agents-server": patch
"llama-agents-dbos": patch
---

Add in-process child workflow composition with typed child declarations, namespaced execution, per-child state, catch-error recovery, timeouts, and opt-in child event streaming.

Annotated Workflow attributes are only auto-attached when they use the typed child workflow contract, so existing manual composition with bare StartEvent/StopEvent workflows remains compatible.
