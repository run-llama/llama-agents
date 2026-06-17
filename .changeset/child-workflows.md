---
"llama-index-workflows": minor
"llama-agents-agentcore": patch
"llama-agents-client": patch
"llama-agents-server": patch
"llama-agents-dbos": patch
---

Add child workflow composition. Child stream events now preserve their origin namespace across server/client event serialization, remain hidden from parent streams by default, and can be included explicitly.

Annotated Workflow attributes are only auto-attached when they use the typed child workflow contract, so existing manual composition with bare StartEvent/StopEvent workflows remains compatible.
