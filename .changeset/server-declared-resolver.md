---
"llama-agents-client": patch
"llama-agents-server": minor
"llama-agents-dbos": patch
"llama-agents-appserver": patch
---

Decode API events and stored results using each workflow's public event types.

Event types that a workflow does not declare through its steps no longer resolve from request payloads or stored handler results. Pass them to `add_workflow(..., additional_events=[...])` to keep accepting them.
