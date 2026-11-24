---
"llama-agents-client": patch
---
Accept a bare start event that has a field named `value`, such as the body `run_workflow` sends, instead of rejecting it as an envelope with no type.
