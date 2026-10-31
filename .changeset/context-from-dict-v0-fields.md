---
"llama-index-workflows": patch
---
Reject serialized context payloads that have no version and none of the legacy V0 fields, so `Context.from_dict` raises `ContextSerdeError` instead of restoring an empty context.
