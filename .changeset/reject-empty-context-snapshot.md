---
"llama-index-workflows": patch
---
Context.from_dict now raises ContextSerdeError for a payload with none of the context fields, instead of restoring an empty context.
