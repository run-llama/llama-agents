---
"llama-agents-core": patch
"llama-agents-control-plane": patch
---
Return 400 when a repository URL or ref is rejected, 409 when a requested deployment ID is taken and 400 when it is reserved, instead of 500. Unresolvable and private repository hosts get the same error detail.
