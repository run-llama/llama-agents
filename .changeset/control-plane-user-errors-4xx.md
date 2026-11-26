---
"llama-agents-control-plane": patch
---
Return 400 when a repository URL or ref is rejected and 409 when a requested deployment ID is taken or reserved, instead of 500.
