---
"llama-index-workflows": patch
"llama-agents-server": patch
"llama-agents-dbos": patch
---

Nested child workflows now behave like the root for state, error handling, and timeouts: a child's `@catch_error` handlers recover the child's own steps, a child's `ctx.store` writes are durable across reload under the server and DBOS runtimes (persisted in the run's single state row), and a child's `timeout` bounds that child's execution as a catchable failure rather than being ignored.
