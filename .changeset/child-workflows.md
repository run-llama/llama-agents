---
"llama-index-workflows": minor
"llama-agents-server": patch
"llama-agents-dbos": patch
---

Add child-workflow composition: declare a child as a typed class field, emit its StartEvent to run it and receive its StopEvent back, with steps namespaced in the parent's broker state, tree-wide checkpoint/resume, per-child `catch_error`/`store`/`timeout` durable across reload under the server and DBOS runtimes, and opt-in child stream events via `stream_events(include_children=True)`.
