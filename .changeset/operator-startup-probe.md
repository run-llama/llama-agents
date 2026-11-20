---
"llama-agents": patch
---
Give the operator a startup probe with a 5 minute budget and 5 second probe timeouts, so a replica that starts slowly on a busy node is not restarted in a loop.
