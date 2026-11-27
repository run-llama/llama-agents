---
"llama-agents-server": patch
---
Prevent late connection pools from escaping shutdown and ensure concurrent, cancelled, or failed pool closes release owned resources.
