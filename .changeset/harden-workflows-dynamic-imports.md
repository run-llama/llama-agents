---
"llama-index-workflows": patch
---

Harden workflow deserialization by validating dynamically imported event/exception types and adding `allow_unknown_types` to `JsonSerializer`.
