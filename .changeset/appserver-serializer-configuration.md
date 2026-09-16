---
"llama-agents-appserver": minor
---

Preserve a deployed `WorkflowServer` serializer and each workflow's additional events. Deployments without an explicit serializer now restore declared types by default, so list other stored classes in `JsonSerializer(allowed_types=[...])` or configure `JsonSerializer()` to retain import lookup.
