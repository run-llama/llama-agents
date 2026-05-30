---
"llama-index-utils-workflow": patch
---

Sanitize the `/` in namespaced child-workflow step ids when rendering an execution Mermaid diagram, so a parent-with-child execution produces valid Mermaid node ids instead of broken ones.
