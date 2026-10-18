---
"llama-index-workflows": patch
---

Fix `@catch_error` handlers never running when a workflow is created with `disable_validation=True`. Handler routing is now wired at construction instead of only during validation.
