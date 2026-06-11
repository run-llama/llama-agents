---
"llama-index-workflows": minor
---

Steps can return `list[E]` to emit one event per element, and declare multiple single-event parameters to fire once when one of each has arrived. Serialized contexts move to version 2: in-progress work keeps its retry counts across resume, and unknown future versions fail loudly instead of loading as empty state.
