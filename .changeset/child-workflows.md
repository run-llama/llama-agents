---
"llama-index-workflows": minor
---

Add child-workflow composition: declare a child workflow as a typed class field, trigger it by emitting the child's StartEvent, and receive its StopEvent back as an ordinary event. Child steps run namespaced inside the parent's broker state, so one checkpoint covers the whole tree and resume skips completed steps across it. Child stream events are tagged by origin and hidden from the parent stream by default; pass `stream_events(include_children=True)` to surface them.
