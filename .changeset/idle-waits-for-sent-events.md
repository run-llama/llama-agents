---
"llama-index-workflows": patch
---

Don't emit WorkflowIdleEvent while events sent with ctx.send_event() are still waiting to be delivered
