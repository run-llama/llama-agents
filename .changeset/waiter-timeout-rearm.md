---
"llama-index-workflows": patch
---
Re-arm pending wait_for_event timeouts on snapshot/resume so a restored run still raises asyncio.TimeoutError instead of waiting forever.
