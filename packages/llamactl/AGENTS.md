<!--
SPDX-License-Identifier: MIT
Copyright (c) 2026 LlamaIndex Inc.
-->

# llamactl

`ProjectClient` and `ControlPlaneClient` own `httpx.AsyncClient` pools. Do not
reuse one client instance across separate `asyncio.run(...)` calls; construct a
fresh client or keep all awaited client work in one event loop.
