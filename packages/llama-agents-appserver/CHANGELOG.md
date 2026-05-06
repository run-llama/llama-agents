# llama-agents-appserver

## 0.11.4

### Patch Changes

- Updated dependencies [c3fac21]
  - llama-agents-core@0.10.2

## 0.11.3

### Patch Changes

- Updated dependencies [463c79d]
  - llama-agents-core@0.10.1

## 0.11.2

### Patch Changes

- Updated dependencies [2280e04]
  - llama-agents-core@0.10.0

## 0.11.1

### Patch Changes

- 916b157: Fix appserver install when the target template is a uv workspace member. Install now targets whichever venv `uv run` resolves to, instead of a hard-coded `<template>/.venv`, so `llamactl dev validate` and `llamactl serve` work in workspace layouts.

## 0.11.0

### Minor Changes

- facbac4: `PUBLIC_*` env var overlay for UI builds: `PUBLIC_X` overrides `X` in the build env so backend and frontend can use different URLs for the same service. Removes dead `VITE_`/`NEXT_PUBLIC_` injection from `llamactl serve`. Helm network policy gains `extraEgressRules`, DNS selector overrides, and `blockPrivateRanges` toggle.

## 0.10.5

### Patch Changes

- Updated dependencies [e8b8f47]
  - llama-agents-core@0.9.0

## 0.10.4

### Patch Changes

- 7ad3049: Reduce full clones from github for config, repo validation, and sha discovery. Reduce dependencies on system git, preferring dulwich
- Updated dependencies [7ad3049]
  - llama-agents-core@0.8.5

## 0.10.3

### Patch Changes

- 286c91a: Loosen appserver deps on llama-agents-server

## 0.10.2

### Patch Changes

- Updated dependencies [f27d98f]
  - llama-agents-core@0.8.4

## 0.10.1

### Patch Changes

- Updated dependencies [3f12660]
  - llama-agents-core@0.8.3

## 0.10.0

### Minor Changes

- 3e2e7b8: Run all containers as non-root with hardened security contexts

## 0.9.1

### Patch Changes

- Updated dependencies [46f2675]
  - llama-agents-core@0.8.2

## 0.9.0

### Minor Changes

- 58e7942: Rename Docker image repos to per-component names (llama-agents-<component>) with plain version tags

### Patch Changes

- Updated dependencies [58e7942]
  - llama-agents-core@0.8.1

## 0.8.1

### Patch Changes

- Updated dependencies [e2f3abd]
  - llama-agents-core@0.8.0

## 0.8.0

## 0.7.2

## 0.7.1

### Patch Changes

- Updated dependencies [7bb9a90]
  - llama-agents-core@0.7.0

## 0.7.0

## 0.6.5

## 0.6.4

## 0.6.3

### Patch Changes

- 4127101: Exclude .pnpm-store directory from build tarballs
- 1594315: Skip auto-upgrade of dependencies (e.g. llama-index-workflows) during container bootstrap to avoid modifying the target project's pyproject.toml

## 0.6.2

### Patch Changes

- Updated dependencies [508b5da]
  - llama-agents-core@0.6.2

## 0.6.1

### Patch Changes

- Updated dependencies [1b86f90]
  - llama-agents-core@0.6.1

## 0.6.0

### Minor Changes

- 4ab011f: Rename packages from llama-deploy to llama-agents.

### Patch Changes

- Updated dependencies [4ab011f]
  - llama-agents-core@0.6.0

## 0.5.3

### Patch Changes

- eee29c1: Warn and upgrade workflows version to avoid obscure import errors

## 0.5.2

### Patch Changes

- e11ad55: Fix version ranges

## 0.5.1

## 0.5.0

### Minor Changes

- ac74af4: Run build separately as a 1x time process per deployment update. Build stored in s3. Allows for fast unsuspend, and better future support for replication
- 4ba0d9d: Switch out agent data workflow store for new journal based workflow store

### Patch Changes

- Updated dependencies [ac74af4]
  - llama-deploy-core@0.5.0
