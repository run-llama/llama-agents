# llama-index-utils-workflow

## 0.11.0

### Minor Changes

- aee5fda: Add typed runtime step identities.

### Patch Changes

- 44887eb: Sanitize slashes in Mermaid execution diagram step IDs.

## 0.10.1

### Patch Changes

- 979d68b: Fix draw_all_possible_flows to accept workflow classes

## 0.10.0

### Minor Changes

- b32ec53: Drop python 3.9 support

## 0.9.5

### Patch Changes

- Updated dependencies [5e7f9e5]
- Updated dependencies [9f26314]
  - llama-index-workflows@2.16.0

## 0.9.4

### Patch Changes

- Updated dependencies [6ec262c]
  - llama-index-workflows@2.15.1

## 0.9.3

### Patch Changes

- Updated dependencies [77a3f9c]
- Updated dependencies [707a254]
- Updated dependencies [05f5f4e]
- Updated dependencies [3c22216]
- Updated dependencies [96e437e]
  - llama-index-workflows@2.15.0

## 0.9.3-rc.1

### Patch Changes

- Updated dependencies [3720c61]
- Updated dependencies [a2aad32]
  - llama-index-workflows@2.15.0-rc.1

## 0.9.3-rc.0

### Patch Changes

- Updated dependencies [e981f73]
- Updated dependencies [b515a46]
- Updated dependencies [7433d4c]
  - llama-index-workflows@2.15.0-rc.0

## 0.9.2

### Patch Changes

- Updated dependencies [3590913]
- Updated dependencies [7433d4c]
  - llama-index-workflows@2.14.2

## 0.9.1

### Patch Changes

- Updated dependencies [6ece797]
  - llama-index-workflows@2.14.1

## 0.9.0

### Minor Changes

- 73c1254: refactor: expand runtime plugin architecture

  - Refactoring to better support alternate distributed backends
  - Some `Context` methods may now raise errors if used in an unexpected context
  - `WorkflowHandler` is no longer a future. Retains compatibility methods for main use cases (exception, cancel, etc)

### Patch Changes

- db90f89: Separate server/client to their own packages under a llama_agents namespace
- 33bbd23: Read workflows from globals rather than sys modules to facilitate more robust/correct class loading
- 0e826b1: Added in the utility function draw_all_possible_flows_nested_mermaid. This draws the possible flows (not execution) of nested workflows
- Updated dependencies [73c1254]
- Updated dependencies [45e7614]
- Updated dependencies [45e7614]
- Updated dependencies [2900f58]
- Updated dependencies [6fdc45c]
  - llama-index-workflows@2.14.0

## 0.8.0

### Minor Changes

- 6dd7fc0: Add resource config node support to workflow representation

## 0.7.1

### Patch Changes

- 40be1c7: add workflow class name to WorkflowGraph representation

## 0.7.0

### Minor Changes

- e53c654: Add further detail to workflow graph, mainly adding `Resource` nodes to workflow graph and visualizations
- 0d72b4d: reorganize workflow graph representation types

## 0.6.0

### Minor Changes

- 96fd9c9: Added new function draw_most_recent_execution_mermaid

  To draw the most recent workflow run in a mermaid format

## 0.5.2

### Patch Changes

- 8e84276: Increase minimum llama-index-workflows version

## 0.5.1

### Patch Changes

- f307253: Update typechecking to support ty
- 91159d7: Moving `_extract_workflow_structure` to its own module in workflow core
- 300fd05: Add stricter ruff formatting checks
- 32ae78a: Switch build backend to uv
