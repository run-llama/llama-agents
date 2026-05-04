# llama-agents

## 0.12.3

### Patch Changes

- Updated dependencies [c3fac21]
  - llama-agents-control-plane@0.12.2
  - llama-agents-appserver@0.11.4

## 0.12.2

### Patch Changes

- Updated dependencies [463c79d]
  - llama-agents-control-plane@0.12.1
  - llama-agents-appserver@0.11.3

## 0.12.1

### Patch Changes

- Updated dependencies [2280e04]
  - llama-agents-control-plane@0.12.0
  - llama-agents-appserver@0.11.2

## 0.12.0

### Minor Changes

- 3ced443: Optional s3proxy sidecar for non-AWS object storage, plus inline-or-BYO creds on both the sidecar and control plane S3.

### Patch Changes

- 9eda189: Document the compatible `llama-agents-crds` chart version via a new `crds.version` values field, auto-synced at release time and surfaced in the README.

## 0.11.1

### Patch Changes

- Updated dependencies [916b157]
  - llama-agents-appserver@0.11.1

## 0.11.0

### Minor Changes

- de5bedc: Add `apps.namespace` to run `LlamaDeployment` CRs and their child resources in a separate namespace from the operator + control plane. Unset = everything in the release namespace.

### Patch Changes

- facbac4: New network policy values: `extraEgressRules`, configurable DNS selectors, and `blockPrivateRanges` toggle for reaching in-cluster services without disabling the policy.
- Updated dependencies [facbac4]
- Updated dependencies [64579a9]
  - llama-agents-appserver@0.11.0
  - llama-agents-control-plane@0.11.1

## 0.10.12

### Patch Changes

- Updated dependencies [fdc1c48]
  - llama-agents-operator@0.11.1

## 0.10.11

### Patch Changes

- Updated dependencies [e8b8f47]
  - llama-agents-control-plane@0.11.0
  - llama-agents-appserver@0.10.5

## 0.10.10

### Patch Changes

- Updated dependencies [7ad3049]
  - llama-agents-control-plane@0.10.5
  - llama-agents-appserver@0.10.4

## 0.10.9

### Patch Changes

- Updated dependencies [286c91a]
  - llama-agents-appserver@0.10.3

## 0.10.8

### Patch Changes

- Updated dependencies [740ee9e]
  - llama-agents-control-plane@0.10.4

## 0.10.7

### Patch Changes

- llama-agents-appserver@0.10.2
- llama-agents-control-plane@0.10.3

## 0.10.6

### Patch Changes

- Updated dependencies [3f12660]
  - llama-agents-control-plane@0.10.2
  - llama-agents-appserver@0.10.1

## 0.10.5

### Patch Changes

- Updated dependencies [3e2e7b8]
  - llama-agents-appserver@0.10.0
  - llama-agents-operator@0.11.0

## 0.10.4

### Patch Changes

- Updated dependencies [46f2675]
  - llama-agents-control-plane@0.10.1
  - llama-agents-appserver@0.9.1

## 0.10.3

### Patch Changes

- Updated dependencies [782939b]
  - llama-agents-operator@0.10.2

## 0.10.2

### Patch Changes

- Updated dependencies [de92a8b]
  - llama-agents-operator@0.10.1

## 0.10.1

### Patch Changes

- Updated dependencies [58e7942]
- Updated dependencies [ea577a1]
  - llama-agents-control-plane@0.10.0
  - llama-agents-appserver@0.9.0
  - llama-agents-operator@0.10.0

## 0.10.0

### Minor Changes

- 7025b30: Rename Helm chart from cloud-llama-deploy-chart to llama-agents-chart with standardized resource naming
