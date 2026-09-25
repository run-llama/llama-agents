# llama-agents-operator

## 0.11.3

### Patch Changes

- c5a5b10: Patch known vulnerabilities in the operator, appserver and control plane images by updating Go, npm, pip and Python dependencies

## 0.11.2

### Patch Changes

- 48e871d: Disable Kubernetes service links on build Job and appserver Deployment pods, including after template overlays.

## 0.11.1

### Patch Changes

- fdc1c48: Publish llama-agents-operator arm images

## 0.11.0

### Minor Changes

- 3e2e7b8: Run all containers as non-root with hardened security contexts

## 0.10.2

### Patch Changes

- 782939b: Strip legacy appserver- prefix from image tags to fix image pull failures

## 0.10.1

### Patch Changes

- de92a8b: Fix hardcoded watch namespace fallback to use pod's actual namespace

## 0.10.0

### Minor Changes

- 58e7942: Rename Docker image repos to per-component names (llama-agents-<component>) with plain version tags

### Patch Changes

- ea577a1: Disable nginx access log for the file server sidecar

## 0.9.0

### Minor Changes

- e2f3abd: Rename deployment name to display_name, add optional explicit id on create

## 0.8.0

### Minor Changes

- e24ebda: Move CRDs to Helm crds/ directory for install-only lifecycle, add llama-agents-crds chart for CRD upgrades, and make build API host configurable via LLAMA_DEPLOY_BUILD_API_HOST env var.

## 0.7.2

## 0.7.1

## 0.7.0

### Patch Changes

- 9641415: Add dulwich-based git serving for internal repos. Users can push code via `llamactl push` and build pods clone via the build API. Bare repos are stored as tarballs in S3.
- 7e241b6: Refactor LlamaDeployment controller for readability and increase coverage

## 0.6.5

### Patch Changes

- 9bb95fe: Fix infinite build retry loop where failed builds for deployments with generation > failedRolloutGeneration would endlessly delete and recreate jobs

## 0.6.4

### Patch Changes

- f3a38d0: Fix build job template overlay to merge resource requirements instead of replacing, so template-specified fields (e.g. ephemeral-storage) don't wipe out default CPU/memory requests and limits
- f3a38d0: Fix suspended deployments getting stuck in Pending/Building phase. Skip builds, capacity gates, and phase resets for suspended deployments. Add `status.lastBuiltGeneration` to allow explicit pre-builds via `spec.buildGeneration` bump while suspended.

## 0.6.3

### Patch Changes

- 4127101: Enable build retries by cleaning up stale failed jobs when the CR generation advances past the failed generation
- 4127101: Fix build job resource fallback to inherit app container resources when no dedicated build container is defined in the template overlay
- 2e1b600: Fix max concurrent rollouts gate to include PhaseBuilding deployments, preventing build jobs from bypassing the concurrency limit
- 4127101: Preserve PhaseBuilding during reconciliation to prevent status flip-flopping while a build job is running
- 4127101: Add self-watch so rollout-gated CRs wake immediately when another CR transitions out of a rolling phase, instead of waiting for the jitter timer
- 4127101: Strip build containers from runtime deployment overlay to prevent strategic merge from adding an invalid container to the pod spec

## 0.6.2

## 0.6.1

## 0.6.0

## 0.5.3

## 0.5.2

## 0.5.1

## 0.5.0

### Minor Changes

- ac74af4: Run build separately as a 1x time process per deployment update. Build stored in s3. Allows for fast unsuspend, and better future support for replication
