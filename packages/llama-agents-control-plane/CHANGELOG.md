# llama-agents-control-plane

## 0.12.2

### Patch Changes

- c3fac21: Validate `appserver_version` as a public PEP 440 version
- Updated dependencies [c3fac21]
  - llama-agents-core@0.10.2

## 0.12.1

### Patch Changes

- 463c79d: Add `follow=false` query param on `GET /deployments/{id}/logs` so clients can fetch currently-available logs and exit. The default stays `follow=true`; existing streaming consumers are unchanged.
- Updated dependencies [463c79d]
  - llama-agents-core@0.10.1

## 0.12.0

### Minor Changes

- 2280e04: Rename deployment field `llama_deploy_version` to `appserver_version`. The old name remains as a deprecated input/output alias so existing clients and servers keep working.

### Patch Changes

- Updated dependencies [2280e04]
  - llama-agents-core@0.10.0

## 0.11.1

### Patch Changes

- 64579a9: Add `controlPlane.objectStorage.s3.unsigned` (Helm) / `S3_UNSIGNED` (env) toggle to send S3 requests unsigned, enabling authless S3-compatible backends (s3proxy, LocalStack, public-read buckets) without placeholder credentials. When enabled, applies to all S3 uses — builds, backups, and code-repo storage.

## 0.11.0

### Minor Changes

- e8b8f47: feat: add support for organizations

### Patch Changes

- Updated dependencies [e8b8f47]
  - llama-agents-core@0.9.0

## 0.10.5

### Patch Changes

- 7ad3049: Reduce full clones from github for config, repo validation, and sha discovery. Reduce dependencies on system git, preferring dulwich
- Updated dependencies [7ad3049]
  - llama-agents-core@0.8.5

## 0.10.4

### Patch Changes

- 740ee9e: Add a grace window to build artifact GC (configurable via `BUILD_ARTIFACT_GC_GRACE_SECONDS`, default 75m) and parallelize its delete loop with bounded concurrency. `llamactl auth`'s non-idempotent key-creation POST now only retries on connect-phase errors (`ConnectError`, `ConnectTimeout`, `PoolTimeout`) so initial-connectivity blips are absorbed without risking duplicate keys from a read-timeout retry.

## 0.10.3

### Patch Changes

- Updated dependencies [f27d98f]
  - llama-agents-core@0.8.4

## 0.10.2

### Patch Changes

- 3f12660: Add SSRF protection to git URL validation, blocking private/internal IP addresses
- Updated dependencies [3f12660]
  - llama-agents-core@0.8.3

## 0.10.1

### Patch Changes

- 46f2675: security patches
- Updated dependencies [46f2675]
  - llama-agents-core@0.8.2

## 0.10.0

### Minor Changes

- 58e7942: Rename Docker image repos to per-component names (llama-agents-<component>) with plain version tags

### Patch Changes

- Updated dependencies [58e7942]
  - llama-agents-core@0.8.1

## 0.9.0

### Minor Changes

- e2f3abd: Rename deployment name to display_name, add optional explicit id on create

### Patch Changes

- Updated dependencies [e2f3abd]
  - llama-agents-core@0.8.0

## 0.8.0

## 0.7.2

### Patch Changes

- e345a9b: Remove chunked encoding header to prevent double decoding

## 0.7.1

### Patch Changes

- Updated dependencies [7bb9a90]
  - llama-agents-core@0.7.0

## 0.7.0

### Minor Changes

- 9641415: Add dulwich-based git serving for internal repos. Users can push code via `llamactl push` and build pods clone via the build API. Bare repos are stored as tarballs in S3.

## 0.6.5

## 0.6.4

## 0.6.3

## 0.6.2

### Patch Changes

- Updated dependencies [508b5da]
  - llama-agents-core@0.6.2

## 0.6.1

### Patch Changes

- a064cc6: Fix duplicate uvicorn logs by preventing propagation to root logger
- Updated dependencies [1b86f90]
  - llama-agents-core@0.6.1

## 0.6.0

### Minor Changes

- 4ab011f: Rename packages from llama-deploy to llama-agents.

### Patch Changes

- Updated dependencies [4ab011f]
  - llama-agents-core@0.6.0

## 0.5.3

## 0.5.2

### Patch Changes

- e11ad55: Fix version ranges

## 0.5.1

## 0.5.0

### Minor Changes

- ac74af4: Run build separately as a 1x time process per deployment update. Build stored in s3. Allows for fast unsuspend, and better future support for replication

### Patch Changes

- Updated dependencies [ac74af4]
  - llama-deploy-core@0.5.0
