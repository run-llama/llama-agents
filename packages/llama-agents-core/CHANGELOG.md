# llama-agents-core

## 0.10.2

### Patch Changes

- c3fac21: Validate `appserver_version` as a public PEP 440 version

## 0.10.1

### Patch Changes

- 463c79d: Add `follow=false` query param on `GET /deployments/{id}/logs` so clients can fetch currently-available logs and exit. The default stays `follow=true`; existing streaming consumers are unchanged.

## 0.10.0

### Minor Changes

- 2280e04: Rename deployment field `llama_deploy_version` to `appserver_version`. The old name remains as a deprecated input/output alias so existing clients and servers keep working.

## 0.9.0

### Minor Changes

- e8b8f47: feat: add support for organizations

## 0.8.5

### Patch Changes

- 7ad3049: Reduce full clones from github for config, repo validation, and sha discovery. Reduce dependencies on system git, preferring dulwich

## 0.8.4

### Patch Changes

- f27d98f: Fix bootstrap bug from SSRF protection applied at wrong boundary

## 0.8.3

### Patch Changes

- 3f12660: Add SSRF protection to git URL validation, blocking private/internal IP addresses

## 0.8.2

### Patch Changes

- 46f2675: security patches

## 0.8.1

### Patch Changes

- 58e7942: Rename Docker image repos to per-component names (llama-agents-<component>) with plain version tags

## 0.8.0

### Minor Changes

- e2f3abd: Rename deployment name to display_name, add optional explicit id on create

## 0.7.0

### Minor Changes

- 7bb9a90: Add dulwich-based git serving for internal repos. Users can push code via llamactl push and build pods clone via the build API. Bare repos are stored as tarballs in S3

## 0.6.2

### Patch Changes

- 508b5da: Fix deployment update, fix github user auth

## 0.6.1

### Patch Changes

- 1b86f90: Fix python -m llama_deploy.\* broken by missing get_code

## 0.6.0

### Minor Changes

- 4ab011f: Rename packages from llama-deploy to llama-agents.

## 0.5.0

### Minor Changes

- ac74af4: Run build separately as a 1x time process per deployment update. Build stored in s3. Allows for fast unsuspend, and better future support for replication
