---
title: Continuous Deployment with GitHub Actions
sidebar:
  order: 19
---
:::caution
Cloud deployments of LlamaAgents are now in beta preview and broadly available for feedback. You can try them out locally or deploy to LlamaCloud and send us feedback with the in-app button.
:::

Use `llamactl deployments apply -f` in GitHub Actions when you want a deployment spec in source control and an automated update on every push.

## Required Secrets and Variables

`llamactl` can authenticate from environment variables, so CI does not need a stored profile:

- `LLAMA_CLOUD_API_KEY`: store as a GitHub Actions secret.
- `LLAMA_AGENTS_PROJECT_ID`: store as a GitHub Actions variable or secret.

If you deploy to a non-default LlamaCloud environment, also set `LLAMA_CLOUD_BASE_URL`.

## Deployment Spec

Keep a deployment spec in your repository, for example `deployment.yaml`:

```yaml
name: my-agent
spec:
  repo_url: https://github.com/${GITHUB_REPOSITORY}
  deployment_file_path: "."
  git_ref: ${GITHUB_SHA}
  secrets:
    OPENAI_API_KEY: ${OPENAI_API_KEY}
```

`deployments apply` resolves `${VAR}` references from the process environment before sending the deployment to LlamaCloud. That lets the same file use GitHub-provided values like `GITHUB_SHA` and secrets injected by the workflow.

Use a stable `name` if the workflow should update the same deployment every run. If `name` is omitted and the spec uses `generate_name`, each apply can create a new deployment.

## Workflow Example

```yaml
name: Deploy LlamaAgents app

on:
  push:
    branches:
      - main

jobs:
  deploy:
    runs-on: ubuntu-latest
    env:
      LLAMA_CLOUD_API_KEY: ${{ secrets.LLAMA_CLOUD_API_KEY }}
      LLAMA_AGENTS_PROJECT_ID: ${{ vars.LLAMA_AGENTS_PROJECT_ID }}
      OPENAI_API_KEY: ${{ secrets.OPENAI_API_KEY }}
    steps:
      - uses: actions/checkout@v4

      - name: Apply deployment
        run: uvx llamactl deployments apply -f deployment.yaml --annotate-on-error
```

`--annotate-on-error` writes validation and API errors back into the YAML input so the Actions log points at the field that failed. The command exits non-zero on failure.

## Capturing Status Output

`llamactl` keeps machine-readable output on stdout and status messages on stderr. If a workflow parses status text from `deployments apply`, capture stderr explicitly:

```bash
status="$(uvx llamactl deployments apply -f deployment.yaml --annotate-on-error 2>&1 >/dev/null)"
printf '%s\n' "$status"
```

Prefer a stable `name` in `deployment.yaml` over parsing the status message when downstream steps need the deployment name.
