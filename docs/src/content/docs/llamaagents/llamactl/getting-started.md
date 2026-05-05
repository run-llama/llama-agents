---
title: Getting Started
sidebar:
  order: 1
---
:::caution
Cloud deployments of LlamaAgents are now in beta preview and broadly available for feedback. You can try them out locally or deploy to LlamaCloud and send us feedback with the in-app button.
:::

## Getting Started with `llamactl`

`llamactl` is the local development and deployment CLI for LlamaAgents. It can scaffold an app, run the app server locally, and manage cloud deployments from your terminal.

:::tip[Prefer a UI?]
You can also deploy starter templates directly from the LlamaCloud dashboard. See [Click-to-Deploy from LlamaCloud](/python/llamaagents/llamactl/click-to-deploy).
:::

Before you start:

- Install [`uv`](https://docs.astral.sh/uv/getting-started/installation/). `llamactl` uses it to manage Python environments and project dependencies.
- Install `git`. Cloud deployments are built from source repositories.
- Install Node.js if you are using a template with a frontend. For macOS and Linux, we recommend [`nvm`](https://github.com/nvm-sh/nvm). For Windows, we recommend [Chocolatey](https://community.chocolatey.org/packages/nodejs).
- Windows support is experimental. For the best experience, use WSL2. If you run directly on Windows, see [the Windows guide](https://github.com/run-llama/llamactl-windows).

## Install

Install `llamactl` with `pip`:

```bash
pip install llamactl
```

Or run it on demand with `uvx`:

```bash
uvx llamactl --help
```

If you use `uvx`, replace `llamactl` with `uvx llamactl` in the commands below.

## Authenticate

Log in with your browser:

```bash
llamactl auth login
```

If browser login is not available, use an API key and project ID:

```bash
llamactl auth token --api-key "$LLAMA_CLOUD_API_KEY" --project "$LLAMA_AGENTS_PROJECT_ID"
```

For CI or other non-interactive environments, you can skip the stored profile and set environment variables instead:

```bash
export LLAMA_CLOUD_API_KEY="llx-..."
export LLAMA_AGENTS_PROJECT_ID="project-id"
```

See [`llamactl auth`](/python/llamaagents/llamactl-reference/commands-auth) and [`llamactl auth env`](/python/llamaagents/llamactl-reference/commands-auth-env) for profile and environment commands.

## Initialize a Project

Create a new LlamaAgents project:

```bash
llamactl init
```

`llamactl init` opens a template picker and writes a project scaffold. Templates may include Python workflows only, or a Python app plus a frontend UI.

:::warning
`llamactl init` uses symlinks. On Windows, enable Developer Mode with `start ms-settings:developers` before running the command.
:::

:::info
The scaffold may include assistant-facing files such as `AGENTS.md`, `CLAUDE.md`, and `GEMINI.md`. They are optional and do not affect builds, runtime, or deployments.
:::

Application configuration lives in your project's `pyproject.toml`, or in `llama_agents.yaml` / `llama_agents.toml`. See the [Deployment Config Reference](/python/llamaagents/llamactl/configuration-reference) for the schema.

## Run Locally

From the project directory, start the local development server:

```bash
llamactl serve
```

`llamactl serve` installs dependencies, reads the workflows configured for the app, serves them as an API, and proxies the frontend development server when the app has a UI.

For example, this configuration serves `my-workflow` under the local deployment named `my-package`:

```toml
[project]
name = "my-package"

[tool.llamaagents.workflows]
my-workflow = "my_package.my_workflow:workflow"

[tool.llamaagents.ui]
directory = "ui"
```

```py
# src/my_package/my_workflow.py
workflow = MyWorkflow()
```

The local API is available at `http://localhost:4501/deployments/my-package`. To run the workflow, make a `POST` request to `/deployments/my-package/workflows/my-workflow/run`.

For flags, see [`llamactl serve`](/python/llamaagents/llamactl-reference/commands-serve). For workflow API details, see [Workflows & App Server API](/python/llamaagents/llamactl/workflow-api).

## Create a Cloud Deployment

Cloud deployments are built from a Git repository. Commit and push your project first:

```bash
git remote add origin https://github.com/org/repo
git add -A
git commit -m "Set up LlamaAgents app"
git push -u origin main
```

Then create the deployment:

```bash
llamactl deployments create
```

`deployments create` opens a YAML deployment spec in your `$EDITOR`. Review the detected repository, deployment config path, Git ref, and secrets. Save and close the file to create the deployment.

For non-interactive creation, pass a file:

```bash
llamactl deployments create -f deployment.yaml
```

Private GitHub repositories require LlamaCloud to have repository access. If access is missing, `llamactl` will return an error with the next step.

## Declarative Deployments

For repeatable deploys, generate an apply-shaped deployment spec:

```bash
llamactl deployments template > deployment.yaml
```

Edit the file, then apply it:

```bash
llamactl deployments apply -f deployment.yaml
```

`apply` creates the deployment when it does not exist and updates it when it does. Secret values can reference local environment variables:

```yaml
name: my-agent
spec:
  repo_url: https://github.com/org/repo
  deployment_file_path: "."
  git_ref: main
  secrets:
    OPENAI_API_KEY: ${OPENAI_API_KEY}
```

Run a dry run before changing cloud state:

```bash
llamactl deployments apply -f deployment.yaml --dry-run
```

## Inspect and Stream Logs

List deployments in the active project:

```bash
llamactl deployments get
```

Fetch one deployment:

```bash
llamactl deployments get NAME
```

Stream logs:

```bash
llamactl deployments logs NAME --follow
```

Use `-o json` or `-o yaml` with `deployments get` when another tool needs structured output.

## Update a Deployment

If the deployment tracks a branch, update it to the latest commit on that branch:

```bash
llamactl deployments update NAME
```

To point at a specific branch, tag, or commit:

```bash
llamactl deployments update NAME --git-ref main
```

To edit the deployment spec in your editor:

```bash
llamactl deployments edit NAME
```

Or keep the spec in version control and re-apply it:

```bash
llamactl deployments apply -f deployment.yaml
```

## Package for Self-Hosted Deployments

If you prefer to build and deploy containers yourself, use `llamactl pkg` to generate container files for your app. See the [`pkg` command reference](/python/llamaagents/llamactl-reference/commands-pkg/).

---

Next: Read about defining and exposing workflows in [Workflows & App Server API](/python/llamaagents/llamactl/workflow-api).
