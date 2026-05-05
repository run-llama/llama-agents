---
title: Deployment Config Reference
sidebar:
  order: 18
---
:::caution
Cloud deployments of LlamaAgents are now in beta preview and broadly available for feedback. You can try them out locally or deploy to LlamaCloud and send us feedback with the in-app button.
:::

LlamaAgents reads configuration from your repository to run your app. The configuration is defined in your project's `pyproject.toml`.

### pyproject.toml

```toml
[tool.llamaagents]
name = "my-app"
env_files = [".env"]

[tool.llamaagents.workflows]
workflow-one = "my_app.workflows:some_workflow"
workflow-two = "my_app.workflows:another_workflow"

[tool.llamaagents.ui]
directory = "ui"
build_output_dir = "ui/static"
```

### Authentication

Deployments can be configured to automatically inject authentication for LlamaCloud.

```toml
[tool.llamaagents]
llama_cloud = true
```

When this is set:
- During development, `llamactl` uses environment variable auth or the active profile, then injects `LLAMA_CLOUD_API_KEY`, `LLAMA_AGENTS_PROJECT_ID`, and `LLAMA_CLOUD_BASE_URL` into your Python server process and JavaScript build.
- When deployed, LlamaCloud automatically injects a dedicated API key into the Python process. The frontend process receives a short-lived session cookie specific to each user visiting the application. Therefore, configure the project ID on the frontend API client so that LlamaCloud API requests from the frontend and backend are scoped to the same project ID.

### `.env` files

Most apps need API keys (e.g., OpenAI). You can specify them via a `.env` file and reference it in your config:

```toml
[tool.llamaagents]
env_files = [".env"]
```

Then set your secrets:

```bash
# .env
OPENAI_API_KEY=sk-xxxx
```

### Alternative file formats (YAML/TOML)

If you prefer to keep your `pyproject.toml` simple, you can write the same configuration in a `llama_agents.yaml` or `llama_agents.toml` file. All fields use the same structure and types; omit the `tool.llamaagents` prefix.

## Schema

### DeploymentConfig fields

| Field | Type | Default | Description |
|---|---|---|---|
| `name` | string | `"default"` | URL-safe deployment name. In `pyproject.toml`, if omitted it falls back to `project.name`. |
| `workflows` | map&lt;string,string&gt; | — | Map of `workflowName -> "module.path:workflow"`. |
| `env_files` | list&lt;string&gt; | `[".env"]` | Paths to env files to load. Relative to the config file. Duplicate entries are removed. |
| `env` | map&lt;string,string&gt; | `{}` | Environment variables injected at runtime. |
| `required_env_vars` | list&lt;string&gt; | `[]` | Environment variable names that must be set at runtime. If any are missing or empty, `llamactl serve` and deployments will fail fast with an error. |
| `llama_cloud` | boolean | false | Indicates that a deployment connects to LlamaCloud. Set to true to automatically inject a LlamaCloud API key. |
| `ui` | `UIConfig` | `null` | Optional UI configuration. `directory` is required if `ui` is present. |

#### Required environment variables

Use `required_env_vars` to ensure critical secrets are present before the app starts:

```toml
[tool.llamaagents]
# Force the app to start only when these are set
required_env_vars = ["OPENAI_API_KEY", "ANTHROPIC_API_KEY"]
```

Tip: Combine with `env_files` for local development or supply values via your deployment's secret manager.

### UIConfig fields

| Field | Type | Default | Description |
|---|---|---|---|
| `directory` | string | — | Path to UI source, relative to the config directory. Required when `ui` is set. |
| `build_output_dir` | string | ``${directory}/dist`` | Built UI output directory. If set in TOML/`pyproject.toml`, the path is relative to the config file. If set via `package.json` (`llamaagents.build_output_dir`), it is resolved as `${directory}/${build_output_dir}`. |
| `package_manager` | string | `"npm"` (or inferred) | Package manager used to build the UI. If not set, inferred from `package.json` `packageManager` (e.g., `pnpm@9.0.0` → `pnpm`). |
| `build_command` | string | `"build"` | NPM script name used to build. |
| `serve_command` | string | `"dev"` | NPM script name used to serve in development. |
| `proxy_port` | integer | `4502` | Port the app server proxies to in development. |

## UI Integration via package.json

Note: after setting `ui.directory` so that `package.json` can be found, you can configure the UI within it instead.

For example:

```json
{
  "name": "my-ui",
  "packageManager": "pnpm@9.7.0",
  "scripts": { "build": "vite build", "dev": "vite" },
  "llamaagents": {
    "build_output_dir": "dist",
    "package_manager": "pnpm",
    "build_command": "build",
    "serve_command": "dev",
    "proxy_port": 5173
  }
}
```
