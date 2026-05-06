## Code Navigation Guide

### Entry Points & Main Classes

**Control Plane** (`packages/llama-deploy-control-plane/`)
- Entry: `src/llama_deploy/control_plane/main.py` - FastAPI app
- K8s management: `src/llama_deploy/control_plane/k8s_client.py` - `K8sClient` class
- API endpoints: `src/llama_deploy/control_plane/endpoints/deployments.py`, `projects.py`

**Operator** (`operator/`)
- Entry: `cmd/main.go` - `func main()`
- Controller: `internal/controller/llamadeployment_controller.go` - `LlamaDeploymentReconciler`
- CRD types: `api/v1/llamadeployment_types.go` - `LlamaDeploymentSpec`, `LlamaDeploymentStatus`

**API Server** (`packages/llama-deploy-appserver/`)
- Entry: `src/llama_deploy/appserver/__main__.py` or `main.py`
- Manager: `src/llama_deploy/appserver/deployment.py` - `Manager` class (orchestrates deployments)
- Deployment: `src/llama_deploy/appserver/deployment.py` - `Deployment` class (runs workflows)
- Config parser: `src/llama_deploy/appserver/deployment_config_parser.py` - `DeploymentConfig.from_yaml()`
- Routers: `src/llama_deploy/appserver/routers/deployments.py`, `status.py`

**CLI** (`packages/llamactl/`)
- Entry: `src/llama_deploy/cli/__init__.py` - `main()` function
- Client: `src/llama_deploy/cli/client.py` - control plane/project client helpers
- Commands: `src/llama_deploy/cli/commands/*` - Click command definitions
- Config: `src/llama_deploy/cli/config/_config.py` - `ConfigManager`, with env support

**Core Schemas** (`packages/llama-deploy-core/`)
- Base: `src/llama_deploy/core/schema/base.py` - `Base` model class
- Deployments: `src/llama_deploy/core/schema/deployments.py` - `DeploymentResponse`, `LlamaDeploymentSpec`
- Projects: `src/llama_deploy/core/schema/projects.py` - `ProjectSummary`

### Key Configuration Files

**Deployment Configuration (preferred)**: Embedded in `pyproject.toml` under `[tool.llamadeploy]`.
```toml
[tool.llamadeploy]
name = "my-deployment"
app = "path.to.module:app"  # or use `workflows = { my_workflow = "path.to.module:workflow" }`

[tool.llamadeploy.ui]
directory = "./ui"
# Optional overrides:
# build_output_dir = "dist"
# package_manager = "pnpm"
# build_command = "build"
# serve_command = "dev"
# proxy_port = 4502
```

**Alternative**: `llama_deploy.toml` or `llama_deploy.yaml` with the same schema as `[tool.llamadeploy]`.

**Helm Chart**: `charts/llama-agents/values.yaml`
**Kubernetes CRD**: `operator/config/crd/bases/deploy.llamaindex.ai_llamadeployments.yaml`
**RBAC**: `operator/config/rbac/role.yaml`

### Key API Endpoints

**Control Plane** (port 8000):
- `POST /{project_id}/deployments` - Create deployment
- `GET /{project_id}/deployments` - List deployments
- `GET /{project_id}/deployments/{id}` - Get deployment details
- `POST /{project_id}/deployments/validate-repository` - Validate Git repo

**Build API** (port 8001, token auth required):
- `GET /health` - Health check

**API Server** (port 8080 in pod):
- `POST /deployments/{name}/tasks/run` - Execute workflow
- `GET /deployments/{name}/tasks/{task_id}/results` - Get task results
- `POST /deployments/{name}/sessions/create` - Create session
- `GET /health` - Health check

### CLI Commands Reference
```bash
llamactl auth env list              # List environments
llamactl auth env switch <URL>      # Switch current environment
llamactl auth token --project <PROJECT_ID>  # Create/select profile via API key
llamactl deployments template > deployment.yaml  # Generate apply YAML
llamactl deployments apply -f deployment.yaml    # Create or update from YAML
llamactl deployments create          # Create new deployment in $EDITOR
llamactl deployments get             # List deployments
llamactl deployments get NAME        # Get deployment details
llamactl deployments edit NAME       # Edit deployment in $EDITOR
llamactl deployments update NAME     # Re-resolve current git ref and release it
llamactl deployments logs NAME --follow  # Stream deployment logs
llamactl deployments delete NAME     # Delete deployment
```

### Commands Reference

**Development Setup:**
```bash
uv sync --all-packages --all-extras  # Install all dependencies including dev
uv run pre-commit run -a             # Lint & format
uv run dev                           # Run all package tests
```

**Development Environment:**
```bash
uv run operator/dev.py up             # Set up kind cluster and start development
uv run operator/dev.py down           # Clean up deployed resources
uv run operator/dev.py down --delete  # Delete the kind cluster
uv run operator/dev.py status         # Show cluster status
```

**Operator Development (Makefile in `operator/`):**
```bash
make -C operator operator-build      # Build operator binary
make -C operator operator-test       # Run operator tests
make -C operator operator-manifests  # Generate CRDs and RBAC
make -C operator operator-generate   # Generate DeepCopy methods
```

### Key Constants & Defaults
- Default discovery order: `llama_deploy.toml` → `pyproject.toml` → `llama_deploy.yaml`
- API Server port: `8080`
- Control Plane port: `8000`
- Build API port: `8001`
- Kubernetes namespace: `llama-agents` (default)
- CRD group: `deploy.llamaindex.ai`
- Container image: `llamaindex/llama-deploy:main-autodeploy`
