# Cloud Llama Deploy - Overall Architecture

This document outlines the architecture of the Cloud Llama Deploy system, which enables deployment and management of LlamaIndex workflows on Kubernetes.

## High-Level System Overview

The system consists of several interconnected components that work together to provide a complete deployment platform:

```mermaid
graph TD
    CLI["CLI (llamactl)"] --> CP["Control Plane (FastAPI backend)"]
    CP --> K8S["Kubernetes"]
    K8S --> OP["Operator (Go)"]
    OP --> POD["App Server (Python Pod)"]
    OP -.-> CRD["LlamaDeployment CRDs"]
    POD --> WF["User Workflows"]
```

## Component Details

### 1. Core Package (`llama-deploy-core`)
**Purpose**: Shared data models and schemas used across all components.

**Role**: Ensures consistent data structures across the entire system.

### 2. Control Plane (`llama-deploy-control-plane`)
**Purpose**: Main orchestration layer that manages deployments via Kubernetes.

**Components**:
- **K8s Client** (`k8s_client.py`): Interfaces with Kubernetes API to create/manage LlamaDeployment CRDs
- **Git Integration**: Clones repositories, validates Git refs, handles GitHub authentication
- **API Endpoints**: REST API for deployment and project management

**Flow**:
```mermaid
graph LR
    UR["User Request"] --> CP["Control Plane API"]
    CP --> KC["K8s Client"]
    KC --> CRD["LlamaDeployment CRD"]
    CRD --> K8S["Kubernetes"]
```

### 3. Kubernetes Operator (`operator/`)
**Purpose**: Kubernetes controller that reconciles LlamaDeployment custom resources.

**Key Components**:
- **CRD Definition**: `LlamaDeployment` custom resource with spec (repo URL, deployment file, git ref)
- **Controller**: Watches for LlamaDeployment changes and creates/updates Kubernetes resources
- **Reconciliation Loop**: Creates deployments, services, secrets, and ingresses

**Managed Resources**:
```mermaid
graph TD
    CRD["LlamaDeployment CRD"] --> DEP["Deployment<br/>(API Server pod)"]
    CRD --> SVC["Service<br/>(Load balancer)"]
    CRD --> SEC["Secret<br/>(PAT tokens, env vars)"]
    CRD --> SA["ServiceAccount"]
    CRD --> ING["Ingress<br/>(optional)"]
```

**Phases**: `Syncing` → `Pending` → `Running` / `Failed` / `RollingOut`

### 4. App Server (`appserver`)
**Purpose**: Runtime environment that executes user workflows and provides APIs.

**Key Components**:
- **Deployment Manager**: Loads and manages workflow deployments from config
- **Workflow Execution**: Runs LlamaIndex workflows with session management
- **Source Managers**: Handle Git, local, and Docker sources for workflow code
- **API Endpoints**: REST and WebSocket APIs for interacting with workflows

**Configuration**:
- **Primary**: Embedded in `pyproject.toml` under `[tool.llamadeploy]`
- **Alternative**: `llama_deploy.toml` or `llama_deploy.yaml` with the same schema

Example (TOML in `pyproject.toml`):
```toml
[tool.llamadeploy]
name = "my-deployment"
app = "path.to.module:app"

[tool.llamadeploy.ui]
directory = "./ui"
```

### 5. CLI Tool (`llamactl`)
**Purpose**: Command-line interface for users to interact with the control plane.
```bash
llamactl auth env switch https://api.cloud.llamaindex.ai
llamactl auth login
llamactl auth token --project <PROJECT_ID>
llamactl auth logout
llamactl deployments template > deployment.yaml
llamactl deployments apply -f deployment.yaml
llamactl deployments create
llamactl deployments get
llamactl deployments get NAME
llamactl deployments edit NAME
llamactl deployments update NAME
llamactl deployments logs NAME --follow
```

## Component Interaction Flow

### Deployment Creation Flow
```mermaid
sequenceDiagram
    participant User
    participant CP as Control Plane
    participant K8s as Kubernetes
    participant Op as Operator
    participant Pod as API Server Pod

    User->>CP: Create deployment request
    CP->>CP: Validate Git repository
    CP->>K8s: Create LlamaDeployment CRD
    K8s->>Op: CRD change detected
    Op->>K8s: Create Deployment/Service/Secret
    K8s->>Pod: Start API Server pod
    Pod->>Pod: Clone workflow code
    Pod->>Pod: Load workflow and expose APIs
    Pod->>K8s: Update status
    K8s->>CP: Status propagation
    CP->>User: Deployment ready
```

### Data Flow
**Configuration**: Git Repo → Control Plane → LlamaDeployment CRD → Operator → API Server Pod
**Runtime**: User Request → API Server → Workflow Engine → Response
**Monitoring**: API Server → Prometheus Metrics | Operator → K8s Events → Status Updates

## Networking & Communication

- **External Access**: Ingress → Service → API Server Pod
- **Internal K8s**: Operator talks to K8s API server
- **Control Plane**: Communicates with K8s via client libraries
- **CLI**: HTTP calls to Control Plane API
- **Workflow APIs**: Direct HTTP/WebSocket to API Server pods

## Namespace Layout

Set `apps.namespace` to run the control plane + operator in the release
namespace and put `LlamaDeployment` CRs and their child resources (Deployments,
Pods, Services, Secrets, ServiceAccounts, ConfigMaps, Ingresses, NetworkPolicies,
build Jobs) in a separate namespace. Unset = everything in the release namespace.

CRs stay co-located with their children (cross-namespace owner references are
not allowed). Only `WATCH_NAMESPACE` (operator) and `KUBERNETES_NAMESPACE`
(control plane) point at the apps namespace; reconciler code is unchanged.

RBAC in split mode: apps-namespace Role with every rule except
`coordination.k8s.io/leases`, release-namespace Role with just that rule for
leader election. Unset collapses to one Role.

`imagePullSecrets` are not mirrored — provision them in the apps namespace, or
use node-level pull credentials.
