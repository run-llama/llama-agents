.PHONY: help

# Default target
help: ## Show this help message
	@echo "Available commands:"
	@grep -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) | sort | awk 'BEGIN {FS = ":.*?## "}; {printf "\033[36m%-20s\033[0m %s\n", $$1, $$2}'

##@ Operator

# Operator build variables
OPERATOR_IMG ?= llamaindex/llama-agents-operator:latest
LOCALBIN ?= $(shell pwd)/bin
CONTROLLER_GEN ?= $(LOCALBIN)/controller-gen
CONTROLLER_TOOLS_VERSION ?= v0.18.0
GOLANGCI_LINT ?= $(LOCALBIN)/golangci-lint
GOLANGCI_LINT_VERSION ?= v2.11.4

$(LOCALBIN):
	mkdir -p $(LOCALBIN)

.PHONY: controller-gen
controller-gen: $(CONTROLLER_GEN) ## Download controller-gen locally if necessary.
$(CONTROLLER_GEN): $(LOCALBIN)
	test -s $(LOCALBIN)/controller-gen && $(LOCALBIN)/controller-gen --version | grep -q $(CONTROLLER_TOOLS_VERSION) || \
	GOBIN=$(LOCALBIN) go install sigs.k8s.io/controller-tools/cmd/controller-gen@$(CONTROLLER_TOOLS_VERSION)

.PHONY: golangci-lint
golangci-lint: $(GOLANGCI_LINT) ## Download golangci-lint locally if necessary.
$(GOLANGCI_LINT): $(LOCALBIN)
	test -s $(LOCALBIN)/golangci-lint && $(LOCALBIN)/golangci-lint --version | grep -q $(GOLANGCI_LINT_VERSION) || \
	GOBIN=$(LOCALBIN) go install github.com/golangci/golangci-lint/v2/cmd/golangci-lint@$(GOLANGCI_LINT_VERSION)

.PHONY: operator-manifests
operator-manifests: controller-gen ## Generate operator CRDs and RBAC manifests
	$(CONTROLLER_GEN) rbac:roleName=manager-role crd webhook paths="./..." output:crd:artifacts:config=config/crd/bases
	@echo "Processing generated manifests for Helm templating..."
	@uv run ../scripts/process_manifests.py

.PHONY: operator-manifests-check
operator-manifests-check: operator-manifests ## Verify generated manifests are up to date
	@git diff --exit-code config/ ../charts/llama-agents/crds/ ../charts/llama-agents-crds/files/ ../charts/llama-agents/templates/rbac.yaml || \
		(echo "ERROR: Generated manifests are out of date. Run 'make -C operator operator-manifests' and commit." && exit 1)

.PHONY: operator-generate
operator-generate: controller-gen ## Generate operator code (DeepCopy methods)
	$(CONTROLLER_GEN) object paths="./..."

.PHONY: operator-build
operator-build: operator-manifests operator-generate ## Build operator binary
	go fmt ./... && go vet ./... && go build -o bin/manager cmd/main.go

.PHONY: operator-lint
operator-lint: golangci-lint ## Run operator linting
	@$(GOLANGCI_LINT) run

.PHONY: operator-test
operator-test: ## Run operator integration tests (envtest, build tag: integration)
	@echo "Installing envtest..."
	@go install sigs.k8s.io/controller-runtime/tools/setup-envtest@latest
	@echo "Running operator integration tests with envtest..."
	@eval "$$($(shell go env GOPATH)/bin/setup-envtest use -p env)" && go test -tags=integration ./... -coverprofile cover.out

.PHONY: operator-unit-test
operator-unit-test: ## Run fast operator unit tests (no envtest)
	@go test -tags='!integration' ./...

.PHONY: operator-docker-build
operator-docker-build: ## Build operator Docker image
	docker build -t $(OPERATOR_IMG) -f ../docker/operator.Dockerfile ..

.PHONY: operator-run
operator-run: operator-manifests operator-generate ## Run operator locally (requires kubeconfig)
	go run ./cmd/main.go


##@ Helm Chart Validation

CHART_DIR := ../charts/llama-agents
CRD_CHART_DIR := ../charts/llama-agents-crds

.PHONY: kube-ensure-kind-context
kube-ensure-kind-context: ## Ensure kubectl context points to a kind cluster
	@set -e; \
	CUR=$$(kubectl config current-context 2>/dev/null || true); \
	if echo $$CUR | grep -q '^kind-'; then \
	  echo "Using kind context: $$CUR"; \
	else \
	  FIRST_KIND=$$(kubectl config get-contexts -o name | grep '^kind-' | head -n1); \
	  if [ -n "$$FIRST_KIND" ]; then \
	    echo "Switching kubectl context to $$FIRST_KIND"; \
	    kubectl config use-context $$FIRST_KIND >/dev/null; \
	  else \
	    echo "No kind context found. Please create one (e.g., kind create cluster)"; \
	    exit 1; \
	  fi; \
	fi

.PHONY: helm-lint
helm-lint: ## Lint Helm chart with default values
	helm lint $(CHART_DIR)

.PHONY: helm-lint-dev
helm-lint-dev: ## Lint Helm chart with dev values
	helm lint $(CHART_DIR) -f ./tilt/helm/values-dev.yaml

.PHONY: helm-unittest-install
helm-unittest-install: ## Install helm-unittest plugin
	@if ! helm plugin list | grep -q "unittest"; then \
		helm plugin install --verify=false https://github.com/helm-unittest/helm-unittest; \
	fi

.PHONY: helm-unittest
helm-unittest: helm-unittest-install ## Run helm unittest
	helm unittest $(CHART_DIR)

.PHONY: helm-template
helm-template: ## Render templates with default values
	helm template $(CHART_DIR) --set controlPlane.objectStorage.s3.bucket=test-bucket >/dev/null

.PHONY: helm-template-dev
helm-template-dev: ## Render templates with dev values
	helm template $(CHART_DIR) -f ./tilt/helm/values-dev.yaml >/dev/null

.PHONY: helm-dry-run
helm-dry-run: kube-ensure-kind-context ## Server-side dry-run apply (requires kube cluster, default values)
	helm template $(CHART_DIR) --set controlPlane.objectStorage.s3.bucket=test-bucket | kubectl apply --dry-run=server -f -

.PHONY: helm-dry-run-dev
helm-dry-run-dev: kube-ensure-kind-context ## Server-side dry-run apply (requires kube cluster, dev values)
	helm template $(CHART_DIR) -f ./tilt/helm/values-dev.yaml | kubectl apply --dry-run=server -f -

.PHONY: helm-lint-crds
helm-lint-crds: ## Lint CRD chart
	helm lint $(CRD_CHART_DIR)

.PHONY: helm-template-crds
helm-template-crds: ## Render CRD chart templates
	helm template $(CRD_CHART_DIR) >/dev/null

.PHONY: helm-crds-prom-operator
helm-crds-prom-operator: kube-ensure-kind-context ## Install Prometheus Operator CRDs (for ServiceMonitor)
	# Use server-side apply to avoid storing huge last-applied annotations on CRDs
	kubectl apply --server-side -f https://raw.githubusercontent.com/prometheus-operator/prometheus-operator/main/bundle.yaml
	# Wait for CRD to be established before proceeding
	timeout=120; \
	while [ $$timeout -gt 0 ]; do \
		if kubectl get crd servicemonitors.monitoring.coreos.com -o jsonpath='{range .status.conditions[*]}{.type}={.status}{"\n"}{end}' 2>/dev/null | grep -q '^Established=True$$'; then \
			exit 0; \
		fi; \
		sleep 2; \
		timeout=$$((timeout-2)); \
	done; \
	echo "Timed out waiting for crd/servicemonitors.monitoring.coreos.com to become Established" >&2; \
	exit 1

.PHONY: helm-docs-install
helm-docs-install: ## Install helm-docs via go install
	go install github.com/norwoodj/helm-docs/cmd/helm-docs@latest

.PHONY: helm-docs
helm-docs: ## Generate Helm chart README from values.yaml comments
	helm-docs --chart-search-root $(CHART_DIR) --sort-values-order file

.PHONY: helm-docs-check
helm-docs-check: ## Verify Helm chart README is up to date
	helm-docs --chart-search-root $(CHART_DIR) --sort-values-order file
	git diff --exit-code $(CHART_DIR)/README.md

.PHONY: helm-validate
helm-validate: helm-lint helm-lint-dev helm-lint-crds helm-template helm-template-dev helm-template-crds helm-unittest helm-dry-run helm-dry-run-dev ## Run full Helm validation suite
