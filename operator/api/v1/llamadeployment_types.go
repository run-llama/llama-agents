package v1

import (
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// EDIT THIS FILE!  THIS IS SCAFFOLDING FOR YOU TO OWN!
// NOTE: json tags are required.  Any new fields you add must have json tags for the fields to be serialized.

// LlamaDeploymentSpec defines the desired state of LlamaDeployment.
type LlamaDeploymentSpec struct {
	// ProjectId is the project ID
	ProjectId string `json:"projectId"`

	// DisplayName is the user-facing deployment label
	DisplayName string `json:"displayName,omitempty"`

	// Name is the deployment name (DEPRECATED: use DisplayName)
	Name string `json:"name,omitempty"`

	// RepoUrl is the URL of the repository to deploy
	RepoUrl string `json:"repoUrl"`

	// DeploymentFilePath is the path to the deployment file within the repository
	// +kubebuilder:default="llama_deployment.yml"
	DeploymentFilePath string `json:"deploymentFilePath,omitempty"`

	// GitRef is the git reference (commit SHA, branch, or tag) to deploy
	GitRef string `json:"gitRef,omitempty"`

	// A resolved git sha for the git ref
	GitSha string `json:"gitSha,omitempty"`

	// SecretName is the name of the Kubernetes Secret containing PAT and deployment secrets
	SecretName string `json:"secretName,omitempty"`

	// Image is the container image registry and name (e.g., "llamaindex/llama-agents-appserver")
	// If not specified, defaults to environment variable or "llamaindex/llama-agents-appserver"
	Image string `json:"image,omitempty"`

	// ImageTag is the container image tag
	// If not specified, defaults to environment variable or "latest"
	ImageTag string `json:"imageTag,omitempty"`

	// StaticAssetsPath is an optional path (relative to /opt/app) containing
	// prebuilt UI assets to be served under /deployments/<deployment-id>/ui
	StaticAssetsPath string `json:"staticAssetsPath,omitempty"`

	// TemplateName optionally specifies a LlamaDeploymentTemplate to apply.
	// When empty, the operator will look up a template named "default".
	TemplateName string `json:"templateName,omitempty"`

	// Suspended scales the underlying Deployment to 0 replicas when true.
	// Setting suspended to false (or removing the field) restores replicas to 1.
	Suspended bool `json:"suspended,omitempty"`

	// BuildGeneration is a monotonically increasing counter that forces a new
	// build when incremented, even if all other inputs (gitSha, imageTag, etc.)
	// are unchanged. This allows retrying a failed build caused by transient
	// errors (e.g. network failures) without requiring a new git commit.
	BuildGeneration int64 `json:"buildGeneration,omitempty"`
}

// LlamaDeploymentStatus defines the observed state of LlamaDeployment.
type LlamaDeploymentStatus struct {
	// Phase represents the current phase of the deployment
	// +kubebuilder:validation:Enum=Pending;Running;Failed;RollingOut;RolloutFailed;Suspended;Building;BuildFailed;AwaitingCode
	Phase string `json:"phase,omitempty"`

	// Message is a human-readable message indicating details about the current status
	Message string `json:"message,omitempty"`

	// LastUpdated is the timestamp of the last status update
	LastUpdated *metav1.Time `json:"lastUpdated,omitempty"`

	// AuthToken is a cryptographically secure token for this deployment
	AuthToken string `json:"authToken,omitempty"`

	// SchemaVersion is the version of the CRD schema used when this resource was last reconciled
	SchemaVersion string `json:"schemaVersion,omitempty"`

	// LastReconciledGeneration tracks the generation that was last successfully reconciled
	LastReconciledGeneration int64 `json:"lastReconciledGeneration,omitempty"`

	// ReleaseHistory keeps the last 20 released git shas with timestamps
	ReleaseHistory []ReleaseHistoryEntry `json:"releaseHistory,omitempty"`

	// RolloutStartedAt is the timestamp when the current rollout began.
	// Set when the phase transitions to Pending or RollingOut, cleared on Running or failure.
	RolloutStartedAt *metav1.Time `json:"rolloutStartedAt,omitempty"`

	// FailedRolloutGeneration records the LlamaDeployment generation whose rollout
	// timed out. This prevents the operator from re-attempting the same failing rollout.
	FailedRolloutGeneration int64 `json:"failedRolloutGeneration,omitempty"`

	// SecretCheckRetries tracks how many times we've retried finding the Secret.
	// This handles informer cache lag when the Secret is created just before the CR.
	SecretCheckRetries int32 `json:"secretCheckRetries,omitempty"`

	// BuildId is the content-addressed identifier for the current build artifact
	BuildId string `json:"buildId,omitempty"`

	// BuildStatus tracks the state of the current build job
	// +kubebuilder:validation:Enum=Pending;Running;Succeeded;Failed
	BuildStatus string `json:"buildStatus,omitempty"`

	// LastBuiltGeneration is the spec.buildGeneration value that was last
	// successfully built. When spec.buildGeneration differs from this value,
	// a new build is triggered even if the deployment is suspended.
	LastBuiltGeneration int64 `json:"lastBuiltGeneration,omitempty"`
}

// ReleaseHistoryEntry represents a single released version entry
type ReleaseHistoryEntry struct {
	// GitSha is the released git commit SHA
	GitSha string `json:"gitSha"`
	// ImageTag is the appserver image tag used for this release
	ImageTag string `json:"imageTag,omitempty"`
	// ReleasedAt is the timestamp when this version was released
	ReleasedAt metav1.Time `json:"releasedAt"`
}

// +kubebuilder:object:root=true
// +kubebuilder:subresource:status
// +kubebuilder:printcolumn:name="Project ID",type=string,JSONPath=`.spec.projectId`
// +kubebuilder:printcolumn:name="Name",type=string,JSONPath=`.spec.displayName`
// +kubebuilder:printcolumn:name="Repo",type=string,JSONPath=`.spec.repoUrl`
// +kubebuilder:printcolumn:name="Phase",type=string,JSONPath=`.status.phase`
// +kubebuilder:printcolumn:name="Age",type=date,JSONPath=`.metadata.creationTimestamp`

// LlamaDeployment is the Schema for the llamadeployments API.
type LlamaDeployment struct {
	metav1.TypeMeta   `json:",inline"`
	metav1.ObjectMeta `json:"metadata,omitempty"`

	Spec   LlamaDeploymentSpec   `json:"spec,omitempty"`
	Status LlamaDeploymentStatus `json:"status,omitempty"`
}

// +kubebuilder:object:root=true

// LlamaDeploymentList contains a list of LlamaDeployment.
type LlamaDeploymentList struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:"metadata,omitempty"`
	Items           []LlamaDeployment `json:"items"`
}

func init() {
	SchemeBuilder.Register(&LlamaDeployment{}, &LlamaDeploymentList{})
}
