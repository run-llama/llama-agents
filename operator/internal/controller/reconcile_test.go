//go:build !integration

package controller

import (
	"context"
	"fmt"
	"testing"

	appsv1 "k8s.io/api/apps/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	llamadeployv1 "llama-agents-operator/api/v1"
)

// ---------------------------------------------------------------------------
// needsFullReconciliation tests
// ---------------------------------------------------------------------------

func TestNeedsFullReconciliation(t *testing.T) {
	tests := []struct {
		name           string
		schemaVersion  string
		generation     int64
		lastReconciled int64
		want           bool
	}{
		{
			name:           "schema version mismatch triggers full reconcile",
			schemaVersion:  "0",
			generation:     1,
			lastReconciled: 1,
			want:           true,
		},
		{
			name:           "generation mismatch triggers full reconcile",
			schemaVersion:  CurrentSchemaVersion,
			generation:     2,
			lastReconciled: 1,
			want:           true,
		},
		{
			name:           "initial reconciliation (lastReconciled=0) triggers full reconcile",
			schemaVersion:  CurrentSchemaVersion,
			generation:     0,
			lastReconciled: 0,
			want:           true,
		},
		{
			name:           "everything matches, no full reconcile needed",
			schemaVersion:  CurrentSchemaVersion,
			generation:     3,
			lastReconciled: 3,
			want:           false,
		},
		{
			name:           "empty schema version triggers full reconcile",
			schemaVersion:  "",
			generation:     1,
			lastReconciled: 1,
			want:           true,
		},
	}

	r := &LlamaDeploymentReconciler{}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ld := &llamadeployv1.LlamaDeployment{
				ObjectMeta: metav1.ObjectMeta{Name: "test", Namespace: "default", Generation: tt.generation},
				Status: llamadeployv1.LlamaDeploymentStatus{
					SchemaVersion:            tt.schemaVersion,
					LastReconciledGeneration: tt.lastReconciled,
				},
			}
			got := r.needsFullReconciliation(ld)
			if got != tt.want {
				t.Errorf("needsFullReconciliation() = %v, want %v", got, tt.want)
			}
		})
	}
}

// ---------------------------------------------------------------------------
// isTerminalFailure tests
// ---------------------------------------------------------------------------

func TestIsTerminalFailure(t *testing.T) {
	tests := []struct {
		phase string
		want  bool
	}{
		{PhaseRolloutFailed, true},
		{PhaseFailed, true},
		{PhaseBuildFailed, true},
		{PhaseRunning, false},
		{PhasePending, false},
		{PhaseBuilding, false},
		{PhaseSuspended, false},
		{PhaseAwaitingCode, false},
		{"", false},
	}
	for _, tt := range tests {
		t.Run(tt.phase, func(t *testing.T) {
			if got := isTerminalFailure(tt.phase); got != tt.want {
				t.Errorf("isTerminalFailure(%q) = %v, want %v", tt.phase, got, tt.want)
			}
		})
	}
}

// ---------------------------------------------------------------------------
// isActivePhase tests
// ---------------------------------------------------------------------------

func TestIsActivePhase(t *testing.T) {
	tests := []struct {
		phase string
		want  bool
	}{
		{PhaseRunning, true},
		{PhasePending, true},
		{PhaseRollingOut, true},
		{PhaseRolloutFailed, true},
		{PhaseSuspended, false},
		{PhaseFailed, false},
		{PhaseBuilding, false},
		{PhaseBuildFailed, false},
		{PhaseAwaitingCode, false},
		{"", false},
	}
	for _, tt := range tests {
		t.Run(tt.phase, func(t *testing.T) {
			if got := isActivePhase(tt.phase); got != tt.want {
				t.Errorf("isActivePhase(%q) = %v, want %v", tt.phase, got, tt.want)
			}
		})
	}
}

// ---------------------------------------------------------------------------
// handleAlreadyFailed tests
// ---------------------------------------------------------------------------

func TestHandleAlreadyFailed_SkipsWhenNoFullReconcile(t *testing.T) {
	scheme := testSchemeWithApps()
	ld := &llamadeployv1.LlamaDeployment{
		ObjectMeta: metav1.ObjectMeta{Name: "my-app", Namespace: "default", Generation: 1},
		Spec:       llamadeployv1.LlamaDeploymentSpec{ProjectId: "p", RepoUrl: "r"},
		Status: llamadeployv1.LlamaDeploymentStatus{
			Phase:                   PhaseFailed,
			FailedRolloutGeneration: 1,
		},
	}
	fakeClient := fake.NewClientBuilder().
		WithScheme(scheme).
		WithObjects(ld).
		WithStatusSubresource(ld).
		Build()
	r := &LlamaDeploymentReconciler{Client: fakeClient, Scheme: scheme}
	ctx := context.Background()

	result, err := r.handleAlreadyFailed(ctx, ld, false)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if result.RequeueAfter != 0 {
		t.Errorf("expected empty result, got %+v", result)
	}
}

func TestHandleAlreadyFailed_RestoresFailedDeploymentToZeroReplicas(t *testing.T) {
	scheme := testSchemeWithApps()
	ld := &llamadeployv1.LlamaDeployment{
		ObjectMeta: metav1.ObjectMeta{Name: "my-app", Namespace: "default", Generation: 1},
		Spec:       llamadeployv1.LlamaDeploymentSpec{ProjectId: "p", RepoUrl: "r"},
		Status: llamadeployv1.LlamaDeploymentStatus{
			Phase:                   PhaseFailed,
			FailedRolloutGeneration: 1,
		},
	}
	deployment := &appsv1.Deployment{
		ObjectMeta: metav1.ObjectMeta{Name: ld.Name, Namespace: ld.Namespace},
		Spec:       appsv1.DeploymentSpec{Replicas: ptr(int32(1))},
	}
	fakeClient := fake.NewClientBuilder().
		WithScheme(scheme).
		WithObjects(ld, deployment).
		WithStatusSubresource(ld).
		Build()
	r := &LlamaDeploymentReconciler{Client: fakeClient, Scheme: scheme}
	ctx := context.Background()

	if _, err := r.handleAlreadyFailed(ctx, ld, false); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if err := fakeClient.Get(ctx, types.NamespacedName{Name: ld.Name, Namespace: ld.Namespace}, deployment); err != nil {
		t.Fatalf("failed to get Deployment: %v", err)
	}
	if deployment.Spec.Replicas == nil || *deployment.Spec.Replicas != 0 {
		t.Fatalf("expected replicas=0, got %v", deployment.Spec.Replicas)
	}
}

// ---------------------------------------------------------------------------
// handleReconcileFailure tests
// ---------------------------------------------------------------------------

func TestHandleReconcileFailure_UpdatesStatusToFailed(t *testing.T) {
	scheme := testSchemeWithApps()
	ld := &llamadeployv1.LlamaDeployment{
		ObjectMeta: metav1.ObjectMeta{Name: "my-app", Namespace: "default"},
		Spec:       llamadeployv1.LlamaDeploymentSpec{ProjectId: "p", RepoUrl: "r"},
		Status: llamadeployv1.LlamaDeploymentStatus{
			Phase:     PhasePending,
			AuthToken: "tok",
		},
	}
	fakeClient := fake.NewClientBuilder().
		WithScheme(scheme).
		WithObjects(ld).
		WithStatusSubresource(ld).
		Build()
	r := &LlamaDeploymentReconciler{Client: fakeClient, Scheme: scheme}
	ctx := context.Background()

	reconcileErr := fmt.Errorf("something went wrong")
	_, err := r.handleReconcileFailure(ctx, ld, reconcileErr)
	if err == nil {
		t.Fatal("expected error to be returned")
	}
	if ld.Status.Phase != PhaseFailed {
		t.Errorf("expected phase %q, got %q", PhaseFailed, ld.Status.Phase)
	}
	if ld.Status.LastUpdated == nil {
		t.Error("expected LastUpdated to be set")
	}
}

func TestHandleReconcileFailure_DoesNotOverwriteAlreadyFailed(t *testing.T) {
	scheme := testSchemeWithApps()
	ld := &llamadeployv1.LlamaDeployment{
		ObjectMeta: metav1.ObjectMeta{Name: "my-app", Namespace: "default"},
		Spec:       llamadeployv1.LlamaDeploymentSpec{ProjectId: "p", RepoUrl: "r"},
		Status: llamadeployv1.LlamaDeploymentStatus{
			Phase:     PhaseFailed,
			Message:   "original failure",
			AuthToken: "tok",
		},
	}
	fakeClient := fake.NewClientBuilder().
		WithScheme(scheme).
		WithObjects(ld).
		WithStatusSubresource(ld).
		Build()
	r := &LlamaDeploymentReconciler{Client: fakeClient, Scheme: scheme}
	ctx := context.Background()

	reconcileErr := fmt.Errorf("new error")
	_, _ = r.handleReconcileFailure(ctx, ld, reconcileErr)

	// Message should remain the original since phase was already Failed
	if ld.Status.Message != "original failure" {
		t.Errorf("expected original message preserved, got %q", ld.Status.Message)
	}
}
