package controller

import (
	"context"
	"crypto/rand"
	"fmt"

	appsv1 "k8s.io/api/apps/v1"
	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/client-go/tools/record"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/builder"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"
	"sigs.k8s.io/controller-runtime/pkg/event"
	"sigs.k8s.io/controller-runtime/pkg/handler"
	"sigs.k8s.io/controller-runtime/pkg/log"
	"sigs.k8s.io/controller-runtime/pkg/predicate"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"

	llamadeployv1 "llama-agents-operator/api/v1"
)

const (
	llamaDeploymentFinalizer = "deploy.llamaindex.ai/finalizer"

	// LlamaDeployment status phases
	PhasePending       = "Pending"
	PhaseRunning       = "Running"
	PhaseFailed        = "Failed"
	PhaseRollingOut    = "RollingOut"
	PhaseRolloutFailed = "RolloutFailed"
	PhaseSuspended     = "Suspended"
	PhaseBuilding      = "Building"
	PhaseBuildFailed   = "BuildFailed"
	PhaseAwaitingCode  = "AwaitingCode"

	// Build status values for status.buildStatus
	BuildStatusPending   = "Pending"
	BuildStatusRunning   = "Running"
	BuildStatusSucceeded = "Succeeded"
	BuildStatusFailed    = "Failed"

	// Schema version for tracking CRD changes and forcing reconciliation
	// Increment this version when making schema changes that require full reconciliation
	// ONLY INCREMENT THIS WHEN MAKING SCHEMA CHANGES THAT REQUIRE FULL RECONCILIATION
	CurrentSchemaVersion = "7"

	// Environment variable for max concurrent rollouts configuration
	EnvMaxConcurrentRollouts = "LLAMA_DEPLOY_MAX_CONCURRENT_ROLLOUTS"
	EnvMaxDeployments        = "LLAMA_DEPLOY_MAX_DEPLOYMENTS"
)

// ptr returns a pointer to the given value
func ptr[T any](v T) *T { return &v }

// generateAuthToken generates a cryptographically secure random token using only alphanumeric characters
func generateAuthToken() (string, error) {
	const charset = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789"
	const tokenLength = 43 // Similar length to base64 encoding of 32 bytes

	bytes := make([]byte, tokenLength)
	if _, err := rand.Read(bytes); err != nil {
		return "", fmt.Errorf("failed to generate random token: %w", err)
	}

	for i := range bytes {
		bytes[i] = charset[bytes[i]%byte(len(charset))]
	}

	return string(bytes), nil
}

// needsFullReconciliation determines if a full reconciliation is required
// This happens when:
// 1. Schema version doesn't match the current version
// 2. The resource generation has changed since last reconciliation
// 3. The resource has never been reconciled (initial creation)
func (r *LlamaDeploymentReconciler) needsFullReconciliation(llamaDeploy *llamadeployv1.LlamaDeployment) bool {
	// Always reconcile if schema version doesn't match
	if llamaDeploy.Status.SchemaVersion != CurrentSchemaVersion {
		return true
	}

	// Always reconcile if generation has changed (spec was updated)
	if llamaDeploy.Status.LastReconciledGeneration != llamaDeploy.Generation {
		return true
	}

	// Always reconcile if this is the first reconciliation
	if llamaDeploy.Status.LastReconciledGeneration == 0 {
		return true
	}

	return false
}

// isTerminalFailure returns true if the phase is a terminal failure state
// where no further reconciliation should occur for this generation.
func isTerminalFailure(phase string) bool {
	return phase == PhaseRolloutFailed || phase == PhaseFailed || phase == PhaseBuildFailed
}

// isActivePhase returns true if the phase represents an active deployment
// (not suspended, not failed, and not empty/unset).
func isActivePhase(phase string) bool {
	return phase != PhaseSuspended && phase != PhaseFailed && phase != PhaseBuilding && phase != PhaseBuildFailed && phase != PhaseAwaitingCode && phase != ""
}

// LlamaDeploymentReconciler reconciles a LlamaDeployment object
type LlamaDeploymentReconciler struct {
	client.Client
	Scheme   *runtime.Scheme
	Recorder record.EventRecorder
	// DirectClient bypasses the informer cache. Use this for types that should
	// NOT be cached (e.g. ReplicaSets) to avoid loading potentially thousands
	// of objects into memory.
	DirectClient client.Client
	// MaxConcurrentRollouts limits how many LlamaDeployments can roll out
	// simultaneously. 0 means unlimited (default).
	MaxConcurrentRollouts int
	// MaxDeployments limits the total number of active (non-suspended, non-failed)
	// LlamaDeployments per namespace. 0 means unlimited (default).
	MaxDeployments int
}

// directClient returns a client that bypasses the informer cache.
func (r *LlamaDeploymentReconciler) directClient() client.Client {
	return r.DirectClient
}

// fetchDeployment retrieves the LlamaDeployment for the given request.
// Returns (nil, nil) when the object has been deleted.
func (r *LlamaDeploymentReconciler) fetchDeployment(ctx context.Context, req ctrl.Request) (*llamadeployv1.LlamaDeployment, error) {
	logger := log.FromContext(ctx)
	ld := &llamadeployv1.LlamaDeployment{}
	if err := r.Get(ctx, req.NamespacedName, ld); err != nil {
		if errors.IsNotFound(err) {
			logger.Info("LlamaDeployment resource not found. Ignoring since object must be deleted")
			return nil, nil
		}
		logger.Error(err, "Failed to get LlamaDeployment")
		return nil, err
	}
	return ld, nil
}

// migrateDisplayName moves spec.name to spec.displayName for existing CRDs.
// Returns true if the object was patched and the reconcile should requeue.
func (r *LlamaDeploymentReconciler) migrateDisplayName(ctx context.Context, ld *llamadeployv1.LlamaDeployment) (bool, error) {
	if ld.Spec.Name == "" || ld.Spec.DisplayName != "" {
		// No migration needed: either no old name or displayName already set
		return false, nil
	}

	logger := log.FromContext(ctx)
	logger.Info("Migrating spec.name to spec.displayName", "name", ld.Name)

	patch := client.MergeFrom(ld.DeepCopy())
	ld.Spec.DisplayName = ld.Spec.Name
	ld.Spec.Name = ""
	if err := r.Patch(ctx, ld, patch); err != nil {
		logger.Error(err, "Failed to migrate displayName")
		return false, err
	}

	// Re-fetch to get the updated generation
	if err := r.Get(ctx, client.ObjectKeyFromObject(ld), ld); err != nil {
		return false, err
	}

	// Update lastReconciledGeneration to the new generation so the spec change
	// doesn't trigger a full reconciliation (no rebuild, no pod restart).
	statusPatch := client.MergeFrom(ld.DeepCopy())
	ld.Status.LastReconciledGeneration = ld.Generation
	if err := r.Status().Patch(ctx, ld, statusPatch); err != nil {
		logger.Error(err, "Failed to update lastReconciledGeneration after displayName migration")
		return false, err
	}

	return true, nil
}

// ensureFinalizer adds the cleanup finalizer if not already present.
// After updating, re-reads the object to pick up the fresh resourceVersion.
func (r *LlamaDeploymentReconciler) ensureFinalizer(ctx context.Context, ld *llamadeployv1.LlamaDeployment) error {
	if controllerutil.ContainsFinalizer(ld, llamaDeploymentFinalizer) {
		return nil
	}
	logger := log.FromContext(ctx)
	controllerutil.AddFinalizer(ld, llamaDeploymentFinalizer)
	if err := r.Update(ctx, ld); err != nil {
		logger.Error(err, "Failed to add finalizer")
		return err
	}
	logger.Info("Added finalizer to LlamaDeployment")
	// Re-read to pick up the new resourceVersion after the Update
	return r.Get(ctx, client.ObjectKeyFromObject(ld), ld)
}

// handleAlreadyFailed handles deployments whose current generation has already
// been marked as a terminal failure. Still updates version tracking during
// schema migration to prevent infinite loops.
func (r *LlamaDeploymentReconciler) handleAlreadyFailed(ctx context.Context, ld *llamadeployv1.LlamaDeployment, needsFullReconcile bool) (ctrl.Result, error) {
	logger := log.FromContext(ctx)
	if ld.Status.Phase == PhaseFailed {
		deployment := &appsv1.Deployment{}
		if err := r.Get(ctx, client.ObjectKeyFromObject(ld), deployment); err != nil {
			if !errors.IsNotFound(err) {
				return ctrl.Result{}, err
			}
		} else if deployment.Spec.Replicas == nil || *deployment.Spec.Replicas != 0 {
			patch := client.MergeFrom(deployment.DeepCopy())
			deployment.Spec.Replicas = ptr(int32(0))
			if err := r.Patch(ctx, deployment, patch); err != nil {
				return ctrl.Result{}, err
			}
			logger.Info("Restored failed Deployment to zero replicas")
		}
	}
	if needsFullReconcile {
		if err := r.updateVersionTracking(ctx, ld); err != nil {
			logger.Error(err, "Failed to update version tracking for terminal deployment")
			return ctrl.Result{}, err
		}
	}
	logger.Info("Skipping reconciliation for already-failed rollout generation",
		"generation", ld.Generation)
	return ctrl.Result{}, nil
}

// +kubebuilder:rbac:groups=deploy.llamaindex.ai,resources=llamadeployments,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=deploy.llamaindex.ai,resources=llamadeployments/status,verbs=get;update;patch
// +kubebuilder:rbac:groups=deploy.llamaindex.ai,resources=llamadeployments/finalizers,verbs=update
// +kubebuilder:rbac:groups=deploy.llamaindex.ai,resources=llamadeploymenttemplates,verbs=get;list;watch

// +kubebuilder:rbac:groups="",resources=secrets,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups="",resources=services,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups="",resources=configmaps,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups="",resources=events,verbs=get;list;watch;create;patch
// Grant access for control plane log streaming and pod discovery
// +kubebuilder:rbac:groups="",resources=pods,verbs=get;list;watch
// +kubebuilder:rbac:groups="",resources=pods/log,verbs=get
// +kubebuilder:rbac:groups=apps,resources=deployments,verbs=get;list;watch;create;update;patch;delete
// Needed to discover and scale down ReplicaSets during rollout timeout remediation
// +kubebuilder:rbac:groups=apps,resources=replicasets,verbs=get;list;watch;update;patch
// +kubebuilder:rbac:groups=networking.k8s.io,resources=ingresses,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=coordination.k8s.io,resources=leases,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups="",resources=serviceaccounts,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=networking.k8s.io,resources=networkpolicies,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=batch,resources=jobs,verbs=get;list;watch;create;update;patch;delete

// Reconcile is part of the main kubernetes reconciliation loop which aims to
// move the current state of the cluster closer to the desired state.
func (r *LlamaDeploymentReconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	logger := log.FromContext(ctx)

	ld, err := r.fetchDeployment(ctx, req)
	if ld == nil || err != nil {
		return ctrl.Result{}, err
	}

	if ld.DeletionTimestamp != nil {
		logger.Info("LlamaDeployment is being deleted")
		return r.handleDeletion(ctx, ld)
	}

	logger.Info("Reconciling LlamaDeployment", "name", ld.Name, "namespace", ld.Namespace, "generation", ld.Generation, "schemaVersion", ld.Status.SchemaVersion)

	// Migrate spec.name → spec.displayName for existing CRDs
	if requeue, err := r.migrateDisplayName(ctx, ld); err != nil {
		return ctrl.Result{}, err
	} else if requeue {
		return ctrl.Result{Requeue: true}, nil
	}

	needsFullReconcile := r.needsFullReconciliation(ld)
	if needsFullReconcile {
		logger.Info("Full reconciliation required",
			"currentSchemaVersion", CurrentSchemaVersion,
			"resourceSchemaVersion", ld.Status.SchemaVersion,
			"generation", ld.Generation,
			"lastReconciledGeneration", ld.Status.LastReconciledGeneration)
	}

	// Capacity gates — must come before initializeStatus so gated deployments
	// stay in their previous phase.
	if result, err := r.checkCapacityGates(ctx, ld, needsFullReconcile); result != nil || err != nil {
		if result != nil {
			return *result, err
		}
		return ctrl.Result{}, err
	}

	if err := r.ensureFinalizer(ctx, ld); err != nil {
		return ctrl.Result{}, err
	}

	if err := r.initializeStatus(ctx, ld, needsFullReconcile); err != nil {
		logger.Error(err, "Failed to initialize status")
		return ctrl.Result{}, err
	}

	// Validation gates — each may short-circuit the reconcile
	if result, err := r.checkSecretGate(ctx, ld); result != nil || err != nil {
		if result != nil {
			return *result, err
		}
		return ctrl.Result{}, err
	}

	if !isValidDNS1035Label(ld.Name) {
		return r.handleInvalidDNSName(ctx, ld)
	}

	if isTerminalFailure(ld.Status.Phase) && ld.Status.FailedRolloutGeneration == ld.Generation {
		return r.handleAlreadyFailed(ctx, ld, needsFullReconcile)
	}

	// No code source configured — skip build and resources, stay Pending.
	if isAwaitingCodePush(ld) {
		logger.Info("RepoUrl is empty, waiting for code push", "name", ld.Name)
		if needsFullReconcile {
			if err := r.updateVersionTracking(ctx, ld); err != nil {
				logger.Error(err, "Failed to update version tracking")
				return ctrl.Result{}, err
			}
		}
		return r.finalizePhase(ctx, ld, PhaseAwaitingCode, "Waiting for code push", 0, false)
	}

	// Build phase — may short-circuit if build is in progress or failed
	buildId, buildResult, err := r.reconcileBuild(ctx, ld)
	if err != nil {
		return ctrl.Result{}, err
	}
	if buildResult != nil {
		return *buildResult, nil
	}

	// Apply runtime resources (ServiceAccount, ConfigMap, Deployment, Service)
	if err := r.reconcileResources(ctx, ld, buildId); err != nil {
		return r.handleReconcileFailure(ctx, ld, err)
	}

	// Track schema version and release history
	if needsFullReconcile {
		if err := r.updateVersionTracking(ctx, ld); err != nil {
			logger.Error(err, "Failed to update version tracking")
			return ctrl.Result{}, err
		}
	}

	// Determine deployment health and act on failures
	phase, message, requeueAfter, statusDirty, err := r.assessDeploymentHealth(ctx, ld)
	if err != nil {
		return ctrl.Result{}, err
	}
	if isFailedPhase(phase) && ld.Status.FailedRolloutGeneration != ld.Generation {
		return r.remediateAndFinalizeFailure(ctx, ld, phase, message, requeueAfter, buildId)
	}

	return r.finalizePhase(ctx, ld, phase, message, requeueAfter, statusDirty)
}

// initializeStatus handles status initialization for new deployments or when full reconciliation is needed
func (r *LlamaDeploymentReconciler) initializeStatus(ctx context.Context, llamaDeploy *llamadeployv1.LlamaDeployment, needsFullReconcile bool) error {
	logger := log.FromContext(ctx)

	// Update status to Pending if not set or if full reconciliation is needed
	if llamaDeploy.Status.Phase == "" || needsFullReconcile {
		statusUpdated := false
		oldSchemaVersion := llamaDeploy.Status.SchemaVersion
		// Do not reset terminal phases, in-progress build phases, or suspended
		// phases during full reconciliation to avoid loops. PhaseBuilding is not
		// a failure state but should be preserved — resetting to Pending causes
		// status flip-flopping while a build Job is running. PhaseSuspended
		// should be preserved so suspended deployments don't get reset to
		// Pending and then consume rollout capacity or trigger builds.
		isTerminal := isTerminalFailure(llamaDeploy.Status.Phase) ||
			llamaDeploy.Status.Phase == PhaseBuilding ||
			llamaDeploy.Status.Phase == PhaseSuspended ||
			llamaDeploy.Status.Phase == PhaseAwaitingCode

		// Generate auth token for new deployments only.
		// We intentionally do NOT regenerate on schema version changes: rotating
		// the token changes the LLAMA_DEPLOY_AUTH_TOKEN env var in the pod
		// template, which triggers a rolling update for every single deployment.
		// With hundreds of deployments this creates a thundering-herd of
		// simultaneous rollouts that can OOM the operator.
		if llamaDeploy.Status.AuthToken == "" {
			authToken, err := generateAuthToken()
			if err != nil {
				return fmt.Errorf("failed to generate auth token: %w", err)
			}
			llamaDeploy.Status.AuthToken = authToken
			statusUpdated = true
		}

		// Update phase to Pending but do NOT update version tracking yet
		// Version tracking will be updated only after successful reconciliation
		if (llamaDeploy.Status.Phase == "" || needsFullReconcile) && !isTerminal {
			llamaDeploy.Status.Phase = PhasePending
			if needsFullReconcile {
				if llamaDeploy.Status.SchemaVersion != CurrentSchemaVersion {
					llamaDeploy.Status.Message = fmt.Sprintf("Schema version migration required (from %s to %s)", llamaDeploy.Status.SchemaVersion, CurrentSchemaVersion)
				} else {
					llamaDeploy.Status.Message = "Full reconciliation required due to spec changes"
				}
			} else {
				llamaDeploy.Status.Message = "Starting reconciliation"
			}
			now := metav1.Now()
			llamaDeploy.Status.LastUpdated = &now
			statusUpdated = true
		}

		if statusUpdated {
			if err := r.Status().Update(ctx, llamaDeploy); err != nil {
				return fmt.Errorf("failed to update status: %w", err)
			}
			logger.Info("Updated status to Pending", "generation", llamaDeploy.Generation)
			if r.Recorder != nil {
				if needsFullReconcile && oldSchemaVersion != CurrentSchemaVersion {
					r.Recorder.Event(llamaDeploy, corev1.EventTypeNormal, "SchemaVersionMigration", fmt.Sprintf("Migrating from schema version %s to %s", oldSchemaVersion, CurrentSchemaVersion))
				} else {
					r.Recorder.Event(llamaDeploy, corev1.EventTypeNormal, PhasePending, "Started reconciliation of LlamaDeployment")
				}
			}
		}
	}

	return nil
}

// handleReconcileFailure handles failures during resource reconciliation
func (r *LlamaDeploymentReconciler) handleReconcileFailure(ctx context.Context, llamaDeploy *llamadeployv1.LlamaDeployment, reconcileErr error) (ctrl.Result, error) {
	logger := log.FromContext(ctx)
	logger.Error(reconcileErr, "Failed to reconcile resources")

	// Update status to Failed only if not already Failed
	if llamaDeploy.Status.Phase != PhaseFailed {
		llamaDeploy.Status.Phase = PhaseFailed
		llamaDeploy.Status.Message = fmt.Sprintf("Failed to reconcile resources: %v", reconcileErr)
		now := metav1.Now()
		llamaDeploy.Status.LastUpdated = &now

		if statusErr := r.Status().Update(ctx, llamaDeploy); statusErr != nil {
			logger.Error(statusErr, "Failed to update LlamaDeployment status to Failed")
		} else {
			logger.Info("Updated status to Failed")
			if r.Recorder != nil {
				r.Recorder.Event(llamaDeploy, corev1.EventTypeWarning, PhaseFailed, fmt.Sprintf("Failed to reconcile resources: %v", reconcileErr))
			}
		}
	}

	return ctrl.Result{}, reconcileErr
}

// handleInvalidDNSName tears down owned resources and marks the deployment as
// terminally failed when the metadata.name is not a valid DNS-1035 label.
func (r *LlamaDeploymentReconciler) handleInvalidDNSName(ctx context.Context, llamaDeploy *llamadeployv1.LlamaDeployment) (ctrl.Result, error) {
	logger := log.FromContext(ctx)
	message := fmt.Sprintf(
		"Deployment name %q is not a valid DNS-1035 label: must start with a lowercase letter, "+
			"contain only lowercase alphanumeric characters or '-', end with an alphanumeric character, "+
			"and be at most 63 characters (regex: '[a-z]([a-z0-9-]*[a-z0-9])?')",
		llamaDeploy.Name,
	)
	logger.Error(fmt.Errorf("invalid DNS-1035 label: %s", llamaDeploy.Name), "Tearing down resources for non-compliant deployment name")

	// Tear down any existing resources that were created with the invalid name
	r.deleteOwnedResources(ctx, llamaDeploy)

	// Mark as Failed (terminal — metadata.name cannot be changed)
	if llamaDeploy.Status.Phase != PhaseFailed || llamaDeploy.Status.Message != message {
		llamaDeploy.Status.Phase = PhaseFailed
		llamaDeploy.Status.Message = message
		now := metav1.Now()
		llamaDeploy.Status.LastUpdated = &now

		if statusErr := r.Status().Update(ctx, llamaDeploy); statusErr != nil {
			logger.Error(statusErr, "Failed to update status for invalid DNS name")
			return ctrl.Result{}, statusErr
		}
		if r.Recorder != nil {
			r.Recorder.Event(llamaDeploy, corev1.EventTypeWarning, PhaseFailed, message)
		}
	}

	// Don't requeue — this is a terminal failure
	return ctrl.Result{}, nil
}

// deleteOwnedResources removes Deployment, Service, ConfigMap, and
// ServiceAccount resources that were created for a LlamaDeployment.
func (r *LlamaDeploymentReconciler) deleteOwnedResources(ctx context.Context, llamaDeploy *llamadeployv1.LlamaDeployment) {
	logger := log.FromContext(ctx)
	ns := llamaDeploy.Namespace
	name := llamaDeploy.Name

	// Delete Deployment
	dep := &appsv1.Deployment{}
	if err := r.Get(ctx, client.ObjectKey{Name: name, Namespace: ns}, dep); err == nil {
		if err := r.Delete(ctx, dep); err != nil && !errors.IsNotFound(err) {
			logger.Error(err, "Failed to delete Deployment during teardown", "name", name)
		} else {
			logger.Info("Deleted Deployment during teardown", "name", name)
		}
	}

	// Delete Service
	svc := &corev1.Service{}
	if err := r.Get(ctx, client.ObjectKey{Name: name, Namespace: ns}, svc); err == nil {
		if err := r.Delete(ctx, svc); err != nil && !errors.IsNotFound(err) {
			logger.Error(err, "Failed to delete Service during teardown", "name", name)
		} else {
			logger.Info("Deleted Service during teardown", "name", name)
		}
	}

	// Delete ConfigMap
	cm := &corev1.ConfigMap{}
	if err := r.Get(ctx, client.ObjectKey{Name: name + "-nginx-config", Namespace: ns}, cm); err == nil {
		if err := r.Delete(ctx, cm); err != nil && !errors.IsNotFound(err) {
			logger.Error(err, "Failed to delete ConfigMap during teardown", "name", name+"-nginx-config")
		} else {
			logger.Info("Deleted ConfigMap during teardown", "name", name+"-nginx-config")
		}
	}

	// Delete ServiceAccount
	sa := &corev1.ServiceAccount{}
	if err := r.Get(ctx, client.ObjectKey{Name: name + "-sa", Namespace: ns}, sa); err == nil {
		if err := r.Delete(ctx, sa); err != nil && !errors.IsNotFound(err) {
			logger.Error(err, "Failed to delete ServiceAccount during teardown", "name", name+"-sa")
		} else {
			logger.Info("Deleted ServiceAccount during teardown", "name", name+"-sa")
		}
	}
}

// updateVersionTracking updates the schema version and generation tracking after successful reconciliation
func (r *LlamaDeploymentReconciler) updateVersionTracking(ctx context.Context, llamaDeploy *llamadeployv1.LlamaDeployment) error {
	logger := log.FromContext(ctx)

	// Refresh the resource to get latest status before updating version tracking
	if err := r.Get(ctx, client.ObjectKey{Name: llamaDeploy.Name, Namespace: llamaDeploy.Namespace}, llamaDeploy); err != nil {
		return fmt.Errorf("failed to refresh LlamaDeployment after reconciliation: %w", err)
	}

	// Append release history entry if git sha changed and is set
	func() {
		// avoid panics on nil slices
		history := llamaDeploy.Status.ReleaseHistory
		currentSha := llamaDeploy.Spec.GitSha
		if currentSha == "" {
			return
		}
		// only add if different from the last entry
		var lastSha string
		if len(history) > 0 {
			lastSha = history[len(history)-1].GitSha
		}
		if lastSha == currentSha {
			return
		}
		// append new entry
		now := metav1.Now()
		entry := llamadeployv1.ReleaseHistoryEntry{
			GitSha:     currentSha,
			ImageTag:   getContainerImageTag(llamaDeploy),
			ReleasedAt: now,
		}
		llamaDeploy.Status.ReleaseHistory = append(history, entry)
		// enforce max 20 entries (keep most recent)
		if len(llamaDeploy.Status.ReleaseHistory) > 20 {
			llamaDeploy.Status.ReleaseHistory = llamaDeploy.Status.ReleaseHistory[1:]
		}
	}()

	llamaDeploy.Status.SchemaVersion = CurrentSchemaVersion
	llamaDeploy.Status.LastReconciledGeneration = llamaDeploy.Generation
	now := metav1.Now()
	llamaDeploy.Status.LastUpdated = &now

	if err := r.Status().Update(ctx, llamaDeploy); err != nil {
		return fmt.Errorf("failed to update version tracking: %w", err)
	}
	logger.Info("Updated version tracking after successful reconciliation",
		"schemaVersion", CurrentSchemaVersion,
		"generation", llamaDeploy.Generation)

	return nil
}

// handleDeletion handles cleanup when a LlamaDeployment is being deleted
func (r *LlamaDeploymentReconciler) handleDeletion(ctx context.Context, llamaDeploy *llamadeployv1.LlamaDeployment) (ctrl.Result, error) {
	logger := log.FromContext(ctx)

	// Delete the secret if it exists and is specified
	if llamaDeploy.Spec.SecretName != "" {
		if err := r.deleteSecret(ctx, llamaDeploy); err != nil {
			logger.Error(err, "Failed to delete secret", "secretName", llamaDeploy.Spec.SecretName)
			return ctrl.Result{}, err
		}
		logger.Info("Deleted secret", "secretName", llamaDeploy.Spec.SecretName)
	}

	// Remove finalizer
	controllerutil.RemoveFinalizer(llamaDeploy, llamaDeploymentFinalizer)
	if err := r.Update(ctx, llamaDeploy); err != nil {
		logger.Error(err, "Failed to remove finalizer")
		return ctrl.Result{}, err
	}

	logger.Info("Finalizer removed, LlamaDeployment cleanup complete")
	return ctrl.Result{}, nil
}

// deleteSecret removes the secret associated with the LlamaDeployment
func (r *LlamaDeploymentReconciler) deleteSecret(ctx context.Context, llamaDeploy *llamadeployv1.LlamaDeployment) error {
	secret := &corev1.Secret{}
	err := r.Get(ctx, client.ObjectKey{Name: llamaDeploy.Spec.SecretName, Namespace: llamaDeploy.Namespace}, secret)
	if err != nil && errors.IsNotFound(err) {
		// Secret doesn't exist, nothing to delete
		return nil
	} else if err != nil {
		return err
	}

	return r.Delete(ctx, secret)
}

// SetupWithManager sets up the controller with the Manager.
func (r *LlamaDeploymentReconciler) SetupWithManager(mgr ctrl.Manager) error {
	return ctrl.NewControllerManagedBy(mgr).
		For(&llamadeployv1.LlamaDeployment{}).
		Owns(&appsv1.Deployment{}).
		Owns(&corev1.Service{}).
		Owns(&corev1.ConfigMap{}).
		Owns(&batchv1.Job{}).
		// Watch template changes and enqueue all LlamaDeployments in the namespace
		Watches(
			&llamadeployv1.LlamaDeploymentTemplate{},
			handler.EnqueueRequestsFromMapFunc(func(ctx context.Context, obj client.Object) []reconcile.Request {
				var list llamadeployv1.LlamaDeploymentList
				if err := mgr.GetClient().List(ctx, &list, &client.ListOptions{Namespace: obj.GetNamespace()}); err != nil {
					return nil
				}
				reqs := make([]reconcile.Request, 0, len(list.Items))
				for i := range list.Items {
					ld := &list.Items[i]
					reqs = append(reqs, reconcile.Request{NamespacedName: client.ObjectKeyFromObject(ld)})
				}
				return reqs
			}),
		).
		// Self-watch: when a LlamaDeployment frees capacity (deleted, or
		// transitions to Suspended/Failed), wake up other deployments that
		// may be gated by MaxDeployments.
		Watches(
			&llamadeployv1.LlamaDeployment{},
			handler.EnqueueRequestsFromMapFunc(func(ctx context.Context, obj client.Object) []reconcile.Request {
				if r.MaxDeployments <= 0 {
					return nil
				}
				var list llamadeployv1.LlamaDeploymentList
				if err := mgr.GetClient().List(ctx, &list, &client.ListOptions{Namespace: obj.GetNamespace()}); err != nil {
					return nil
				}
				var reqs []reconcile.Request
				for i := range list.Items {
					ld := &list.Items[i]
					if ld.Name == obj.GetName() && ld.Namespace == obj.GetNamespace() {
						continue
					}
					if ld.Generation != ld.Status.LastReconciledGeneration {
						reqs = append(reqs, reconcile.Request{NamespacedName: client.ObjectKeyFromObject(ld)})
					}
				}
				return reqs
			}),
			builder.WithPredicates(predicate.Funcs{
				CreateFunc: func(e event.CreateEvent) bool {
					return false
				},
				UpdateFunc: func(e event.UpdateEvent) bool {
					oldLD, ok1 := e.ObjectOld.(*llamadeployv1.LlamaDeployment)
					newLD, ok2 := e.ObjectNew.(*llamadeployv1.LlamaDeployment)
					if !ok1 || !ok2 {
						return false
					}
					// Trigger when an active deployment becomes suspended or failed
					return isActivePhase(oldLD.Status.Phase) && !isActivePhase(newLD.Status.Phase)
				},
				DeleteFunc: func(e event.DeleteEvent) bool {
					return true
				},
				GenericFunc: func(e event.GenericEvent) bool {
					return false
				},
			}),
		).
		// Self-watch: when a LlamaDeployment transitions OUT of a rolling
		// phase (Pending/RollingOut/Building), immediately wake CRs gated by
		// checkRolloutCapacity instead of waiting for their jitter timer.
		Watches(
			&llamadeployv1.LlamaDeployment{},
			handler.EnqueueRequestsFromMapFunc(func(ctx context.Context, obj client.Object) []reconcile.Request {
				if r.MaxConcurrentRollouts <= 0 {
					return nil
				}
				var list llamadeployv1.LlamaDeploymentList
				if err := mgr.GetClient().List(ctx, &list, &client.ListOptions{Namespace: obj.GetNamespace()}); err != nil {
					return nil
				}
				var reqs []reconcile.Request
				for i := range list.Items {
					ld := &list.Items[i]
					if ld.Name == obj.GetName() && ld.Namespace == obj.GetNamespace() {
						continue
					}
					if ld.Generation != ld.Status.LastReconciledGeneration {
						reqs = append(reqs, reconcile.Request{NamespacedName: client.ObjectKeyFromObject(ld)})
					}
				}
				return reqs
			}),
			builder.WithPredicates(predicate.Funcs{
				CreateFunc: func(e event.CreateEvent) bool {
					return false
				},
				UpdateFunc: func(e event.UpdateEvent) bool {
					oldLD, ok1 := e.ObjectOld.(*llamadeployv1.LlamaDeployment)
					newLD, ok2 := e.ObjectNew.(*llamadeployv1.LlamaDeployment)
					if !ok1 || !ok2 {
						return false
					}
					// Fire when transitioning OUT of a rolling phase
					return isRollingPhase(oldLD.Status.Phase) && !isRollingPhase(newLD.Status.Phase)
				},
				DeleteFunc: func(e event.DeleteEvent) bool {
					// A deleted CR frees a rollout slot
					return true
				},
				GenericFunc: func(e event.GenericEvent) bool {
					return false
				},
			}),
		).
		Complete(r)
}

// Note: No custom EventHandler needed; we map template changes to all LlamaDeployments via EnqueueRequestsFromMapFunc.
