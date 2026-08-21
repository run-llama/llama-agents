package controller

import (
	"context"
	"fmt"
	mathrand "math/rand"
	"strconv"
	"time"

	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/log"

	llamadeployv1 "llama-agents-operator/api/v1"
)

const (
	activeRolloutPollInterval = 30 * time.Second
	crashLoopBackOffReason    = "CrashLoopBackOff"
)

// isFailedPhase returns true for phases where failure remediation may be needed.
func isFailedPhase(phase string) bool {
	return phase == PhaseRolloutFailed || phase == PhaseFailed
}

// isAwaitingCodePush returns true when the deployment has no code source
// configured and is waiting for an initial git push.
func isAwaitingCodePush(ld *llamadeployv1.LlamaDeployment) bool {
	return ld.Spec.RepoUrl == ""
}

// isRollingPhase returns true for phases that consume a rollout slot.
func isRollingPhase(phase string) bool {
	return phase == PhasePending || phase == PhaseRollingOut || phase == PhaseBuilding
}

// checkRolloutCapacity returns (true, result, nil) when the reconcile should be
// requeued because the max concurrent rollout limit has been reached.
func (r *LlamaDeploymentReconciler) checkRolloutCapacity(ctx context.Context, current *llamadeployv1.LlamaDeployment) (bool, ctrl.Result, error) {
	if r.MaxConcurrentRollouts <= 0 {
		return false, ctrl.Result{}, nil
	}

	logger := log.FromContext(ctx)

	var list llamadeployv1.LlamaDeploymentList
	if err := r.List(ctx, &list, &client.ListOptions{Namespace: current.Namespace}); err != nil {
		return false, ctrl.Result{}, fmt.Errorf("failed to list LlamaDeployments for rollout capacity check: %w", err)
	}

	rolling := 0
	for i := range list.Items {
		ld := &list.Items[i]
		if ld.Name == current.Name {
			continue
		}
		// Only count non-suspended deployments toward the rollout limit.
		// Suspended deployments may transiently remain in Pending/Building
		// phase until the next reconcile updates them to Suspended.
		if ld.Spec.Suspended {
			continue
		}
		if isRollingPhase(ld.Status.Phase) {
			rolling++
		}
	}

	if rolling >= r.MaxConcurrentRollouts {
		// Jittered requeue: 10-20 seconds
		jitter := time.Duration(10+mathrand.Intn(11)) * time.Second
		logger.Info("Max concurrent rollouts reached, requeuing",
			"limit", r.MaxConcurrentRollouts,
			"inProgress", rolling,
			"requeueAfter", jitter)
		return true, ctrl.Result{RequeueAfter: jitter}, nil
	}

	return false, ctrl.Result{}, nil
}

// checkDeploymentCapacity returns (true, result, nil) when the reconcile should
// be requeued because the max deployments limit has been reached.
func (r *LlamaDeploymentReconciler) checkDeploymentCapacity(ctx context.Context, current *llamadeployv1.LlamaDeployment) (bool, ctrl.Result, error) {
	if r.MaxDeployments <= 0 {
		return false, ctrl.Result{}, nil
	}

	logger := log.FromContext(ctx)

	var list llamadeployv1.LlamaDeploymentList
	if err := r.List(ctx, &list, &client.ListOptions{Namespace: current.Namespace}); err != nil {
		return false, ctrl.Result{}, fmt.Errorf("failed to list LlamaDeployments for deployment capacity check: %w", err)
	}

	active := 0
	for i := range list.Items {
		ld := &list.Items[i]
		if ld.Name == current.Name {
			continue
		}
		if isActivePhase(ld.Status.Phase) {
			active++
		}
	}

	if active >= r.MaxDeployments {
		logger.Info("Max deployments limit reached, requeuing",
			"limit", r.MaxDeployments,
			"active", active,
			"requeueAfter", 5*time.Minute)
		return true, ctrl.Result{RequeueAfter: 5 * time.Minute}, nil
	}

	return false, ctrl.Result{}, nil
}

// checkCapacityGates short-circuits the reconcile when deployment or rollout
// capacity limits are reached. Returns (*result, nil) to requeue, or (nil, nil)
// to continue. Must be called before initializeStatus so gated deployments
// stay in their previous phase.
func (r *LlamaDeploymentReconciler) checkCapacityGates(ctx context.Context, ld *llamadeployv1.LlamaDeployment, needsFullReconcile bool) (*ctrl.Result, error) {
	if !needsFullReconcile || ld.Spec.Suspended || ld.Status.Phase == PhaseAwaitingCode {
		return nil, nil
	}
	if requeue, result, err := r.checkDeploymentCapacity(ctx, ld); err != nil {
		return nil, err
	} else if requeue {
		return &result, nil
	}
	if requeue, result, err := r.checkRolloutCapacity(ctx, ld); err != nil {
		return nil, err
	} else if requeue {
		return &result, nil
	}
	return nil, nil
}

// checkSecretGate verifies that a referenced Secret exists. Returns (*result, nil)
// to short-circuit the reconcile, or (nil, nil) to continue.
func (r *LlamaDeploymentReconciler) checkSecretGate(ctx context.Context, ld *llamadeployv1.LlamaDeployment) (*ctrl.Result, error) {
	if ld.Spec.SecretName == "" {
		return nil, nil
	}
	if done, result, err := r.checkSecretExists(ctx, ld); done {
		return &result, err
	}
	return nil, nil
}

// checkSecretExists verifies the referenced Secret exists, retrying a few times
// to handle informer cache lag. Returns (done, result, err) where done=true means
// the caller should return result/err immediately.
func (r *LlamaDeploymentReconciler) checkSecretExists(ctx context.Context, llamaDeploy *llamadeployv1.LlamaDeployment) (bool, ctrl.Result, error) {
	logger := log.FromContext(ctx)

	secret := &corev1.Secret{}
	if err := r.Get(ctx, client.ObjectKey{Name: llamaDeploy.Spec.SecretName, Namespace: llamaDeploy.Namespace}, secret); err != nil {
		if errors.IsNotFound(err) {
			secretRetries := llamaDeploy.Status.SecretCheckRetries
			if secretRetries < 3 {
				llamaDeploy.Status.SecretCheckRetries = secretRetries + 1
				if statusErr := r.Status().Update(ctx, llamaDeploy); statusErr != nil {
					logger.Error(statusErr, "Failed to update secret check retry count")
				}
				logger.Info("Secret not found, will retry",
					"secret", llamaDeploy.Spec.SecretName,
					"retry", secretRetries+1)
				return true, ctrl.Result{RequeueAfter: 5 * time.Second}, nil
			}
			message := fmt.Sprintf("Secret %s not found after retries - rollout failed. Create the Secret and update the resource to retry.", llamaDeploy.Spec.SecretName)
			if llamaDeploy.Status.Phase != PhaseRolloutFailed || llamaDeploy.Status.Message != message {
				llamaDeploy.Status.Phase = PhaseRolloutFailed
				llamaDeploy.Status.Message = message
				now := metav1.Now()
				llamaDeploy.Status.LastUpdated = &now
				if statusErr := r.Status().Update(ctx, llamaDeploy); statusErr != nil {
					logger.Error(statusErr, "Failed to update status for missing Secret")
				}
				if r.Recorder != nil {
					r.Recorder.Event(llamaDeploy, corev1.EventTypeWarning, PhaseRolloutFailed, message)
				}
			}
			return true, ctrl.Result{}, nil
		}
		// Any other error fetching the Secret is retriable
		return true, ctrl.Result{}, err
	}
	// Reset retry counter on success
	if llamaDeploy.Status.SecretCheckRetries > 0 {
		llamaDeploy.Status.SecretCheckRetries = 0
		if statusErr := r.Status().Update(ctx, llamaDeploy); statusErr != nil {
			logger.Error(statusErr, "Failed to reset secret check retry count")
		}
	}
	return false, ctrl.Result{}, nil
}

// assessDeploymentHealth determines the deployment phase, tracks rollout timing,
// and checks for rollout timeouts. Returns the assessed phase, status message,
// requeue duration, and whether status fields were mutated (RolloutStartedAt).
func (r *LlamaDeploymentReconciler) assessDeploymentHealth(ctx context.Context, ld *llamadeployv1.LlamaDeployment) (phase string, message string, requeueAfter time.Duration, statusDirty bool, err error) {
	logger := log.FromContext(ctx)

	phase, message, err = r.determineDeploymentPhase(ctx, ld)
	if err != nil {
		logger.Error(err, "Failed to determine deployment phase")
		return "", "", 0, false, err
	}

	// Track rollout start time
	if phase == PhasePending || phase == PhaseRollingOut {
		if ld.Status.RolloutStartedAt == nil {
			now := metav1.Now()
			ld.Status.RolloutStartedAt = &now
			statusDirty = true
		}
	} else if ld.Status.RolloutStartedAt != nil {
		ld.Status.RolloutStartedAt = nil
		statusDirty = true
	}

	// Check operator-level rollout timeout for in-progress phases
	if phase == PhasePending || phase == PhaseRollingOut {
		if crashPhase, crashMessage, found := r.detectCurrentRolloutCrashLoop(ctx, ld); found {
			phase = crashPhase
			message = crashMessage
		} else {
			toResult := r.checkRolloutTimeout(ctx, ld)
			if toResult.TimedOut {
				phase = toResult.Phase
				message = toResult.Message
			} else if toResult.RequeueAfter > 0 {
				requeueAfter = min(toResult.RequeueAfter, activeRolloutPollInterval)
			} else {
				requeueAfter = activeRolloutPollInterval
			}
		}
	}

	return phase, message, requeueAfter, statusDirty, nil
}

// detectCurrentRolloutCrashLoop checks only the newest ReplicaSet's active pods.
// Pod and ReplicaSet reads bypass the informer cache, so active rollouts poll.
func (r *LlamaDeploymentReconciler) detectCurrentRolloutCrashLoop(ctx context.Context, ld *llamadeployv1.LlamaDeployment) (string, string, bool) {
	if r.DirectClient == nil {
		return "", "", false
	}

	logger := log.FromContext(ctx)
	deployment, newestRS, hasOlderAvailable, err := r.currentRolloutReplicaSet(ctx, ld)
	if err != nil {
		logger.Error(err, "Failed to inspect current rollout ReplicaSet")
		return "", "", false
	}
	if newestRS == nil {
		return "", "", false
	}

	pods := &corev1.PodList{}
	if err := r.directClient().List(ctx, pods,
		client.InNamespace(ld.Namespace),
		client.MatchingLabels(deployment.Spec.Selector.MatchLabels),
	); err != nil {
		logger.Error(err, "Failed to inspect current rollout pods")
		return "", "", false
	}

	for i := range pods.Items {
		pod := &pods.Items[i]
		owner := metav1.GetControllerOf(pod)
		if !pod.DeletionTimestamp.IsZero() || owner == nil || owner.UID != newestRS.UID {
			continue
		}
		if podHasAppCrashLoop(pod) {
			if hasOlderAvailable {
				return PhaseRolloutFailed, "New rollout is crash-looping; previous version remains available", true
			}
			return PhaseFailed, "Deployment app is crash-looping; failing pods stopped", true
		}
	}

	return "", "", false
}

func podHasAppCrashLoop(pod *corev1.Pod) bool {
	for _, status := range pod.Status.InitContainerStatuses {
		if status.State.Waiting != nil && status.State.Waiting.Reason == crashLoopBackOffReason {
			return true
		}
	}
	for _, status := range pod.Status.ContainerStatuses {
		if status.Name == ContainerNameApp && status.State.Waiting != nil && status.State.Waiting.Reason == crashLoopBackOffReason {
			return true
		}
	}
	return false
}

// determineDeploymentPhase analyzes the deployment status and returns the appropriate phase and message
func (r *LlamaDeploymentReconciler) determineDeploymentPhase(ctx context.Context, llamaDeploy *llamadeployv1.LlamaDeployment) (string, string, error) {
	if llamaDeploy.Spec.Suspended {
		return PhaseSuspended, "Deployment is suspended (scaled to 0 replicas)", nil
	}

	deployment := &appsv1.Deployment{}
	err := r.Get(ctx, client.ObjectKey{Name: llamaDeploy.Name, Namespace: llamaDeploy.Namespace}, deployment)
	if err != nil {
		if errors.IsNotFound(err) {
			// Deployment doesn't exist yet
			return PhasePending, "Waiting for deployment to be created", nil
		}
		return "", "", err
	}

	desiredReplicas := int32(1)
	if deployment.Spec.Replicas != nil {
		desiredReplicas = *deployment.Spec.Replicas
	}

	status := deployment.Status

	// Get deployment conditions
	var availableCondition, progressingCondition *appsv1.DeploymentCondition
	for i := range status.Conditions {
		condition := &status.Conditions[i]
		switch condition.Type {
		case appsv1.DeploymentAvailable:
			availableCondition = condition
		case appsv1.DeploymentProgressing:
			progressingCondition = condition
		}
	}

	// Determine phase based on Kubernetes deployment conditions
	switch {
	case status.AvailableReplicas == 0:
		// No pods available - either starting up or completely failed
		if progressingCondition != nil && progressingCondition.Status == corev1.ConditionFalse {
			return PhaseFailed, "Deployment has failed and no pods are available", nil
		}
		return PhasePending, "Waiting for deployment pods to become available", nil

	case progressingCondition != nil && progressingCondition.Status == corev1.ConditionFalse:
		// Progress deadline exceeded - rollout failed
		if status.AvailableReplicas > 0 {
			return PhaseRolloutFailed, fmt.Sprintf("Deployment rollout failed but %d pods from previous version are still serving traffic", status.AvailableReplicas), nil
		}
		return PhaseFailed, "Deployment rollout failed", nil

	case availableCondition != nil && availableCondition.Status == corev1.ConditionTrue &&
		progressingCondition != nil && progressingCondition.Status == corev1.ConditionTrue &&
		status.ReadyReplicas == desiredReplicas && status.Replicas == desiredReplicas:
		// Deployment is available, progressing successfully, and has the right number of ready replicas
		return PhaseRunning, "Deployment is healthy and running", nil

	case progressingCondition != nil && progressingCondition.Status == corev1.ConditionTrue &&
		(status.Replicas > desiredReplicas || status.ReadyReplicas < desiredReplicas):
		// Deployment is progressing but not yet complete (rollout in progress)
		if status.Replicas > desiredReplicas {
			return PhaseRollingOut, fmt.Sprintf("Rolling update in progress (%d/%d pods ready, %d total)", status.ReadyReplicas, desiredReplicas, status.Replicas), nil
		}
		return PhasePending, fmt.Sprintf("Deployment starting up (%d/%d pods ready)", status.ReadyReplicas, desiredReplicas), nil

	default:
		// Fallback - deployment exists but conditions are unclear
		if status.ReadyReplicas > 0 {
			return PhasePending, fmt.Sprintf("Deployment status unclear (%d/%d pods ready)", status.ReadyReplicas, desiredReplicas), nil
		}
		return PhasePending, "Waiting for deployment to be ready", nil
	}
}

// rolloutTimeoutResult holds the outcome of a rollout timeout check.
type rolloutTimeoutResult struct {
	TimedOut     bool
	Phase        string
	Message      string
	RequeueAfter time.Duration
}

// checkRolloutTimeout checks whether the current rollout has exceeded the configured timeout.
// Side-effect-free — the caller is responsible for remediation and status writes.
func (r *LlamaDeploymentReconciler) checkRolloutTimeout(ctx context.Context, llamaDeploy *llamadeployv1.LlamaDeployment) rolloutTimeoutResult {
	if llamaDeploy.Status.RolloutStartedAt == nil {
		return rolloutTimeoutResult{}
	}

	timeout := getRolloutTimeout()
	elapsed := time.Since(llamaDeploy.Status.RolloutStartedAt.Time)
	remaining := timeout - elapsed

	if remaining > 0 {
		return rolloutTimeoutResult{RequeueAfter: remaining}
	}

	// Timed out — determine whether this is a full failure (no healthy pods)
	// or a rollout failure (old RS still serving traffic).
	logger := log.FromContext(ctx)
	logger.Info("Rollout timeout exceeded", "elapsed", elapsed.Truncate(time.Second), "timeout", timeout)

	// Check the k8s Deployment for available replicas to decide the phase
	deployment := &appsv1.Deployment{}
	failurePhase := PhaseRolloutFailed
	if err := r.Get(ctx, client.ObjectKey{Name: llamaDeploy.Name, Namespace: llamaDeploy.Namespace}, deployment); err == nil {
		if deployment.Status.AvailableReplicas == 0 {
			failurePhase = PhaseFailed
		}
	}

	message := fmt.Sprintf("Rollout timed out after %s (limit: %s); failing pods stopped",
		elapsed.Truncate(time.Second), timeout)

	return rolloutTimeoutResult{TimedOut: true, Phase: failurePhase, Message: message}
}

// remediateFailedRollout handles failure remediation: classifies pod failures,
// records the failed generation, and scales down the appropriate resources.
// Returns a non-nil result to short-circuit the reconcile (infra failures).
func (r *LlamaDeploymentReconciler) remediateFailedRollout(ctx context.Context, ld *llamadeployv1.LlamaDeployment, phase string, buildId string) (*ctrl.Result, error) {
	logger := log.FromContext(ctx)

	// Classify pod failures before acting — infrastructure issues should not
	// trigger scale-down.
	classification := r.classifyPodFailures(ctx, ld)
	if classification == failureInfra {
		logger.Info("Pod failures are infrastructure-related; not scaling down",
			"deployment", ld.Name)
		if r.Recorder != nil {
			r.Recorder.Event(ld, corev1.EventTypeWarning, "InfrastructureIssue",
				"Pods are being evicted or preempted; not scaling down — check node/resource configuration")
		}
		result := ctrl.Result{RequeueAfter: 30 * time.Second}
		return &result, nil
	}

	if phase == PhaseRolloutFailed {
		// Rollout failed but old RS has healthy traffic — pause and scale
		// down the newest RS to stop crash-looping while preserving old pods.
		if scaleErr := r.scaleDownFailingReplicaSet(ctx, ld); scaleErr != nil {
			return nil, scaleErr
		}
	} else {
		// Fully failed (no healthy pods) — set replicas=0 via the normal
		// SSA path. Set phase before reconcileDeployment so
		// createDeploymentForLlama sees PhaseFailed and produces replicas=0.
		oldPhase := ld.Status.Phase
		oldFailedGeneration := ld.Status.FailedRolloutGeneration
		ld.Status.Phase = PhaseFailed
		ld.Status.FailedRolloutGeneration = ld.Generation
		if reconcileErr := r.reconcileDeployment(ctx, ld, buildId); reconcileErr != nil {
			ld.Status.Phase = oldPhase
			ld.Status.FailedRolloutGeneration = oldFailedGeneration
			return nil, reconcileErr
		}
		if r.Recorder != nil {
			r.Recorder.Event(ld, corev1.EventTypeWarning, "DeploymentScaledDown",
				"Deployment has no healthy pods and failed to progress; scaled to 0 replicas")
		}
	}

	ld.Status.FailedRolloutGeneration = ld.Generation
	ld.Status.RolloutStartedAt = nil
	return nil, nil
}

func (r *LlamaDeploymentReconciler) remediateAndFinalizeFailure(ctx context.Context, ld *llamadeployv1.LlamaDeployment, phase, message string, requeueAfter time.Duration, buildId string) (ctrl.Result, error) {
	result, err := r.remediateFailedRollout(ctx, ld, phase, buildId)
	if err != nil {
		return ctrl.Result{}, err
	}
	if result != nil {
		return *result, nil
	}
	return r.finalizePhase(ctx, ld, phase, message, requeueAfter, true)
}

// failureType classifies the kind of pod failure.
type failureType int

const (
	failureUnknown failureType = iota
	failureApp
	failureInfra
)

func (f failureType) String() string {
	switch f {
	case failureApp:
		return "app"
	case failureInfra:
		return "infra"
	default:
		return "unknown"
	}
}

// classifyPodFailures inspects pods for a deployment and classifies the failure
// as app-level (CrashLoopBackOff, ImagePullBackOff, OOMKilled, etc.) or
// infra-level (eviction, preemption, scheduling). Uses directClient to avoid
// informer cache bloat.
func (r *LlamaDeploymentReconciler) classifyPodFailures(ctx context.Context, llamaDeploy *llamadeployv1.LlamaDeployment) failureType {
	logger := log.FromContext(ctx)

	podList := &corev1.PodList{}
	if err := r.directClient().List(ctx, podList,
		client.InNamespace(llamaDeploy.Namespace),
		client.MatchingLabels{"app": llamaDeploy.Name},
	); err != nil {
		logger.Error(err, "Failed to list pods for failure classification")
		return failureUnknown
	}

	if len(podList.Items) == 0 {
		return failureUnknown
	}

	hasInfra := false
	for i := range podList.Items {
		pod := &podList.Items[i]
		classification := classifyPod(pod)
		if classification == failureApp {
			return failureApp
		}
		if classification == failureInfra {
			hasInfra = true
		}
	}

	if hasInfra {
		return failureInfra
	}
	return failureUnknown
}

// classifyPod classifies a single pod's failure type.
func classifyPod(pod *corev1.Pod) failureType {
	// Evicted pods are infrastructure failures
	if pod.Status.Reason == "Evicted" {
		return failureInfra
	}

	// Pending pods with no container statuses are scheduling issues
	if pod.Status.Phase == corev1.PodPending && len(pod.Status.ContainerStatuses) == 0 && len(pod.Status.InitContainerStatuses) == 0 {
		return failureInfra
	}

	// Check all container statuses (init + regular)
	for _, statuses := range [][]corev1.ContainerStatus{pod.Status.InitContainerStatuses, pod.Status.ContainerStatuses} {
		for _, cs := range statuses {
			if cs.State.Waiting != nil {
				switch cs.State.Waiting.Reason {
				case crashLoopBackOffReason, "ImagePullBackOff", "ErrImagePull", "CreateContainerConfigError":
					return failureApp
				}
			}
			if cs.State.Terminated != nil && cs.State.Terminated.ExitCode != 0 {
				return failureApp
			}
			// Also check LastTerminationState for containers that restarted
			if cs.LastTerminationState.Terminated != nil && cs.LastTerminationState.Terminated.ExitCode != 0 {
				return failureApp
			}
		}
	}

	return failureUnknown
}

// scaleDownFailingReplicaSet finds the newest ReplicaSet for the Deployment,
// pauses the Deployment to prevent the controller from fighting the scale-down,
// and scales the newest RS to zero. The older RS (if any) is left intact.
func (r *LlamaDeploymentReconciler) scaleDownFailingReplicaSet(ctx context.Context, llamaDeploy *llamadeployv1.LlamaDeployment) error {
	logger := log.FromContext(ctx)

	deployment, newestRS, hasOtherHealthyRS, err := r.currentRolloutReplicaSet(ctx, llamaDeploy)
	if err != nil {
		return err
	}

	// Pause the Deployment so the Deployment controller does not fight our RS changes.
	if !deployment.Spec.Paused {
		patch := client.MergeFrom(deployment.DeepCopy())
		deployment.Spec.Paused = true
		if err := r.Patch(ctx, deployment, patch); err != nil {
			return fmt.Errorf("failed to pause deployment: %w", err)
		}
		logger.Info("Paused Deployment for rollout timeout remediation")
	}

	if newestRS == nil {
		logger.Info("No ReplicaSets found for deployment")
		return nil
	}

	// Safety: skip scale-down when the newest RS is the sole source of healthy
	// traffic. This function is now only called for PhaseRolloutFailed where
	// an older RS is serving traffic, so this guards against edge cases.
	if newestRS.Status.AvailableReplicas > 0 && !hasOtherHealthyRS {
		logger.Info("Newest ReplicaSet is the only one serving traffic; not scaling down",
			"replicaSet", newestRS.Name)
		return nil
	}

	// Scale down to 0 if not already
	currentReplicas := int32(1)
	if newestRS.Spec.Replicas != nil {
		currentReplicas = *newestRS.Spec.Replicas
	}
	if currentReplicas == 0 {
		logger.Info("Newest ReplicaSet already scaled to 0", "replicaSet", newestRS.Name)
		return nil
	}

	newestRS.Spec.Replicas = ptr(int32(0))
	if err := r.directClient().Update(ctx, newestRS); err != nil {
		return fmt.Errorf("failed to scale down ReplicaSet %s: %w", newestRS.Name, err)
	}

	logger.Info("Scaled down failing ReplicaSet to 0",
		"replicaSet", newestRS.Name,
		"previousReplicas", currentReplicas)

	if r.Recorder != nil {
		r.Recorder.Event(llamaDeploy, corev1.EventTypeWarning, "ReplicaSetScaledDown",
			fmt.Sprintf("Rollout failed with healthy pods still serving; scaled down failing ReplicaSet %s from %d to 0 replicas", newestRS.Name, currentReplicas))
	}

	return nil
}

// currentRolloutReplicaSet returns the newest owned ReplicaSet and whether a
// distinct older ReplicaSet is still available.
func (r *LlamaDeploymentReconciler) currentRolloutReplicaSet(ctx context.Context, ld *llamadeployv1.LlamaDeployment) (*appsv1.Deployment, *appsv1.ReplicaSet, bool, error) {
	directClient := r.directClient()
	deployment := &appsv1.Deployment{}
	if err := directClient.Get(ctx, client.ObjectKey{Name: ld.Name, Namespace: ld.Namespace}, deployment); err != nil {
		return nil, nil, false, fmt.Errorf("failed to get deployment: %w", err)
	}

	replicaSets := &appsv1.ReplicaSetList{}
	if err := directClient.List(ctx, replicaSets,
		client.InNamespace(ld.Namespace),
		client.MatchingLabels(deployment.Spec.Selector.MatchLabels),
	); err != nil {
		return nil, nil, false, fmt.Errorf("failed to list ReplicaSets: %w", err)
	}

	var newest *appsv1.ReplicaSet
	for i := range replicaSets.Items {
		candidate := &replicaSets.Items[i]
		if !hasOwnerUID(candidate.OwnerReferences, deployment.UID) {
			continue
		}
		if newest == nil || replicaSetIsNewer(candidate, newest) {
			newest = candidate
		}
	}
	if newest == nil {
		return deployment, nil, false, nil
	}

	hasOlderAvailable := false
	for i := range replicaSets.Items {
		candidate := &replicaSets.Items[i]
		if candidate.UID != newest.UID && hasOwnerUID(candidate.OwnerReferences, deployment.UID) && candidate.Status.AvailableReplicas > 0 {
			hasOlderAvailable = true
			break
		}
	}

	return deployment, newest, hasOlderAvailable, nil
}

func hasOwnerUID(ownerReferences []metav1.OwnerReference, uid types.UID) bool {
	for _, owner := range ownerReferences {
		if owner.UID == uid {
			return true
		}
	}
	return false
}

func replicaSetIsNewer(candidate, current *appsv1.ReplicaSet) bool {
	candidateRevision, candidateOK := replicaSetRevision(candidate)
	currentRevision, currentOK := replicaSetRevision(current)
	if candidateOK != currentOK {
		return candidateOK
	}
	if candidateOK && candidateRevision != currentRevision {
		return candidateRevision > currentRevision
	}
	return candidate.CreationTimestamp.After(current.CreationTimestamp.Time)
}

func replicaSetRevision(replicaSet *appsv1.ReplicaSet) (int64, bool) {
	revision, err := strconv.ParseInt(replicaSet.Annotations["deployment.kubernetes.io/revision"], 10, 64)
	return revision, err == nil
}

// finalizePhase writes the assessed phase to the status subresource and emits
// events for phase transitions. Only writes when phase changed or statusDirty
// indicates other status fields (e.g. RolloutStartedAt) were mutated.
func (r *LlamaDeploymentReconciler) finalizePhase(ctx context.Context, ld *llamadeployv1.LlamaDeployment, phase, message string, requeueAfter time.Duration, statusDirty bool) (ctrl.Result, error) {
	logger := log.FromContext(ctx)

	if ld.Status.Phase != phase || statusDirty {
		oldPhase := ld.Status.Phase
		ld.Status.Phase = phase
		ld.Status.Message = message
		now := metav1.Now()
		ld.Status.LastUpdated = &now

		if err := r.Status().Update(ctx, ld); err != nil {
			logger.Error(err, "Failed to update LlamaDeployment status", "phase", phase)
			return ctrl.Result{}, err
		}
		logger.Info("Updated deployment status", "phase", phase, "message", message)

		if oldPhase != phase {
			eventType := corev1.EventTypeNormal
			eventMessage := fmt.Sprintf("Phase changed from %s to %s: %s", oldPhase, phase, message)
			if phase == PhaseFailed || phase == PhaseRolloutFailed {
				eventType = corev1.EventTypeWarning
			}
			if r.Recorder != nil {
				r.Recorder.Event(ld, eventType, phase, eventMessage)
			}
		}
	}

	return ctrl.Result{RequeueAfter: requeueAfter}, nil
}
