package controller

import (
	"context"
	"crypto/sha256"
	"encoding/json"
	"fmt"
	"os"
	"path"
	"strconv"
	"strings"
	"time"

	appsv1 "k8s.io/api/apps/v1"
	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/util/intstr"
	"k8s.io/apimachinery/pkg/util/strategicpatch"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"
	"sigs.k8s.io/controller-runtime/pkg/log"

	llamadeployv1 "llama-agents-operator/api/v1"
)

const (
	// Container image configuration
	DefaultImage    = "llamaindex/llama-agents-appserver"
	DefaultImageTag = "latest"

	// Environment variables for image configuration
	EnvImageName       = "LLAMA_DEPLOY_IMAGE"
	EnvImageTag        = "LLAMA_DEPLOY_IMAGE_TAG"
	EnvImagePullPolicy = "LLAMA_DEPLOY_IMAGE_PULL_POLICY"

	// Nginx sidecar image configuration (configurable via Helm → operator env)
	DefaultNginxImage       = "nginxinc/nginx-unprivileged"
	DefaultNginxImageTag    = "1.27-alpine"
	EnvNginxImageName       = "LLAMA_DEPLOY_NGINX_IMAGE"
	EnvNginxImageTag        = "LLAMA_DEPLOY_NGINX_IMAGE_TAG"
	EnvNginxImagePullPolicy = "LLAMA_DEPLOY_NGINX_IMAGE_PULL_POLICY"

	// Default resource request env overrides for app container
	EnvDefaultCPURequest    = "LLAMA_DEPLOY_DEFAULT_CPU_REQUEST"
	EnvDefaultMemoryRequest = "LLAMA_DEPLOY_DEFAULT_MEMORY_REQUEST"

	// Default resource limit env overrides for app container
	// CPU limit defaults to unset (empty); memory limit defaults to 4096Mi
	EnvDefaultCPULimit    = "LLAMA_DEPLOY_DEFAULT_CPU_LIMIT"
	EnvDefaultMemoryLimit = "LLAMA_DEPLOY_DEFAULT_MEMORY_LIMIT"

	// Container name constants
	ContainerNameApp   = "app"
	ContainerNameBuild = "build"

	// Container user/group IDs
	AppServerUID int64 = 1001
	AppServerGID int64 = 1001
	NginxUID     int64 = 101
	NginxGID     int64 = 101

	// Default pull policy
	DefaultImagePullPolicy = "IfNotPresent"

	// Default rollout timeout in seconds (30 minutes)
	DefaultRolloutTimeoutSeconds int32 = 1800

	// Build artifact GC walks ReplicaSets up to this limit to discover
	// referenced buildIds, so pin it rather than relying on the apps/v1 default.
	DeploymentRevisionHistoryLimit int32 = 10

	// Environment variable for rollout timeout configuration
	EnvRolloutTimeoutSeconds = "LLAMA_DEPLOY_ROLLOUT_TIMEOUT_SECONDS"

	// appserverTagPrefix is retained for backward compatibility with old-style
	// image tags like "appserver-0.8.1". New tags are plain versions ("0.8.1").
	appserverTagPrefix = "appserver-"
)

// buildJobName computes the Job name for a given deployment/buildId combo,
// truncated to Kubernetes' 63-character name limit.
func buildJobName(deploymentName, buildId string) string {
	name := fmt.Sprintf("%s-build-%s", deploymentName, buildId)
	if len(name) > 63 {
		name = name[:63]
	}
	return name
}

// looksLikeFilePath provides a lightweight heuristic to determine if a
// deployment file path refers to a file rather than a directory. It avoids
// filesystem access and relies on common file extensions and presence of an extension.
func looksLikeFilePath(p string) bool {
	base := path.Base(p)
	if p == "." || p == "/" {
		return false
	}
	if strings.Contains(base, ".") {
		return true
	}
	return false
}

// getDefaultImage returns the operator-level default container image,
// ignoring any per-deployment spec overrides.
// 1. Use environment variable LLAMA_DEPLOY_IMAGE if set
// 2. Fall back to default: "llamaindex/llama-agents-appserver"
func getDefaultImage() string {
	if envImage := os.Getenv(EnvImageName); envImage != "" {
		return envImage
	}
	return DefaultImage
}

// getDefaultImageTag returns the operator-level default container image tag,
// ignoring any per-deployment spec overrides.
// 1. Use environment variable LLAMA_DEPLOY_IMAGE_TAG if set
// 2. Fall back to default: "latest"
func getDefaultImageTag() string {
	if envTag := os.Getenv(EnvImageTag); envTag != "" {
		return envTag
	}
	return DefaultImageTag
}

// getContainerImage returns the container image to use, with fallback logic:
// 1. Use spec.Image if specified
// 2. Use environment variable LLAMA_DEPLOY_IMAGE if set
// 3. Fall back to default: "llamaindex/llama-agents-appserver"
func getContainerImage(llamaDeploy *llamadeployv1.LlamaDeployment) string {
	if llamaDeploy.Spec.Image != "" {
		return llamaDeploy.Spec.Image
	}
	if envImage := os.Getenv(EnvImageName); envImage != "" {
		return envImage
	}
	return DefaultImage
}

// getContainerImageTag returns the container image tag to use, with fallback logic:
// 1. Use spec.ImageTag if specified (per-deployment pinning via control plane)
// 2. Use environment variable LLAMA_DEPLOY_IMAGE_TAG if set (fallback for CRDs without imageTag)
// 3. Fall back to default: "appserver-latest"
func getContainerImageTag(llamaDeploy *llamadeployv1.LlamaDeployment) string {
	if llamaDeploy.Spec.ImageTag != "" {
		// Backward compat: strip legacy "appserver-" prefix if present.
		// New-style tags are plain versions (e.g. "0.8.1"), old-style are "appserver-0.8.1".
		return strings.TrimPrefix(llamaDeploy.Spec.ImageTag, appserverTagPrefix)
	}
	if envTag := os.Getenv(EnvImageTag); envTag != "" {
		return envTag
	}
	return DefaultImageTag
}

// getContainerImagePullPolicy returns the container image pull policy to use, with fallback logic:
// 1. Use environment variable LLAMA_DEPLOY_IMAGE_PULL_POLICY if set
// 2. Fall back to default: "IfNotPresent"
func getContainerImagePullPolicy() corev1.PullPolicy {
	if envPullPolicy := os.Getenv(EnvImagePullPolicy); envPullPolicy != "" {
		switch envPullPolicy {
		case "Always":
			return corev1.PullAlways
		case "Never":
			return corev1.PullNever
		case "IfNotPresent":
			return corev1.PullIfNotPresent
		default:
			// Log warning for invalid value and use default
			return corev1.PullIfNotPresent
		}
	}
	return corev1.PullIfNotPresent
}

// getPullPolicyFromEnv returns a pull policy based on the given env var name.
func getPullPolicyFromEnv(envVarName string) corev1.PullPolicy {
	if envPullPolicy := os.Getenv(envVarName); envPullPolicy != "" {
		switch envPullPolicy {
		case "Always":
			return corev1.PullAlways
		case "Never":
			return corev1.PullNever
		case "IfNotPresent":
			return corev1.PullIfNotPresent
		default:
			return corev1.PullIfNotPresent
		}
	}
	return corev1.PullIfNotPresent
}

// getNginxImage returns the nginx sidecar image repository to use.
func getNginxImage() string {
	if envImage := os.Getenv(EnvNginxImageName); envImage != "" {
		return envImage
	}
	return DefaultNginxImage
}

// getNginxImageTag returns the nginx sidecar image tag to use.
func getNginxImageTag() string {
	if envTag := os.Getenv(EnvNginxImageTag); envTag != "" {
		return envTag
	}
	return DefaultNginxImageTag
}

// getNginxImagePullPolicy returns the nginx sidecar image pull policy to use.
func getNginxImagePullPolicy() corev1.PullPolicy {
	return getPullPolicyFromEnv(EnvNginxImagePullPolicy)
}

// parseOptionalQuantityFromEnv reads a quantity from an env var.
// Returns (nil, true) if the env var is present but intentionally unset
// (empty/none/unset/null/nil). Returns (quantity, true) if present and valid;
// (nil, false) if not present or invalid.
func parseOptionalQuantityFromEnv(envVarName string) (*resource.Quantity, bool) {
	raw, present := os.LookupEnv(envVarName)
	if !present {
		return nil, false
	}
	v := strings.TrimSpace(raw)
	if v == "" {
		return nil, true
	}
	lower := strings.ToLower(v)
	if lower == "none" || lower == "unset" || lower == "null" || lower == "nil" {
		return nil, true
	}
	if q, err := resource.ParseQuantity(v); err == nil {
		return &q, true
	}
	return nil, false
}

// getDefaultResourceRequests constructs the default ResourceList for requests.
// Defaults: CPU 750m, memory 2Gi. If env var is present but empty/none, the field is omitted.
func getDefaultResourceRequests() corev1.ResourceList {
	requests := corev1.ResourceList{}
	if q, present := parseOptionalQuantityFromEnv(EnvDefaultCPURequest); present {
		if q != nil {
			requests[corev1.ResourceCPU] = *q
		}
	} else {
		requests[corev1.ResourceCPU] = resource.MustParse("750m")
	}
	if q, present := parseOptionalQuantityFromEnv(EnvDefaultMemoryRequest); present {
		if q != nil {
			requests[corev1.ResourceMemory] = *q
		}
	} else {
		requests[corev1.ResourceMemory] = resource.MustParse("2Gi")
	}
	return requests
}

// getDefaultResourceLimits constructs the default ResourceList for limits.
// Defaults: CPU unset, memory 4096Mi. If env var is present but empty/none, the field is omitted.
func getDefaultResourceLimits() corev1.ResourceList {
	limits := corev1.ResourceList{}
	if q, present := parseOptionalQuantityFromEnv(EnvDefaultCPULimit); present {
		if q != nil {
			limits[corev1.ResourceCPU] = *q
		}
	}
	if q, present := parseOptionalQuantityFromEnv(EnvDefaultMemoryLimit); present {
		if q != nil {
			limits[corev1.ResourceMemory] = *q
		}
	} else {
		limits[corev1.ResourceMemory] = resource.MustParse("4096Mi")
	}
	return limits
}

// defaultPodSecurityContext returns the shared pod-level security context.
func defaultPodSecurityContext() *corev1.PodSecurityContext {
	return &corev1.PodSecurityContext{
		FSGroup: ptr(AppServerGID),
	}
}

// defaultContainerSecurityContext returns the hardened container security context
// for containers running as the appserver user (uid/gid 1001).
func defaultContainerSecurityContext() *corev1.SecurityContext {
	return &corev1.SecurityContext{
		RunAsNonRoot:             ptr(true),
		RunAsUser:                ptr(AppServerUID),
		RunAsGroup:               ptr(AppServerGID),
		AllowPrivilegeEscalation: ptr(false),
		Capabilities:             &corev1.Capabilities{Drop: []corev1.Capability{"ALL"}},
	}
}

// hardenedSecurityContext returns a minimal hardened security context that drops
// all capabilities and disables privilege escalation, without setting uid/gid
// (for containers whose uid is set by the image, e.g. the appserver).
func hardenedSecurityContext() *corev1.SecurityContext {
	return &corev1.SecurityContext{
		AllowPrivilegeEscalation: ptr(false),
		Capabilities:             &corev1.Capabilities{Drop: []corev1.Capability{"ALL"}},
	}
}

// getRolloutTimeout returns the configured rollout timeout duration.
// Reads from LLAMA_DEPLOY_ROLLOUT_TIMEOUT_SECONDS env var, falling back to DefaultRolloutTimeoutSeconds.
func getRolloutTimeout() time.Duration {
	return time.Duration(getRolloutTimeoutSecondsValue()) * time.Second
}

// getRolloutTimeoutSeconds returns the configured rollout timeout as *int32.
func getRolloutTimeoutSeconds() *int32 {
	v := getRolloutTimeoutSecondsValue()
	return &v
}

// getRolloutTimeoutSecondsValue reads the timeout from env var or returns the default.
func getRolloutTimeoutSecondsValue() int32 {
	if raw := os.Getenv(EnvRolloutTimeoutSeconds); raw != "" {
		if v, err := strconv.ParseInt(raw, 10, 32); err == nil && v > 0 {
			return int32(v)
		}
	}
	return DefaultRolloutTimeoutSeconds
}

// isValidDNS1035Label validates that a name is a valid DNS-1035 label.
// DNS-1035 labels must: start with [a-z], end with [a-z0-9], contain only
// [a-z0-9-], and be 1-63 characters long.
func isValidDNS1035Label(name string) bool {
	if len(name) == 0 || len(name) > 63 {
		return false
	}
	for i := 0; i < len(name); i++ {
		c := name[i]
		if c >= 'a' && c <= 'z' {
			continue
		}
		if c >= '0' && c <= '9' {
			if i == 0 {
				return false
			}
			continue
		}
		if c == '-' {
			if i == 0 || i == len(name)-1 {
				return false
			}
			continue
		}
		return false
	}
	return true
}

// computeBuildId computes a deterministic build identifier from the inputs that
// affect build output: name + gitSha + buildGeneration. spec.ImageTag is NOT
// included because the build Job always uses the operator's default image, not
// the deployment's pinned image.
func computeBuildId(llamaDeploy *llamadeployv1.LlamaDeployment) string {
	parts := []string{llamaDeploy.Name, llamaDeploy.Spec.GitSha}
	if llamaDeploy.Spec.BuildGeneration > 0 {
		parts = append(parts, fmt.Sprintf("bg:%d", llamaDeploy.Spec.BuildGeneration))
	}
	content := strings.Join(parts, "|")
	hash := fmt.Sprintf("%x", sha256.Sum256([]byte(content)))
	return hash[:16]
}

// supersedeStaleBuildJob cancels any in-flight build Job whose buildId has
// been superseded by a new spec. Succeeded stale Jobs are preserved for the
// A → B → A rollback-by-cache-hit path. A missing Job is a no-op (TTL may
// have reaped it, or a prior reconcile deleted it).
func (r *LlamaDeploymentReconciler) supersedeStaleBuildJob(
	ctx context.Context,
	llamaDeploy *llamadeployv1.LlamaDeployment,
	newBuildId string,
) error {
	if llamaDeploy.Status.BuildId == "" || llamaDeploy.Status.BuildId == newBuildId {
		return nil
	}
	if llamaDeploy.Status.BuildStatus != BuildStatusPending &&
		llamaDeploy.Status.BuildStatus != BuildStatusRunning {
		return nil
	}

	logger := log.FromContext(ctx)
	staleBuildId := llamaDeploy.Status.BuildId
	staleJobName := buildJobName(llamaDeploy.Name, staleBuildId)
	staleJob := &batchv1.Job{}
	getErr := r.Get(ctx, client.ObjectKey{Name: staleJobName, Namespace: llamaDeploy.Namespace}, staleJob)
	switch {
	case errors.IsNotFound(getErr):
		return nil
	case getErr != nil:
		return fmt.Errorf("failed to check stale build job: %w", getErr)
	case staleJob.Status.Succeeded > 0:
		logger.V(1).Info("Prior build succeeded; leaving Job in place",
			"staleBuildId", staleBuildId, "jobName", staleJobName)
		return nil
	}

	logger.Info("Superseding in-flight build Job for previous buildId",
		"staleBuildId", staleBuildId,
		"newBuildId", newBuildId,
		"jobName", staleJobName)
	if err := r.Delete(ctx, staleJob,
		client.PropagationPolicy(metav1.DeletePropagationBackground),
	); err != nil && !errors.IsNotFound(err) {
		return fmt.Errorf("failed to delete superseded build job: %w", err)
	}
	if r.Recorder != nil {
		r.Recorder.Event(llamaDeploy, corev1.EventTypeNormal, PhaseBuilding,
			fmt.Sprintf("Superseded stale build Job: %s", staleJobName))
	}
	return nil
}

// reconcileBuild ensures a build artifact exists for the current deployment inputs.
// Returns the buildId if a build is ready, or ("", result, nil) if a build is in progress
// and the caller should requeue.
func (r *LlamaDeploymentReconciler) reconcileBuild(ctx context.Context, llamaDeploy *llamadeployv1.LlamaDeployment) (string, *ctrl.Result, error) {
	logger := log.FromContext(ctx)

	// No code source configured — wait for a git push to set RepoUrl.
	if isAwaitingCodePush(llamaDeploy) {
		logger.Info("Skipping build: RepoUrl is empty, waiting for code push")
		return "", nil, nil
	}

	// Suspended deployments should not start or continue builds unless
	// spec.buildGeneration was explicitly bumped past status.lastBuiltGeneration.
	// This lets operators pre-build artifacts for suspended deployments
	// so that unsuspending is instant.
	if llamaDeploy.Spec.Suspended &&
		llamaDeploy.Spec.BuildGeneration <= llamaDeploy.Status.LastBuiltGeneration {
		logger.V(1).Info("Skipping build for suspended deployment")
		return "", nil, nil
	}

	buildId := computeBuildId(llamaDeploy)

	// If the CRD already has this buildId marked as succeeded, skip the build
	if llamaDeploy.Status.BuildId == buildId && llamaDeploy.Status.BuildStatus == BuildStatusSucceeded {
		logger.V(1).Info("Build artifact already exists", "buildId", buildId)
		return buildId, nil, nil
	}

	// If the spec advanced mid-build, cancel the stale in-flight Job so it
	// doesn't race the new one uploading.
	if err := r.supersedeStaleBuildJob(ctx, llamaDeploy, buildId); err != nil {
		return "", nil, err
	}

	// Check if a build Job already exists for this buildId
	jobName := buildJobName(llamaDeploy.Name, buildId)

	existingJob := &batchv1.Job{}
	err := r.Get(ctx, client.ObjectKey{Name: jobName, Namespace: llamaDeploy.Namespace}, existingJob)

	if err != nil && !errors.IsNotFound(err) {
		return "", nil, fmt.Errorf("failed to check build job: %w", err)
	}

	if errors.IsNotFound(err) {
		// No existing job — create one
		logger.Info("Creating build job", "buildId", buildId, "jobName", jobName)

		job := r.createBuildJob(llamaDeploy, buildId)
		if err := r.applyBuildJobTemplateOverlay(ctx, llamaDeploy, job); err != nil {
			logger.Error(err, "Failed to apply template overlay to build job")
			// Continue without overlay — scheduling preferences are optional
		}
		if err := controllerutil.SetControllerReference(llamaDeploy, job, r.Scheme); err != nil {
			return "", nil, fmt.Errorf("failed to set owner reference on build job: %w", err)
		}
		if err := r.Create(ctx, job); err != nil {
			if errors.IsAlreadyExists(err) {
				// Race condition — another reconcile created it. Requeue to check status.
				result := ctrl.Result{RequeueAfter: 5 * time.Second}
				return "", &result, nil
			}
			return "", nil, fmt.Errorf("failed to create build job: %w", err)
		}

		// Update CRD status to Building
		llamaDeploy.Status.BuildId = buildId
		llamaDeploy.Status.BuildStatus = BuildStatusPending
		llamaDeploy.Status.Phase = PhaseBuilding
		llamaDeploy.Status.Message = fmt.Sprintf("Build job created: %s", jobName)
		now := metav1.Now()
		llamaDeploy.Status.LastUpdated = &now
		if err := r.Status().Update(ctx, llamaDeploy); err != nil {
			logger.Error(err, "Failed to update build status")
			return "", nil, err
		}
		if r.Recorder != nil {
			r.Recorder.Event(llamaDeploy, corev1.EventTypeNormal, PhaseBuilding, fmt.Sprintf("Build job created: %s", jobName))
		}

		result := ctrl.Result{RequeueAfter: 60 * time.Second}
		return "", &result, nil
	}

	// Job exists — check its status
	if existingJob.Status.Succeeded > 0 {
		logger.Info("Build job succeeded", "buildId", buildId, "jobName", jobName)

		// Update CRD status
		llamaDeploy.Status.BuildId = buildId
		llamaDeploy.Status.BuildStatus = BuildStatusSucceeded
		llamaDeploy.Status.LastBuiltGeneration = llamaDeploy.Spec.BuildGeneration
		now := metav1.Now()
		llamaDeploy.Status.LastUpdated = &now
		if err := r.Status().Update(ctx, llamaDeploy); err != nil {
			logger.Error(err, "Failed to update build status to Succeeded")
			return "", nil, err
		}

		return buildId, nil, nil
	}

	if existingJob.Status.Failed > 0 {
		// If generation advanced (user wants a retry), delete the stale Job
		// and fall through to create a new one
		if llamaDeploy.Status.FailedRolloutGeneration != llamaDeploy.Generation {
			logger.Info("Generation advanced past failed build, deleting stale job for retry",
				"buildId", buildId, "jobName", jobName,
				"failedGeneration", llamaDeploy.Status.FailedRolloutGeneration,
				"currentGeneration", llamaDeploy.Generation)
			if err := r.Delete(ctx, existingJob, client.PropagationPolicy(metav1.DeletePropagationBackground)); err != nil && !errors.IsNotFound(err) {
				return "", nil, fmt.Errorf("failed to delete stale build job: %w", err)
			}
			// Record that we've consumed this generation's retry attempt.
			// If the new Job also fails, the "same generation" path below
			// will mark it as a genuine BuildFailed instead of looping.
			llamaDeploy.Status.FailedRolloutGeneration = llamaDeploy.Generation
			if err := r.Status().Update(ctx, llamaDeploy); err != nil {
				logger.Error(err, "Failed to update failedRolloutGeneration after retry")
				return "", nil, err
			}
			// Requeue to let the deletion propagate, then create a new Job
			result := ctrl.Result{RequeueAfter: 5 * time.Second}
			return "", &result, nil
		}

		// Same generation — genuine failure, mark as BuildFailed
		logger.Info("Build job failed", "buildId", buildId, "jobName", jobName)

		llamaDeploy.Status.BuildId = buildId
		llamaDeploy.Status.BuildStatus = BuildStatusFailed
		llamaDeploy.Status.Phase = PhaseBuildFailed
		llamaDeploy.Status.Message = fmt.Sprintf("Build job failed: %s", jobName)
		now := metav1.Now()
		llamaDeploy.Status.LastUpdated = &now
		llamaDeploy.Status.FailedRolloutGeneration = llamaDeploy.Generation
		if err := r.Status().Update(ctx, llamaDeploy); err != nil {
			logger.Error(err, "Failed to update build status to Failed")
			return "", nil, err
		}
		if r.Recorder != nil {
			r.Recorder.Event(llamaDeploy, corev1.EventTypeWarning, PhaseBuildFailed, fmt.Sprintf("Build job failed: %s", jobName))
		}

		// Don't requeue — wait for spec change (new generation)
		result := ctrl.Result{}
		return "", &result, nil
	}

	// Job is still running
	if llamaDeploy.Status.BuildStatus != BuildStatusRunning {
		llamaDeploy.Status.BuildStatus = BuildStatusRunning
		llamaDeploy.Status.Phase = PhaseBuilding
		llamaDeploy.Status.Message = fmt.Sprintf("Build in progress: %s", jobName)
		now := metav1.Now()
		llamaDeploy.Status.LastUpdated = &now
		if err := r.Status().Update(ctx, llamaDeploy); err != nil {
			logger.Error(err, "Failed to update build status to Running")
		}
	}

	result := ctrl.Result{RequeueAfter: 60 * time.Second}
	return "", &result, nil
}

// createBuildJob creates a Job that runs the build process and uploads artifacts to S3.
func (r *LlamaDeploymentReconciler) createBuildJob(llamaDeploy *llamadeployv1.LlamaDeployment, buildId string) *batchv1.Job {
	// Build environment variables from common helper, plus build-specific BUILD_ID
	envVars := append(r.commonEnvVars(llamaDeploy), corev1.EnvVar{
		Name:  "LLAMA_DEPLOY_BUILD_ID",
		Value: buildId,
	})

	envFrom := r.commonEnvFrom(llamaDeploy)

	jobName := buildJobName(llamaDeploy.Name, buildId)

	return &batchv1.Job{
		ObjectMeta: metav1.ObjectMeta{
			Name:      jobName,
			Namespace: llamaDeploy.Namespace,
			Labels: map[string]string{
				"app.kubernetes.io/managed-by":    "llama-deploy-operator",
				"deploy.llamaindex.ai/deployment": llamaDeploy.Name,
				"deploy.llamaindex.ai/build-id":   buildId,
			},
		},
		Spec: batchv1.JobSpec{
			BackoffLimit:            ptr(int32(1)),
			TTLSecondsAfterFinished: ptr(int32(3600)),
			ActiveDeadlineSeconds:   ptr(int64(1800)),
			Template: corev1.PodTemplateSpec{
				ObjectMeta: metav1.ObjectMeta{
					Labels: map[string]string{
						"app.kubernetes.io/managed-by":    "llama-deploy-operator",
						"deploy.llamaindex.ai/deployment": llamaDeploy.Name,
						"deploy.llamaindex.ai/build-id":   buildId,
					},
				},
				Spec: corev1.PodSpec{
					SecurityContext:    defaultPodSecurityContext(),
					EnableServiceLinks: ptr(false),
					RestartPolicy:      corev1.RestartPolicyNever,
					Volumes: []corev1.Volume{
						{
							Name: "app-data",
							VolumeSource: corev1.VolumeSource{
								EmptyDir: &corev1.EmptyDirVolumeSource{},
							},
						},
					},
					Containers: []corev1.Container{
						{
							Name:            ContainerNameBuild,
							Image:           fmt.Sprintf("%s:%s", getDefaultImage(), getDefaultImageTag()),
							ImagePullPolicy: getContainerImagePullPolicy(),
							Command:         []string{"python", "-m", "llama_deploy.appserver.build"},
							Env:             envVars,
							EnvFrom:         envFrom,
							VolumeMounts: []corev1.VolumeMount{
								{Name: "app-data", MountPath: "/opt/app"},
							},
							Resources:       corev1.ResourceRequirements{Requests: getDefaultResourceRequests(), Limits: getDefaultResourceLimits()},
							SecurityContext: defaultContainerSecurityContext(),
						},
					},
				},
			},
		},
	}
}

// applyBuildJobTemplateOverlay applies scheduling fields from a LlamaDeploymentTemplate to a build Job.
// Pod-level scheduling fields are always applied (nodeSelector, tolerations, affinity, priorityClassName,
// serviceAccountName). Container-level resource overrides are merged (not replaced) from a "build"
// container if present in the template, otherwise from the "app" container as a fallback.
func (r *LlamaDeploymentReconciler) applyBuildJobTemplateOverlay(ctx context.Context, ld *llamadeployv1.LlamaDeployment, job *batchv1.Job) error {
	templateName := ld.Spec.TemplateName
	if templateName == "" {
		templateName = "default"
	}

	tmpl := &llamadeployv1.LlamaDeploymentTemplate{}
	if err := r.Get(ctx, client.ObjectKey{Name: templateName, Namespace: ld.Namespace}, tmpl); err != nil {
		if errors.IsNotFound(err) {
			return nil
		}
		return err
	}

	overlay := tmpl.Spec.PodSpec.Spec

	// Apply pod-level scheduling fields
	if overlay.NodeSelector != nil {
		job.Spec.Template.Spec.NodeSelector = overlay.NodeSelector
	}
	if overlay.Tolerations != nil {
		job.Spec.Template.Spec.Tolerations = overlay.Tolerations
	}
	if overlay.Affinity != nil {
		job.Spec.Template.Spec.Affinity = overlay.Affinity
	}
	if overlay.PriorityClassName != "" {
		job.Spec.Template.Spec.PriorityClassName = overlay.PriorityClassName
	}
	if overlay.ServiceAccountName != "" {
		job.Spec.Template.Spec.ServiceAccountName = overlay.ServiceAccountName
	}
	if overlay.SecurityContext != nil {
		job.Spec.Template.Spec.SecurityContext = overlay.SecurityContext
	}

	// Merge template metadata (annotations and labels) into the build job pod template
	overlayMeta := tmpl.Spec.PodSpec.ObjectMeta
	if len(overlayMeta.Annotations) > 0 {
		if job.Spec.Template.Annotations == nil {
			job.Spec.Template.Annotations = make(map[string]string)
		}
		for k, v := range overlayMeta.Annotations {
			job.Spec.Template.Annotations[k] = v
		}
	}
	if len(overlayMeta.Labels) > 0 {
		if job.Spec.Template.Labels == nil {
			job.Spec.Template.Labels = make(map[string]string)
		}
		for k, v := range overlayMeta.Labels {
			job.Spec.Template.Labels[k] = v
		}
	}

	// Build the final resource requirements by starting from the template and
	// backfilling defaults for any keys the template doesn't set. Look for a
	// dedicated "build" container first, falling back to "app".
	templateResources := findContainerResources(overlay.Containers, ContainerNameBuild)
	if templateResources == nil {
		templateResources = findContainerResources(overlay.Containers, ContainerNameApp)
	}
	if templateResources != nil {
		for i := range job.Spec.Template.Spec.Containers {
			if job.Spec.Template.Spec.Containers[i].Name == ContainerNameBuild {
				job.Spec.Template.Spec.Containers[i].Resources = mergeResourceRequirements(templateResources, &job.Spec.Template.Spec.Containers[i].Resources)
				break
			}
		}
	}

	// Propagate container-level SecurityContext from the template overlay
	templateSC := findContainerSecurityContext(overlay.Containers, ContainerNameBuild)
	if templateSC == nil {
		templateSC = findContainerSecurityContext(overlay.Containers, ContainerNameApp)
	}
	if templateSC != nil {
		for i := range job.Spec.Template.Spec.Containers {
			if job.Spec.Template.Spec.Containers[i].Name == ContainerNameBuild {
				job.Spec.Template.Spec.Containers[i].SecurityContext = templateSC
				break
			}
		}
	}

	// Do not expose the namespace's service directory to user workloads.
	job.Spec.Template.Spec.EnableServiceLinks = ptr(false)

	return nil
}

// findContainerSecurityContext returns the SecurityContext for the named container,
// or nil if the container is not found or has no security context set.
func findContainerSecurityContext(containers []corev1.Container, name string) *corev1.SecurityContext {
	for _, c := range containers {
		if c.Name == name {
			if c.SecurityContext != nil {
				return c.SecurityContext
			}
			return nil
		}
	}
	return nil
}

// findContainerResources returns the ResourceRequirements for the named container,
// or nil if the container is not found or has no resources set.
func findContainerResources(containers []corev1.Container, name string) *corev1.ResourceRequirements {
	for _, c := range containers {
		if c.Name == name {
			if c.Resources.Requests != nil || c.Resources.Limits != nil {
				return &c.Resources
			}
			return nil
		}
	}
	return nil
}

// mergeResourceRequirements starts from the overlay resources and backfills any
// keys from defaults that the overlay doesn't set.
func mergeResourceRequirements(overlay, defaults *corev1.ResourceRequirements) corev1.ResourceRequirements {
	merged := overlay.DeepCopy()
	if merged.Requests == nil {
		merged.Requests = corev1.ResourceList{}
	}
	for k, v := range defaults.Requests {
		if _, exists := merged.Requests[k]; !exists {
			merged.Requests[k] = v
		}
	}
	if merged.Limits == nil {
		merged.Limits = corev1.ResourceList{}
	}
	for k, v := range defaults.Limits {
		if _, exists := merged.Limits[k]; !exists {
			merged.Limits[k] = v
		}
	}
	return *merged
}

// reconcileResources handles the creation of all Kubernetes resources for the LlamaDeployment.
// buildId is the resolved build artifact ID (empty string if no build was needed).
func (r *LlamaDeploymentReconciler) reconcileResources(ctx context.Context, llamaDeploy *llamadeployv1.LlamaDeployment, buildId string) error {
	// Apply ServiceAccount via SSA
	sa := &corev1.ServiceAccount{
		TypeMeta:                     metav1.TypeMeta{APIVersion: "v1", Kind: "ServiceAccount"},
		ObjectMeta:                   metav1.ObjectMeta{Name: llamaDeploy.Name + "-sa", Namespace: llamaDeploy.Namespace},
		AutomountServiceAccountToken: ptr(false),
	}
	if err := controllerutil.SetControllerReference(llamaDeploy, sa, r.Scheme); err != nil {
		return err
	}
	saPatchOpts := []client.PatchOption{client.FieldOwner("llama-deploy-operator")}
	if r.shouldForceOwnership(llamaDeploy) {
		saPatchOpts = append(saPatchOpts, client.ForceOwnership)
	}
	if err := r.Patch(ctx, sa, client.Apply, saPatchOpts...); err != nil {
		return err
	}

	// Verify secret exists if specified
	if llamaDeploy.Spec.SecretName != "" {
		if err := r.verifySecret(ctx, llamaDeploy); err != nil {
			return err
		}
	}

	// CreateOrUpdate ConfigMap for nginx configuration
	if err := r.reconcileNginxConfigMap(ctx, llamaDeploy); err != nil {
		return err
	}

	// CreateOrUpdate Deployment
	if err := r.reconcileDeployment(ctx, llamaDeploy, buildId); err != nil {
		return err
	}

	// Create Service
	if err := r.reconcileService(ctx, llamaDeploy); err != nil {
		return err
	}

	return nil
}

// verifySecret ensures the Secret exists but does NOT create it
// Secrets should be created by the API server or user before creating LlamaDeployment
func (r *LlamaDeploymentReconciler) verifySecret(ctx context.Context, llamaDeploy *llamadeployv1.LlamaDeployment) error {
	found := &corev1.Secret{}
	err := r.Get(ctx, client.ObjectKey{Name: llamaDeploy.Spec.SecretName, Namespace: llamaDeploy.Namespace}, found)
	if err != nil && errors.IsNotFound(err) {
		return fmt.Errorf("secret %s not found - secrets must be created separately before creating LlamaDeployment", llamaDeploy.Spec.SecretName)
	}
	return err
}

// reconcileDeployment creates or updates the Deployment for the LlamaDeployment.
// buildId is the resolved build artifact ID passed explicitly rather than read from Status.
func (r *LlamaDeploymentReconciler) reconcileDeployment(ctx context.Context, llamaDeploy *llamadeployv1.LlamaDeployment, buildId string) error {
	logger := log.FromContext(ctx)

	// Unpause the Deployment if it was paused by a previous timeout remediation,
	// but ONLY when the spec generation has advanced past the failed one.
	// We require FailedRolloutGeneration > 0 to guard against a stale informer
	// cache: a racing reconcile triggered by the Deployment-pause event may read
	// the LlamaDeployment before the timeout handler's status update (which sets
	// FailedRolloutGeneration) has landed. In that case FailedRolloutGeneration
	// is still 0 and we must not unpause, otherwise the Kubernetes Deployment
	// controller scales the failing ReplicaSet back up.
	existing := &appsv1.Deployment{}
	if err := r.Get(ctx, client.ObjectKey{Name: llamaDeploy.Name, Namespace: llamaDeploy.Namespace}, existing); err == nil {
		if existing.Spec.Paused &&
			llamaDeploy.Status.FailedRolloutGeneration > 0 &&
			llamaDeploy.Generation > llamaDeploy.Status.FailedRolloutGeneration {
			patch := client.MergeFrom(existing.DeepCopy())
			existing.Spec.Paused = false
			if patchErr := r.Patch(ctx, existing, patch); patchErr != nil {
				logger.Error(patchErr, "Failed to unpause deployment")
				return patchErr
			}
			logger.Info("Unpaused Deployment for new rollout")
		}
	}

	desired := r.createDeploymentForLlama(llamaDeploy, buildId)

	// Apply LlamaDeploymentTemplate via strategic merge, template takes precedence
	if err := r.applyTemplateOverlay(ctx, llamaDeploy, desired); err != nil {
		return err
	}

	dep := &appsv1.Deployment{
		TypeMeta: metav1.TypeMeta{APIVersion: "apps/v1", Kind: "Deployment"},
		ObjectMeta: metav1.ObjectMeta{
			Name:      llamaDeploy.Name,
			Namespace: llamaDeploy.Namespace,
		},
		Spec: desired.Spec,
	}
	if err := controllerutil.SetControllerReference(llamaDeploy, dep, r.Scheme); err != nil {
		return err
	}
	depPatchOpts := []client.PatchOption{client.FieldOwner("llama-deploy-operator")}
	if r.shouldForceOwnership(llamaDeploy) {
		depPatchOpts = append(depPatchOpts, client.ForceOwnership)
	}
	if err := r.Patch(ctx, dep, client.Apply, depPatchOpts...); err != nil {
		return err
	}
	logger.V(1).Info("Reconciled Deployment", "name", llamaDeploy.Name)
	return nil
}

// reconcileNginxConfigMap creates or updates the ConfigMap containing nginx configuration
func (r *LlamaDeploymentReconciler) reconcileNginxConfigMap(ctx context.Context, llamaDeploy *llamadeployv1.LlamaDeployment) error {
	cm := &corev1.ConfigMap{
		TypeMeta:   metav1.TypeMeta{APIVersion: "v1", Kind: "ConfigMap"},
		ObjectMeta: metav1.ObjectMeta{Name: llamaDeploy.Name + "-nginx-config", Namespace: llamaDeploy.Namespace},
		Data:       map[string]string{"nginx.conf": r.generateNginxConfig(llamaDeploy)},
	}
	if err := controllerutil.SetControllerReference(llamaDeploy, cm, r.Scheme); err != nil {
		return err
	}
	cmPatchOpts := []client.PatchOption{client.FieldOwner("llama-deploy-operator")}
	if r.shouldForceOwnership(llamaDeploy) {
		cmPatchOpts = append(cmPatchOpts, client.ForceOwnership)
	}
	return r.Patch(ctx, cm, client.Apply, cmPatchOpts...)
}

// reconcileService creates or updates the Service for the LlamaDeployment
func (r *LlamaDeploymentReconciler) reconcileService(ctx context.Context, llamaDeploy *llamadeployv1.LlamaDeployment) error {
	svc := &corev1.Service{
		TypeMeta: metav1.TypeMeta{APIVersion: "v1", Kind: "Service"},
		ObjectMeta: metav1.ObjectMeta{
			Name:      llamaDeploy.Name,
			Namespace: llamaDeploy.Namespace,
			Labels: map[string]string{
				"app":                          llamaDeploy.Name,
				"app.kubernetes.io/managed-by": "llama-deploy-operator",
				"component":                    "appserver",
			},
		},
		Spec: corev1.ServiceSpec{
			Selector: map[string]string{"app": llamaDeploy.Name},
			Type:     corev1.ServiceTypeClusterIP,
			Ports: []corev1.ServicePort{
				{
					Protocol:   corev1.ProtocolTCP,
					Name:       "http",
					Port:       80,
					TargetPort: intstr.FromInt(8081),
				},
			},
		},
	}
	if err := controllerutil.SetControllerReference(llamaDeploy, svc, r.Scheme); err != nil {
		return err
	}
	svcPatchOpts := []client.PatchOption{client.FieldOwner("llama-deploy-operator")}
	if r.shouldForceOwnership(llamaDeploy) {
		svcPatchOpts = append(svcPatchOpts, client.ForceOwnership)
	}
	return r.Patch(ctx, svc, client.Apply, svcPatchOpts...)
}

// commonEnvVars returns the environment variables shared by deployment pods and build jobs.
func (r *LlamaDeploymentReconciler) commonEnvVars(llamaDeploy *llamadeployv1.LlamaDeployment) []corev1.EnvVar {
	deploymentFilePath := llamaDeploy.Spec.DeploymentFilePath
	if deploymentFilePath == "" {
		deploymentFilePath = "."
	}

	// Build API base URL for git proxy
	buildAPIHost := os.Getenv("LLAMA_DEPLOY_BUILD_API_HOST")
	if buildAPIHost == "" {
		buildAPIHost = "llama-agents-build.llama-agents.svc.cluster.local:8001"
	}

	// Build authenticated repo URL with embedded token
	repoURL := fmt.Sprintf("http://%s/deployments/%s", buildAPIHost, llamaDeploy.Name)

	envVars := []corev1.EnvVar{
		// used in llama_deploy/docker/bootstrap.sh to determine the repo to clone via build API
		{Name: "LLAMA_DEPLOY_REPO_URL", Value: repoURL},
		// used in llama_deploy/docker/bootstrap.sh to determine the git ref to clone via build API
		{Name: "LLAMA_DEPLOY_GIT_REF", Value: llamaDeploy.Spec.GitRef},
		// used in llama_deploy/docker/bootstrap.sh to determine the git sha to clone via build API
		{Name: "LLAMA_DEPLOY_GIT_SHA", Value: llamaDeploy.Spec.GitSha},
		// used in llama_deploy/docker/bootstrap.sh to determine the config to autodeploy
		{Name: "LLAMA_DEPLOY_DEPLOYMENT_FILE_PATH", Value: deploymentFilePath},
		// used in llama_cloud_services/llama_cloud_services/beta/agent_data/client.py to infer an agent environment
		{Name: "LLAMA_DEPLOY_DEPLOYMENT_NAME", Value: llamaDeploy.Name},
		// Auth token for accessing build control plane API
		{Name: "LLAMA_DEPLOY_AUTH_TOKEN", Value: llamaDeploy.Status.AuthToken},
		// Build API service address for git proxy
		{Name: "LLAMA_DEPLOY_BUILD_API_HOST", Value: buildAPIHost},
		// the project ID
		{Name: "LLAMA_DEPLOY_PROJECT_ID", Value: llamaDeploy.Spec.ProjectId},
		// used in llama_deploy/apiserver/deployment.py to determine if the deployment is running locally or in a deployed environment
		{Name: "LLAMA_DEPLOY_IS_DEPLOYED", Value: "true"},
		// configure structured JSON logging for the appserver
		{Name: "LOG_FORMAT", Value: "json"},
		// Trust /opt/app regardless of ownership so git operations work when
		// the container uid differs from the EmptyDir volume owner.
		{Name: "GIT_CONFIG_COUNT", Value: "1"},
		{Name: "GIT_CONFIG_KEY_0", Value: "safe.directory"},
		{Name: "GIT_CONFIG_VALUE_0", Value: "/opt/app"},
	}

	// If the deployment is pinned to a specific appserver version via imageTag,
	// pass that version to the build Job / init container so it installs the
	// correct appserver instead of the one bundled in the image.
	if llamaDeploy.Spec.ImageTag != "" {
		envVars = append(envVars, corev1.EnvVar{
			Name:  "LLAMA_DEPLOY_APPSERVER_VERSION",
			Value: getContainerImageTag(llamaDeploy),
		})
	}

	return envVars
}

// commonEnvFrom returns the envFrom sources shared by deployment pods and build jobs.
func (r *LlamaDeploymentReconciler) commonEnvFrom(llamaDeploy *llamadeployv1.LlamaDeployment) []corev1.EnvFromSource {
	if llamaDeploy.Spec.SecretName != "" {
		return []corev1.EnvFromSource{
			{
				SecretRef: &corev1.SecretEnvSource{
					LocalObjectReference: corev1.LocalObjectReference{
						Name: llamaDeploy.Spec.SecretName,
					},
				},
			},
		}
	}
	return nil
}

// createDeploymentForLlama creates a Deployment object for the LlamaDeployment.
// buildId is passed explicitly rather than read from Status.BuildId.
func (r *LlamaDeploymentReconciler) createDeploymentForLlama(llamaDeploy *llamadeployv1.LlamaDeployment, buildId string) *appsv1.Deployment {
	deploymentFilePath := llamaDeploy.Spec.DeploymentFilePath
	if deploymentFilePath == "" {
		deploymentFilePath = "."
	}

	// Compute container working directory based on deployment file path
	workingDir := "/opt/app"
	if deploymentFilePath != "." {
		normalized := strings.TrimPrefix(deploymentFilePath, "./")
		normalized = strings.TrimPrefix(normalized, "/")
		resolved := normalized
		if looksLikeFilePath(normalized) {
			resolved = path.Dir(normalized)
			if resolved == "." || resolved == "/" {
				resolved = ""
			}
		}
		if resolved != "" {
			workingDir = "/opt/app/" + resolved
		}
	}

	// Build environment variables from common helper
	envVars := r.commonEnvVars(llamaDeploy)

	// If a build artifact exists, tell the init container to download it instead of building
	if buildId != "" {
		envVars = append(envVars, corev1.EnvVar{
			Name:  "LLAMA_DEPLOY_BUILD_ID",
			Value: buildId,
		})
	}

	envFrom := r.commonEnvFrom(llamaDeploy)

	// Prepare pod template annotations
	podAnnotations := map[string]string{}

	// Add git source annotation to trigger redeployment when repo or ref changes
	gitSource := llamaDeploy.Spec.RepoUrl
	if llamaDeploy.Spec.GitSha != "" || llamaDeploy.Spec.GitRef != "" {
		gitSource = fmt.Sprintf("%s@%s", llamaDeploy.Spec.RepoUrl, llamaDeploy.Spec.GitSha)
	}
	podAnnotations["deploy.llamaindex.ai/git-source"] = gitSource

	// Add secret hash annotation if present on the LlamaDeployment
	if llamaDeploy.Annotations != nil {
		if secretHash, exists := llamaDeploy.Annotations["deploy.llamaindex.ai/secret-hash"]; exists {
			podAnnotations["deploy.llamaindex.ai/secret-hash"] = secretHash
		}
	}

	replicas := int32(1)
	if llamaDeploy.Spec.Suspended ||
		(llamaDeploy.Status.FailedRolloutGeneration == llamaDeploy.Generation &&
			llamaDeploy.Status.Phase == PhaseFailed) {
		replicas = int32(0)
	}
	return &appsv1.Deployment{
		ObjectMeta: metav1.ObjectMeta{
			Name:      llamaDeploy.Name,
			Namespace: llamaDeploy.Namespace,
		},
		Spec: appsv1.DeploymentSpec{
			Replicas:                &replicas,
			ProgressDeadlineSeconds: getRolloutTimeoutSeconds(),
			RevisionHistoryLimit:    ptr(DeploymentRevisionHistoryLimit),
			Selector: &metav1.LabelSelector{
				MatchLabels: map[string]string{"app": llamaDeploy.Name},
			},
			Template: corev1.PodTemplateSpec{
				ObjectMeta: metav1.ObjectMeta{
					Labels: map[string]string{
						"app":                          llamaDeploy.Name,
						"app.kubernetes.io/managed-by": "llama-deploy-operator",
					},
					Annotations: podAnnotations,
				},
				Spec: corev1.PodSpec{
					SecurityContext:              defaultPodSecurityContext(),
					ServiceAccountName:           llamaDeploy.Name + "-sa",
					AutomountServiceAccountToken: ptr(false),
					EnableServiceLinks:           ptr(false),
					Volumes: []corev1.Volume{
						{
							Name: "app-data",
							VolumeSource: corev1.VolumeSource{
								EmptyDir: &corev1.EmptyDirVolumeSource{},
							},
						},
						{
							Name: "nginx-config",
							VolumeSource: corev1.VolumeSource{
								ConfigMap: &corev1.ConfigMapVolumeSource{
									LocalObjectReference: corev1.LocalObjectReference{
										Name: llamaDeploy.Name + "-nginx-config",
									},
								},
							},
						},
					},
					InitContainers: []corev1.Container{
						{
							Name:            "bootstrap",
							Image:           fmt.Sprintf("%s:%s", getDefaultImage(), getDefaultImageTag()),
							ImagePullPolicy: getContainerImagePullPolicy(),
							Env:             envVars,
							EnvFrom:         envFrom,
							Command:         []string{"python", "-m", "llama_deploy.appserver.bootstrap"},
							VolumeMounts: []corev1.VolumeMount{
								{Name: "app-data", MountPath: "/opt/app"},
							},
							SecurityContext: defaultContainerSecurityContext(),
						},
					},
					Containers: []corev1.Container{
						{
							Name:            "file-server",
							Image:           fmt.Sprintf("%s:%s", getNginxImage(), getNginxImageTag()),
							ImagePullPolicy: getNginxImagePullPolicy(),
							Ports:           []corev1.ContainerPort{{ContainerPort: 8081, Protocol: corev1.ProtocolTCP}},
							VolumeMounts: []corev1.VolumeMount{
								{Name: "app-data", MountPath: "/opt/app"},
								{Name: "nginx-config", MountPath: "/etc/nginx/nginx.conf", SubPath: "nginx.conf"},
							},
							Command: []string{"nginx", "-g", "daemon off;"},
							SecurityContext: &corev1.SecurityContext{
								RunAsNonRoot:             ptr(true),
								RunAsUser:                ptr(NginxUID),
								RunAsGroup:               ptr(NginxGID),
								AllowPrivilegeEscalation: ptr(false),
								Capabilities:             &corev1.Capabilities{Drop: []corev1.Capability{"ALL"}},
							},
						},
						{
							Name:            ContainerNameApp,
							Image:           fmt.Sprintf("%s:%s", getContainerImage(llamaDeploy), getContainerImageTag(llamaDeploy)),
							ImagePullPolicy: getContainerImagePullPolicy(),
							WorkingDir:      workingDir,
							Command:         []string{"uv", "run", "--no-sync", "python", "-m", "llama_deploy.appserver.app"},
							Env:             envVars,
							EnvFrom:         envFrom,
							VolumeMounts:    []corev1.VolumeMount{{Name: "app-data", MountPath: "/opt/app"}},
							Ports:           []corev1.ContainerPort{{ContainerPort: 8080, Protocol: corev1.ProtocolTCP}},
							StartupProbe:    &corev1.Probe{ProbeHandler: corev1.ProbeHandler{HTTPGet: &corev1.HTTPGetAction{Path: "/health", Port: intstr.FromInt(8080)}}, PeriodSeconds: 5, FailureThreshold: 24},
							LivenessProbe:   &corev1.Probe{ProbeHandler: corev1.ProbeHandler{HTTPGet: &corev1.HTTPGetAction{Path: "/health", Port: intstr.FromInt(8080)}}, PeriodSeconds: 5, FailureThreshold: 12},
							Resources:       corev1.ResourceRequirements{Requests: getDefaultResourceRequests(), Limits: getDefaultResourceLimits()},
							SecurityContext: hardenedSecurityContext(),
						},
					},
				},
			},
		},
	}
}

// generateNginxConfig builds the nginx.conf content for the deployment
func (r *LlamaDeploymentReconciler) generateNginxConfig(llamaDeploy *llamadeployv1.LlamaDeployment) string {
	basePath := fmt.Sprintf("/deployments/%s/ui", llamaDeploy.GetObjectMeta().GetName())
	assetsPath := llamaDeploy.Spec.StaticAssetsPath
	var staticLocation string
	if assetsPath != "" {
		// Serve static files from /opt/app/<assetsPath>, fallback to python for misses
		staticLocation = fmt.Sprintf("location %s { alias /opt/app/%s/; try_files $uri $uri/ /index.html @python_upstream; }", basePath, assetsPath)
	} else {
		// If not provided, proxy UI base to python
		staticLocation = fmt.Sprintf("location %s { proxy_pass http://127.0.0.1:8080; proxy_http_version 1.1; proxy_set_header Upgrade $http_upgrade; proxy_set_header Connection 'upgrade'; proxy_set_header Host $host; proxy_set_header X-Forwarded-Proto $scheme; proxy_set_header X-Forwarded-For $proxy_add_x_forwarded_for; }", basePath)
	}

	// Everything else proxies to python on 8081; also define named upstream for try_files fallback
	proxyLocation := "location / { proxy_pass http://127.0.0.1:8080; proxy_http_version 1.1; proxy_set_header Upgrade $http_upgrade; proxy_set_header Connection 'upgrade'; proxy_set_header Host $host; proxy_set_header X-Forwarded-Proto $scheme; proxy_set_header X-Forwarded-For $proxy_add_x_forwarded_for; }\nlocation @python_upstream { proxy_pass http://127.0.0.1:8081; proxy_http_version 1.1; proxy_set_header Upgrade $http_upgrade; proxy_set_header Connection 'upgrade'; proxy_set_header Host $host; proxy_set_header X-Forwarded-Proto $scheme; proxy_set_header X-Forwarded-For $proxy_add_x_forwarded_for; }"

	return fmt.Sprintf(`pid /tmp/nginx.pid;
worker_processes  1;
events { worker_connections  1024; }
http {
  client_body_temp_path /tmp/client_temp;
  proxy_temp_path       /tmp/proxy_temp;
  fastcgi_temp_path     /tmp/fastcgi_temp;
  uwsgi_temp_path       /tmp/uwsgi_temp;
  scgi_temp_path        /tmp/scgi_temp;
  include       mime.types;
  default_type  application/octet-stream;
  sendfile        on;
  keepalive_timeout  65;
  access_log    off;
  server {
    # Ensure redirects are relative (do not include scheme/host)
    absolute_redirect off;
    listen 8081;
    %s
    %s
  }
}`, staticLocation, proxyLocation)
}

// shouldForceOwnership returns true when the controller should force SSA field ownership
// during migration windows, i.e., when the controller's static schema version is
// greater than the resource's current status schema version.
func (r *LlamaDeploymentReconciler) shouldForceOwnership(ld *llamadeployv1.LlamaDeployment) bool {
	// Force when CR spec changed since last successful reconcile
	if ld.Status.LastReconciledGeneration != ld.Generation {
		return true
	}
	// Explicit override via annotation
	if ld.Annotations != nil {
		if ld.Annotations["deploy.llamaindex.ai/force-ownership"] == "true" {
			return true
		}
	}
	// Treat empty status schema version as -1 (always migrate up)
	parse := func(s string) (int, bool) {
		if s == "" {
			return -1, true
		}
		n := 0
		for i := 0; i < len(s); i++ {
			c := s[i]
			if c < '0' || c > '9' {
				return 0, false
			}
			n = n*10 + int(c-'0')
		}
		return n, true
	}

	cur, okCur := parse(CurrentSchemaVersion)
	prev, okPrev := parse(ld.Status.SchemaVersion)
	if okCur && okPrev {
		return cur > prev
	}
	// Fallback to lexicographic comparison if parsing fails
	return CurrentSchemaVersion > ld.Status.SchemaVersion
}

// applyTemplateOverlay merges a referenced LlamaDeploymentTemplate into the desired Deployment's PodTemplate.
// The template's values take precedence over operator defaults.
func (r *LlamaDeploymentReconciler) applyTemplateOverlay(ctx context.Context, ld *llamadeployv1.LlamaDeployment, dep *appsv1.Deployment) error {
	// Determine template name: spec.templateName or "default"
	templateName := ld.Spec.TemplateName
	if templateName == "" {
		templateName = "default"
	}

	// Try to fetch template in the same namespace
	tmpl := &llamadeployv1.LlamaDeploymentTemplate{}
	if err := r.Get(ctx, client.ObjectKey{Name: templateName, Namespace: ld.Namespace}, tmpl); err != nil {
		if errors.IsNotFound(err) {
			return nil // no template; nothing to merge
		}
		return err
	}

	// Strategic merge allows specifying containers by name to override specific fields.
	// We don't need to validate this here - the merge will fail naturally if something is wrong.

	// Use Kubernetes strategic merge patch semantics to merge overlay into the desired PodTemplateSpec.
	// This respects list merge keys (e.g., containers by name) and map merges (labels/annotations).
	base := dep.Spec.Template
	patch := tmpl.Spec.PodSpec

	baseJSON, err := json.Marshal(base)
	if err != nil {
		return fmt.Errorf("marshal base template: %w", err)
	}
	patchJSON, err := json.Marshal(patch)
	if err != nil {
		return fmt.Errorf("marshal overlay template: %w", err)
	}

	// Clean up the patch to remove empty/nil fields that would override base values
	var patchMap map[string]interface{}
	if err := json.Unmarshal(patchJSON, &patchMap); err != nil {
		return fmt.Errorf("unmarshal patch for cleanup: %w", err)
	}

	// Remove metadata.creationTimestamp which can appear in serialized objects
	if metadata, ok := patchMap["metadata"].(map[string]interface{}); ok {
		delete(metadata, "creationTimestamp")
		if len(metadata) == 0 {
			delete(patchMap, "metadata")
		}
	}

	// Clean empty arrays and nil values from spec to avoid clearing base values
	if spec, ok := patchMap["spec"].(map[string]interface{}); ok {
		// Remove nil or empty container-related arrays
		for _, field := range []string{"containers", "initContainers", "volumes", "imagePullSecrets", "hostAliases"} {
			if val := spec[field]; val == nil {
				delete(spec, field)
			} else if arr, ok := val.([]interface{}); ok && len(arr) == 0 {
				delete(spec, field)
			}
		}
		// Remove nil or empty maps
		for _, field := range []string{"nodeSelector", "securityContext", "affinity"} {
			if val := spec[field]; val == nil {
				delete(spec, field)
			} else if m, ok := val.(map[string]interface{}); ok && len(m) == 0 {
				delete(spec, field)
			}
		}

		// Strip "build" containers from the runtime overlay. The "build" container
		// is only meaningful for build jobs (handled by applyBuildJobTemplateOverlay).
		// Strategic merge would add it as a new container to the runtime Deployment
		// with no image or command, producing an invalid pod spec.
		if containers, ok := spec["containers"].([]interface{}); ok {
			filtered := make([]interface{}, 0, len(containers))
			for _, c := range containers {
				if cm, ok := c.(map[string]interface{}); ok {
					if cm["name"] == ContainerNameBuild {
						continue
					}
				}
				filtered = append(filtered, c)
			}
			if len(filtered) == 0 {
				delete(spec, "containers")
			} else {
				spec["containers"] = filtered
			}
		}
	}

	patchJSON, err = json.Marshal(patchMap)
	if err != nil {
		return fmt.Errorf("remarshal cleaned patch: %w", err)
	}

	mergedJSON, err := strategicpatch.StrategicMergePatch(baseJSON, patchJSON, corev1.PodTemplateSpec{})
	if err != nil {
		return fmt.Errorf("strategic merge template: %w", err)
	}
	var merged corev1.PodTemplateSpec
	if err := json.Unmarshal(mergedJSON, &merged); err != nil {
		return fmt.Errorf("unmarshal merged template: %w", err)
	}
	// Do not expose the namespace's service directory to user workloads.
	merged.Spec.EnableServiceLinks = ptr(false)

	dep.Spec.Template = merged
	return nil
}
