//go:build !integration

package controller

import (
	"strings"
	"testing"
	"time"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	llamadeployv1 "llama-agents-operator/api/v1"
)

const (
	containerNameApp        = "app"
	containerNameFileServer = "file-server"
)

// Test that nginx config string generation maps inputs correctly without envtest
func TestGenerateNginxConfig_StaticAndProxy(t *testing.T) {
	ld := &llamadeployv1.LlamaDeployment{
		ObjectMeta: metav1.ObjectMeta{Name: "demo"},
		Spec:       llamadeployv1.LlamaDeploymentSpec{StaticAssetsPath: "frontend/dist"},
	}

	conf := (&LlamaDeploymentReconciler{}).generateNginxConfig(ld, "")
	if !strings.Contains(conf, "location /deployments/demo/ui {") {
		t.Fatalf("expected UI base location, got: %s", conf)
	}
	if !strings.Contains(conf, "alias /opt/app/frontend/dist/") {
		t.Fatalf("expected alias for static assets, got: %s", conf)
	}
	if !strings.Contains(conf, "location @python_upstream { proxy_pass http://127.0.0.1:8081;") {
		t.Fatalf("expected named upstream for fallback, got: %s", conf)
	}
	if !strings.Contains(conf, "access_log    off;") {
		t.Fatalf("expected access_log off, got: %s", conf)
	}
}

func TestCreateDeploymentForLlama_ShapesPodSpec(t *testing.T) {
	reconciler := &LlamaDeploymentReconciler{}
	ld := &llamadeployv1.LlamaDeployment{
		ObjectMeta: metav1.ObjectMeta{Name: "demo", Namespace: "default"},
		Spec:       llamadeployv1.LlamaDeploymentSpec{ProjectId: "p", RepoUrl: "r", GitRef: "main"},
		Status:     llamadeployv1.LlamaDeploymentStatus{AuthToken: "tok"},
	}

	dep := reconciler.createDeploymentForLlama(ld, "")
	if dep.Spec.Template.Spec.ServiceAccountName != "demo-sa" {
		t.Fatalf("expected SA name demo-sa, got %s", dep.Spec.Template.Spec.ServiceAccountName)
	}
	if len(dep.Spec.Template.Spec.Containers) != 2 {
		t.Fatalf("expected 2 containers, got %d", len(dep.Spec.Template.Spec.Containers))
	}
	var appC, nginxC corev1.Container
	for _, c := range dep.Spec.Template.Spec.Containers {
		if c.Name == containerNameApp {
			appC = c
		}
		if c.Name == containerNameFileServer {
			nginxC = c
		}
	}
	if appC.Name != containerNameApp || nginxC.Name != containerNameFileServer {
		t.Fatalf("expected app and file-server containers")
	}
	if appC.Ports[0].ContainerPort != 8080 || nginxC.Ports[0].ContainerPort != 8081 {
		t.Fatalf("expected app:8080 and file-server:8081")
	}
}

func TestGetRolloutTimeout_Default(t *testing.T) {
	t.Setenv(EnvRolloutTimeoutSeconds, "")
	got := getRolloutTimeout()
	expected := time.Duration(DefaultRolloutTimeoutSeconds) * time.Second
	if got != expected {
		t.Fatalf("expected %v for default, got %v", expected, got)
	}
}

func TestGetRolloutTimeout_Custom(t *testing.T) {
	t.Setenv(EnvRolloutTimeoutSeconds, "120")
	got := getRolloutTimeout()
	if got != 120*time.Second {
		t.Fatalf("expected 120s, got %v", got)
	}
}

func TestGetRolloutTimeoutSeconds_Default(t *testing.T) {
	t.Setenv(EnvRolloutTimeoutSeconds, "")
	got := getRolloutTimeoutSeconds()
	if got == nil || *got != DefaultRolloutTimeoutSeconds {
		t.Fatalf("expected %d, got %v", DefaultRolloutTimeoutSeconds, got)
	}
}

func TestGetRolloutTimeoutSeconds_Custom(t *testing.T) {
	t.Setenv(EnvRolloutTimeoutSeconds, "600")
	got := getRolloutTimeoutSeconds()
	if got == nil || *got != 600 {
		t.Fatalf("expected 600, got %v", got)
	}
}

func TestCreateDeploymentForLlama_ProgressDeadlineSeconds_Default(t *testing.T) {
	reconciler := &LlamaDeploymentReconciler{}
	ld := &llamadeployv1.LlamaDeployment{
		ObjectMeta: metav1.ObjectMeta{Name: "demo", Namespace: "default"},
		Spec:       llamadeployv1.LlamaDeploymentSpec{ProjectId: "p", RepoUrl: "r", GitRef: "main"},
		Status:     llamadeployv1.LlamaDeploymentStatus{AuthToken: "tok"},
	}
	dep := reconciler.createDeploymentForLlama(ld, "")
	if dep.Spec.ProgressDeadlineSeconds == nil {
		t.Fatalf("expected progressDeadlineSeconds to be set")
	}
	if *dep.Spec.ProgressDeadlineSeconds != DefaultRolloutTimeoutSeconds {
		t.Fatalf("expected %d, got %d", DefaultRolloutTimeoutSeconds, *dep.Spec.ProgressDeadlineSeconds)
	}
}

func TestCreateDeploymentForLlama_ProgressDeadlineSeconds_Custom(t *testing.T) {
	t.Setenv(EnvRolloutTimeoutSeconds, "120")
	reconciler := &LlamaDeploymentReconciler{}
	ld := &llamadeployv1.LlamaDeployment{
		ObjectMeta: metav1.ObjectMeta{Name: "demo", Namespace: "default"},
		Spec: llamadeployv1.LlamaDeploymentSpec{
			ProjectId: "p",
			RepoUrl:   "r",
			GitRef:    "main",
		},
		Status: llamadeployv1.LlamaDeploymentStatus{AuthToken: "tok"},
	}
	dep := reconciler.createDeploymentForLlama(ld, "")
	if dep.Spec.ProgressDeadlineSeconds == nil || *dep.Spec.ProgressDeadlineSeconds != 120 {
		t.Fatalf("expected 120, got %v", dep.Spec.ProgressDeadlineSeconds)
	}
}

func TestCreateDeploymentForLlama_BuildIdEnvVar(t *testing.T) {
	r := &LlamaDeploymentReconciler{}
	ld := &llamadeployv1.LlamaDeployment{
		ObjectMeta: metav1.ObjectMeta{Name: "demo", Namespace: "default"},
		Spec:       llamadeployv1.LlamaDeploymentSpec{ProjectId: "p", RepoUrl: "r", GitRef: "main"},
		Status:     llamadeployv1.LlamaDeploymentStatus{AuthToken: "tok"},
	}

	dep := r.createDeploymentForLlama(ld, "build-abc123")

	// Check init container env vars for LLAMA_DEPLOY_BUILD_ID
	found := false
	for _, env := range dep.Spec.Template.Spec.InitContainers[0].Env {
		if env.Name == envBuildID {
			found = true
			if env.Value != "build-abc123" {
				t.Errorf("expected LLAMA_DEPLOY_BUILD_ID=build-abc123, got %q", env.Value)
			}
		}
	}
	if !found {
		t.Error("expected LLAMA_DEPLOY_BUILD_ID env var to be present when buildId is non-empty")
	}

	// Also check app container
	found = false
	for _, c := range dep.Spec.Template.Spec.Containers {
		if c.Name == containerNameApp {
			for _, env := range c.Env {
				if env.Name == envBuildID {
					found = true
				}
			}
		}
	}
	if !found {
		t.Error("expected LLAMA_DEPLOY_BUILD_ID env var on app container when buildId is non-empty")
	}
}

func TestCreateDeploymentForLlama_NoBuildIdEnvVar(t *testing.T) {
	r := &LlamaDeploymentReconciler{}
	ld := &llamadeployv1.LlamaDeployment{
		ObjectMeta: metav1.ObjectMeta{Name: "demo", Namespace: "default"},
		Spec:       llamadeployv1.LlamaDeploymentSpec{ProjectId: "p", RepoUrl: "r", GitRef: "main"},
		Status:     llamadeployv1.LlamaDeploymentStatus{AuthToken: "tok"},
	}

	dep := r.createDeploymentForLlama(ld, "")

	// Check init container: LLAMA_DEPLOY_BUILD_ID should NOT be present
	for _, env := range dep.Spec.Template.Spec.InitContainers[0].Env {
		if env.Name == envBuildID {
			t.Error("expected LLAMA_DEPLOY_BUILD_ID env var to NOT be present when buildId is empty")
		}
	}

	// Check app container
	for _, c := range dep.Spec.Template.Spec.Containers {
		if c.Name == containerNameApp {
			for _, env := range c.Env {
				if env.Name == envBuildID {
					t.Error("expected LLAMA_DEPLOY_BUILD_ID env var to NOT be present on app container when buildId is empty")
				}
			}
		}
	}
}

func TestCreateDeploymentForLlama_ReplicasZeroForFailedPhase(t *testing.T) {
	r := &LlamaDeploymentReconciler{}
	ld := &llamadeployv1.LlamaDeployment{
		ObjectMeta: metav1.ObjectMeta{Name: "demo", Namespace: "default", Generation: 5},
		Spec:       llamadeployv1.LlamaDeploymentSpec{ProjectId: "p", RepoUrl: "r", GitRef: "main"},
		Status: llamadeployv1.LlamaDeploymentStatus{
			AuthToken:               "tok",
			Phase:                   PhaseFailed,
			FailedRolloutGeneration: 5, // matches Generation
		},
	}

	dep := r.createDeploymentForLlama(ld, "")
	if dep.Spec.Replicas == nil || *dep.Spec.Replicas != 0 {
		t.Errorf("expected replicas=0 for failed phase with matching generation, got %v", dep.Spec.Replicas)
	}
}

func TestCreateDeploymentForLlama_ReplicasZeroForSuspended(t *testing.T) {
	r := &LlamaDeploymentReconciler{}
	ld := &llamadeployv1.LlamaDeployment{
		ObjectMeta: metav1.ObjectMeta{Name: "demo", Namespace: "default"},
		Spec:       llamadeployv1.LlamaDeploymentSpec{ProjectId: "p", RepoUrl: "r", GitRef: "main", Suspended: true},
		Status:     llamadeployv1.LlamaDeploymentStatus{AuthToken: "tok"},
	}

	dep := r.createDeploymentForLlama(ld, "")
	if dep.Spec.Replicas == nil || *dep.Spec.Replicas != 0 {
		t.Errorf("expected replicas=0 for suspended deployment, got %v", dep.Spec.Replicas)
	}
}

// ---------------------------------------------------------------------------
// looksLikeFilePath tests
// ---------------------------------------------------------------------------

func TestLooksLikeFilePath(t *testing.T) {
	tests := []struct {
		input string
		want  bool
	}{
		{"deployment.yml", true},
		{"path/to/file.yaml", true},
		{"main.py", true},
		{"src/app.js", true},
		{"mydir", false},
		{"path/to/dir", false},
		{".", false},
		{"/", false},
		{".hidden", true},
	}
	for _, tt := range tests {
		t.Run(tt.input, func(t *testing.T) {
			if got := looksLikeFilePath(tt.input); got != tt.want {
				t.Errorf("looksLikeFilePath(%q) = %v, want %v", tt.input, got, tt.want)
			}
		})
	}
}

// ---------------------------------------------------------------------------
// getContainerImage tests
// ---------------------------------------------------------------------------

func TestGetContainerImage(t *testing.T) {
	t.Run("spec override wins", func(t *testing.T) {
		t.Setenv(EnvImageName, "env-image")
		ld := &llamadeployv1.LlamaDeployment{
			ObjectMeta: metav1.ObjectMeta{Name: "demo"},
			Spec:       llamadeployv1.LlamaDeploymentSpec{Image: "spec-image"},
		}
		if got := getContainerImage(ld); got != "spec-image" {
			t.Errorf("expected spec-image, got %q", got)
		}
	})
	t.Run("env fallback", func(t *testing.T) {
		t.Setenv(EnvImageName, "env-image")
		ld := &llamadeployv1.LlamaDeployment{
			ObjectMeta: metav1.ObjectMeta{Name: "demo"},
		}
		if got := getContainerImage(ld); got != "env-image" {
			t.Errorf("expected env-image, got %q", got)
		}
	})
	t.Run("default", func(t *testing.T) {
		t.Setenv(EnvImageName, "")
		ld := &llamadeployv1.LlamaDeployment{
			ObjectMeta: metav1.ObjectMeta{Name: "demo"},
		}
		if got := getContainerImage(ld); got != DefaultImage {
			t.Errorf("expected %q, got %q", DefaultImage, got)
		}
	})
}

// ---------------------------------------------------------------------------
// getContainerImageTag tests
// ---------------------------------------------------------------------------

func TestGetContainerImageTag(t *testing.T) {
	t.Run("spec override wins", func(t *testing.T) {
		t.Setenv(EnvImageTag, "env-tag")
		ld := &llamadeployv1.LlamaDeployment{
			ObjectMeta: metav1.ObjectMeta{Name: "demo"},
			Spec:       llamadeployv1.LlamaDeploymentSpec{ImageTag: "spec-tag"},
		}
		if got := getContainerImageTag(ld); got != "spec-tag" {
			t.Errorf("expected spec-tag, got %q", got)
		}
	})
	t.Run("env fallback", func(t *testing.T) {
		t.Setenv(EnvImageTag, "env-tag")
		ld := &llamadeployv1.LlamaDeployment{
			ObjectMeta: metav1.ObjectMeta{Name: "demo"},
		}
		if got := getContainerImageTag(ld); got != "env-tag" {
			t.Errorf("expected env-tag, got %q", got)
		}
	})
	t.Run("default", func(t *testing.T) {
		t.Setenv(EnvImageTag, "")
		ld := &llamadeployv1.LlamaDeployment{
			ObjectMeta: metav1.ObjectMeta{Name: "demo"},
		}
		if got := getContainerImageTag(ld); got != DefaultImageTag {
			t.Errorf("expected %q, got %q", DefaultImageTag, got)
		}
	})
}

// ---------------------------------------------------------------------------
// shouldForceOwnership tests
// ---------------------------------------------------------------------------

func TestShouldForceOwnership(t *testing.T) {
	r := &LlamaDeploymentReconciler{}

	t.Run("generation mismatch forces ownership", func(t *testing.T) {
		ld := &llamadeployv1.LlamaDeployment{
			ObjectMeta: metav1.ObjectMeta{Name: "demo", Generation: 2},
			Status: llamadeployv1.LlamaDeploymentStatus{
				LastReconciledGeneration: 1,
				SchemaVersion:            CurrentSchemaVersion,
			},
		}
		if !r.shouldForceOwnership(ld) {
			t.Error("expected true when generation mismatches")
		}
	})

	t.Run("schema version mismatch forces ownership", func(t *testing.T) {
		ld := &llamadeployv1.LlamaDeployment{
			ObjectMeta: metav1.ObjectMeta{Name: "demo", Generation: 1},
			Status: llamadeployv1.LlamaDeploymentStatus{
				LastReconciledGeneration: 1,
				SchemaVersion:            "0",
			},
		}
		if !r.shouldForceOwnership(ld) {
			t.Error("expected true when schema version is old")
		}
	})

	t.Run("annotation override forces ownership", func(t *testing.T) {
		ld := &llamadeployv1.LlamaDeployment{
			ObjectMeta: metav1.ObjectMeta{
				Name:        "demo",
				Generation:  1,
				Annotations: map[string]string{"deploy.llamaindex.ai/force-ownership": "true"},
			},
			Status: llamadeployv1.LlamaDeploymentStatus{
				LastReconciledGeneration: 1,
				SchemaVersion:            CurrentSchemaVersion,
			},
		}
		if !r.shouldForceOwnership(ld) {
			t.Error("expected true with force-ownership annotation")
		}
	})

	t.Run("all matching does not force ownership", func(t *testing.T) {
		ld := &llamadeployv1.LlamaDeployment{
			ObjectMeta: metav1.ObjectMeta{Name: "demo", Generation: 1},
			Status: llamadeployv1.LlamaDeploymentStatus{
				LastReconciledGeneration: 1,
				SchemaVersion:            CurrentSchemaVersion,
			},
		}
		if r.shouldForceOwnership(ld) {
			t.Error("expected false when everything matches")
		}
	})

	t.Run("empty schema version forces ownership", func(t *testing.T) {
		ld := &llamadeployv1.LlamaDeployment{
			ObjectMeta: metav1.ObjectMeta{Name: "demo", Generation: 1},
			Status: llamadeployv1.LlamaDeploymentStatus{
				LastReconciledGeneration: 1,
				SchemaVersion:            "",
			},
		}
		if !r.shouldForceOwnership(ld) {
			t.Error("expected true when schema version is empty")
		}
	})
}

// ---------------------------------------------------------------------------
// getContainerImagePullPolicy tests
// ---------------------------------------------------------------------------

func TestGetContainerImagePullPolicy(t *testing.T) {
	tests := []struct {
		name   string
		envVal string
		want   corev1.PullPolicy
	}{
		{"default", "", corev1.PullIfNotPresent},
		{"always", "Always", corev1.PullAlways},
		{"never", "Never", corev1.PullNever},
		{"if not present", "IfNotPresent", corev1.PullIfNotPresent},
		{"invalid falls back to default", "BadValue", corev1.PullIfNotPresent},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Setenv(EnvImagePullPolicy, tt.envVal)
			if got := getContainerImagePullPolicy(); got != tt.want {
				t.Errorf("getContainerImagePullPolicy() = %q, want %q", got, tt.want)
			}
		})
	}
}

// ---------------------------------------------------------------------------
// getDefaultImage / getDefaultImageTag tests
// ---------------------------------------------------------------------------

func TestGetDefaultImage(t *testing.T) {
	t.Run("env override", func(t *testing.T) {
		t.Setenv(EnvImageName, "custom-image")
		if got := getDefaultImage(); got != "custom-image" {
			t.Errorf("expected custom-image, got %q", got)
		}
	})
	t.Run("default", func(t *testing.T) {
		t.Setenv(EnvImageName, "")
		if got := getDefaultImage(); got != DefaultImage {
			t.Errorf("expected %q, got %q", DefaultImage, got)
		}
	})
}

func TestGetDefaultImageTag(t *testing.T) {
	t.Run("env override", func(t *testing.T) {
		t.Setenv(EnvImageTag, "custom-tag")
		if got := getDefaultImageTag(); got != "custom-tag" {
			t.Errorf("expected custom-tag, got %q", got)
		}
	})
	t.Run("default", func(t *testing.T) {
		t.Setenv(EnvImageTag, "")
		if got := getDefaultImageTag(); got != DefaultImageTag {
			t.Errorf("expected %q, got %q", DefaultImageTag, got)
		}
	})
}

// ---------------------------------------------------------------------------
// getNginxImage / getNginxImageTag tests
// ---------------------------------------------------------------------------

func TestGetNginxImage(t *testing.T) {
	t.Run("env override", func(t *testing.T) {
		t.Setenv(EnvNginxImageName, "custom-nginx")
		if got := getNginxImage(); got != "custom-nginx" {
			t.Errorf("expected custom-nginx, got %q", got)
		}
	})
	t.Run("default", func(t *testing.T) {
		t.Setenv(EnvNginxImageName, "")
		if got := getNginxImage(); got != DefaultNginxImage {
			t.Errorf("expected %q, got %q", DefaultNginxImage, got)
		}
	})
}

func TestGetNginxImageTag(t *testing.T) {
	t.Run("env override", func(t *testing.T) {
		t.Setenv(EnvNginxImageTag, "custom-tag")
		if got := getNginxImageTag(); got != "custom-tag" {
			t.Errorf("expected custom-tag, got %q", got)
		}
	})
	t.Run("default", func(t *testing.T) {
		t.Setenv(EnvNginxImageTag, "")
		if got := getNginxImageTag(); got != DefaultNginxImageTag {
			t.Errorf("expected %q, got %q", DefaultNginxImageTag, got)
		}
	})
}

// ---------------------------------------------------------------------------
// Working directory logic tests
// ---------------------------------------------------------------------------

func TestCreateDeploymentForLlama_WorkingDir(t *testing.T) {
	tests := []struct {
		name     string
		filePath string
		wantDir  string
	}{
		{"default path", "", "/opt/app"},
		{"dot path", ".", "/opt/app"},
		{"file in subdir", "subdir/deploy.yml", "/opt/app/subdir"},
		{"directory path", "subdir", "/opt/app/subdir"},
		{"nested file", "a/b/c/deploy.yml", "/opt/app/a/b/c"},
		{"root file", "deploy.yml", "/opt/app"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			r := &LlamaDeploymentReconciler{}
			ld := &llamadeployv1.LlamaDeployment{
				ObjectMeta: metav1.ObjectMeta{Name: "demo", Namespace: "default"},
				Spec: llamadeployv1.LlamaDeploymentSpec{
					ProjectId:          "p",
					RepoUrl:            "r",
					DeploymentFilePath: tt.filePath,
				},
				Status: llamadeployv1.LlamaDeploymentStatus{AuthToken: "tok"},
			}
			dep := r.createDeploymentForLlama(ld, "")
			var appC corev1.Container
			for _, c := range dep.Spec.Template.Spec.Containers {
				if c.Name == containerNameApp {
					appC = c
				}
			}
			if appC.WorkingDir != tt.wantDir {
				t.Errorf("expected WorkingDir %q, got %q", tt.wantDir, appC.WorkingDir)
			}
		})
	}
}

// ---------------------------------------------------------------------------
// commonEnvVars tests
// ---------------------------------------------------------------------------

func TestCommonEnvVars_AppserverVersion(t *testing.T) {
	const envAppserverVersion = "LLAMA_DEPLOY_APPSERVER_VERSION"
	r := &LlamaDeploymentReconciler{}

	t.Run("appserver tag sets version env", func(t *testing.T) {
		ld := &llamadeployv1.LlamaDeployment{
			ObjectMeta: metav1.ObjectMeta{Name: "demo"},
			Spec:       llamadeployv1.LlamaDeploymentSpec{ImageTag: "appserver-0.4.15"},
		}
		envs := r.commonEnvVars(ld)
		found := false
		for _, e := range envs {
			if e.Name == envAppserverVersion {
				found = true
				if e.Value != "0.4.15" {
					t.Errorf("expected 0.4.15, got %q", e.Value)
				}
			}
		}
		if !found {
			t.Error("expected LLAMA_DEPLOY_APPSERVER_VERSION env var")
		}
	})

	t.Run("plain version tag sets version env", func(t *testing.T) {
		ld := &llamadeployv1.LlamaDeployment{
			ObjectMeta: metav1.ObjectMeta{Name: "demo"},
			Spec:       llamadeployv1.LlamaDeploymentSpec{ImageTag: "0.8.1"},
		}
		envs := r.commonEnvVars(ld)
		found := false
		for _, e := range envs {
			if e.Name == envAppserverVersion {
				found = true
				if e.Value != "0.8.1" {
					t.Errorf("expected 0.8.1, got %q", e.Value)
				}
			}
		}
		if !found {
			t.Error("expected LLAMA_DEPLOY_APPSERVER_VERSION env var for plain version tag")
		}
	})

	t.Run("empty tag does not set version env", func(t *testing.T) {
		ld := &llamadeployv1.LlamaDeployment{
			ObjectMeta: metav1.ObjectMeta{Name: "demo"},
			Spec:       llamadeployv1.LlamaDeploymentSpec{ImageTag: ""},
		}
		envs := r.commonEnvVars(ld)
		for _, e := range envs {
			if e.Name == envAppserverVersion {
				t.Error("did not expect LLAMA_DEPLOY_APPSERVER_VERSION for empty tag")
			}
		}
	})
}

// ---------------------------------------------------------------------------
// commonEnvFrom tests
// ---------------------------------------------------------------------------

func TestCommonEnvFrom(t *testing.T) {
	r := &LlamaDeploymentReconciler{}

	t.Run("with secret", func(t *testing.T) {
		ld := &llamadeployv1.LlamaDeployment{
			ObjectMeta: metav1.ObjectMeta{Name: "demo"},
			Spec:       llamadeployv1.LlamaDeploymentSpec{SecretName: "my-secret"},
		}
		envFrom := r.commonEnvFrom(ld)
		if len(envFrom) != 1 {
			t.Fatalf("expected 1 envFrom, got %d", len(envFrom))
		}
		if envFrom[0].SecretRef.Name != "my-secret" {
			t.Errorf("expected secret name my-secret, got %q", envFrom[0].SecretRef.Name)
		}
	})

	t.Run("without secret", func(t *testing.T) {
		ld := &llamadeployv1.LlamaDeployment{
			ObjectMeta: metav1.ObjectMeta{Name: "demo"},
		}
		envFrom := r.commonEnvFrom(ld)
		if envFrom != nil {
			t.Errorf("expected nil envFrom, got %v", envFrom)
		}
	})
}

func TestIsValidDNS1035Label(t *testing.T) {
	tests := []struct {
		name  string
		input string
		want  bool
	}{
		{"valid simple", "my-service", true},
		{"valid single char", "a", true},
		{"valid alphanumeric end", "abc-123", true},
		{"valid long", strings.Repeat("a", 63), true},
		{"starts with digit", "10101010", false},
		{"starts with digit then letters", "123-service", false},
		{"starts with dash", "-service", false},
		{"ends with dash", "service-", false},
		{"uppercase", "MyService", false},
		{"empty", "", false},
		{"too long", strings.Repeat("a", 64), false},
		{"contains underscore", "my_service", false},
		{"contains dot", "my.service", false},
		{"single digit", "1", false},
		{"only dashes", "---", false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := isValidDNS1035Label(tt.input)
			if got != tt.want {
				t.Errorf("isValidDNS1035Label(%q) = %v, want %v", tt.input, got, tt.want)
			}
		})
	}
}
