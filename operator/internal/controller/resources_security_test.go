//go:build !integration

package controller

import (
	"context"
	"strings"
	"testing"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	llamadeployv1 "llama-agents-operator/api/v1"
)

// ---------------------------------------------------------------------------
// findContainerSecurityContext tests
// ---------------------------------------------------------------------------

func TestFindContainerSecurityContext(t *testing.T) {
	sc := &corev1.SecurityContext{
		RunAsNonRoot:             ptr(true),
		AllowPrivilegeEscalation: ptr(false),
	}
	containers := []corev1.Container{
		{Name: "app", SecurityContext: sc},
		{Name: "sidecar"},
	}

	t.Run("found with security context", func(t *testing.T) {
		got := findContainerSecurityContext(containers, "app")
		if got == nil {
			t.Fatal("expected non-nil security context for app container")
		}
		if got != sc {
			t.Error("expected same SecurityContext pointer")
		}
	})

	t.Run("found without security context", func(t *testing.T) {
		got := findContainerSecurityContext(containers, "sidecar")
		if got != nil {
			t.Error("expected nil for container with no security context")
		}
	})

	t.Run("not found", func(t *testing.T) {
		got := findContainerSecurityContext(containers, "nonexistent")
		if got != nil {
			t.Error("expected nil for nonexistent container")
		}
	})
}

// ---------------------------------------------------------------------------
// Build job security context tests
// ---------------------------------------------------------------------------

func TestCreateBuildJob_SecurityContext(t *testing.T) {
	r := &LlamaDeploymentReconciler{}
	ld := &llamadeployv1.LlamaDeployment{
		ObjectMeta: metav1.ObjectMeta{Name: "my-app", Namespace: "default"},
		Spec: llamadeployv1.LlamaDeploymentSpec{
			ProjectId: "proj-123",
			RepoUrl:   "https://github.com/example/repo",
			GitRef:    "main",
		},
	}
	job := r.createBuildJob(ld, "abc123")

	podSC := job.Spec.Template.Spec.SecurityContext
	if podSC == nil {
		t.Fatal("expected pod security context")
	}
	if podSC.FSGroup == nil || *podSC.FSGroup != AppServerGID {
		t.Errorf("expected FSGroup %d, got %v", AppServerGID, podSC.FSGroup)
	}

	c := job.Spec.Template.Spec.Containers[0]
	assertFullSecurityContext(t, c.SecurityContext, AppServerUID, AppServerGID)
}

func TestCreateBuildJob_DisablesServiceLinks(t *testing.T) {
	r := &LlamaDeploymentReconciler{}
	ld := &llamadeployv1.LlamaDeployment{
		ObjectMeta: metav1.ObjectMeta{Name: "my-app", Namespace: "default"},
	}

	job := r.createBuildJob(ld, "abc123")

	if job.Spec.Template.Spec.EnableServiceLinks == nil || *job.Spec.Template.Spec.EnableServiceLinks {
		t.Error("expected build job service links to be disabled")
	}
}

// ---------------------------------------------------------------------------
// Deployment security context tests
// ---------------------------------------------------------------------------

func TestCreateDeployment_SecurityContexts(t *testing.T) {
	r := &LlamaDeploymentReconciler{}
	ld := &llamadeployv1.LlamaDeployment{
		ObjectMeta: metav1.ObjectMeta{Name: "my-app", Namespace: "default"},
		Spec: llamadeployv1.LlamaDeploymentSpec{
			ProjectId: "proj-123",
			RepoUrl:   "https://github.com/example/repo",
			GitRef:    "main",
		},
	}
	dep := r.createDeploymentForLlama(ld, "build123")

	podSC := dep.Spec.Template.Spec.SecurityContext
	if podSC == nil {
		t.Fatal("expected pod security context")
	}
	if podSC.FSGroup == nil || *podSC.FSGroup != AppServerGID {
		t.Errorf("expected FSGroup %d, got %v", AppServerGID, podSC.FSGroup)
	}

	t.Run("bootstrap init container", func(t *testing.T) {
		initContainers := dep.Spec.Template.Spec.InitContainers
		if len(initContainers) == 0 {
			t.Fatal("expected at least one init container")
		}
		assertFullSecurityContext(t, initContainers[0].SecurityContext, AppServerUID, AppServerGID)
	})

	t.Run("file-server container", func(t *testing.T) {
		c := findContainer(dep.Spec.Template.Spec.Containers, "file-server")
		if c == nil {
			t.Fatal("expected file-server container")
		}
		assertFullSecurityContext(t, c.SecurityContext, NginxUID, NginxGID)
	})

	t.Run("app container", func(t *testing.T) {
		c := findContainer(dep.Spec.Template.Spec.Containers, ContainerNameApp)
		if c == nil {
			t.Fatal("expected app container")
		}
		sc := c.SecurityContext
		if sc == nil {
			t.Fatal("expected security context on app container")
		}
		if sc.AllowPrivilegeEscalation == nil || *sc.AllowPrivilegeEscalation != false {
			t.Error("expected AllowPrivilegeEscalation=false")
		}
		if sc.Capabilities == nil || len(sc.Capabilities.Drop) == 0 || sc.Capabilities.Drop[0] != "ALL" {
			t.Error("expected capabilities drop ALL")
		}
		// App container intentionally omits RunAsUser/RunAsNonRoot for backward compat
		if sc.RunAsUser != nil {
			t.Error("expected RunAsUser to be nil on app container (backward compat)")
		}
		if sc.RunAsNonRoot != nil {
			t.Error("expected RunAsNonRoot to be nil on app container (backward compat)")
		}
	})
}

func TestCreateDeployment_DisablesServiceLinks(t *testing.T) {
	r := &LlamaDeploymentReconciler{}
	ld := &llamadeployv1.LlamaDeployment{
		ObjectMeta: metav1.ObjectMeta{Name: "my-app", Namespace: "default"},
	}

	dep := r.createDeploymentForLlama(ld, "")

	if dep.Spec.Template.Spec.EnableServiceLinks == nil || *dep.Spec.Template.Spec.EnableServiceLinks {
		t.Error("expected deployment service links to be disabled")
	}
}

// ---------------------------------------------------------------------------
// GIT_CONFIG env vars
// ---------------------------------------------------------------------------

func TestCreateBuildJob_HasGitSafeDirectoryEnvVars(t *testing.T) {
	r := &LlamaDeploymentReconciler{}
	ld := &llamadeployv1.LlamaDeployment{
		ObjectMeta: metav1.ObjectMeta{Name: "my-app", Namespace: "default"},
		Spec: llamadeployv1.LlamaDeploymentSpec{
			ProjectId: "proj-123",
			RepoUrl:   "https://github.com/example/repo",
			GitRef:    "main",
		},
	}
	job := r.createBuildJob(ld, "abc123")

	envs := job.Spec.Template.Spec.Containers[0].Env
	assertEnvVar(t, envs, "GIT_CONFIG_COUNT", "1")
	assertEnvVar(t, envs, "GIT_CONFIG_KEY_0", "safe.directory")
	assertEnvVar(t, envs, "GIT_CONFIG_VALUE_0", "/opt/app")
}

// ---------------------------------------------------------------------------
// Overlay security context propagation
// ---------------------------------------------------------------------------

func TestApplyBuildJobTemplateOverlay_PropagatesSecurityContext(t *testing.T) {
	scheme := newTestScheme()

	overrideSC := &corev1.SecurityContext{
		RunAsNonRoot:             ptr(true),
		RunAsUser:                ptr(int64(2000)),
		RunAsGroup:               ptr(int64(2000)),
		AllowPrivilegeEscalation: ptr(false),
	}

	t.Run("from build container", func(t *testing.T) {
		tmpl := &llamadeployv1.LlamaDeploymentTemplate{
			ObjectMeta: metav1.ObjectMeta{Name: "sc-build", Namespace: "default"},
			Spec: llamadeployv1.LlamaDeploymentTemplateSpec{
				PodSpec: corev1.PodTemplateSpec{
					Spec: corev1.PodSpec{
						Containers: []corev1.Container{
							{Name: ContainerNameBuild, SecurityContext: overrideSC},
						},
					},
				},
			},
		}

		fakeClient := fake.NewClientBuilder().WithScheme(scheme).WithObjects(tmpl).Build()
		r := &LlamaDeploymentReconciler{Client: fakeClient, Scheme: scheme}
		ld := &llamadeployv1.LlamaDeployment{
			ObjectMeta: metav1.ObjectMeta{Name: "my-app", Namespace: "default"},
			Spec:       llamadeployv1.LlamaDeploymentSpec{TemplateName: "sc-build"},
		}

		job := r.createBuildJob(ld, "abc123")
		if err := r.applyBuildJobTemplateOverlay(context.Background(), ld, job); err != nil {
			t.Fatalf("unexpected error: %v", err)
		}

		sc := job.Spec.Template.Spec.Containers[0].SecurityContext
		if sc == nil || sc.RunAsUser == nil || *sc.RunAsUser != 2000 {
			t.Error("expected build container SecurityContext to be replaced by template overlay (RunAsUser=2000)")
		}
	})

	t.Run("falls back from app container", func(t *testing.T) {
		tmpl := &llamadeployv1.LlamaDeploymentTemplate{
			ObjectMeta: metav1.ObjectMeta{Name: "sc-app", Namespace: "default"},
			Spec: llamadeployv1.LlamaDeploymentTemplateSpec{
				PodSpec: corev1.PodTemplateSpec{
					Spec: corev1.PodSpec{
						Containers: []corev1.Container{
							{Name: ContainerNameApp, SecurityContext: overrideSC},
						},
					},
				},
			},
		}

		fakeClient := fake.NewClientBuilder().WithScheme(scheme).WithObjects(tmpl).Build()
		r := &LlamaDeploymentReconciler{Client: fakeClient, Scheme: scheme}
		ld := &llamadeployv1.LlamaDeployment{
			ObjectMeta: metav1.ObjectMeta{Name: "my-app", Namespace: "default"},
			Spec:       llamadeployv1.LlamaDeploymentSpec{TemplateName: "sc-app"},
		}

		job := r.createBuildJob(ld, "abc123")
		if err := r.applyBuildJobTemplateOverlay(context.Background(), ld, job); err != nil {
			t.Fatalf("unexpected error: %v", err)
		}

		sc := job.Spec.Template.Spec.Containers[0].SecurityContext
		if sc == nil || sc.RunAsUser == nil || *sc.RunAsUser != 2000 {
			t.Error("expected build container SecurityContext to fall back to app container overlay (RunAsUser=2000)")
		}
	})

	t.Run("pod-level security context override", func(t *testing.T) {
		podSC := &corev1.PodSecurityContext{
			RunAsNonRoot: ptr(true),
			FSGroup:      ptr(int64(3000)),
		}
		tmpl := &llamadeployv1.LlamaDeploymentTemplate{
			ObjectMeta: metav1.ObjectMeta{Name: "sc-pod", Namespace: "default"},
			Spec: llamadeployv1.LlamaDeploymentTemplateSpec{
				PodSpec: corev1.PodTemplateSpec{
					Spec: corev1.PodSpec{
						SecurityContext: podSC,
					},
				},
			},
		}

		fakeClient := fake.NewClientBuilder().WithScheme(scheme).WithObjects(tmpl).Build()
		r := &LlamaDeploymentReconciler{Client: fakeClient, Scheme: scheme}
		ld := &llamadeployv1.LlamaDeployment{
			ObjectMeta: metav1.ObjectMeta{Name: "my-app", Namespace: "default"},
			Spec:       llamadeployv1.LlamaDeploymentSpec{TemplateName: "sc-pod"},
		}

		job := r.createBuildJob(ld, "abc123")
		if err := r.applyBuildJobTemplateOverlay(context.Background(), ld, job); err != nil {
			t.Fatalf("unexpected error: %v", err)
		}

		got := job.Spec.Template.Spec.SecurityContext
		if got == nil || got.FSGroup == nil || *got.FSGroup != 3000 {
			t.Error("expected pod SecurityContext FSGroup to be overridden to 3000")
		}
	})
}

func TestApplyBuildJobTemplateOverlay_CannotEnableServiceLinks(t *testing.T) {
	scheme := newTestScheme()
	tmpl := &llamadeployv1.LlamaDeploymentTemplate{
		ObjectMeta: metav1.ObjectMeta{Name: "default", Namespace: "default"},
		Spec: llamadeployv1.LlamaDeploymentTemplateSpec{
			PodSpec: corev1.PodTemplateSpec{
				Spec: corev1.PodSpec{EnableServiceLinks: ptr(true)},
			},
		},
	}
	fakeClient := fake.NewClientBuilder().WithScheme(scheme).WithObjects(tmpl).Build()
	r := &LlamaDeploymentReconciler{Client: fakeClient, Scheme: scheme}
	ld := &llamadeployv1.LlamaDeployment{
		ObjectMeta: metav1.ObjectMeta{Name: "my-app", Namespace: "default"},
	}
	job := r.createBuildJob(ld, "abc123")

	if err := r.applyBuildJobTemplateOverlay(context.Background(), ld, job); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if job.Spec.Template.Spec.EnableServiceLinks == nil || *job.Spec.Template.Spec.EnableServiceLinks {
		t.Error("expected build job template overlay not to enable service links")
	}
}

func TestApplyTemplateOverlay_CannotEnableServiceLinks(t *testing.T) {
	scheme := newTestScheme()
	tmpl := &llamadeployv1.LlamaDeploymentTemplate{
		ObjectMeta: metav1.ObjectMeta{Name: "default", Namespace: "default"},
		Spec: llamadeployv1.LlamaDeploymentTemplateSpec{
			PodSpec: corev1.PodTemplateSpec{
				Spec: corev1.PodSpec{EnableServiceLinks: ptr(true)},
			},
		},
	}
	fakeClient := fake.NewClientBuilder().WithScheme(scheme).WithObjects(tmpl).Build()
	r := &LlamaDeploymentReconciler{Client: fakeClient, Scheme: scheme}
	ld := &llamadeployv1.LlamaDeployment{
		ObjectMeta: metav1.ObjectMeta{Name: "my-app", Namespace: "default"},
	}
	dep := r.createDeploymentForLlama(ld, "")

	if err := r.applyTemplateOverlay(context.Background(), ld, dep); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if dep.Spec.Template.Spec.EnableServiceLinks == nil || *dep.Spec.Template.Spec.EnableServiceLinks {
		t.Error("expected deployment template overlay not to enable service links")
	}
}

// ---------------------------------------------------------------------------
// Nginx config tests
// ---------------------------------------------------------------------------

func TestGenerateNginxConfig_NonRootDirectives(t *testing.T) {
	r := &LlamaDeploymentReconciler{}
	ld := &llamadeployv1.LlamaDeployment{
		ObjectMeta: metav1.ObjectMeta{Name: "my-app", Namespace: "default"},
		Spec:       llamadeployv1.LlamaDeploymentSpec{},
	}
	config := r.generateNginxConfig(ld)

	required := []string{
		"pid /tmp/nginx.pid",
		"client_body_temp_path /tmp/client_temp",
		"proxy_temp_path       /tmp/proxy_temp",
		"fastcgi_temp_path     /tmp/fastcgi_temp",
		"uwsgi_temp_path       /tmp/uwsgi_temp",
		"scgi_temp_path        /tmp/scgi_temp",
	}

	for _, directive := range required {
		if !strings.Contains(config, directive) {
			t.Errorf("expected nginx config to contain %q", directive)
		}
	}

	if strings.Contains(config, "user root") {
		t.Error("nginx config should not contain 'user root' directive")
	}
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

func assertFullSecurityContext(t *testing.T, sc *corev1.SecurityContext, uid, gid int64) {
	t.Helper()
	if sc == nil {
		t.Fatal("expected non-nil security context")
	}
	if sc.RunAsNonRoot == nil || *sc.RunAsNonRoot != true {
		t.Error("expected RunAsNonRoot=true")
	}
	if sc.RunAsUser == nil || *sc.RunAsUser != uid {
		t.Errorf("expected RunAsUser=%d, got %v", uid, sc.RunAsUser)
	}
	if sc.RunAsGroup == nil || *sc.RunAsGroup != gid {
		t.Errorf("expected RunAsGroup=%d, got %v", gid, sc.RunAsGroup)
	}
	if sc.AllowPrivilegeEscalation == nil || *sc.AllowPrivilegeEscalation != false {
		t.Error("expected AllowPrivilegeEscalation=false")
	}
	if sc.Capabilities == nil || len(sc.Capabilities.Drop) == 0 || sc.Capabilities.Drop[0] != "ALL" {
		t.Error("expected capabilities drop ALL")
	}
}

func assertEnvVar(t *testing.T, envs []corev1.EnvVar, name, value string) {
	t.Helper()
	for _, e := range envs {
		if e.Name == name {
			if e.Value != value {
				t.Errorf("expected %s=%q, got %q", name, value, e.Value)
			}
			return
		}
	}
	t.Errorf("expected env var %s not found", name)
}

func findContainer(containers []corev1.Container, name string) *corev1.Container {
	for i := range containers {
		if containers[i].Name == name {
			return &containers[i]
		}
	}
	return nil
}
