//go:build !integration

// SPDX-License-Identifier: MIT
// Copyright (c) 2026 LlamaIndex Inc.

package controller

import (
	"context"
	"strings"
	"testing"

	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	llamadeployv1 "llama-agents-operator/api/v1"
)

func newAssetsDeployment() *llamadeployv1.LlamaDeployment {
	return &llamadeployv1.LlamaDeployment{
		ObjectMeta: metav1.ObjectMeta{Name: "my-app", Namespace: "default"},
		Spec: llamadeployv1.LlamaDeploymentSpec{
			ProjectId: "proj-123",
			RepoUrl:   "https://github.com/example/repo",
			GitSha:    "abc123",
		},
	}
}

// buildPod is a finished pod for buildId whose build container wrote message
// to its termination log.
func buildPod(name, buildId, message string, exitCode int32) *corev1.Pod {
	return &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:      name,
			Namespace: "default",
			Labels: map[string]string{
				"deploy.llamaindex.ai/deployment": "my-app",
				"deploy.llamaindex.ai/build-id":   buildId,
			},
		},
		Status: corev1.PodStatus{ContainerStatuses: []corev1.ContainerStatus{{
			Name: ContainerNameBuild,
			State: corev1.ContainerState{Terminated: &corev1.ContainerStateTerminated{
				ExitCode: exitCode,
				Message:  message,
			}},
		}}},
	}
}

// reconcileSucceededBuild runs reconcileBuild for ld after its current build
// Job succeeded, with pods in the cluster.
func reconcileSucceededBuild(t *testing.T, ld *llamadeployv1.LlamaDeployment, pods ...client.Object) (*LlamaDeploymentReconciler, client.Client) {
	t.Helper()
	buildId := computeBuildId(ld)
	job := &batchv1.Job{
		ObjectMeta: metav1.ObjectMeta{Name: buildJobName(ld.Name, buildId), Namespace: "default"},
		Status:     batchv1.JobStatus{Succeeded: 1},
	}
	scheme := newTestScheme()
	c := fake.NewClientBuilder().
		WithScheme(scheme).
		WithObjects(append(pods, ld, job)...).
		WithStatusSubresource(ld).
		Build()
	r := &LlamaDeploymentReconciler{Client: c, DirectClient: c, Scheme: scheme}
	got, result, err := r.reconcileBuild(context.Background(), ld)
	if err != nil || result != nil || got != buildId {
		t.Fatalf("reconcileBuild = (%q, %v, %v), want (%q, nil, nil)", got, result, err, buildId)
	}
	return r, c
}

func TestReconcileBuild_RecordsStaticAssetsPathFromBuildPod(t *testing.T) {
	cases := []struct {
		name    string
		message string
		want    *string
	}{
		{"built UI", `"ui/dist"`, ptr("ui/dist")},
		{"no built UI", `""`, ptr("")},
		{"JSON null is unknown", `null`, nil},
		{"no message is unknown", ``, nil},
		{"non-string is unknown", `{"path":"ui/dist"}`, nil},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			ld := newAssetsDeployment()
			buildId := computeBuildId(ld)
			reconcileSucceededBuild(t, ld,
				buildPod("current", buildId, tc.message, 0),
				// A failed attempt of this build and another build's pod must be ignored.
				buildPod("current-failed", buildId, `"wrong/failed"`, 1),
				buildPod("other", "other-build", `"wrong/other"`, 0),
			)
			got := ld.Status.StaticAssetsPath
			if (got == nil) != (tc.want == nil) || (got != nil && *got != *tc.want) {
				t.Fatalf("status.staticAssetsPath = %v, want %v", got, tc.want)
			}
		})
	}
}

func TestReconcileBuild_KeepsStaticAssetsPathAfterBuildPodsAreGone(t *testing.T) {
	ld := newAssetsDeployment()
	buildId := computeBuildId(ld)
	r, c := reconcileSucceededBuild(t, ld, buildPod("current", buildId, `"ui/dist"`, 0))

	// The Job TTL reaps the Job and its pods. The next reconcile takes the
	// succeeded fast path and must keep the recorded path.
	ctx := context.Background()
	if err := c.DeleteAllOf(ctx, &corev1.Pod{}, client.InNamespace("default")); err != nil {
		t.Fatal(err)
	}
	if err := c.DeleteAllOf(ctx, &batchv1.Job{}, client.InNamespace("default")); err != nil {
		t.Fatal(err)
	}
	stored := &llamadeployv1.LlamaDeployment{}
	if err := c.Get(ctx, client.ObjectKeyFromObject(ld), stored); err != nil {
		t.Fatal(err)
	}
	if got, _, err := r.reconcileBuild(ctx, stored); err != nil || got != buildId {
		t.Fatalf("reconcileBuild = (%q, %v), want %q", got, err, buildId)
	}
	if !strings.Contains(r.generateNginxConfig(stored, buildId), "alias /opt/app/ui/dist/") {
		t.Fatal("expected nginx to serve the recorded path")
	}
}

func TestGenerateNginxConfig_UsesStaticAssetsPathOfSelectedBuild(t *testing.T) {
	const alias = "alias /opt/app/"
	cases := []struct {
		name        string
		spec        string
		statusBuild string
		status      *string
		buildId     string
		wantAlias   string
	}{
		{"build path for selected build", "", "b1", ptr("ui/dist"), "b1", "ui/dist"},
		{"build path overrides legacy spec path", "old/dist", "b1", ptr("ui/dist"), "b1", "ui/dist"},
		{"build with no UI clears legacy spec path", "old/dist", "b1", ptr(""), "b1", ""},
		{"unknown build path uses legacy spec path", "old/dist", "b1", nil, "b1", "old/dist"},
		{"other build's path is not used", "old/dist", "b0", ptr("ui/dist"), "b1", "old/dist"},
		{"other build's path is not used without spec", "", "b0", ptr("ui/dist"), "b1", ""},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			ld := &llamadeployv1.LlamaDeployment{
				ObjectMeta: metav1.ObjectMeta{Name: "demo"},
				Spec:       llamadeployv1.LlamaDeploymentSpec{StaticAssetsPath: tc.spec},
				Status:     llamadeployv1.LlamaDeploymentStatus{BuildId: tc.statusBuild, StaticAssetsPath: tc.status},
			}
			conf := (&LlamaDeploymentReconciler{}).generateNginxConfig(ld, tc.buildId)
			if tc.wantAlias == "" {
				if strings.Contains(conf, alias) {
					t.Fatalf("expected UI to proxy to python, got: %s", conf)
				}
				return
			}
			if !strings.Contains(conf, alias+tc.wantAlias+"/;") {
				t.Fatalf("expected alias for %q, got: %s", tc.wantAlias, conf)
			}
		})
	}
}
