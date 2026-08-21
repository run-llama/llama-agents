//go:build integration

package controller

import (
	"context"
	"fmt"
	"os"
	"strings"
	"time"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	appsv1 "k8s.io/api/apps/v1"
	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	networkingv1 "k8s.io/api/networking/v1"
	"k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/tools/record"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"

	llamadeployv1 "llama-agents-operator/api/v1"
)

const LLAMA_DEPLOY_REPO_URL = "LLAMA_DEPLOY_REPO_URL"
const APP = "app"

var _ = Describe("LlamaDeployment Controller", func() {
	Context("When reconciling a resource", func() {
		const (
			testNamespace = "default"
			testProjectID = "test-project"
			testRepoURL   = "https://github.com/test/repo.git"
			testGitRef    = "abc123"
			testGitSha    = "1234567"
			testGitSha2   = "7654321"
		)

		var (
			ctx                  context.Context
			controllerReconciler *LlamaDeploymentReconciler
		)

		BeforeEach(func() {
			ctx = context.Background()
			controllerReconciler = &LlamaDeploymentReconciler{
				Client:       k8sClient,
				Scheme:       k8sClient.Scheme(),
				Recorder:     record.NewFakeRecorder(100), // Buffered fake recorder for tests
				DirectClient: k8sClient,
			}
		})
		Describe("LlamaDeploymentTemplate overlays", func() {
			It("should not modify pod spec when template does not exist", func() {
				testName := "test-template-none"
				ld := &llamadeployv1.LlamaDeployment{
					ObjectMeta: metav1.ObjectMeta{Name: testName, Namespace: testNamespace},
					Spec:       llamadeployv1.LlamaDeploymentSpec{ProjectId: testProjectID, RepoUrl: testRepoURL, GitRef: testGitRef},
				}
				Expect(k8sClient.Create(ctx, ld)).To(Succeed())
				defer CleanupLlama(ctx, testName, testNamespace)

				_, err := controllerReconciler.Reconcile(ctx, reconcile.Request{NamespacedName: types.NamespacedName{Name: testName, Namespace: testNamespace}})
				Expect(err).NotTo(HaveOccurred())
				CompleteBuild(ctx, controllerReconciler, ld)

				dep := GetDeploymentEventually(ctx, testName, testNamespace)
				Expect(dep.Spec.Template.Spec.Tolerations).To(BeEmpty())
				Expect(dep.Spec.Template.Spec.NodeSelector).To(BeEmpty())
			})

			It("should apply default template overlay when present before deployment", func() {
				testName := "test-template-default-before"

				// Create default template
				tmpl := &llamadeployv1.LlamaDeploymentTemplate{
					ObjectMeta: metav1.ObjectMeta{Name: "default", Namespace: testNamespace},
					Spec: llamadeployv1.LlamaDeploymentTemplateSpec{
						PodSpec: corev1.PodTemplateSpec{
							Spec: corev1.PodSpec{
								NodeSelector: map[string]string{"disktype": "ssd"},
								Tolerations:  []corev1.Toleration{{Key: "dedicated", Operator: corev1.TolerationOpExists, Effect: corev1.TaintEffectNoSchedule}},
							},
						},
					},
				}
				Expect(k8sClient.Create(ctx, tmpl)).To(Succeed())
				defer func() {
					_ = k8sClient.Delete(ctx, tmpl)
				}()

				// Create deployment
				ld := &llamadeployv1.LlamaDeployment{
					ObjectMeta: metav1.ObjectMeta{Name: testName, Namespace: testNamespace},
					Spec:       llamadeployv1.LlamaDeploymentSpec{ProjectId: testProjectID, RepoUrl: testRepoURL, GitRef: testGitRef},
				}
				Expect(k8sClient.Create(ctx, ld)).To(Succeed())
				defer CleanupLlama(ctx, testName, testNamespace)

				_, err := controllerReconciler.Reconcile(ctx, reconcile.Request{NamespacedName: types.NamespacedName{Name: testName, Namespace: testNamespace}})
				Expect(err).NotTo(HaveOccurred())
				CompleteBuild(ctx, controllerReconciler, ld)

				dep := GetDeploymentEventually(ctx, testName, testNamespace)
				Expect(dep.Spec.Template.Spec.NodeSelector).To(HaveKeyWithValue("disktype", "ssd"))
				Expect(dep.Spec.Template.Spec.Tolerations).To(ContainElement(
					SatisfyAll(
						WithTransform(func(t corev1.Toleration) string { return t.Key }, Equal("dedicated")),
						WithTransform(func(t corev1.Toleration) corev1.TaintEffect { return t.Effect }, Equal(corev1.TaintEffectNoSchedule)),
					)))
			})

			It("should apply template overlay when templateName is specified", func() {
				testName := "test-template-named"

				// Create a named template
				tmpl := &llamadeployv1.LlamaDeploymentTemplate{
					ObjectMeta: metav1.ObjectMeta{Name: "high-priority", Namespace: testNamespace},
					Spec: llamadeployv1.LlamaDeploymentTemplateSpec{
						PodSpec: corev1.PodTemplateSpec{
							Spec: corev1.PodSpec{PriorityClassName: "system-cluster-critical"},
						},
					},
				}
				Expect(k8sClient.Create(ctx, tmpl)).To(Succeed())
				defer func() {
					_ = k8sClient.Delete(ctx, tmpl)
				}()

				ld := &llamadeployv1.LlamaDeployment{
					ObjectMeta: metav1.ObjectMeta{Name: testName, Namespace: testNamespace},
					Spec:       llamadeployv1.LlamaDeploymentSpec{ProjectId: testProjectID, RepoUrl: testRepoURL, GitRef: testGitRef, TemplateName: "high-priority"},
				}
				Expect(k8sClient.Create(ctx, ld)).To(Succeed())
				defer CleanupLlama(ctx, testName, testNamespace)

				_, err := controllerReconciler.Reconcile(ctx, reconcile.Request{NamespacedName: types.NamespacedName{Name: testName, Namespace: testNamespace}})
				Expect(err).NotTo(HaveOccurred())
				CompleteBuild(ctx, controllerReconciler, ld)

				dep := GetDeploymentEventually(ctx, testName, testNamespace)
				Expect(dep.Spec.Template.Spec.PriorityClassName).To(Equal("system-cluster-critical"))
			})

			It("should apply overlay after deployment exists when template is added", func() {
				testName := "test-template-added-after"

				// Create deployment first
				ld := &llamadeployv1.LlamaDeployment{
					ObjectMeta: metav1.ObjectMeta{Name: testName, Namespace: testNamespace},
					Spec:       llamadeployv1.LlamaDeploymentSpec{ProjectId: testProjectID, RepoUrl: testRepoURL, GitRef: testGitRef},
				}
				Expect(k8sClient.Create(ctx, ld)).To(Succeed())
				defer CleanupLlama(ctx, testName, testNamespace)

				// First reconcile
				_, err := controllerReconciler.Reconcile(ctx, reconcile.Request{NamespacedName: types.NamespacedName{Name: testName, Namespace: testNamespace}})
				Expect(err).NotTo(HaveOccurred())
				CompleteBuild(ctx, controllerReconciler, ld)

				// Create default template
				tmpl := &llamadeployv1.LlamaDeploymentTemplate{
					ObjectMeta: metav1.ObjectMeta{Name: "default", Namespace: testNamespace},
					Spec: llamadeployv1.LlamaDeploymentTemplateSpec{
						PodSpec: corev1.PodTemplateSpec{Spec: corev1.PodSpec{Tolerations: []corev1.Toleration{{Key: "gpu", Operator: corev1.TolerationOpExists, Effect: corev1.TaintEffectNoSchedule}}}},
					},
				}
				Expect(k8sClient.Create(ctx, tmpl)).To(Succeed())
				defer func() {
					_ = k8sClient.Delete(ctx, tmpl)
				}()

				// Reconcile again to pick up template
				_, err = controllerReconciler.Reconcile(ctx, reconcile.Request{NamespacedName: types.NamespacedName{Name: testName, Namespace: testNamespace}})
				Expect(err).NotTo(HaveOccurred())

				dep := GetDeploymentEventually(ctx, testName, testNamespace)
				Expect(dep.Spec.Template.Spec.Tolerations).To(ContainElement(
					SatisfyAll(
						WithTransform(func(t corev1.Toleration) string { return t.Key }, Equal("gpu")),
						WithTransform(func(t corev1.Toleration) corev1.TaintEffect { return t.Effect }, Equal(corev1.TaintEffectNoSchedule)),
					)))
			})

			It("should override container resources when template specifies them", func() {
				testName := "test-template-container-resources"

				// Create template with container resource overrides
				tmpl := &llamadeployv1.LlamaDeploymentTemplate{
					ObjectMeta: metav1.ObjectMeta{Name: "resource-limits", Namespace: testNamespace},
					Spec: llamadeployv1.LlamaDeploymentTemplateSpec{
						PodSpec: corev1.PodTemplateSpec{
							Spec: corev1.PodSpec{
								Containers: []corev1.Container{
									{
										Name: "app",
										Resources: corev1.ResourceRequirements{
											Requests: corev1.ResourceList{
												corev1.ResourceCPU:    resource.MustParse("4000m"),
												corev1.ResourceMemory: resource.MustParse("8192Mi"),
											},
											Limits: corev1.ResourceList{
												corev1.ResourceMemory: resource.MustParse("8192Mi"),
											},
										},
									},
								},
							},
						},
					},
				}
				Expect(k8sClient.Create(ctx, tmpl)).To(Succeed())
				defer func() {
					_ = k8sClient.Delete(ctx, tmpl)
				}()

				// Create deployment with template reference
				ld := &llamadeployv1.LlamaDeployment{
					ObjectMeta: metav1.ObjectMeta{Name: testName, Namespace: testNamespace},
					Spec:       llamadeployv1.LlamaDeploymentSpec{ProjectId: testProjectID, RepoUrl: testRepoURL, GitRef: testGitRef, TemplateName: "resource-limits"},
				}
				Expect(k8sClient.Create(ctx, ld)).To(Succeed())
				defer CleanupLlama(ctx, testName, testNamespace)

				_, err := controllerReconciler.Reconcile(ctx, reconcile.Request{NamespacedName: types.NamespacedName{Name: testName, Namespace: testNamespace}})
				Expect(err).NotTo(HaveOccurred())
				CompleteBuild(ctx, controllerReconciler, ld)

				dep := GetDeploymentEventually(ctx, testName, testNamespace)

				// Find the app container
				var appContainer *corev1.Container
				for i := range dep.Spec.Template.Spec.Containers {
					if dep.Spec.Template.Spec.Containers[i].Name == "app" {
						appContainer = &dep.Spec.Template.Spec.Containers[i]
						break
					}
				}
				Expect(appContainer).NotTo(BeNil(), "app container should exist")

				// Verify resource overrides were applied
				Expect(appContainer.Resources.Requests.Cpu().String()).To(Equal("4"))
				// Memory may be normalized to different units (8192Mi == 8Gi)
				expectedMemory := resource.MustParse("8192Mi")
				Expect(appContainer.Resources.Requests.Memory().Cmp(expectedMemory)).To(Equal(0), "Request memory should equal 8192Mi")
				Expect(appContainer.Resources.Limits.Memory().Cmp(expectedMemory)).To(Equal(0), "Limit memory should equal 8192Mi")

				// Verify other containers (build, file-server) are unchanged
				Expect(dep.Spec.Template.Spec.InitContainers).NotTo(BeEmpty(), "build init container should exist")
				Expect(dep.Spec.Template.Spec.Containers).To(HaveLen(2), "should have app and file-server containers")
			})

			It("should apply app container resources to build job when no build container in template", func() {
				testName := "test-template-build-fallback"

				tmpl := &llamadeployv1.LlamaDeploymentTemplate{
					ObjectMeta: metav1.ObjectMeta{Name: "build-fallback", Namespace: testNamespace},
					Spec: llamadeployv1.LlamaDeploymentTemplateSpec{
						PodSpec: corev1.PodTemplateSpec{
							Spec: corev1.PodSpec{
								Containers: []corev1.Container{
									{
										Name: "app",
										Resources: corev1.ResourceRequirements{
											Requests: corev1.ResourceList{
												corev1.ResourceCPU:    resource.MustParse("2000m"),
												corev1.ResourceMemory: resource.MustParse("4Gi"),
											},
											Limits: corev1.ResourceList{
												corev1.ResourceMemory: resource.MustParse("8Gi"),
											},
										},
									},
								},
							},
						},
					},
				}
				Expect(k8sClient.Create(ctx, tmpl)).To(Succeed())
				defer func() {
					_ = k8sClient.Delete(ctx, tmpl)
				}()

				ld := &llamadeployv1.LlamaDeployment{
					ObjectMeta: metav1.ObjectMeta{Name: testName, Namespace: testNamespace},
					Spec:       llamadeployv1.LlamaDeploymentSpec{ProjectId: testProjectID, RepoUrl: testRepoURL, GitRef: testGitRef, GitSha: testGitSha, TemplateName: "build-fallback"},
				}
				Expect(k8sClient.Create(ctx, ld)).To(Succeed())
				defer CleanupLlama(ctx, testName, testNamespace)

				// First reconcile creates the build job
				_, err := controllerReconciler.Reconcile(ctx, reconcile.Request{NamespacedName: types.NamespacedName{Name: testName, Namespace: testNamespace}})
				Expect(err).NotTo(HaveOccurred())

				// Find the build job and verify its resources match the app container from template
				Expect(k8sClient.Get(ctx, types.NamespacedName{Name: testName, Namespace: testNamespace}, ld)).To(Succeed())
				buildId := computeBuildId(ld)
				jobName := fmt.Sprintf("%s-build-%s", testName, buildId)
				if len(jobName) > 63 {
					jobName = jobName[:63]
				}
				job := &batchv1.Job{}
				Expect(k8sClient.Get(ctx, client.ObjectKey{Name: jobName, Namespace: testNamespace}, job)).To(Succeed())

				buildContainer, found := FindContainer(job.Spec.Template.Spec, "build")
				Expect(found).To(BeTrue(), "build container should exist in job")
				Expect(buildContainer.Resources.Requests.Cpu().String()).To(Equal("2"), "build CPU request should match app container from template")
				Expect(buildContainer.Resources.Requests.Memory().Cmp(resource.MustParse("4Gi"))).To(Equal(0), "build memory request should match app container from template")
				Expect(buildContainer.Resources.Limits.Memory().Cmp(resource.MustParse("8Gi"))).To(Equal(0), "build memory limit should match app container from template")
			})

			It("should not add build container to runtime deployment when template has one", func() {
				testName := "test-template-build-stripped"

				tmpl := &llamadeployv1.LlamaDeploymentTemplate{
					ObjectMeta: metav1.ObjectMeta{Name: "with-build", Namespace: testNamespace},
					Spec: llamadeployv1.LlamaDeploymentTemplateSpec{
						PodSpec: corev1.PodTemplateSpec{
							Spec: corev1.PodSpec{
								Containers: []corev1.Container{
									{
										Name: "build",
										Resources: corev1.ResourceRequirements{
											Requests: corev1.ResourceList{
												corev1.ResourceCPU:    resource.MustParse("1000m"),
												corev1.ResourceMemory: resource.MustParse("3Gi"),
											},
										},
									},
								},
							},
						},
					},
				}
				Expect(k8sClient.Create(ctx, tmpl)).To(Succeed())
				defer func() {
					_ = k8sClient.Delete(ctx, tmpl)
				}()

				ld := &llamadeployv1.LlamaDeployment{
					ObjectMeta: metav1.ObjectMeta{Name: testName, Namespace: testNamespace},
					Spec:       llamadeployv1.LlamaDeploymentSpec{ProjectId: testProjectID, RepoUrl: testRepoURL, GitRef: testGitRef, GitSha: testGitSha, TemplateName: "with-build"},
				}
				Expect(k8sClient.Create(ctx, ld)).To(Succeed())
				defer CleanupLlama(ctx, testName, testNamespace)

				_, err := controllerReconciler.Reconcile(ctx, reconcile.Request{NamespacedName: types.NamespacedName{Name: testName, Namespace: testNamespace}})
				Expect(err).NotTo(HaveOccurred())
				CompleteBuild(ctx, controllerReconciler, ld)

				dep := GetDeploymentEventually(ctx, testName, testNamespace)

				// Runtime deployment should NOT have a "build" container
				_, found := FindContainer(dep.Spec.Template.Spec, "build")
				Expect(found).To(BeFalse(), "build container should be stripped from runtime deployment")

				// Should still have exactly app + file-server
				Expect(dep.Spec.Template.Spec.Containers).To(HaveLen(2), "should have app and file-server containers only")
			})
		})

		// Helper function to clean up resources with proper finalizer handling
		cleanupResource := func(name string) {
			By(fmt.Sprintf("Cleaning up LlamaDeployment %s", name))
			llamaDeploy := &llamadeployv1.LlamaDeployment{}
			if err := k8sClient.Get(ctx, types.NamespacedName{Name: name, Namespace: testNamespace}, llamaDeploy); err == nil {
				// Remove finalizer first to allow deletion
				if len(llamaDeploy.Finalizers) > 0 {
					llamaDeploy.Finalizers = []string{}
					Expect(k8sClient.Update(ctx, llamaDeploy)).To(Succeed())
				}
				Expect(k8sClient.Delete(ctx, llamaDeploy)).To(Succeed())
				// Wait for actual deletion
				Eventually(func() bool {
					err := k8sClient.Get(ctx, types.NamespacedName{Name: name, Namespace: testNamespace}, llamaDeploy)
					return errors.IsNotFound(err)
				}, "5s", "100ms").Should(BeTrue())
			}

			// Clean up ServiceAccount
			By(fmt.Sprintf("Cleaning up ServiceAccount %s-sa", name))
			serviceAccount := &corev1.ServiceAccount{}
			if err := k8sClient.Get(ctx, types.NamespacedName{Name: name + "-sa", Namespace: testNamespace}, serviceAccount); err == nil {
				Expect(k8sClient.Delete(ctx, serviceAccount)).To(Succeed())
			}

			// Clean up NetworkPolicy
			By(fmt.Sprintf("Cleaning up NetworkPolicy %s-egress", name))
			networkPolicy := &networkingv1.NetworkPolicy{}
			if err := k8sClient.Get(ctx, types.NamespacedName{Name: name + "-egress", Namespace: testNamespace}, networkPolicy); err == nil {
				Expect(k8sClient.Delete(ctx, networkPolicy)).To(Succeed())
			}
		}

		cleanupSecret := func(name string) {
			By(fmt.Sprintf("Cleaning up Secret %s", name))
			secret := &corev1.Secret{}
			if err := k8sClient.Get(ctx, types.NamespacedName{Name: name, Namespace: testNamespace}, secret); err == nil {
				Expect(k8sClient.Delete(ctx, secret)).To(Succeed())
			}
		}

		Describe("Basic reconciliation", func() {
			It("should create all required resources for a basic LlamaDeployment", func() {
				testName := "test-basic-reconciliation"

				By("Creating a LlamaDeployment resource")
				llamaDeploy := &llamadeployv1.LlamaDeployment{
					ObjectMeta: metav1.ObjectMeta{
						Name:      testName,
						Namespace: testNamespace,
					},
					Spec: llamadeployv1.LlamaDeploymentSpec{
						ProjectId: testProjectID,
						RepoUrl:   testRepoURL,
						GitRef:    testGitRef,
					},
				}
				Expect(k8sClient.Create(ctx, llamaDeploy)).To(Succeed())

				defer cleanupResource(testName)

				By("Reconciling the LlamaDeployment")
				_, err := controllerReconciler.Reconcile(ctx, reconcile.Request{
					NamespacedName: types.NamespacedName{Name: testName, Namespace: testNamespace},
				})
				Expect(err).NotTo(HaveOccurred())
				CompleteBuild(ctx, controllerReconciler, llamaDeploy)

				By("Verifying the ServiceAccount was created")
				serviceAccount := &corev1.ServiceAccount{}
				Eventually(func() error {
					return k8sClient.Get(ctx, types.NamespacedName{Name: testName + "-sa", Namespace: testNamespace}, serviceAccount)
				}).Should(Succeed())
				Expect(serviceAccount.AutomountServiceAccountToken).NotTo(BeNil())
				Expect(*serviceAccount.AutomountServiceAccountToken).To(BeFalse())

				By("Verifying the Deployment was created")
				deployment := GetDeploymentEventually(ctx, testName, testNamespace)
				Expect(*deployment.Spec.Replicas).To(Equal(int32(1)))
				// Expect two containers: file-server and appserver
				Expect(deployment.Spec.Template.Spec.Containers).To(HaveLen(2))
				// Verify app container image on the container named APP
				var appContainer corev1.Container
				for _, c := range deployment.Spec.Template.Spec.Containers {
					if c.Name == APP {
						appContainer = c
						break
					}
				}
				Expect(appContainer.Image).To(Equal("llamaindex/llama-agents-appserver:latest"))

				// Verify ServiceAccount is set in deployment
				Expect(deployment.Spec.Template.Spec.ServiceAccountName).To(Equal(testName + "-sa"))
				Expect(deployment.Spec.Template.Spec.AutomountServiceAccountToken).NotTo(BeNil())
				Expect(*deployment.Spec.Template.Spec.AutomountServiceAccountToken).To(BeFalse())

				By("Verifying the Service was created")
				ExpectServicePort(ctx, testName, testNamespace, 80, 8081)

				By("Verifying the status was updated to Pending")
				Eventually(func() string {
					err := k8sClient.Get(ctx, types.NamespacedName{Name: testName, Namespace: testNamespace}, llamaDeploy)
					if err != nil {
						return ""
					}
					return llamaDeploy.Status.Phase
				}).Should(Equal("Pending"))
				Expect(llamaDeploy.Status.Message).To(Equal("Waiting for deployment pods to become available"))

				// Verify init container exists
				Expect(deployment.Spec.Template.Spec.InitContainers).To(HaveLen(1))
				Expect(deployment.Spec.Template.Spec.InitContainers[0].Name).To(Equal("bootstrap"))
				Expect(deployment.Spec.Template.Spec.InitContainers[0].Command).To(Equal([]string{"python", "-m", "llama_deploy.appserver.bootstrap"}))
			})
		})

		Describe("Secret handling", func() {
			It("should successfully reconcile when secret exists", func() {
				testName := "test-secret-exists"
				testSecretName := "test-secret-exists"

				By("Creating a test secret")
				secret := &corev1.Secret{
					ObjectMeta: metav1.ObjectMeta{
						Name:      testSecretName,
						Namespace: testNamespace,
					},
					Data: map[string][]byte{
						"GITHUB_PAT": []byte("test-token"),
					},
				}
				Expect(k8sClient.Create(ctx, secret)).To(Succeed())

				defer cleanupSecret(testSecretName)

				By("Creating a LlamaDeployment with secret reference")
				llamaDeploy := &llamadeployv1.LlamaDeployment{
					ObjectMeta: metav1.ObjectMeta{
						Name:      testName,
						Namespace: testNamespace,
					},
					Spec: llamadeployv1.LlamaDeploymentSpec{
						ProjectId:  testProjectID,
						RepoUrl:    testRepoURL,
						GitRef:     testGitRef,
						SecretName: testSecretName,
					},
				}
				Expect(k8sClient.Create(ctx, llamaDeploy)).To(Succeed())

				defer cleanupResource(testName)

				By("Reconciling the LlamaDeployment")
				_, err := controllerReconciler.Reconcile(ctx, reconcile.Request{
					NamespacedName: types.NamespacedName{Name: testName, Namespace: testNamespace},
				})
				Expect(err).NotTo(HaveOccurred())
				CompleteBuild(ctx, controllerReconciler, llamaDeploy)

				By("Verifying the status was updated to Pending")
				Eventually(func() string {
					err := k8sClient.Get(ctx, types.NamespacedName{Name: testName, Namespace: testNamespace}, llamaDeploy)
					if err != nil {
						return ""
					}
					return llamaDeploy.Status.Phase
				}).Should(Equal("Pending"))
			})

			It("should retry and then mark RolloutFailed when secret missing", func() {
				testName := "test-secret-missing"
				nn := types.NamespacedName{Name: testName, Namespace: testNamespace}

				By("Creating a LlamaDeployment with non-existent secret reference")
				llamaDeploy := &llamadeployv1.LlamaDeployment{
					ObjectMeta: metav1.ObjectMeta{
						Name:      testName,
						Namespace: testNamespace,
					},
					Spec: llamadeployv1.LlamaDeploymentSpec{
						ProjectId:  testProjectID,
						RepoUrl:    testRepoURL,
						GitRef:     testGitRef,
						SecretName: "non-existent-secret",
					},
				}
				Expect(k8sClient.Create(ctx, llamaDeploy)).To(Succeed())

				defer cleanupResource(testName)

				By("Reconciling 3 times — should retry, not fail yet")
				for i := 0; i < 3; i++ {
					result, err := controllerReconciler.Reconcile(ctx, reconcile.Request{NamespacedName: nn})
					Expect(err).NotTo(HaveOccurred())
					Expect(result.RequeueAfter).To(BeNumerically(">", 0), "should requeue for retry")

					Expect(k8sClient.Get(ctx, nn, llamaDeploy)).To(Succeed())
					Expect(llamaDeploy.Status.Phase).To(Equal("Pending"), "should stay Pending during retries")
					Expect(llamaDeploy.Status.SecretCheckRetries).To(Equal(int32(i + 1)))
				}

				By("Reconciling a 4th time — retries exhausted, should mark RolloutFailed")
				_, err := controllerReconciler.Reconcile(ctx, reconcile.Request{NamespacedName: nn})
				Expect(err).NotTo(HaveOccurred())

				Expect(k8sClient.Get(ctx, nn, llamaDeploy)).To(Succeed())
				Expect(llamaDeploy.Status.Phase).To(Equal("RolloutFailed"))

				By("Reconciling again; phase should remain RolloutFailed")
				_, err = controllerReconciler.Reconcile(ctx, reconcile.Request{NamespacedName: nn})
				Expect(err).NotTo(HaveOccurred())
				Consistently(func() string {
					_ = k8sClient.Get(ctx, nn, llamaDeploy)
					return llamaDeploy.Status.Phase
				}, "1s", "100ms").Should(Equal("RolloutFailed"))
			})
		})

		Describe("Status transitions", func() {
			It("should set status to Pending initially", func() {
				testName := "test-status-syncing"

				By("Creating a LlamaDeployment resource")
				llamaDeploy := &llamadeployv1.LlamaDeployment{
					ObjectMeta: metav1.ObjectMeta{
						Name:      testName,
						Namespace: testNamespace,
					},
					Spec: llamadeployv1.LlamaDeploymentSpec{
						ProjectId: testProjectID,
						RepoUrl:   testRepoURL,
						GitRef:    testGitRef,
					},
				}
				Expect(k8sClient.Create(ctx, llamaDeploy)).To(Succeed())

				defer cleanupResource(testName)

				By("Reconciling the LlamaDeployment")
				_, err := controllerReconciler.Reconcile(ctx, reconcile.Request{
					NamespacedName: types.NamespacedName{Name: testName, Namespace: testNamespace},
				})
				Expect(err).NotTo(HaveOccurred())
				CompleteBuild(ctx, controllerReconciler, llamaDeploy)

				By("Verifying status was set to Pending first")
				err = k8sClient.Get(ctx, types.NamespacedName{Name: testName, Namespace: testNamespace}, llamaDeploy)
				Expect(err).NotTo(HaveOccurred())
				Expect(llamaDeploy.Status.Phase).To(Equal("Pending")) // Could be either due to fast reconciliation
				Expect(llamaDeploy.Status.LastUpdated).NotTo(BeNil())
			})

			It("should transition to Running when deployment becomes healthy", func() {
				testName := "test-status-running"

				By("Creating a LlamaDeployment resource")
				llamaDeploy := &llamadeployv1.LlamaDeployment{
					ObjectMeta: metav1.ObjectMeta{
						Name:      testName,
						Namespace: testNamespace,
					},
					Spec: llamadeployv1.LlamaDeploymentSpec{
						ProjectId: testProjectID,
						RepoUrl:   testRepoURL,
						GitRef:    testGitRef,
					},
				}
				Expect(k8sClient.Create(ctx, llamaDeploy)).To(Succeed())

				defer cleanupResource(testName)

				By("First reconciliation - should create deployment and set to Pending")
				_, err := controllerReconciler.Reconcile(ctx, reconcile.Request{
					NamespacedName: types.NamespacedName{Name: testName, Namespace: testNamespace},
				})
				Expect(err).NotTo(HaveOccurred())
				CompleteBuild(ctx, controllerReconciler, llamaDeploy)

				By("Verifying status is Pending")
				err = k8sClient.Get(ctx, types.NamespacedName{Name: testName, Namespace: testNamespace}, llamaDeploy)
				Expect(err).NotTo(HaveOccurred())
				Expect(llamaDeploy.Status.Phase).To(Equal("Pending"))

				By("Simulating healthy deployment by updating deployment status")
				deployment := &appsv1.Deployment{}
				Eventually(func() error {
					return k8sClient.Get(ctx, types.NamespacedName{Name: testName, Namespace: testNamespace}, deployment)
				}).Should(Succeed())

				// Simulate healthy deployment status
				deployment.Status.ReadyReplicas = 1
				deployment.Status.AvailableReplicas = 1
				deployment.Status.UpdatedReplicas = 1
				deployment.Status.Replicas = 1
				deployment.Status.Conditions = []appsv1.DeploymentCondition{
					{
						Type:   appsv1.DeploymentAvailable,
						Status: corev1.ConditionTrue,
						Reason: "MinimumReplicasAvailable",
					},
					{
						Type:   appsv1.DeploymentProgressing,
						Status: corev1.ConditionTrue,
						Reason: "NewReplicaSetAvailable",
					},
				}
				Expect(k8sClient.Status().Update(ctx, deployment)).To(Succeed())

				By("Second reconciliation - should detect healthy deployment and set to Running")
				_, err = controllerReconciler.Reconcile(ctx, reconcile.Request{
					NamespacedName: types.NamespacedName{Name: testName, Namespace: testNamespace},
				})
				Expect(err).NotTo(HaveOccurred())

				By("Verifying status is now Running")
				err = k8sClient.Get(ctx, types.NamespacedName{Name: testName, Namespace: testNamespace}, llamaDeploy)
				Expect(err).NotTo(HaveOccurred())
				Expect(llamaDeploy.Status.Phase).To(Equal("Running"))
				Expect(llamaDeploy.Status.Message).To(Equal("Deployment is healthy and running"))
			})

			It("should transition to RollingOut during rolling update", func() {
				testName := "test-status-rollingout"

				By("Creating a LlamaDeployment resource")
				llamaDeploy := &llamadeployv1.LlamaDeployment{
					ObjectMeta: metav1.ObjectMeta{
						Name:      testName,
						Namespace: testNamespace,
					},
					Spec: llamadeployv1.LlamaDeploymentSpec{
						ProjectId: testProjectID,
						RepoUrl:   testRepoURL,
						GitRef:    testGitRef,
					},
				}
				Expect(k8sClient.Create(ctx, llamaDeploy)).To(Succeed())

				defer cleanupResource(testName)

				By("First reconciliation")
				_, err := controllerReconciler.Reconcile(ctx, reconcile.Request{
					NamespacedName: types.NamespacedName{Name: testName, Namespace: testNamespace},
				})
				Expect(err).NotTo(HaveOccurred())
				CompleteBuild(ctx, controllerReconciler, llamaDeploy)

				By("Simulating rolling update in progress")
				deployment := &appsv1.Deployment{}
				Eventually(func() error {
					return k8sClient.Get(ctx, types.NamespacedName{Name: testName, Namespace: testNamespace}, deployment)
				}).Should(Succeed())

				// Simulate rolling update: old pods still available, but not all updated
				deployment.Status.ReadyReplicas = 1     // Old pods are ready
				deployment.Status.AvailableReplicas = 1 // Old pods are available
				deployment.Status.UpdatedReplicas = 1   // New pod created but not ready
				deployment.Status.Replicas = 2          // Total: 1 old + 1 new
				deployment.Status.Conditions = []appsv1.DeploymentCondition{
					{
						Type:   appsv1.DeploymentAvailable,
						Status: corev1.ConditionTrue,
						Reason: "MinimumReplicasAvailable",
					},
					{
						Type:   appsv1.DeploymentProgressing,
						Status: corev1.ConditionTrue,
						Reason: "ReplicaSetUpdated",
					},
				}
				Expect(k8sClient.Status().Update(ctx, deployment)).To(Succeed())

				By("Reconciling during rolling update")
				_, err = controllerReconciler.Reconcile(ctx, reconcile.Request{
					NamespacedName: types.NamespacedName{Name: testName, Namespace: testNamespace},
				})
				Expect(err).NotTo(HaveOccurred())

				By("Verifying status is RollingOut")
				err = k8sClient.Get(ctx, types.NamespacedName{Name: testName, Namespace: testNamespace}, llamaDeploy)
				Expect(err).NotTo(HaveOccurred())
				Expect(llamaDeploy.Status.Phase).To(Equal("RollingOut"))
				Expect(llamaDeploy.Status.Message).To(ContainSubstring("Rolling update in progress (1/1 pods ready, 2 total)"))
			})

			It("should transition to RolloutFailed when new deployment fails but old pods are available", func() {
				testName := "test-status-rolloutfailed"

				By("Creating a LlamaDeployment resource")
				llamaDeploy := &llamadeployv1.LlamaDeployment{
					ObjectMeta: metav1.ObjectMeta{
						Name:      testName,
						Namespace: testNamespace,
					},
					Spec: llamadeployv1.LlamaDeploymentSpec{
						ProjectId: testProjectID,
						RepoUrl:   testRepoURL,
						GitRef:    testGitRef,
					},
				}
				Expect(k8sClient.Create(ctx, llamaDeploy)).To(Succeed())

				defer cleanupResource(testName)

				By("First reconciliation")
				_, err := controllerReconciler.Reconcile(ctx, reconcile.Request{
					NamespacedName: types.NamespacedName{Name: testName, Namespace: testNamespace},
				})
				Expect(err).NotTo(HaveOccurred())
				CompleteBuild(ctx, controllerReconciler, llamaDeploy)

				By("Simulating rollout failure with old pods still available")
				deployment := &appsv1.Deployment{}
				Eventually(func() error {
					return k8sClient.Get(ctx, types.NamespacedName{Name: testName, Namespace: testNamespace}, deployment)
				}).Should(Succeed())

				// Simulate failed rollout: old pods available, progress failed
				deployment.Status.ReadyReplicas = 1     // Old pods ready
				deployment.Status.AvailableReplicas = 1 // Old pods still available
				deployment.Status.UpdatedReplicas = 0   // New pods failed to update
				deployment.Status.Replicas = 1
				deployment.Status.Conditions = []appsv1.DeploymentCondition{
					{
						Type:   appsv1.DeploymentProgressing,
						Status: corev1.ConditionFalse, // Progress failed
						Reason: "ProgressDeadlineExceeded",
					},
				}
				Expect(k8sClient.Status().Update(ctx, deployment)).To(Succeed())

				By("Reconciling during rollout failure")
				_, err = controllerReconciler.Reconcile(ctx, reconcile.Request{
					NamespacedName: types.NamespacedName{Name: testName, Namespace: testNamespace},
				})
				Expect(err).NotTo(HaveOccurred())

				By("Verifying status is RolloutFailed")
				err = k8sClient.Get(ctx, types.NamespacedName{Name: testName, Namespace: testNamespace}, llamaDeploy)
				Expect(err).NotTo(HaveOccurred())
				Expect(llamaDeploy.Status.Phase).To(Equal("RolloutFailed"))
				Expect(llamaDeploy.Status.Message).To(ContainSubstring("Deployment rollout failed but 1 pods from previous version are still serving traffic"))
			})

			It("should transition to Failed when deployment fails with no available pods", func() {
				testName := "test-status-failed-no-pods"

				By("Creating a LlamaDeployment resource")
				llamaDeploy := &llamadeployv1.LlamaDeployment{
					ObjectMeta: metav1.ObjectMeta{
						Name:      testName,
						Namespace: testNamespace,
					},
					Spec: llamadeployv1.LlamaDeploymentSpec{
						ProjectId: testProjectID,
						RepoUrl:   testRepoURL,
						GitRef:    testGitRef,
					},
				}
				Expect(k8sClient.Create(ctx, llamaDeploy)).To(Succeed())

				defer cleanupResource(testName)

				By("First reconciliation")
				_, err := controllerReconciler.Reconcile(ctx, reconcile.Request{
					NamespacedName: types.NamespacedName{Name: testName, Namespace: testNamespace},
				})
				Expect(err).NotTo(HaveOccurred())
				CompleteBuild(ctx, controllerReconciler, llamaDeploy)

				By("Simulating complete deployment failure")
				deployment := &appsv1.Deployment{}
				Eventually(func() error {
					return k8sClient.Get(ctx, types.NamespacedName{Name: testName, Namespace: testNamespace}, deployment)
				}).Should(Succeed())

				// Simulate complete failure: no pods available
				deployment.Status.ReadyReplicas = 0
				deployment.Status.AvailableReplicas = 0 // No pods available
				deployment.Status.UpdatedReplicas = 0
				deployment.Status.Replicas = 0
				deployment.Status.Conditions = []appsv1.DeploymentCondition{
					{
						Type:   appsv1.DeploymentProgressing,
						Status: corev1.ConditionFalse,
						Reason: "ProgressDeadlineExceeded",
					},
				}
				Expect(k8sClient.Status().Update(ctx, deployment)).To(Succeed())

				By("Reconciling during complete failure")
				_, err = controllerReconciler.Reconcile(ctx, reconcile.Request{
					NamespacedName: types.NamespacedName{Name: testName, Namespace: testNamespace},
				})
				Expect(err).NotTo(HaveOccurred())

				By("Verifying status is Failed")
				err = k8sClient.Get(ctx, types.NamespacedName{Name: testName, Namespace: testNamespace}, llamaDeploy)
				Expect(err).NotTo(HaveOccurred())
				Expect(llamaDeploy.Status.Phase).To(Equal("Failed"))
				Expect(llamaDeploy.Status.Message).To(Equal("Deployment has failed and no pods are available"))
			})

			It("should handle RolloutFailed when some pods are ready but progress failed", func() {
				testName := "test-rollout-failed-partial"

				By("Creating a LlamaDeployment resource")
				llamaDeploy := &llamadeployv1.LlamaDeployment{
					ObjectMeta: metav1.ObjectMeta{
						Name:      testName,
						Namespace: testNamespace,
					},
					Spec: llamadeployv1.LlamaDeploymentSpec{
						ProjectId: testProjectID,
						RepoUrl:   testRepoURL,
						GitRef:    testGitRef,
					},
				}
				Expect(k8sClient.Create(ctx, llamaDeploy)).To(Succeed())

				defer cleanupResource(testName)

				By("First reconciliation")
				_, err := controllerReconciler.Reconcile(ctx, reconcile.Request{
					NamespacedName: types.NamespacedName{Name: testName, Namespace: testNamespace},
				})
				Expect(err).NotTo(HaveOccurred())
				CompleteBuild(ctx, controllerReconciler, llamaDeploy)

				By("Simulating partial rollout failure")
				deployment := &appsv1.Deployment{}
				Eventually(func() error {
					return k8sClient.Get(ctx, types.NamespacedName{Name: testName, Namespace: testNamespace}, deployment)
				}).Should(Succeed())

				// Simulate scenario: some pods ready but not all, and progress failed
				deployment.Status.ReadyReplicas = 2     // Old pods ready
				deployment.Status.AvailableReplicas = 2 // Some old pods still available
				deployment.Status.UpdatedReplicas = 0   // No updated replicas
				deployment.Status.Replicas = 2
				deployment.Status.Conditions = []appsv1.DeploymentCondition{
					{
						Type:   appsv1.DeploymentProgressing,
						Status: corev1.ConditionFalse,
						Reason: "ProgressDeadlineExceeded",
					},
				}
				Expect(k8sClient.Status().Update(ctx, deployment)).To(Succeed())

				By("Reconciling during partial rollout failure")
				_, err = controllerReconciler.Reconcile(ctx, reconcile.Request{
					NamespacedName: types.NamespacedName{Name: testName, Namespace: testNamespace},
				})
				Expect(err).NotTo(HaveOccurred())

				By("Verifying status is RolloutFailed with correct message")
				err = k8sClient.Get(ctx, types.NamespacedName{Name: testName, Namespace: testNamespace}, llamaDeploy)
				Expect(err).NotTo(HaveOccurred())
				Expect(llamaDeploy.Status.Phase).To(Equal("RolloutFailed"))
				Expect(llamaDeploy.Status.Message).To(ContainSubstring("Deployment rollout failed but 2 pods from previous version are still serving traffic"))
			})
		})

		Describe("Update detection", func() {
			It("should detect and handle deployment updates", func() {
				testName := "test-update-detection"

				By("Creating a LlamaDeployment resource")
				llamaDeploy := &llamadeployv1.LlamaDeployment{
					ObjectMeta: metav1.ObjectMeta{
						Name:      testName,
						Namespace: testNamespace,
					},
					Spec: llamadeployv1.LlamaDeploymentSpec{
						ProjectId: testProjectID,
						RepoUrl:   testRepoURL,
						GitRef:    testGitRef,
					},
				}
				Expect(k8sClient.Create(ctx, llamaDeploy)).To(Succeed())

				defer cleanupResource(testName)

				By("First reconciliation")
				_, err := controllerReconciler.Reconcile(ctx, reconcile.Request{
					NamespacedName: types.NamespacedName{Name: testName, Namespace: testNamespace},
				})
				Expect(err).NotTo(HaveOccurred())
				CompleteBuild(ctx, controllerReconciler, llamaDeploy)

				By("Modifying the deployment pod template annotation directly")
				deployment := &appsv1.Deployment{}
				Eventually(func() error {
					return k8sClient.Get(ctx, types.NamespacedName{Name: testName, Namespace: testNamespace}, deployment)
				}).Should(Succeed())

				if deployment.Spec.Template.Annotations == nil {
					deployment.Spec.Template.Annotations = map[string]string{}
				}
				// Add an unmanaged annotation that the operator does not own
				deployment.Spec.Template.Annotations["test.example/extra"] = "present"
				Expect(k8sClient.Update(ctx, deployment)).To(Succeed())

				By("Second reconciliation should succeed without conflicts and preserve unmanaged annotation")
				_, err = controllerReconciler.Reconcile(ctx, reconcile.Request{
					NamespacedName: types.NamespacedName{Name: testName, Namespace: testNamespace},
				})
				Expect(err).NotTo(HaveOccurred())

				By("Verifying operator-owned and unmanaged annotations are correct")
				Eventually(func() string {
					_ = k8sClient.Get(ctx, types.NamespacedName{Name: testName, Namespace: testNamespace}, deployment)
					if deployment.Spec.Template.Annotations == nil {
						return ""
					}
					return deployment.Spec.Template.Annotations["deploy.llamaindex.ai/git-source"]
				}).Should(ContainSubstring(testRepoURL))

				Eventually(func() string {
					_ = k8sClient.Get(ctx, types.NamespacedName{Name: testName, Namespace: testNamespace}, deployment)
					if deployment.Spec.Template.Annotations == nil {
						return ""
					}
					return deployment.Spec.Template.Annotations["test.example/extra"]
				}).Should(Equal("present"))
			})

			It("should force ownership on schema migration and override replicas", func() {
				testName := "test-update-detection-migration"

				By("Creating a LlamaDeployment resource")
				llamaDeploy := &llamadeployv1.LlamaDeployment{
					ObjectMeta: metav1.ObjectMeta{
						Name:      testName,
						Namespace: testNamespace,
					},
					Spec: llamadeployv1.LlamaDeploymentSpec{
						ProjectId: testProjectID,
						RepoUrl:   testRepoURL,
						GitRef:    testGitRef,
					},
				}
				Expect(k8sClient.Create(ctx, llamaDeploy)).To(Succeed())
				defer cleanupResource(testName)

				By("First reconciliation")
				_, err := controllerReconciler.Reconcile(ctx, reconcile.Request{
					NamespacedName: types.NamespacedName{Name: testName, Namespace: testNamespace},
				})
				Expect(err).NotTo(HaveOccurred())
				CompleteBuild(ctx, controllerReconciler, llamaDeploy)

				deployment := &appsv1.Deployment{}
				Eventually(func() error {
					return k8sClient.Get(ctx, types.NamespacedName{Name: testName, Namespace: testNamespace}, deployment)
				}).Should(Succeed())

				originalReplicas := *deployment.Spec.Replicas

				By("Modifying the deployment replicas and lowering schema version to trigger migration")
				newReplicas := originalReplicas + 1
				deployment.Spec.Replicas = &newReplicas
				Expect(k8sClient.Update(ctx, deployment)).To(Succeed())

				// Lower the status schema version to simulate a controller schema bump
				Expect(k8sClient.Get(ctx, types.NamespacedName{Name: testName, Namespace: testNamespace}, llamaDeploy)).To(Succeed())
				llamaDeploy.Status.SchemaVersion = "0"
				Expect(k8sClient.Status().Update(ctx, llamaDeploy)).To(Succeed())

				By("Second reconciliation should force ownership and restore replicas")
				_, err = controllerReconciler.Reconcile(ctx, reconcile.Request{
					NamespacedName: types.NamespacedName{Name: testName, Namespace: testNamespace},
				})
				Expect(err).NotTo(HaveOccurred())

				By("Verifying deployment was restored to original replica count")
				Eventually(func() int32 {
					_ = k8sClient.Get(ctx, types.NamespacedName{Name: testName, Namespace: testNamespace}, deployment)
					return *deployment.Spec.Replicas
				}).Should(Equal(originalReplicas))
			})
		})

		Describe("Owner references", func() {
			It("should set owner references on created resources", func() {
				testName := "test-owner-references"

				By("Creating a LlamaDeployment resource")
				llamaDeploy := &llamadeployv1.LlamaDeployment{
					ObjectMeta: metav1.ObjectMeta{
						Name:      testName,
						Namespace: testNamespace,
					},
					Spec: llamadeployv1.LlamaDeploymentSpec{
						ProjectId: testProjectID,
						RepoUrl:   testRepoURL,
						GitRef:    testGitRef,
					},
				}
				Expect(k8sClient.Create(ctx, llamaDeploy)).To(Succeed())

				defer cleanupResource(testName)

				By("Reconciling the LlamaDeployment")
				_, err := controllerReconciler.Reconcile(ctx, reconcile.Request{
					NamespacedName: types.NamespacedName{Name: testName, Namespace: testNamespace},
				})
				Expect(err).NotTo(HaveOccurred())
				CompleteBuild(ctx, controllerReconciler, llamaDeploy)

				By("Verifying ServiceAccount has correct owner reference")
				serviceAccount := &corev1.ServiceAccount{}
				Eventually(func() error {
					return k8sClient.Get(ctx, types.NamespacedName{Name: testName + "-sa", Namespace: testNamespace}, serviceAccount)
				}).Should(Succeed())
				Expect(serviceAccount.OwnerReferences).To(HaveLen(1))
				Expect(serviceAccount.OwnerReferences[0].Name).To(Equal(testName))

				By("Verifying Deployment has correct owner reference")
				deployment := &appsv1.Deployment{}
				Eventually(func() error {
					return k8sClient.Get(ctx, types.NamespacedName{Name: testName, Namespace: testNamespace}, deployment)
				}).Should(Succeed())
				Expect(deployment.OwnerReferences).To(HaveLen(1))
				Expect(deployment.OwnerReferences[0].Name).To(Equal(testName))

				By("Verifying Service has correct owner reference")
				service := &corev1.Service{}
				Eventually(func() error {
					return k8sClient.Get(ctx, types.NamespacedName{Name: testName, Namespace: testNamespace}, service)
				}).Should(Succeed())
				Expect(service.OwnerReferences).To(HaveLen(1))
				Expect(service.OwnerReferences[0].Name).To(Equal(testName))
			})
		})

		Describe("Resource deletion", func() {
			It("should handle graceful deletion", func() {
				testName := "test-deletion"

				By("Creating a LlamaDeployment resource")
				llamaDeploy := &llamadeployv1.LlamaDeployment{
					ObjectMeta: metav1.ObjectMeta{
						Name:      testName,
						Namespace: testNamespace,
					},
					Spec: llamadeployv1.LlamaDeploymentSpec{
						ProjectId: testProjectID,
						RepoUrl:   testRepoURL,
						GitRef:    testGitRef,
					},
				}
				Expect(k8sClient.Create(ctx, llamaDeploy)).To(Succeed())

				By("Reconciling the LlamaDeployment")
				_, err := controllerReconciler.Reconcile(ctx, reconcile.Request{
					NamespacedName: types.NamespacedName{Name: testName, Namespace: testNamespace},
				})
				Expect(err).NotTo(HaveOccurred())

				By("Deleting the LlamaDeployment")
				Expect(k8sClient.Delete(ctx, llamaDeploy)).To(Succeed())

				By("Reconciling after deletion should not error")
				_, err = controllerReconciler.Reconcile(ctx, reconcile.Request{
					NamespacedName: types.NamespacedName{Name: testName, Namespace: testNamespace},
				})
				Expect(err).NotTo(HaveOccurred())
			})

			It("should handle non-existent resource gracefully", func() {
				By("Reconciling a non-existent resource")
				_, err := controllerReconciler.Reconcile(ctx, reconcile.Request{
					NamespacedName: types.NamespacedName{Name: "non-existent", Namespace: testNamespace},
				})
				Expect(err).NotTo(HaveOccurred())
			})
		})

		Describe("Environment variables", func() {
			It("should set correct environment variables in deployment", func() {
				testName := "test-env-vars"

				By("Creating a LlamaDeployment resource with custom deployment file path")
				llamaDeploy := &llamadeployv1.LlamaDeployment{
					ObjectMeta: metav1.ObjectMeta{
						Name:      testName,
						Namespace: testNamespace,
					},
					Spec: llamadeployv1.LlamaDeploymentSpec{
						ProjectId:          testProjectID,
						RepoUrl:            testRepoURL,
						GitRef:             testGitRef,
						GitSha:             testGitSha,
						DeploymentFilePath: "custom_deployment.yml",
					},
				}
				Expect(k8sClient.Create(ctx, llamaDeploy)).To(Succeed())

				defer cleanupResource(testName)

				By("Reconciling the LlamaDeployment")
				_, err := controllerReconciler.Reconcile(ctx, reconcile.Request{
					NamespacedName: types.NamespacedName{Name: testName, Namespace: testNamespace},
				})
				Expect(err).NotTo(HaveOccurred())

				By("Completing the build")
				CompleteBuild(ctx, controllerReconciler, llamaDeploy)

				By("Verifying environment variables in deployment")
				deployment := &appsv1.Deployment{}
				Eventually(func() error {
					return k8sClient.Get(ctx, types.NamespacedName{Name: testName, Namespace: testNamespace}, deployment)
				}).Should(Succeed())

				// Read envs from the app container (named APP)
				container, ok := FindContainer(deployment.Spec.Template.Spec, APP)
				Expect(ok).To(BeTrue())
				envVars := make(map[string]string)
				for _, env := range container.Env {
					if env.Value != "" {
						envVars[env.Name] = env.Value
					}
				}

				// LLAMA_DEPLOY_REPO_URL should now point to the build API endpoint with embedded auth token
				expectedRepoURLPattern := fmt.Sprintf("http://llama-agents-build.llama-agents.svc.cluster.local:8001/deployments/%s", testName)
				Expect(envVars["LLAMA_DEPLOY_REPO_URL"]).To(MatchRegexp(expectedRepoURLPattern))
				Expect(envVars["LLAMA_DEPLOY_GIT_REF"]).To(Equal(testGitRef))
				Expect(envVars["LLAMA_DEPLOY_GIT_SHA"]).To(Equal(testGitSha))
				// LLAMA_DEPLOY_BUILD_API_HOST should be set to the build API host
				Expect(envVars["LLAMA_DEPLOY_BUILD_API_HOST"]).To(Equal("llama-agents-build.llama-agents.svc.cluster.local:8001"))
				// LLAMA_DEPLOY_AUTH_TOKEN should be set
				Expect(envVars["LLAMA_DEPLOY_AUTH_TOKEN"]).NotTo(BeEmpty())
				Expect(envVars["LLAMA_DEPLOY_DEPLOYMENT_FILE_PATH"]).To(Equal("custom_deployment.yml"))
				Expect(envVars["LLAMA_DEPLOY_DEPLOYMENT_NAME"]).To(Equal(testName))
			})

			It("should set envFrom when secret is specified", func() {
				testName := "test-envfrom"
				testSecretName := "test-multi-secret"

				By("Creating a test secret with multiple keys")
				secret := &corev1.Secret{
					ObjectMeta: metav1.ObjectMeta{
						Name:      testSecretName,
						Namespace: testNamespace,
					},
					Data: map[string][]byte{
						"GITHUB_PAT": []byte("test-token"),
						"OPENAI_KEY": []byte("openai-key"),
						"CUSTOM_VAR": []byte("custom-value"),
					},
				}
				Expect(k8sClient.Create(ctx, secret)).To(Succeed())

				defer cleanupSecret(testSecretName)

				By("Creating a LlamaDeployment with secret reference")
				llamaDeploy := &llamadeployv1.LlamaDeployment{
					ObjectMeta: metav1.ObjectMeta{
						Name:      testName,
						Namespace: testNamespace,
					},
					Spec: llamadeployv1.LlamaDeploymentSpec{
						ProjectId:  testProjectID,
						RepoUrl:    testRepoURL,
						GitRef:     testGitRef,
						SecretName: testSecretName,
					},
				}
				Expect(k8sClient.Create(ctx, llamaDeploy)).To(Succeed())

				defer cleanupResource(testName)

				By("Reconciling the LlamaDeployment")
				_, err := controllerReconciler.Reconcile(ctx, reconcile.Request{
					NamespacedName: types.NamespacedName{Name: testName, Namespace: testNamespace},
				})
				Expect(err).NotTo(HaveOccurred())
				CompleteBuild(ctx, controllerReconciler, llamaDeploy)

				By("Verifying envFrom is set in deployment")
				deployment := &appsv1.Deployment{}
				Eventually(func() error {
					return k8sClient.Get(ctx, types.NamespacedName{Name: testName, Namespace: testNamespace}, deployment)
				}).Should(Succeed())

				// App container envFrom should reference the secret
				container, ok := FindContainer(deployment.Spec.Template.Spec, APP)
				Expect(ok).To(BeTrue())
				Expect(container.EnvFrom).To(HaveLen(1))
				Expect(container.EnvFrom[0].SecretRef).NotTo(BeNil())
				Expect(container.EnvFrom[0].SecretRef.Name).To(Equal(testSecretName))

				// Verify the new environment variables are also set
				envVars := make(map[string]string)
				for _, env := range container.Env {
					if env.Value != "" {
						envVars[env.Name] = env.Value
					}
				}
				Expect(envVars["LLAMA_DEPLOY_BUILD_API_HOST"]).To(Equal("llama-agents-build.llama-agents.svc.cluster.local:8001"))
				Expect(envVars["LLAMA_DEPLOY_AUTH_TOKEN"]).NotTo(BeEmpty())
			})
		})

		Describe("Container image configuration", func() {
			It("should resolve image and tag from spec and defaults", func() {
				cases := []struct {
					name     string
					opts     []LlamaSpecOption
					expected string
				}{
					{"default-image", []LlamaSpecOption{}, "llamaindex/llama-agents-appserver:latest"},
					{"custom-image", []LlamaSpecOption{WithImage("custom/llama-deploy"), WithImageTag("v2.0.0")}, "custom/llama-deploy:v2.0.0"},
					{"custom-image-only", []LlamaSpecOption{WithImage("custom/llama-deploy")}, "custom/llama-deploy:latest"},
					{"custom-tag-only", []LlamaSpecOption{WithImageTag("v3.0.0")}, "llamaindex/llama-agents-appserver:v3.0.0"},
				}

				for _, tc := range cases {
					testName := "test-" + tc.name
					ld := NewLlama(testName, testNamespace, testProjectID, testRepoURL, append(tc.opts, WithGitRef(testGitRef))...)
					Expect(k8sClient.Create(ctx, ld)).To(Succeed())
					defer cleanupResource(testName)

					_, err := controllerReconciler.Reconcile(ctx, reconcile.Request{NamespacedName: types.NamespacedName{Name: testName, Namespace: testNamespace}})
					Expect(err).NotTo(HaveOccurred())
					CompleteBuild(ctx, controllerReconciler, ld)

					deployment := GetDeploymentEventually(ctx, testName, testNamespace)
					Expect(deployment.Spec.Template.Spec.Containers).To(HaveLen(2))
					appContainer, ok := FindContainer(deployment.Spec.Template.Spec, APP)
					Expect(ok).To(BeTrue())
					Expect(appContainer.Image).To(Equal(tc.expected))
				}
			})

			It("should detect image changes and trigger deployment updates", func() {
				testName := "test-image-update"

				By("Creating a LlamaDeployment resource with initial image")
				llamaDeploy := &llamadeployv1.LlamaDeployment{
					ObjectMeta: metav1.ObjectMeta{
						Name:      testName,
						Namespace: testNamespace,
					},
					Spec: llamadeployv1.LlamaDeploymentSpec{
						ProjectId: testProjectID,
						RepoUrl:   testRepoURL,
						GitRef:    testGitRef,
						Image:     "initial/image",
						ImageTag:  "v1.0.0",
					},
				}
				Expect(k8sClient.Create(ctx, llamaDeploy)).To(Succeed())

				defer cleanupResource(testName)

				By("Initial reconciliation")
				_, err := controllerReconciler.Reconcile(ctx, reconcile.Request{
					NamespacedName: types.NamespacedName{Name: testName, Namespace: testNamespace},
				})
				Expect(err).NotTo(HaveOccurred())
				CompleteBuild(ctx, controllerReconciler, llamaDeploy)

				By("Verifying initial deployment image")
				deployment := &appsv1.Deployment{}
				Eventually(func() error {
					return k8sClient.Get(ctx, types.NamespacedName{Name: testName, Namespace: testNamespace}, deployment)
				}).Should(Succeed())
				// Verify initial app container image
				var appContainer corev1.Container
				for _, c := range deployment.Spec.Template.Spec.Containers {
					if c.Name == APP {
						appContainer = c
						break
					}
				}
				Expect(appContainer.Image).To(Equal("initial/image:v1.0.0"))
				initialResourceVersion := deployment.ResourceVersion

				By("Updating the image in spec")
				Expect(k8sClient.Get(ctx, types.NamespacedName{Name: testName, Namespace: testNamespace}, llamaDeploy)).To(Succeed())
				llamaDeploy.Spec.Image = "updated/image"
				llamaDeploy.Spec.ImageTag = "v2.0.0"
				Expect(k8sClient.Update(ctx, llamaDeploy)).To(Succeed())

				By("Reconciling after image change")
				_, err = controllerReconciler.Reconcile(ctx, reconcile.Request{
					NamespacedName: types.NamespacedName{Name: testName, Namespace: testNamespace},
				})
				Expect(err).NotTo(HaveOccurred())

				By("Verifying deployment was updated with new image")
				Eventually(func() string {
					err := k8sClient.Get(ctx, types.NamespacedName{Name: testName, Namespace: testNamespace}, deployment)
					if err != nil {
						return ""
					}
					for _, c := range deployment.Spec.Template.Spec.Containers {
						if c.Name == APP {
							return c.Image
						}
					}
					return ""
				}).Should(Equal("updated/image:v2.0.0"))

				By("Verifying deployment resource version changed")
				Expect(k8sClient.Get(ctx, types.NamespacedName{Name: testName, Namespace: testNamespace}, deployment)).To(Succeed())
				Expect(deployment.ResourceVersion).NotTo(Equal(initialResourceVersion))
			})
		})

		Describe("Git reference functionality", func() {
			It("should trigger deployment update when git ref changes", func() {
				testName := "test-git-ref-change"

				By("Creating a LlamaDeployment resource with initial git ref")
				llamaDeploy := &llamadeployv1.LlamaDeployment{
					ObjectMeta: metav1.ObjectMeta{
						Name:      testName,
						Namespace: testNamespace,
					},
					Spec: llamadeployv1.LlamaDeploymentSpec{
						ProjectId: testProjectID,
						RepoUrl:   testRepoURL,
						GitRef:    "main",
					},
				}
				Expect(k8sClient.Create(ctx, llamaDeploy)).To(Succeed())

				defer cleanupResource(testName)

				By("Reconciling the LlamaDeployment")
				_, err := controllerReconciler.Reconcile(ctx, reconcile.Request{
					NamespacedName: types.NamespacedName{Name: testName, Namespace: testNamespace},
				})
				Expect(err).NotTo(HaveOccurred())
				CompleteBuild(ctx, controllerReconciler, llamaDeploy)

				By("Verifying initial deployment was created with main branch")
				deployment := &appsv1.Deployment{}
				Eventually(func() error {
					return k8sClient.Get(ctx, types.NamespacedName{Name: testName, Namespace: testNamespace}, deployment)
				}).Should(Succeed())

				// Store initial resource version to verify update
				initialResourceVersion := deployment.ResourceVersion

				var container corev1.Container
				for _, c := range deployment.Spec.Template.Spec.Containers {
					if c.Name == APP {
						container = c
						break
					}
				}
				envVars := make(map[string]string)
				for _, env := range container.Env {
					if env.Value != "" {
						envVars[env.Name] = env.Value
					}
				}
				expectedRepoURLPattern := fmt.Sprintf("http://llama-agents-build.llama-agents.svc.cluster.local:8001/deployments/%s", testName)
				Expect(envVars["LLAMA_DEPLOY_REPO_URL"]).To(MatchRegexp(expectedRepoURLPattern))
				Expect(envVars["LLAMA_DEPLOY_GIT_REF"]).To(Equal("main"))
				Expect(envVars["LLAMA_DEPLOY_GIT_SHA"]).To(BeEmpty())

				By("Updating the git ref to a different branch")
				// Fetch the latest version of llamaDeploy
				Expect(k8sClient.Get(ctx, types.NamespacedName{Name: testName, Namespace: testNamespace}, llamaDeploy)).To(Succeed())
				llamaDeploy.Spec.GitRef = "feature-branch"
				Expect(k8sClient.Update(ctx, llamaDeploy)).To(Succeed())

				By("Reconciling after git ref change")
				_, err = controllerReconciler.Reconcile(ctx, reconcile.Request{
					NamespacedName: types.NamespacedName{Name: testName, Namespace: testNamespace},
				})
				Expect(err).NotTo(HaveOccurred())

				By("Verifying deployment was updated with new git ref")
				Eventually(func() string {
					err := k8sClient.Get(ctx, types.NamespacedName{Name: testName, Namespace: testNamespace}, deployment)
					if err != nil {
						return ""
					}
					for _, c := range deployment.Spec.Template.Spec.Containers {
						if c.Name == APP {
							for _, env := range c.Env {
								if env.Name == LLAMA_DEPLOY_REPO_URL {
									return env.Value
								}
							}
						}
					}
					return ""
				}, "5s", "100ms").Should(MatchRegexp(fmt.Sprintf("http://llama-agents-build.llama-agents.svc.cluster.local:8001/deployments/%s", testName)))

				By("Verifying deployment resource version changed (indicating update occurred)")
				Expect(k8sClient.Get(ctx, types.NamespacedName{Name: testName, Namespace: testNamespace}, deployment)).To(Succeed())
				Expect(deployment.ResourceVersion).NotTo(Equal(initialResourceVersion))
			})

			It("should use build API URL for LLAMA_DEPLOY_REPO_URL regardless of spec changes", func() {
				testName := "test-build-api-url"

				By("Creating a LlamaDeployment resource")
				llamaDeploy := &llamadeployv1.LlamaDeployment{
					ObjectMeta: metav1.ObjectMeta{
						Name:      testName,
						Namespace: testNamespace,
					},
					Spec: llamadeployv1.LlamaDeploymentSpec{
						ProjectId: testProjectID,
						RepoUrl:   "https://github.com/original/repo.git",
						GitRef:    "v1.0.0",
					},
				}
				Expect(k8sClient.Create(ctx, llamaDeploy)).To(Succeed())

				defer cleanupResource(testName)

				By("Initial reconciliation")
				_, err := controllerReconciler.Reconcile(ctx, reconcile.Request{
					NamespacedName: types.NamespacedName{Name: testName, Namespace: testNamespace},
				})
				Expect(err).NotTo(HaveOccurred())
				CompleteBuild(ctx, controllerReconciler, llamaDeploy)

				By("Verifying deployment uses build API URL for LLAMA_DEPLOY_REPO_URL")
				deployment := &appsv1.Deployment{}
				Eventually(func() error {
					return k8sClient.Get(ctx, types.NamespacedName{Name: testName, Namespace: testNamespace}, deployment)
				}).Should(Succeed())

				expectedRepoURLPattern := fmt.Sprintf("http://llama-agents-build.llama-agents.svc.cluster.local:8001/deployments/%s", testName)
				Eventually(func() string {
					_ = k8sClient.Get(ctx, types.NamespacedName{Name: testName, Namespace: testNamespace}, deployment)
					for _, c := range deployment.Spec.Template.Spec.Containers {
						if c.Name == APP {
							for _, env := range c.Env {
								if env.Name == LLAMA_DEPLOY_REPO_URL {
									return env.Value
								}
							}
						}
					}
					return ""
				}, "5s", "100ms").Should(MatchRegexp(expectedRepoURLPattern))
				Eventually(func() string {
					_ = k8sClient.Get(ctx, types.NamespacedName{Name: testName, Namespace: testNamespace}, deployment)
					for _, c := range deployment.Spec.Template.Spec.Containers {
						if c.Name == APP {
							for _, env := range c.Env {
								if env.Name == "LLAMA_DEPLOY_BUILD_API_HOST" {
									return env.Value
								}
							}
						}
					}
					return ""
				}, "5s", "100ms").Should(Equal("llama-agents-build.llama-agents.svc.cluster.local:8001"))
				Eventually(func() string {
					_ = k8sClient.Get(ctx, types.NamespacedName{Name: testName, Namespace: testNamespace}, deployment)
					for _, c := range deployment.Spec.Template.Spec.Containers {
						if c.Name == APP {
							for _, env := range c.Env {
								if env.Name == "LLAMA_DEPLOY_AUTH_TOKEN" {
									return env.Value
								}
							}
						}
					}
					return ""
				}, "5s", "100ms").ShouldNot(BeEmpty())

				By("Updating repo URL and git ref in spec")
				Expect(k8sClient.Get(ctx, types.NamespacedName{Name: testName, Namespace: testNamespace}, llamaDeploy)).To(Succeed())
				llamaDeploy.Spec.RepoUrl = "https://github.com/updated/repo.git"
				llamaDeploy.Spec.GitRef = "v2.0.0"
				Expect(k8sClient.Update(ctx, llamaDeploy)).To(Succeed())

				By("Reconciling after spec change")
				_, err = controllerReconciler.Reconcile(ctx, reconcile.Request{
					NamespacedName: types.NamespacedName{Name: testName, Namespace: testNamespace},
				})
				Expect(err).NotTo(HaveOccurred())

				By("Verifying LLAMA_DEPLOY_REPO_URL still uses build API (spec changes don't affect LLAMA_DEPLOY_REPO_URL)")
				Eventually(func() string {
					_ = k8sClient.Get(ctx, types.NamespacedName{Name: testName, Namespace: testNamespace}, deployment)
					for _, c := range deployment.Spec.Template.Spec.Containers {
						if c.Name == APP {
							for _, env := range c.Env {
								if env.Name == LLAMA_DEPLOY_REPO_URL {
									return env.Value
								}
							}
						}
					}
					return ""
				}, "5s", "100ms").Should(MatchRegexp(fmt.Sprintf("http://llama-agents-build.llama-agents.svc.cluster.local:8001/deployments/%s", testName)))
			})

			It("should detect deployment file path changes and trigger updates", func() {
				testName := "test-deployment-file-path-change"

				By("Creating a LlamaDeployment resource with initial deployment file path")
				llamaDeploy := &llamadeployv1.LlamaDeployment{
					ObjectMeta: metav1.ObjectMeta{
						Name:      testName,
						Namespace: testNamespace,
					},
					Spec: llamadeployv1.LlamaDeploymentSpec{
						ProjectId:          testProjectID,
						RepoUrl:            testRepoURL,
						GitRef:             testGitRef,
						DeploymentFilePath: "deploy.yml",
					},
				}
				Expect(k8sClient.Create(ctx, llamaDeploy)).To(Succeed())

				defer cleanupResource(testName)

				By("Initial reconciliation")
				_, err := controllerReconciler.Reconcile(ctx, reconcile.Request{
					NamespacedName: types.NamespacedName{Name: testName, Namespace: testNamespace},
				})
				Expect(err).NotTo(HaveOccurred())
				CompleteBuild(ctx, controllerReconciler, llamaDeploy)

				By("Getting initial deployment state")
				deployment := &appsv1.Deployment{}
				Eventually(func() error {
					return k8sClient.Get(ctx, types.NamespacedName{Name: testName, Namespace: testNamespace}, deployment)
				}).Should(Succeed())

				initialResourceVersion := deployment.ResourceVersion

				By("Updating deployment file path")
				Expect(k8sClient.Get(ctx, types.NamespacedName{Name: testName, Namespace: testNamespace}, llamaDeploy)).To(Succeed())
				llamaDeploy.Spec.DeploymentFilePath = "production_deploy.yml"
				Expect(k8sClient.Update(ctx, llamaDeploy)).To(Succeed())

				By("Reconciling after deployment file path change")
				_, err = controllerReconciler.Reconcile(ctx, reconcile.Request{
					NamespacedName: types.NamespacedName{Name: testName, Namespace: testNamespace},
				})
				Expect(err).NotTo(HaveOccurred())

				By("Verifying deployment was updated due to critical environment variable change")
				Eventually(func() string {
					err := k8sClient.Get(ctx, types.NamespacedName{Name: testName, Namespace: testNamespace}, deployment)
					if err != nil {
						return ""
					}
					return deployment.ResourceVersion
				}).ShouldNot(Equal(initialResourceVersion))

				By("Verifying LLAMA_DEPLOY_DEPLOYMENT_FILE_PATH environment variable")
				Eventually(func() string {
					_ = k8sClient.Get(ctx, types.NamespacedName{Name: testName, Namespace: testNamespace}, deployment)
					for _, c := range deployment.Spec.Template.Spec.Containers {
						if c.Name == APP {
							for _, env := range c.Env {
								if env.Name == "LLAMA_DEPLOY_DEPLOYMENT_FILE_PATH" {
									return env.Value
								}
							}
						}
					}
					return ""
				}, "5s", "100ms").Should(Equal("production_deploy.yml"))
			})
		})

		Describe("Rollout timeout", func() {
			It("should set rolloutStartedAt when phase is Pending", func() {
				testName := "test-rollout-started-at"

				By("Creating a LlamaDeployment resource")
				llamaDeploy := &llamadeployv1.LlamaDeployment{
					ObjectMeta: metav1.ObjectMeta{
						Name:      testName,
						Namespace: testNamespace,
					},
					Spec: llamadeployv1.LlamaDeploymentSpec{
						ProjectId: testProjectID,
						RepoUrl:   testRepoURL,
						GitRef:    testGitRef,
					},
				}
				Expect(k8sClient.Create(ctx, llamaDeploy)).To(Succeed())
				defer cleanupResource(testName)

				By("Reconciling the LlamaDeployment")
				result, err := controllerReconciler.Reconcile(ctx, reconcile.Request{
					NamespacedName: types.NamespacedName{Name: testName, Namespace: testNamespace},
				})
				Expect(err).NotTo(HaveOccurred())
				By("Verifying RequeueAfter is set for timeout check")
				Expect(result.RequeueAfter).To(BeNumerically(">", 0))

				CompleteBuild(ctx, controllerReconciler, llamaDeploy)

				By("Verifying rolloutStartedAt is set")
				Expect(k8sClient.Get(ctx, types.NamespacedName{Name: testName, Namespace: testNamespace}, llamaDeploy)).To(Succeed())
				Expect(llamaDeploy.Status.Phase).To(Equal("Pending"))
				Expect(llamaDeploy.Status.RolloutStartedAt).NotTo(BeNil())

			})

			It("should clear rolloutStartedAt when deployment becomes Running", func() {
				testName := "test-rollout-started-cleared"

				By("Creating a LlamaDeployment resource")
				llamaDeploy := &llamadeployv1.LlamaDeployment{
					ObjectMeta: metav1.ObjectMeta{
						Name:      testName,
						Namespace: testNamespace,
					},
					Spec: llamadeployv1.LlamaDeploymentSpec{
						ProjectId: testProjectID,
						RepoUrl:   testRepoURL,
						GitRef:    testGitRef,
					},
				}
				Expect(k8sClient.Create(ctx, llamaDeploy)).To(Succeed())
				defer cleanupResource(testName)

				By("Reconciling to Pending")
				_, err := controllerReconciler.Reconcile(ctx, reconcile.Request{
					NamespacedName: types.NamespacedName{Name: testName, Namespace: testNamespace},
				})
				Expect(err).NotTo(HaveOccurred())
				CompleteBuild(ctx, controllerReconciler, llamaDeploy)

				By("Simulating healthy deployment")
				deployment := &appsv1.Deployment{}
				Eventually(func() error {
					return k8sClient.Get(ctx, types.NamespacedName{Name: testName, Namespace: testNamespace}, deployment)
				}).Should(Succeed())
				deployment.Status.ReadyReplicas = 1
				deployment.Status.AvailableReplicas = 1
				deployment.Status.UpdatedReplicas = 1
				deployment.Status.Replicas = 1
				deployment.Status.Conditions = []appsv1.DeploymentCondition{
					{Type: appsv1.DeploymentAvailable, Status: corev1.ConditionTrue, Reason: "MinimumReplicasAvailable"},
					{Type: appsv1.DeploymentProgressing, Status: corev1.ConditionTrue, Reason: "NewReplicaSetAvailable"},
				}
				Expect(k8sClient.Status().Update(ctx, deployment)).To(Succeed())

				By("Reconciling again — should transition to Running")
				_, err = controllerReconciler.Reconcile(ctx, reconcile.Request{
					NamespacedName: types.NamespacedName{Name: testName, Namespace: testNamespace},
				})
				Expect(err).NotTo(HaveOccurred())

				By("Verifying rolloutStartedAt is cleared")
				Expect(k8sClient.Get(ctx, types.NamespacedName{Name: testName, Namespace: testNamespace}, llamaDeploy)).To(Succeed())
				Expect(llamaDeploy.Status.Phase).To(Equal("Running"))
				Expect(llamaDeploy.Status.RolloutStartedAt).To(BeNil())
			})

			It("should mark RolloutFailed on timeout and set FailedRolloutGeneration", func() {
				testName := "test-rollout-timeout"
				os.Setenv(EnvRolloutTimeoutSeconds, "30")
				DeferCleanup(os.Unsetenv, EnvRolloutTimeoutSeconds)

				By("Creating a LlamaDeployment with short rollout timeout")
				llamaDeploy := &llamadeployv1.LlamaDeployment{
					ObjectMeta: metav1.ObjectMeta{
						Name:      testName,
						Namespace: testNamespace,
					},
					Spec: llamadeployv1.LlamaDeploymentSpec{
						ProjectId: testProjectID,
						RepoUrl:   testRepoURL,
						GitRef:    testGitRef,
					},
				}
				Expect(k8sClient.Create(ctx, llamaDeploy)).To(Succeed())
				defer cleanupResource(testName)

				By("First reconciliation — should set rolloutStartedAt")
				_, err := controllerReconciler.Reconcile(ctx, reconcile.Request{
					NamespacedName: types.NamespacedName{Name: testName, Namespace: testNamespace},
				})
				Expect(err).NotTo(HaveOccurred())
				CompleteBuild(ctx, controllerReconciler, llamaDeploy)

				Expect(k8sClient.Get(ctx, types.NamespacedName{Name: testName, Namespace: testNamespace}, llamaDeploy)).To(Succeed())
				Expect(llamaDeploy.Status.RolloutStartedAt).NotTo(BeNil())

				By("Setting available replicas so timeout picks RolloutFailed (not Failed)")
				SetDeploymentAvailableReplicas(ctx, testName, testNamespace, 1)

				By("Backdating rolloutStartedAt to simulate timeout")
				past := metav1.NewTime(time.Now().Add(-60 * time.Second))
				llamaDeploy.Status.RolloutStartedAt = &past
				Expect(k8sClient.Status().Update(ctx, llamaDeploy)).To(Succeed())

				By("Reconciling after timeout — should transition to RolloutFailed")
				_, err = controllerReconciler.Reconcile(ctx, reconcile.Request{
					NamespacedName: types.NamespacedName{Name: testName, Namespace: testNamespace},
				})
				Expect(err).NotTo(HaveOccurred())

				By("Verifying phase and failedRolloutGeneration")
				Expect(k8sClient.Get(ctx, types.NamespacedName{Name: testName, Namespace: testNamespace}, llamaDeploy)).To(Succeed())
				Expect(llamaDeploy.Status.Phase).To(Equal("RolloutFailed"))
				Expect(llamaDeploy.Status.Message).To(ContainSubstring("Rollout timed out"))
				Expect(llamaDeploy.Status.RolloutStartedAt).To(BeNil())
				Expect(llamaDeploy.Status.FailedRolloutGeneration).To(Equal(llamaDeploy.Generation))
			})

			It("should not re-trigger reconciliation for the same failed generation", func() {
				testName := "test-rollout-no-retrigger"
				os.Setenv(EnvRolloutTimeoutSeconds, "30")
				DeferCleanup(os.Unsetenv, EnvRolloutTimeoutSeconds)

				By("Creating a LlamaDeployment and timing it out")
				llamaDeploy := &llamadeployv1.LlamaDeployment{
					ObjectMeta: metav1.ObjectMeta{
						Name:      testName,
						Namespace: testNamespace,
					},
					Spec: llamadeployv1.LlamaDeploymentSpec{
						ProjectId: testProjectID,
						RepoUrl:   testRepoURL,
						GitRef:    testGitRef,
					},
				}
				Expect(k8sClient.Create(ctx, llamaDeploy)).To(Succeed())
				defer cleanupResource(testName)

				// Reconcile to set rolloutStartedAt
				_, err := controllerReconciler.Reconcile(ctx, reconcile.Request{
					NamespacedName: types.NamespacedName{Name: testName, Namespace: testNamespace},
				})
				Expect(err).NotTo(HaveOccurred())
				CompleteBuild(ctx, controllerReconciler, llamaDeploy)

				// Set available replicas so timeout picks RolloutFailed
				SetDeploymentAvailableReplicas(ctx, testName, testNamespace, 1)

				// Backdate and timeout
				Expect(k8sClient.Get(ctx, types.NamespacedName{Name: testName, Namespace: testNamespace}, llamaDeploy)).To(Succeed())
				past := metav1.NewTime(time.Now().Add(-60 * time.Second))
				llamaDeploy.Status.RolloutStartedAt = &past
				Expect(k8sClient.Status().Update(ctx, llamaDeploy)).To(Succeed())

				_, err = controllerReconciler.Reconcile(ctx, reconcile.Request{
					NamespacedName: types.NamespacedName{Name: testName, Namespace: testNamespace},
				})
				Expect(err).NotTo(HaveOccurred())

				By("Verifying phase is RolloutFailed")
				Expect(k8sClient.Get(ctx, types.NamespacedName{Name: testName, Namespace: testNamespace}, llamaDeploy)).To(Succeed())
				Expect(llamaDeploy.Status.Phase).To(Equal("RolloutFailed"))

				By("Reconciling again; phase should remain RolloutFailed and not re-trigger")
				_, err = controllerReconciler.Reconcile(ctx, reconcile.Request{
					NamespacedName: types.NamespacedName{Name: testName, Namespace: testNamespace},
				})
				Expect(err).NotTo(HaveOccurred())
				Consistently(func() string {
					_ = k8sClient.Get(ctx, types.NamespacedName{Name: testName, Namespace: testNamespace}, llamaDeploy)
					return llamaDeploy.Status.Phase
				}, "1s", "100ms").Should(Equal("RolloutFailed"))
			})

			It("should retry rollout when spec is updated after timeout failure", func() {
				testName := "test-rollout-retry-after-timeout"
				os.Setenv(EnvRolloutTimeoutSeconds, "30")
				DeferCleanup(os.Unsetenv, EnvRolloutTimeoutSeconds)

				By("Creating a LlamaDeployment and timing it out")
				llamaDeploy := &llamadeployv1.LlamaDeployment{
					ObjectMeta: metav1.ObjectMeta{
						Name:      testName,
						Namespace: testNamespace,
					},
					Spec: llamadeployv1.LlamaDeploymentSpec{
						ProjectId: testProjectID,
						RepoUrl:   testRepoURL,
						GitRef:    testGitRef,
						GitSha:    testGitSha,
					},
				}
				Expect(k8sClient.Create(ctx, llamaDeploy)).To(Succeed())
				defer cleanupResource(testName)

				// Reconcile to create build, then complete it
				_, err := controllerReconciler.Reconcile(ctx, reconcile.Request{
					NamespacedName: types.NamespacedName{Name: testName, Namespace: testNamespace},
				})
				Expect(err).NotTo(HaveOccurred())
				CompleteBuild(ctx, controllerReconciler, llamaDeploy)

				// Set available replicas so timeout picks RolloutFailed
				SetDeploymentAvailableReplicas(ctx, testName, testNamespace, 1)

				Expect(k8sClient.Get(ctx, types.NamespacedName{Name: testName, Namespace: testNamespace}, llamaDeploy)).To(Succeed())
				past := metav1.NewTime(time.Now().Add(-60 * time.Second))
				llamaDeploy.Status.RolloutStartedAt = &past
				Expect(k8sClient.Status().Update(ctx, llamaDeploy)).To(Succeed())

				_, err = controllerReconciler.Reconcile(ctx, reconcile.Request{
					NamespacedName: types.NamespacedName{Name: testName, Namespace: testNamespace},
				})
				Expect(err).NotTo(HaveOccurred())

				Expect(k8sClient.Get(ctx, types.NamespacedName{Name: testName, Namespace: testNamespace}, llamaDeploy)).To(Succeed())
				Expect(llamaDeploy.Status.Phase).To(Equal("RolloutFailed"))
				failedGen := llamaDeploy.Generation

				By("Updating the spec to trigger a new generation")
				llamaDeploy.Spec.GitSha = testGitSha2
				Expect(k8sClient.Update(ctx, llamaDeploy)).To(Succeed())
				// After update, generation should change
				Expect(k8sClient.Get(ctx, types.NamespacedName{Name: testName, Namespace: testNamespace}, llamaDeploy)).To(Succeed())
				Expect(llamaDeploy.Generation).To(BeNumerically(">", failedGen))

				By("Reconciling with new spec — should start a new build")
				_, err = controllerReconciler.Reconcile(ctx, reconcile.Request{
					NamespacedName: types.NamespacedName{Name: testName, Namespace: testNamespace},
				})
				Expect(err).NotTo(HaveOccurred())
				CompleteBuild(ctx, controllerReconciler, llamaDeploy)

				Expect(k8sClient.Get(ctx, types.NamespacedName{Name: testName, Namespace: testNamespace}, llamaDeploy)).To(Succeed())
				Expect(llamaDeploy.Status.Phase).To(Equal("Pending"))
			})

			It("should set progressDeadlineSeconds on the Kubernetes Deployment", func() {
				testName := "test-progress-deadline"
				os.Setenv(EnvRolloutTimeoutSeconds, "120")
				DeferCleanup(os.Unsetenv, EnvRolloutTimeoutSeconds)

				By("Creating a LlamaDeployment with custom rollout timeout")
				ld := NewLlama(testName, testNamespace, testProjectID, testRepoURL,
					WithGitRef(testGitRef))
				Expect(k8sClient.Create(ctx, ld)).To(Succeed())
				defer CleanupLlama(ctx, testName, testNamespace)

				_, err := controllerReconciler.Reconcile(ctx, reconcile.Request{
					NamespacedName: types.NamespacedName{Name: testName, Namespace: testNamespace},
				})
				Expect(err).NotTo(HaveOccurred())
				CompleteBuild(ctx, controllerReconciler, ld)

				By("Verifying progressDeadlineSeconds on the Deployment")
				dep := GetDeploymentEventually(ctx, testName, testNamespace)
				Expect(dep.Spec.ProgressDeadlineSeconds).NotTo(BeNil())
				Expect(*dep.Spec.ProgressDeadlineSeconds).To(Equal(int32(120)))
			})

			It("should set default progressDeadlineSeconds when rolloutTimeoutSeconds is not specified", func() {
				testName := "test-progress-deadline-default"

				By("Creating a LlamaDeployment without rollout timeout")
				ld := NewLlama(testName, testNamespace, testProjectID, testRepoURL,
					WithGitRef(testGitRef))
				Expect(k8sClient.Create(ctx, ld)).To(Succeed())
				defer CleanupLlama(ctx, testName, testNamespace)

				_, err := controllerReconciler.Reconcile(ctx, reconcile.Request{
					NamespacedName: types.NamespacedName{Name: testName, Namespace: testNamespace},
				})
				Expect(err).NotTo(HaveOccurred())
				CompleteBuild(ctx, controllerReconciler, ld)

				By("Verifying progressDeadlineSeconds is set to default")
				dep := GetDeploymentEventually(ctx, testName, testNamespace)
				Expect(dep.Spec.ProgressDeadlineSeconds).NotTo(BeNil())
				Expect(*dep.Spec.ProgressDeadlineSeconds).To(Equal(DefaultRolloutTimeoutSeconds))
			})

			It("should scale down failing ReplicaSet on ProgressDeadlineExceeded", func() {
				testName := "test-rs-scaledown-progress"

				By("Creating a LlamaDeployment resource")
				llamaDeploy := &llamadeployv1.LlamaDeployment{
					ObjectMeta: metav1.ObjectMeta{
						Name:      testName,
						Namespace: testNamespace,
					},
					Spec: llamadeployv1.LlamaDeploymentSpec{
						ProjectId: testProjectID,
						RepoUrl:   testRepoURL,
						GitRef:    testGitRef,
					},
				}
				Expect(k8sClient.Create(ctx, llamaDeploy)).To(Succeed())
				defer cleanupResource(testName)

				By("Reconciling to create the Deployment")
				_, err := controllerReconciler.Reconcile(ctx, reconcile.Request{
					NamespacedName: types.NamespacedName{Name: testName, Namespace: testNamespace},
				})
				Expect(err).NotTo(HaveOccurred())
				CompleteBuild(ctx, controllerReconciler, llamaDeploy)

				By("Getting the Deployment")
				deployment := GetDeploymentEventually(ctx, testName, testNamespace)

				By("Creating an old ReplicaSet (healthy, serving traffic)")
				oldRS := &appsv1.ReplicaSet{
					ObjectMeta: metav1.ObjectMeta{
						Name:      testName + "-old",
						Namespace: testNamespace,
						Labels:    map[string]string{"app": testName},
						Annotations: map[string]string{
							"deployment.kubernetes.io/revision": "1",
						},
						OwnerReferences: []metav1.OwnerReference{
							{
								APIVersion: "apps/v1",
								Kind:       "Deployment",
								Name:       deployment.Name,
								UID:        deployment.UID,
							},
						},
					},
					Spec: appsv1.ReplicaSetSpec{
						Replicas: ptr(int32(1)),
						Selector: &metav1.LabelSelector{
							MatchLabels: map[string]string{"app": testName},
						},
						Template: deployment.Spec.Template,
					},
				}
				Expect(k8sClient.Create(ctx, oldRS)).To(Succeed())
				defer func() { _ = k8sClient.Delete(ctx, oldRS) }()
				oldRS.Status.Replicas = 1
				oldRS.Status.AvailableReplicas = 1
				oldRS.Status.ReadyReplicas = 1
				Expect(k8sClient.Status().Update(ctx, oldRS)).To(Succeed())

				By("Creating a new ReplicaSet (failing, crash-looping)")
				newRS := &appsv1.ReplicaSet{
					ObjectMeta: metav1.ObjectMeta{
						Name:      testName + "-new",
						Namespace: testNamespace,
						Labels:    map[string]string{"app": testName},
						Annotations: map[string]string{
							"deployment.kubernetes.io/revision": "2",
						},
						OwnerReferences: []metav1.OwnerReference{
							{
								APIVersion: "apps/v1",
								Kind:       "Deployment",
								Name:       deployment.Name,
								UID:        deployment.UID,
							},
						},
					},
					Spec: appsv1.ReplicaSetSpec{
						Replicas: ptr(int32(1)),
						Selector: &metav1.LabelSelector{
							MatchLabels: map[string]string{"app": testName},
						},
						Template: deployment.Spec.Template,
					},
				}
				Expect(k8sClient.Create(ctx, newRS)).To(Succeed())
				defer func() { _ = k8sClient.Delete(ctx, newRS) }()
				newRS.Status.Replicas = 1
				newRS.Status.AvailableReplicas = 0 // not available (crash-looping)
				newRS.Status.ReadyReplicas = 0
				Expect(k8sClient.Status().Update(ctx, newRS)).To(Succeed())

				By("Simulating ProgressDeadlineExceeded on the Deployment")
				Expect(k8sClient.Get(ctx, types.NamespacedName{Name: testName, Namespace: testNamespace}, deployment)).To(Succeed())
				deployment.Status.ReadyReplicas = 1
				deployment.Status.AvailableReplicas = 1
				deployment.Status.Replicas = 2
				deployment.Status.Conditions = []appsv1.DeploymentCondition{
					{Type: appsv1.DeploymentProgressing, Status: corev1.ConditionFalse, Reason: "ProgressDeadlineExceeded"},
				}
				Expect(k8sClient.Status().Update(ctx, deployment)).To(Succeed())

				By("Reconciling — should scale down the new RS and mark RolloutFailed")
				_, err = controllerReconciler.Reconcile(ctx, reconcile.Request{
					NamespacedName: types.NamespacedName{Name: testName, Namespace: testNamespace},
				})
				Expect(err).NotTo(HaveOccurred())

				By("Verifying phase is RolloutFailed")
				Expect(k8sClient.Get(ctx, types.NamespacedName{Name: testName, Namespace: testNamespace}, llamaDeploy)).To(Succeed())
				Expect(llamaDeploy.Status.Phase).To(Equal("RolloutFailed"))
				Expect(llamaDeploy.Status.FailedRolloutGeneration).To(Equal(llamaDeploy.Generation))

				By("Verifying the Deployment is paused")
				Expect(k8sClient.Get(ctx, types.NamespacedName{Name: testName, Namespace: testNamespace}, deployment)).To(Succeed())
				Expect(deployment.Spec.Paused).To(BeTrue())

				By("Verifying the new ReplicaSet was scaled to 0")
				Eventually(func() int32 {
					rs := &appsv1.ReplicaSet{}
					err := k8sClient.Get(ctx, types.NamespacedName{Name: testName + "-new", Namespace: testNamespace}, rs)
					if err != nil {
						return -1
					}
					if rs.Spec.Replicas == nil {
						return 1
					}
					return *rs.Spec.Replicas
				}).Should(Equal(int32(0)))

				By("Verifying the old ReplicaSet is untouched")
				updatedOldRS := &appsv1.ReplicaSet{}
				Expect(k8sClient.Get(ctx, types.NamespacedName{Name: testName + "-old", Namespace: testNamespace}, updatedOldRS)).To(Succeed())
				Expect(*updatedOldRS.Spec.Replicas).To(Equal(int32(1)))
			})

			It("should scale down the only RS when it is unhealthy", func() {
				testName := "test-rs-scaledown-all-unhealthy"

				By("Creating a LlamaDeployment resource")
				llamaDeploy := &llamadeployv1.LlamaDeployment{
					ObjectMeta: metav1.ObjectMeta{
						Name:      testName,
						Namespace: testNamespace,
					},
					Spec: llamadeployv1.LlamaDeploymentSpec{
						ProjectId: testProjectID,
						RepoUrl:   testRepoURL,
						GitRef:    testGitRef,
					},
				}
				Expect(k8sClient.Create(ctx, llamaDeploy)).To(Succeed())
				defer cleanupResource(testName)

				By("Reconciling to create the Deployment")
				_, err := controllerReconciler.Reconcile(ctx, reconcile.Request{
					NamespacedName: types.NamespacedName{Name: testName, Namespace: testNamespace},
				})
				Expect(err).NotTo(HaveOccurred())
				CompleteBuild(ctx, controllerReconciler, llamaDeploy)

				By("Getting the Deployment")
				deployment := GetDeploymentEventually(ctx, testName, testNamespace)

				By("Creating a single ReplicaSet that is completely unhealthy (crash-looping)")
				unhealthyRS := &appsv1.ReplicaSet{
					ObjectMeta: metav1.ObjectMeta{
						Name:      testName + "-only",
						Namespace: testNamespace,
						Labels:    map[string]string{"app": testName},
						Annotations: map[string]string{
							"deployment.kubernetes.io/revision": "1",
						},
						OwnerReferences: []metav1.OwnerReference{
							{
								APIVersion: "apps/v1",
								Kind:       "Deployment",
								Name:       deployment.Name,
								UID:        deployment.UID,
							},
						},
					},
					Spec: appsv1.ReplicaSetSpec{
						Replicas: ptr(int32(1)),
						Selector: &metav1.LabelSelector{
							MatchLabels: map[string]string{"app": testName},
						},
						Template: deployment.Spec.Template,
					},
				}
				Expect(k8sClient.Create(ctx, unhealthyRS)).To(Succeed())
				defer func() { _ = k8sClient.Delete(ctx, unhealthyRS) }()
				// Zero available replicas — everything is crash-looping
				unhealthyRS.Status.Replicas = 1
				unhealthyRS.Status.AvailableReplicas = 0
				unhealthyRS.Status.ReadyReplicas = 0
				Expect(k8sClient.Status().Update(ctx, unhealthyRS)).To(Succeed())

				By("Simulating ProgressDeadlineExceeded on the Deployment")
				Expect(k8sClient.Get(ctx, types.NamespacedName{Name: testName, Namespace: testNamespace}, deployment)).To(Succeed())
				deployment.Status.ReadyReplicas = 0
				deployment.Status.AvailableReplicas = 0
				deployment.Status.Replicas = 1
				deployment.Status.Conditions = []appsv1.DeploymentCondition{
					{Type: appsv1.DeploymentProgressing, Status: corev1.ConditionFalse, Reason: "ProgressDeadlineExceeded"},
				}
				Expect(k8sClient.Status().Update(ctx, deployment)).To(Succeed())

				By("Reconciling — should scale down even the only RS since it is unhealthy")
				_, err = controllerReconciler.Reconcile(ctx, reconcile.Request{
					NamespacedName: types.NamespacedName{Name: testName, Namespace: testNamespace},
				})
				Expect(err).NotTo(HaveOccurred())

				By("Verifying phase is Failed (no available replicas)")
				Expect(k8sClient.Get(ctx, types.NamespacedName{Name: testName, Namespace: testNamespace}, llamaDeploy)).To(Succeed())
				Expect(llamaDeploy.Status.Phase).To(Equal("Failed"))

				By("Verifying the Deployment was scaled to 0 replicas (PhaseFailed uses replicas, not RS manipulation)")
				Expect(k8sClient.Get(ctx, types.NamespacedName{Name: testName, Namespace: testNamespace}, deployment)).To(Succeed())
				Expect(*deployment.Spec.Replicas).To(Equal(int32(0)))
			})

			It("should scale down the only RS when phase is Failed even if AvailableReplicas > 0", func() {
				testName := "test-rs-scaledown-failed-avail"

				By("Creating a LlamaDeployment resource")
				llamaDeploy := &llamadeployv1.LlamaDeployment{
					ObjectMeta: metav1.ObjectMeta{
						Name:      testName,
						Namespace: testNamespace,
					},
					Spec: llamadeployv1.LlamaDeploymentSpec{
						ProjectId: testProjectID,
						RepoUrl:   testRepoURL,
						GitRef:    testGitRef,
					},
				}
				Expect(k8sClient.Create(ctx, llamaDeploy)).To(Succeed())
				defer cleanupResource(testName)

				By("Reconciling to create the Deployment")
				_, err := controllerReconciler.Reconcile(ctx, reconcile.Request{
					NamespacedName: types.NamespacedName{Name: testName, Namespace: testNamespace},
				})
				Expect(err).NotTo(HaveOccurred())
				CompleteBuild(ctx, controllerReconciler, llamaDeploy)

				By("Getting the Deployment")
				deployment := GetDeploymentEventually(ctx, testName, testNamespace)

				By("Creating a single ReplicaSet with AvailableReplicas > 0 (crash-looping but briefly available)")
				rs := &appsv1.ReplicaSet{
					ObjectMeta: metav1.ObjectMeta{
						Name:      testName + "-only",
						Namespace: testNamespace,
						Labels:    map[string]string{"app": testName},
						Annotations: map[string]string{
							"deployment.kubernetes.io/revision": "1",
						},
						OwnerReferences: []metav1.OwnerReference{
							{
								APIVersion: "apps/v1",
								Kind:       "Deployment",
								Name:       deployment.Name,
								UID:        deployment.UID,
							},
						},
					},
					Spec: appsv1.ReplicaSetSpec{
						Replicas: ptr(int32(1)),
						Selector: &metav1.LabelSelector{
							MatchLabels: map[string]string{"app": testName},
						},
						Template: deployment.Spec.Template,
					},
				}
				Expect(k8sClient.Create(ctx, rs)).To(Succeed())
				defer func() { _ = k8sClient.Delete(ctx, rs) }()
				// AvailableReplicas > 0 but deployment-level status says Failed
				rs.Status.Replicas = 1
				rs.Status.AvailableReplicas = 1
				rs.Status.ReadyReplicas = 1
				Expect(k8sClient.Status().Update(ctx, rs)).To(Succeed())

				By("Simulating ProgressDeadlineExceeded with zero available at deployment level")
				Expect(k8sClient.Get(ctx, types.NamespacedName{Name: testName, Namespace: testNamespace}, deployment)).To(Succeed())
				deployment.Status.ReadyReplicas = 0
				deployment.Status.AvailableReplicas = 0
				deployment.Status.Replicas = 1
				deployment.Status.Conditions = []appsv1.DeploymentCondition{
					{Type: appsv1.DeploymentProgressing, Status: corev1.ConditionFalse, Reason: "ProgressDeadlineExceeded"},
				}
				Expect(k8sClient.Status().Update(ctx, deployment)).To(Succeed())

				By("Reconciling — should scale down the RS despite AvailableReplicas > 0 because phase is Failed")
				_, err = controllerReconciler.Reconcile(ctx, reconcile.Request{
					NamespacedName: types.NamespacedName{Name: testName, Namespace: testNamespace},
				})
				Expect(err).NotTo(HaveOccurred())

				By("Verifying phase is Failed")
				Expect(k8sClient.Get(ctx, types.NamespacedName{Name: testName, Namespace: testNamespace}, llamaDeploy)).To(Succeed())
				Expect(llamaDeploy.Status.Phase).To(Equal("Failed"))

				By("Verifying the Deployment was scaled to 0 replicas (PhaseFailed uses replicas, not RS manipulation)")
				deployment2 := &appsv1.Deployment{}
				Expect(k8sClient.Get(ctx, types.NamespacedName{Name: testName, Namespace: testNamespace}, deployment2)).To(Succeed())
				Expect(*deployment2.Spec.Replicas).To(Equal(int32(0)))
			})

			It("should skip reconciliation for PhaseFailed with matching FailedRolloutGeneration", func() {
				testName := "test-skip-failed-gen"

				By("Creating a LlamaDeployment resource")
				llamaDeploy := &llamadeployv1.LlamaDeployment{
					ObjectMeta: metav1.ObjectMeta{
						Name:      testName,
						Namespace: testNamespace,
					},
					Spec: llamadeployv1.LlamaDeploymentSpec{
						ProjectId: testProjectID,
						RepoUrl:   testRepoURL,
						GitRef:    testGitRef,
					},
				}
				Expect(k8sClient.Create(ctx, llamaDeploy)).To(Succeed())
				defer cleanupResource(testName)

				By("Reconciling to create the Deployment")
				_, err := controllerReconciler.Reconcile(ctx, reconcile.Request{
					NamespacedName: types.NamespacedName{Name: testName, Namespace: testNamespace},
				})
				Expect(err).NotTo(HaveOccurred())
				CompleteBuild(ctx, controllerReconciler, llamaDeploy)

				By("Setting the LlamaDeployment to PhaseFailed with FailedRolloutGeneration == Generation")
				Expect(k8sClient.Get(ctx, types.NamespacedName{Name: testName, Namespace: testNamespace}, llamaDeploy)).To(Succeed())
				llamaDeploy.Status.Phase = PhaseFailed
				llamaDeploy.Status.FailedRolloutGeneration = llamaDeploy.Generation
				llamaDeploy.Status.Message = "Deployment has failed"
				now := metav1.Now()
				llamaDeploy.Status.LastUpdated = &now
				Expect(k8sClient.Status().Update(ctx, llamaDeploy)).To(Succeed())

				By("Re-reconciling — should skip reconciliation entirely")
				_, err = controllerReconciler.Reconcile(ctx, reconcile.Request{
					NamespacedName: types.NamespacedName{Name: testName, Namespace: testNamespace},
				})
				Expect(err).NotTo(HaveOccurred())

				By("Verifying the Deployment is not paused (PhaseFailed uses replicas, not pause)")
				deployment := &appsv1.Deployment{}
				Expect(k8sClient.Get(ctx, types.NamespacedName{Name: testName, Namespace: testNamespace}, deployment)).To(Succeed())
				Expect(deployment.Spec.Paused).To(BeFalse())
			})

			It("should scale deployment to 0 replicas when PhaseFailed is detected", func() {
				testName := "test-failed-replicas-zero"

				By("Creating a LlamaDeployment resource")
				llamaDeploy := &llamadeployv1.LlamaDeployment{
					ObjectMeta: metav1.ObjectMeta{
						Name:      testName,
						Namespace: testNamespace,
					},
					Spec: llamadeployv1.LlamaDeploymentSpec{
						ProjectId: testProjectID,
						RepoUrl:   testRepoURL,
						GitRef:    testGitRef,
					},
				}
				Expect(k8sClient.Create(ctx, llamaDeploy)).To(Succeed())
				defer cleanupResource(testName)

				By("Reconciling to create the Deployment")
				_, err := controllerReconciler.Reconcile(ctx, reconcile.Request{
					NamespacedName: types.NamespacedName{Name: testName, Namespace: testNamespace},
				})
				Expect(err).NotTo(HaveOccurred())
				CompleteBuild(ctx, controllerReconciler, llamaDeploy)

				By("Simulating a failed deployment (Progressing=False, 0 available replicas)")
				deployment := GetDeploymentEventually(ctx, testName, testNamespace)
				deployment.Status.Conditions = []appsv1.DeploymentCondition{
					{
						Type:    appsv1.DeploymentProgressing,
						Status:  corev1.ConditionFalse,
						Reason:  "ProgressDeadlineExceeded",
						Message: "deadline exceeded",
					},
				}
				deployment.Status.AvailableReplicas = 0
				deployment.Status.ReadyReplicas = 0
				Expect(k8sClient.Status().Update(ctx, deployment)).To(Succeed())

				By("Reconciling — should detect PhaseFailed and set replicas=0")
				_, err = controllerReconciler.Reconcile(ctx, reconcile.Request{
					NamespacedName: types.NamespacedName{Name: testName, Namespace: testNamespace},
				})
				Expect(err).NotTo(HaveOccurred())

				By("Verifying the Deployment has replicas=0 and is NOT paused")
				Expect(k8sClient.Get(ctx, types.NamespacedName{Name: testName, Namespace: testNamespace}, deployment)).To(Succeed())
				Expect(*deployment.Spec.Replicas).To(Equal(int32(0)))
				Expect(deployment.Spec.Paused).To(BeFalse())

				By("Verifying FailedRolloutGeneration is set")
				Expect(k8sClient.Get(ctx, types.NamespacedName{Name: testName, Namespace: testNamespace}, llamaDeploy)).To(Succeed())
				Expect(llamaDeploy.Status.FailedRolloutGeneration).To(Equal(llamaDeploy.Generation))
				Expect(llamaDeploy.Status.Phase).To(Equal(PhaseFailed))
			})

			It("should restore replicas to 1 when spec is updated after PhaseFailed", func() {
				testName := "test-failed-recovery"

				By("Creating a LlamaDeployment and setting it to PhaseFailed")
				llamaDeploy := &llamadeployv1.LlamaDeployment{
					ObjectMeta: metav1.ObjectMeta{
						Name:      testName,
						Namespace: testNamespace,
					},
					Spec: llamadeployv1.LlamaDeploymentSpec{
						ProjectId: testProjectID,
						RepoUrl:   testRepoURL,
						GitRef:    testGitRef,
						GitSha:    testGitSha,
					},
				}
				Expect(k8sClient.Create(ctx, llamaDeploy)).To(Succeed())
				defer cleanupResource(testName)

				By("Reconciling to create the build and completing it")
				_, err := controllerReconciler.Reconcile(ctx, reconcile.Request{
					NamespacedName: types.NamespacedName{Name: testName, Namespace: testNamespace},
				})
				Expect(err).NotTo(HaveOccurred())
				CompleteBuild(ctx, controllerReconciler, llamaDeploy)

				By("Manually setting PhaseFailed with FailedRolloutGeneration and replicas=0")
				Expect(k8sClient.Get(ctx, types.NamespacedName{Name: testName, Namespace: testNamespace}, llamaDeploy)).To(Succeed())
				llamaDeploy.Status.Phase = PhaseFailed
				llamaDeploy.Status.FailedRolloutGeneration = llamaDeploy.Generation
				now := metav1.Now()
				llamaDeploy.Status.LastUpdated = &now
				Expect(k8sClient.Status().Update(ctx, llamaDeploy)).To(Succeed())

				deployment := &appsv1.Deployment{}
				Expect(k8sClient.Get(ctx, types.NamespacedName{Name: testName, Namespace: testNamespace}, deployment)).To(Succeed())
				patch := client.MergeFrom(deployment.DeepCopy())
				deployment.Spec.Replicas = ptr(int32(0))
				Expect(k8sClient.Patch(ctx, deployment, patch)).To(Succeed())

				By("Updating the spec to trigger a new generation")
				Expect(k8sClient.Get(ctx, types.NamespacedName{Name: testName, Namespace: testNamespace}, llamaDeploy)).To(Succeed())
				llamaDeploy.Spec.GitSha = testGitSha2
				Expect(k8sClient.Update(ctx, llamaDeploy)).To(Succeed())

				By("Reconciling after spec update — should start build, then restore replicas to 1")
				_, err = controllerReconciler.Reconcile(ctx, reconcile.Request{
					NamespacedName: types.NamespacedName{Name: testName, Namespace: testNamespace},
				})
				Expect(err).NotTo(HaveOccurred())
				CompleteBuild(ctx, controllerReconciler, llamaDeploy)

				By("Verifying the Deployment has replicas=1")
				Expect(k8sClient.Get(ctx, types.NamespacedName{Name: testName, Namespace: testNamespace}, deployment)).To(Succeed())
				Expect(*deployment.Spec.Replicas).To(Equal(int32(1)))
			})

			It("should unpause the Deployment when spec is updated after timeout", func() {
				testName := "test-unpause-after-timeout"
				os.Setenv(EnvRolloutTimeoutSeconds, "30")
				DeferCleanup(os.Unsetenv, EnvRolloutTimeoutSeconds)

				By("Creating and timing out a LlamaDeployment")
				llamaDeploy := &llamadeployv1.LlamaDeployment{
					ObjectMeta: metav1.ObjectMeta{
						Name:      testName,
						Namespace: testNamespace,
					},
					Spec: llamadeployv1.LlamaDeploymentSpec{
						ProjectId: testProjectID,
						RepoUrl:   testRepoURL,
						GitRef:    testGitRef,
						GitSha:    testGitSha,
					},
				}
				Expect(k8sClient.Create(ctx, llamaDeploy)).To(Succeed())
				defer cleanupResource(testName)

				// Reconcile to create build, complete it, then proceed
				_, err := controllerReconciler.Reconcile(ctx, reconcile.Request{
					NamespacedName: types.NamespacedName{Name: testName, Namespace: testNamespace},
				})
				Expect(err).NotTo(HaveOccurred())
				CompleteBuild(ctx, controllerReconciler, llamaDeploy)

				// Set available replicas so timeout picks RolloutFailed (pauses deployment)
				SetDeploymentAvailableReplicas(ctx, testName, testNamespace, 1)

				Expect(k8sClient.Get(ctx, types.NamespacedName{Name: testName, Namespace: testNamespace}, llamaDeploy)).To(Succeed())
				past := metav1.NewTime(time.Now().Add(-60 * time.Second))
				llamaDeploy.Status.RolloutStartedAt = &past
				Expect(k8sClient.Status().Update(ctx, llamaDeploy)).To(Succeed())

				_, err = controllerReconciler.Reconcile(ctx, reconcile.Request{
					NamespacedName: types.NamespacedName{Name: testName, Namespace: testNamespace},
				})
				Expect(err).NotTo(HaveOccurred())

				By("Verifying the Deployment is paused")
				deployment := &appsv1.Deployment{}
				Expect(k8sClient.Get(ctx, types.NamespacedName{Name: testName, Namespace: testNamespace}, deployment)).To(Succeed())
				Expect(deployment.Spec.Paused).To(BeTrue())

				By("Updating the spec to trigger a new generation")
				Expect(k8sClient.Get(ctx, types.NamespacedName{Name: testName, Namespace: testNamespace}, llamaDeploy)).To(Succeed())
				llamaDeploy.Spec.GitSha = testGitSha2
				Expect(k8sClient.Update(ctx, llamaDeploy)).To(Succeed())

				By("Reconciling after spec update — should start build, then unpause and proceed")
				_, err = controllerReconciler.Reconcile(ctx, reconcile.Request{
					NamespacedName: types.NamespacedName{Name: testName, Namespace: testNamespace},
				})
				Expect(err).NotTo(HaveOccurred())
				CompleteBuild(ctx, controllerReconciler, llamaDeploy)

				By("Verifying the Deployment is no longer paused")
				Expect(k8sClient.Get(ctx, types.NamespacedName{Name: testName, Namespace: testNamespace}, deployment)).To(Succeed())
				Expect(deployment.Spec.Paused).To(BeFalse())
			})

			It("should NOT unpause the Deployment when re-reconciling the same failed generation", func() {
				testName := "test-no-unpause-same-gen"
				os.Setenv(EnvRolloutTimeoutSeconds, "30")
				DeferCleanup(os.Unsetenv, EnvRolloutTimeoutSeconds)

				By("Creating a LlamaDeployment and getting it to RolloutFailed state")
				llamaDeploy := &llamadeployv1.LlamaDeployment{
					ObjectMeta: metav1.ObjectMeta{
						Name:      testName,
						Namespace: testNamespace,
					},
					Spec: llamadeployv1.LlamaDeploymentSpec{
						ProjectId: testProjectID,
						RepoUrl:   testRepoURL,
						GitRef:    testGitRef,
						GitSha:    testGitSha,
					},
				}
				Expect(k8sClient.Create(ctx, llamaDeploy)).To(Succeed())
				defer cleanupResource(testName)

				// Initial reconcile to create build, then complete it
				_, err := controllerReconciler.Reconcile(ctx, reconcile.Request{
					NamespacedName: types.NamespacedName{Name: testName, Namespace: testNamespace},
				})
				Expect(err).NotTo(HaveOccurred())
				CompleteBuild(ctx, controllerReconciler, llamaDeploy)

				// Set available replicas so timeout picks RolloutFailed
				SetDeploymentAvailableReplicas(ctx, testName, testNamespace, 1)

				// Backdate rollout start to trigger timeout
				Expect(k8sClient.Get(ctx, types.NamespacedName{Name: testName, Namespace: testNamespace}, llamaDeploy)).To(Succeed())
				past := metav1.NewTime(time.Now().Add(-60 * time.Second))
				llamaDeploy.Status.RolloutStartedAt = &past
				Expect(k8sClient.Status().Update(ctx, llamaDeploy)).To(Succeed())

				// Reconcile to trigger timeout — this pauses the deployment
				_, err = controllerReconciler.Reconcile(ctx, reconcile.Request{
					NamespacedName: types.NamespacedName{Name: testName, Namespace: testNamespace},
				})
				Expect(err).NotTo(HaveOccurred())

				By("Verifying the Deployment is paused and phase is RolloutFailed")
				deployment := &appsv1.Deployment{}
				Expect(k8sClient.Get(ctx, types.NamespacedName{Name: testName, Namespace: testNamespace}, deployment)).To(Succeed())
				Expect(deployment.Spec.Paused).To(BeTrue())

				Expect(k8sClient.Get(ctx, types.NamespacedName{Name: testName, Namespace: testNamespace}, llamaDeploy)).To(Succeed())
				Expect(llamaDeploy.Status.Phase).To(Equal(PhaseRolloutFailed))
				Expect(llamaDeploy.Status.FailedRolloutGeneration).To(Equal(llamaDeploy.Generation))

				By("Re-reconciling the SAME generation (simulates a racing reconcile)")
				_, err = controllerReconciler.Reconcile(ctx, reconcile.Request{
					NamespacedName: types.NamespacedName{Name: testName, Namespace: testNamespace},
				})
				Expect(err).NotTo(HaveOccurred())

				By("Verifying the Deployment is STILL paused — must not unpause for same generation")
				Expect(k8sClient.Get(ctx, types.NamespacedName{Name: testName, Namespace: testNamespace}, deployment)).To(Succeed())
				Expect(deployment.Spec.Paused).To(BeTrue(), "Deployment should remain paused when re-reconciling a failed generation")
			})

			It("should NOT unpause the Deployment when reconciling with stale cache (FailedRolloutGeneration not yet visible)", func() {
				// This reproduces the race condition observed in production:
				// 1. Reconcile A: timeout fires, pauses deployment, scales down RS, sets FailedRolloutGeneration
				// 2. Reconcile B: triggered by the deployment pause event, reads stale LlamaDeployment
				//    where FailedRolloutGeneration is NOT set yet — unpauses the deployment, undoing the rollback
				testName := "test-no-unpause-stale"
				os.Setenv(EnvRolloutTimeoutSeconds, "30")
				DeferCleanup(os.Unsetenv, EnvRolloutTimeoutSeconds)

				By("Creating a LlamaDeployment")
				llamaDeploy := &llamadeployv1.LlamaDeployment{
					ObjectMeta: metav1.ObjectMeta{
						Name:      testName,
						Namespace: testNamespace,
					},
					Spec: llamadeployv1.LlamaDeploymentSpec{
						ProjectId: testProjectID,
						RepoUrl:   testRepoURL,
						GitRef:    testGitRef,
						GitSha:    testGitSha,
					},
				}
				Expect(k8sClient.Create(ctx, llamaDeploy)).To(Succeed())
				defer cleanupResource(testName)

				// Initial reconcile to create build, then complete it
				_, err := controllerReconciler.Reconcile(ctx, reconcile.Request{
					NamespacedName: types.NamespacedName{Name: testName, Namespace: testNamespace},
				})
				Expect(err).NotTo(HaveOccurred())
				CompleteBuild(ctx, controllerReconciler, llamaDeploy)

				By("Manually setting up the stale-cache state: deployment paused, but FailedRolloutGeneration=0")
				// Pause the Deployment (as the timeout handler would)
				deployment := &appsv1.Deployment{}
				Expect(k8sClient.Get(ctx, types.NamespacedName{Name: testName, Namespace: testNamespace}, deployment)).To(Succeed())
				patch := client.MergeFrom(deployment.DeepCopy())
				deployment.Spec.Paused = true
				Expect(k8sClient.Patch(ctx, deployment, patch)).To(Succeed())

				// Set phase to RollingOut (the phase the stale reconcile would see
				// since it reads the object before the timeout handler's status update lands)
				Expect(k8sClient.Get(ctx, types.NamespacedName{Name: testName, Namespace: testNamespace}, llamaDeploy)).To(Succeed())
				llamaDeploy.Status.Phase = PhaseRollingOut
				llamaDeploy.Status.FailedRolloutGeneration = 0 // stale: not yet updated
				now := metav1.Now()
				llamaDeploy.Status.RolloutStartedAt = &now
				Expect(k8sClient.Status().Update(ctx, llamaDeploy)).To(Succeed())

				By("Reconciling with this stale state — must NOT unpause the deployment")
				_, err = controllerReconciler.Reconcile(ctx, reconcile.Request{
					NamespacedName: types.NamespacedName{Name: testName, Namespace: testNamespace},
				})
				Expect(err).NotTo(HaveOccurred())

				By("Verifying the Deployment is STILL paused")
				Expect(k8sClient.Get(ctx, types.NamespacedName{Name: testName, Namespace: testNamespace}, deployment)).To(Succeed())
				Expect(deployment.Spec.Paused).To(BeTrue(), "Deployment must remain paused — reconcileDeployment should not unpause when deployment is paused by timeout handler")
			})

			It("should NOT scale down when pods are evicted (infrastructure failure)", func() {
				testName := "test-infra-evicted"

				By("Creating a LlamaDeployment resource")
				llamaDeploy := &llamadeployv1.LlamaDeployment{
					ObjectMeta: metav1.ObjectMeta{
						Name:      testName,
						Namespace: testNamespace,
					},
					Spec: llamadeployv1.LlamaDeploymentSpec{
						ProjectId: testProjectID,
						RepoUrl:   testRepoURL,
						GitRef:    testGitRef,
					},
				}
				Expect(k8sClient.Create(ctx, llamaDeploy)).To(Succeed())
				defer cleanupResource(testName)

				By("Reconciling to create the Deployment")
				_, err := controllerReconciler.Reconcile(ctx, reconcile.Request{
					NamespacedName: types.NamespacedName{Name: testName, Namespace: testNamespace},
				})
				Expect(err).NotTo(HaveOccurred())
				CompleteBuild(ctx, controllerReconciler, llamaDeploy)

				By("Getting the Deployment")
				deployment := GetDeploymentEventually(ctx, testName, testNamespace)

				By("Creating an old ReplicaSet (healthy, serving traffic)")
				oldRS := &appsv1.ReplicaSet{
					ObjectMeta: metav1.ObjectMeta{
						Name:      testName + "-old",
						Namespace: testNamespace,
						Labels:    map[string]string{"app": testName},
						Annotations: map[string]string{
							"deployment.kubernetes.io/revision": "1",
						},
						OwnerReferences: []metav1.OwnerReference{
							{
								APIVersion: "apps/v1",
								Kind:       "Deployment",
								Name:       deployment.Name,
								UID:        deployment.UID,
							},
						},
					},
					Spec: appsv1.ReplicaSetSpec{
						Replicas: ptr(int32(1)),
						Selector: &metav1.LabelSelector{
							MatchLabels: map[string]string{"app": testName},
						},
						Template: deployment.Spec.Template,
					},
				}
				Expect(k8sClient.Create(ctx, oldRS)).To(Succeed())
				defer func() { _ = k8sClient.Delete(ctx, oldRS) }()
				oldRS.Status.Replicas = 1
				oldRS.Status.AvailableReplicas = 1
				oldRS.Status.ReadyReplicas = 1
				Expect(k8sClient.Status().Update(ctx, oldRS)).To(Succeed())

				By("Creating a new ReplicaSet (failing)")
				newRS := &appsv1.ReplicaSet{
					ObjectMeta: metav1.ObjectMeta{
						Name:      testName + "-new",
						Namespace: testNamespace,
						Labels:    map[string]string{"app": testName},
						Annotations: map[string]string{
							"deployment.kubernetes.io/revision": "2",
						},
						OwnerReferences: []metav1.OwnerReference{
							{
								APIVersion: "apps/v1",
								Kind:       "Deployment",
								Name:       deployment.Name,
								UID:        deployment.UID,
							},
						},
					},
					Spec: appsv1.ReplicaSetSpec{
						Replicas: ptr(int32(1)),
						Selector: &metav1.LabelSelector{
							MatchLabels: map[string]string{"app": testName},
						},
						Template: deployment.Spec.Template,
					},
				}
				Expect(k8sClient.Create(ctx, newRS)).To(Succeed())
				defer func() { _ = k8sClient.Delete(ctx, newRS) }()
				newRS.Status.Replicas = 1
				newRS.Status.AvailableReplicas = 0
				newRS.Status.ReadyReplicas = 0
				Expect(k8sClient.Status().Update(ctx, newRS)).To(Succeed())

				By("Creating an evicted Pod")
				pod := &corev1.Pod{
					ObjectMeta: metav1.ObjectMeta{
						Name:      testName + "-evicted",
						Namespace: testNamespace,
						Labels:    map[string]string{"app": testName},
					},
					Spec: corev1.PodSpec{
						Containers: []corev1.Container{
							{Name: "app", Image: "busybox"},
						},
					},
				}
				Expect(k8sClient.Create(ctx, pod)).To(Succeed())
				defer func() { _ = k8sClient.Delete(ctx, pod) }()
				pod.Status.Phase = corev1.PodFailed
				pod.Status.Reason = "Evicted"
				Expect(k8sClient.Status().Update(ctx, pod)).To(Succeed())

				By("Simulating ProgressDeadlineExceeded on the Deployment")
				Expect(k8sClient.Get(ctx, types.NamespacedName{Name: testName, Namespace: testNamespace}, deployment)).To(Succeed())
				deployment.Status.ReadyReplicas = 1
				deployment.Status.AvailableReplicas = 1
				deployment.Status.Replicas = 2
				deployment.Status.Conditions = []appsv1.DeploymentCondition{
					{Type: appsv1.DeploymentProgressing, Status: corev1.ConditionFalse, Reason: "ProgressDeadlineExceeded"},
				}
				Expect(k8sClient.Status().Update(ctx, deployment)).To(Succeed())

				By("Reconciling — should NOT scale down due to infrastructure failure")
				result, err := controllerReconciler.Reconcile(ctx, reconcile.Request{
					NamespacedName: types.NamespacedName{Name: testName, Namespace: testNamespace},
				})
				Expect(err).NotTo(HaveOccurred())

				By("Verifying RequeueAfter is 30 seconds (infrastructure requeue)")
				Expect(result.RequeueAfter).To(Equal(30 * time.Second))

				By("Verifying FailedRolloutGeneration is NOT set")
				Expect(k8sClient.Get(ctx, types.NamespacedName{Name: testName, Namespace: testNamespace}, llamaDeploy)).To(Succeed())
				Expect(llamaDeploy.Status.FailedRolloutGeneration).To(Equal(int64(0)))

				By("Verifying the Deployment is NOT paused")
				Expect(k8sClient.Get(ctx, types.NamespacedName{Name: testName, Namespace: testNamespace}, deployment)).To(Succeed())
				Expect(deployment.Spec.Paused).To(BeFalse())

				By("Verifying an InfrastructureIssue event was recorded")
				recorder := controllerReconciler.Recorder.(*record.FakeRecorder)
				var found bool
				for len(recorder.Events) > 0 {
					event := <-recorder.Events
					if strings.Contains(event, "InfrastructureIssue") {
						found = true
					}
				}
				Expect(found).To(BeTrue())
			})

			It("should scale down when pods have CrashLoopBackOff (app failure)", func() {
				testName := "test-infra-crashloop"

				By("Creating a LlamaDeployment resource")
				llamaDeploy := &llamadeployv1.LlamaDeployment{
					ObjectMeta: metav1.ObjectMeta{
						Name:      testName,
						Namespace: testNamespace,
					},
					Spec: llamadeployv1.LlamaDeploymentSpec{
						ProjectId: testProjectID,
						RepoUrl:   testRepoURL,
						GitRef:    testGitRef,
					},
				}
				Expect(k8sClient.Create(ctx, llamaDeploy)).To(Succeed())
				defer cleanupResource(testName)

				By("Reconciling to create the Deployment")
				_, err := controllerReconciler.Reconcile(ctx, reconcile.Request{
					NamespacedName: types.NamespacedName{Name: testName, Namespace: testNamespace},
				})
				Expect(err).NotTo(HaveOccurred())
				CompleteBuild(ctx, controllerReconciler, llamaDeploy)

				By("Getting the Deployment")
				deployment := GetDeploymentEventually(ctx, testName, testNamespace)

				By("Creating an old ReplicaSet (healthy, serving traffic)")
				oldRS := &appsv1.ReplicaSet{
					ObjectMeta: metav1.ObjectMeta{
						Name:      testName + "-old",
						Namespace: testNamespace,
						Labels:    map[string]string{"app": testName},
						Annotations: map[string]string{
							"deployment.kubernetes.io/revision": "1",
						},
						OwnerReferences: []metav1.OwnerReference{
							{
								APIVersion: "apps/v1",
								Kind:       "Deployment",
								Name:       deployment.Name,
								UID:        deployment.UID,
							},
						},
					},
					Spec: appsv1.ReplicaSetSpec{
						Replicas: ptr(int32(1)),
						Selector: &metav1.LabelSelector{
							MatchLabels: map[string]string{"app": testName},
						},
						Template: deployment.Spec.Template,
					},
				}
				Expect(k8sClient.Create(ctx, oldRS)).To(Succeed())
				defer func() { _ = k8sClient.Delete(ctx, oldRS) }()
				oldRS.Status.Replicas = 1
				oldRS.Status.AvailableReplicas = 1
				oldRS.Status.ReadyReplicas = 1
				Expect(k8sClient.Status().Update(ctx, oldRS)).To(Succeed())

				By("Creating a new ReplicaSet (failing)")
				newRS := &appsv1.ReplicaSet{
					ObjectMeta: metav1.ObjectMeta{
						Name:      testName + "-new",
						Namespace: testNamespace,
						Labels:    map[string]string{"app": testName},
						Annotations: map[string]string{
							"deployment.kubernetes.io/revision": "2",
						},
						OwnerReferences: []metav1.OwnerReference{
							{
								APIVersion: "apps/v1",
								Kind:       "Deployment",
								Name:       deployment.Name,
								UID:        deployment.UID,
							},
						},
					},
					Spec: appsv1.ReplicaSetSpec{
						Replicas: ptr(int32(1)),
						Selector: &metav1.LabelSelector{
							MatchLabels: map[string]string{"app": testName},
						},
						Template: deployment.Spec.Template,
					},
				}
				Expect(k8sClient.Create(ctx, newRS)).To(Succeed())
				defer func() { _ = k8sClient.Delete(ctx, newRS) }()
				newRS.Status.Replicas = 1
				newRS.Status.AvailableReplicas = 0
				newRS.Status.ReadyReplicas = 0
				Expect(k8sClient.Status().Update(ctx, newRS)).To(Succeed())

				By("Creating a Pod with CrashLoopBackOff")
				pod := &corev1.Pod{
					ObjectMeta: metav1.ObjectMeta{
						Name:      testName + "-crash",
						Namespace: testNamespace,
						Labels:    map[string]string{"app": testName},
						OwnerReferences: []metav1.OwnerReference{
							{
								APIVersion: "apps/v1",
								Kind:       "ReplicaSet",
								Name:       newRS.Name,
								UID:        newRS.UID,
								Controller: ptr(true),
							},
						},
					},
					Spec: corev1.PodSpec{
						Containers: []corev1.Container{
							{Name: "app", Image: "busybox"},
						},
					},
				}
				Expect(k8sClient.Create(ctx, pod)).To(Succeed())
				defer func() { _ = k8sClient.Delete(ctx, pod) }()
				pod.Status.Phase = corev1.PodRunning
				pod.Status.ContainerStatuses = []corev1.ContainerStatus{
					{
						Name: "app",
						State: corev1.ContainerState{
							Waiting: &corev1.ContainerStateWaiting{
								Reason: "CrashLoopBackOff",
							},
						},
					},
				}
				Expect(k8sClient.Status().Update(ctx, pod)).To(Succeed())

				By("Keeping the Deployment rollout in progress")
				Expect(k8sClient.Get(ctx, types.NamespacedName{Name: testName, Namespace: testNamespace}, deployment)).To(Succeed())
				deployment.Status.ReadyReplicas = 1
				deployment.Status.AvailableReplicas = 1
				deployment.Status.Replicas = 2
				deployment.Status.Conditions = []appsv1.DeploymentCondition{
					{Type: appsv1.DeploymentProgressing, Status: corev1.ConditionTrue, Reason: "ReplicaSetUpdated"},
				}
				Expect(k8sClient.Status().Update(ctx, deployment)).To(Succeed())

				By("Reconciling — should proceed with scale-down for app failure")
				_, err = controllerReconciler.Reconcile(ctx, reconcile.Request{
					NamespacedName: types.NamespacedName{Name: testName, Namespace: testNamespace},
				})
				Expect(err).NotTo(HaveOccurred())

				By("Verifying FailedRolloutGeneration is set")
				Expect(k8sClient.Get(ctx, types.NamespacedName{Name: testName, Namespace: testNamespace}, llamaDeploy)).To(Succeed())
				Expect(llamaDeploy.Status.FailedRolloutGeneration).To(Equal(llamaDeploy.Generation))

				By("Verifying the Deployment is paused")
				Expect(k8sClient.Get(ctx, types.NamespacedName{Name: testName, Namespace: testNamespace}, deployment)).To(Succeed())
				Expect(deployment.Spec.Paused).To(BeTrue())

				By("Verifying the new ReplicaSet was scaled to 0")
				Eventually(func() int32 {
					rs := &appsv1.ReplicaSet{}
					err := k8sClient.Get(ctx, types.NamespacedName{Name: testName + "-new", Namespace: testNamespace}, rs)
					if err != nil {
						return -1
					}
					if rs.Spec.Replicas == nil {
						return 1
					}
					return *rs.Spec.Replicas
				}).Should(Equal(int32(0)))
			})

			It("should scale down with mixed evicted and CrashLoopBackOff pods", func() {
				testName := "test-infra-mixed"

				By("Creating a LlamaDeployment resource")
				llamaDeploy := &llamadeployv1.LlamaDeployment{
					ObjectMeta: metav1.ObjectMeta{
						Name:      testName,
						Namespace: testNamespace,
					},
					Spec: llamadeployv1.LlamaDeploymentSpec{
						ProjectId: testProjectID,
						RepoUrl:   testRepoURL,
						GitRef:    testGitRef,
					},
				}
				Expect(k8sClient.Create(ctx, llamaDeploy)).To(Succeed())
				defer cleanupResource(testName)

				By("Reconciling to create the Deployment")
				_, err := controllerReconciler.Reconcile(ctx, reconcile.Request{
					NamespacedName: types.NamespacedName{Name: testName, Namespace: testNamespace},
				})
				Expect(err).NotTo(HaveOccurred())
				CompleteBuild(ctx, controllerReconciler, llamaDeploy)

				By("Getting the Deployment")
				deployment := GetDeploymentEventually(ctx, testName, testNamespace)

				By("Creating an old ReplicaSet (healthy, serving traffic)")
				oldRS := &appsv1.ReplicaSet{
					ObjectMeta: metav1.ObjectMeta{
						Name:      testName + "-old",
						Namespace: testNamespace,
						Labels:    map[string]string{"app": testName},
						Annotations: map[string]string{
							"deployment.kubernetes.io/revision": "1",
						},
						OwnerReferences: []metav1.OwnerReference{
							{
								APIVersion: "apps/v1",
								Kind:       "Deployment",
								Name:       deployment.Name,
								UID:        deployment.UID,
							},
						},
					},
					Spec: appsv1.ReplicaSetSpec{
						Replicas: ptr(int32(1)),
						Selector: &metav1.LabelSelector{
							MatchLabels: map[string]string{"app": testName},
						},
						Template: deployment.Spec.Template,
					},
				}
				Expect(k8sClient.Create(ctx, oldRS)).To(Succeed())
				defer func() { _ = k8sClient.Delete(ctx, oldRS) }()
				oldRS.Status.Replicas = 1
				oldRS.Status.AvailableReplicas = 1
				oldRS.Status.ReadyReplicas = 1
				Expect(k8sClient.Status().Update(ctx, oldRS)).To(Succeed())

				By("Creating a new ReplicaSet (failing)")
				newRS := &appsv1.ReplicaSet{
					ObjectMeta: metav1.ObjectMeta{
						Name:      testName + "-new",
						Namespace: testNamespace,
						Labels:    map[string]string{"app": testName},
						Annotations: map[string]string{
							"deployment.kubernetes.io/revision": "2",
						},
						OwnerReferences: []metav1.OwnerReference{
							{
								APIVersion: "apps/v1",
								Kind:       "Deployment",
								Name:       deployment.Name,
								UID:        deployment.UID,
							},
						},
					},
					Spec: appsv1.ReplicaSetSpec{
						Replicas: ptr(int32(1)),
						Selector: &metav1.LabelSelector{
							MatchLabels: map[string]string{"app": testName},
						},
						Template: deployment.Spec.Template,
					},
				}
				Expect(k8sClient.Create(ctx, newRS)).To(Succeed())
				defer func() { _ = k8sClient.Delete(ctx, newRS) }()
				newRS.Status.Replicas = 1
				newRS.Status.AvailableReplicas = 0
				newRS.Status.ReadyReplicas = 0
				Expect(k8sClient.Status().Update(ctx, newRS)).To(Succeed())

				By("Creating an evicted Pod")
				evictedPod := &corev1.Pod{
					ObjectMeta: metav1.ObjectMeta{
						Name:      testName + "-evicted",
						Namespace: testNamespace,
						Labels:    map[string]string{"app": testName},
					},
					Spec: corev1.PodSpec{
						Containers: []corev1.Container{
							{Name: "app", Image: "busybox"},
						},
					},
				}
				Expect(k8sClient.Create(ctx, evictedPod)).To(Succeed())
				defer func() { _ = k8sClient.Delete(ctx, evictedPod) }()
				evictedPod.Status.Phase = corev1.PodFailed
				evictedPod.Status.Reason = "Evicted"
				Expect(k8sClient.Status().Update(ctx, evictedPod)).To(Succeed())

				By("Creating a Pod with CrashLoopBackOff")
				crashPod := &corev1.Pod{
					ObjectMeta: metav1.ObjectMeta{
						Name:      testName + "-crash",
						Namespace: testNamespace,
						Labels:    map[string]string{"app": testName},
					},
					Spec: corev1.PodSpec{
						Containers: []corev1.Container{
							{Name: "app", Image: "busybox"},
						},
					},
				}
				Expect(k8sClient.Create(ctx, crashPod)).To(Succeed())
				defer func() { _ = k8sClient.Delete(ctx, crashPod) }()
				crashPod.Status.Phase = corev1.PodRunning
				crashPod.Status.ContainerStatuses = []corev1.ContainerStatus{
					{
						Name: "app",
						State: corev1.ContainerState{
							Waiting: &corev1.ContainerStateWaiting{
								Reason: "CrashLoopBackOff",
							},
						},
					},
				}
				Expect(k8sClient.Status().Update(ctx, crashPod)).To(Succeed())

				By("Simulating ProgressDeadlineExceeded on the Deployment")
				Expect(k8sClient.Get(ctx, types.NamespacedName{Name: testName, Namespace: testNamespace}, deployment)).To(Succeed())
				deployment.Status.ReadyReplicas = 1
				deployment.Status.AvailableReplicas = 1
				deployment.Status.Replicas = 2
				deployment.Status.Conditions = []appsv1.DeploymentCondition{
					{Type: appsv1.DeploymentProgressing, Status: corev1.ConditionFalse, Reason: "ProgressDeadlineExceeded"},
				}
				Expect(k8sClient.Status().Update(ctx, deployment)).To(Succeed())

				By("Reconciling — should proceed with scale-down (app failure wins over infra)")
				_, err = controllerReconciler.Reconcile(ctx, reconcile.Request{
					NamespacedName: types.NamespacedName{Name: testName, Namespace: testNamespace},
				})
				Expect(err).NotTo(HaveOccurred())

				By("Verifying FailedRolloutGeneration is set")
				Expect(k8sClient.Get(ctx, types.NamespacedName{Name: testName, Namespace: testNamespace}, llamaDeploy)).To(Succeed())
				Expect(llamaDeploy.Status.FailedRolloutGeneration).To(Equal(llamaDeploy.Generation))

				By("Verifying the Deployment is paused")
				Expect(k8sClient.Get(ctx, types.NamespacedName{Name: testName, Namespace: testNamespace}, deployment)).To(Succeed())
				Expect(deployment.Spec.Paused).To(BeTrue())

				By("Verifying the new ReplicaSet was scaled to 0")
				Eventually(func() int32 {
					rs := &appsv1.ReplicaSet{}
					err := k8sClient.Get(ctx, types.NamespacedName{Name: testName + "-new", Namespace: testNamespace}, rs)
					if err != nil {
						return -1
					}
					if rs.Spec.Replicas == nil {
						return 1
					}
					return *rs.Spec.Replicas
				}).Should(Equal(int32(0)))
			})
		})

		Describe("DNS-1035 name validation", func() {
			It("should mark deployment as Failed and tear down resources for non-compliant name", func() {
				// "123-invalid-dns" is valid RFC-1123 (k8s metadata.name) but
				// NOT valid DNS-1035 (starts with digit), which Kubernetes
				// requires for Service names.
				testName := "123-invalid-dns"

				By("Pre-creating resources that simulate prior operator reconciliation")
				sa := &corev1.ServiceAccount{
					ObjectMeta: metav1.ObjectMeta{Name: testName + "-sa", Namespace: testNamespace},
				}
				Expect(k8sClient.Create(ctx, sa)).To(Succeed())

				cm := &corev1.ConfigMap{
					ObjectMeta: metav1.ObjectMeta{Name: testName + "-nginx-config", Namespace: testNamespace},
					Data:       map[string]string{"nginx.conf": "fake"},
				}
				Expect(k8sClient.Create(ctx, cm)).To(Succeed())

				replicas := int32(1)
				dep := &appsv1.Deployment{
					ObjectMeta: metav1.ObjectMeta{Name: testName, Namespace: testNamespace},
					Spec: appsv1.DeploymentSpec{
						Replicas: &replicas,
						Selector: &metav1.LabelSelector{MatchLabels: map[string]string{"app": testName}},
						Template: corev1.PodTemplateSpec{
							ObjectMeta: metav1.ObjectMeta{Labels: map[string]string{"app": testName}},
							Spec:       corev1.PodSpec{Containers: []corev1.Container{{Name: "app", Image: "busybox"}}},
						},
					},
				}
				Expect(k8sClient.Create(ctx, dep)).To(Succeed())

				By("Creating LlamaDeployment with non-compliant DNS-1035 name")
				llamaDeploy := &llamadeployv1.LlamaDeployment{
					ObjectMeta: metav1.ObjectMeta{
						Name:      testName,
						Namespace: testNamespace,
					},
					Spec: llamadeployv1.LlamaDeploymentSpec{
						ProjectId: testProjectID,
						RepoUrl:   testRepoURL,
						GitRef:    testGitRef,
					},
				}
				Expect(k8sClient.Create(ctx, llamaDeploy)).To(Succeed())
				defer cleanupResource(testName)

				By("Reconciling the LlamaDeployment")
				result, err := controllerReconciler.Reconcile(ctx, reconcile.Request{
					NamespacedName: types.NamespacedName{Name: testName, Namespace: testNamespace},
				})
				Expect(err).NotTo(HaveOccurred())
				Expect(result.Requeue).To(BeFalse())

				By("Verifying status is Failed with DNS-1035 message")
				updatedDeploy := &llamadeployv1.LlamaDeployment{}
				Expect(k8sClient.Get(ctx, types.NamespacedName{Name: testName, Namespace: testNamespace}, updatedDeploy)).To(Succeed())
				Expect(updatedDeploy.Status.Phase).To(Equal(PhaseFailed))
				Expect(updatedDeploy.Status.Message).To(ContainSubstring("not a valid DNS-1035 label"))

				By("Verifying Deployment was torn down")
				Eventually(func() bool {
					err := k8sClient.Get(ctx, types.NamespacedName{Name: testName, Namespace: testNamespace}, &appsv1.Deployment{})
					return errors.IsNotFound(err)
				}).Should(BeTrue())

				By("Verifying ConfigMap was torn down")
				Eventually(func() bool {
					err := k8sClient.Get(ctx, types.NamespacedName{Name: testName + "-nginx-config", Namespace: testNamespace}, &corev1.ConfigMap{})
					return errors.IsNotFound(err)
				}).Should(BeTrue())

				By("Verifying ServiceAccount was torn down")
				Eventually(func() bool {
					err := k8sClient.Get(ctx, types.NamespacedName{Name: testName + "-sa", Namespace: testNamespace}, &corev1.ServiceAccount{})
					return errors.IsNotFound(err)
				}).Should(BeTrue())

				By("Verifying reconciliation is idempotent on second run")
				result, err = controllerReconciler.Reconcile(ctx, reconcile.Request{
					NamespacedName: types.NamespacedName{Name: testName, Namespace: testNamespace},
				})
				Expect(err).NotTo(HaveOccurred())
				Expect(result.Requeue).To(BeFalse())
			})

			It("should not tear down resources for a valid DNS-1035 name", func() {
				testName := "valid-dns-name"

				By("Creating and reconciling a valid LlamaDeployment")
				llamaDeploy := &llamadeployv1.LlamaDeployment{
					ObjectMeta: metav1.ObjectMeta{
						Name:      testName,
						Namespace: testNamespace,
					},
					Spec: llamadeployv1.LlamaDeploymentSpec{
						ProjectId: testProjectID,
						RepoUrl:   testRepoURL,
						GitRef:    testGitRef,
					},
				}
				Expect(k8sClient.Create(ctx, llamaDeploy)).To(Succeed())
				defer cleanupResource(testName)

				_, err := controllerReconciler.Reconcile(ctx, reconcile.Request{
					NamespacedName: types.NamespacedName{Name: testName, Namespace: testNamespace},
				})
				Expect(err).NotTo(HaveOccurred())
				CompleteBuild(ctx, controllerReconciler, llamaDeploy)

				By("Verifying resources were created (not torn down)")
				GetDeploymentEventually(ctx, testName, testNamespace)
				GetConfigMapEventually(ctx, testName+"-nginx-config", testNamespace)
				saObj := &corev1.ServiceAccount{}
				Eventually(func() error {
					return k8sClient.Get(ctx, types.NamespacedName{Name: testName + "-sa", Namespace: testNamespace}, saObj)
				}).Should(Succeed())

				By("Verifying status is NOT Failed")
				updatedDeploy := &llamadeployv1.LlamaDeployment{}
				Expect(k8sClient.Get(ctx, types.NamespacedName{Name: testName, Namespace: testNamespace}, updatedDeploy)).To(Succeed())
				Expect(updatedDeploy.Status.Phase).NotTo(Equal(PhaseFailed))
			})
		})

		Describe("Nginx container configuration", func() {
			It("should serve static assets when StaticAssetsPath is set", func() {
				testName := "test-nginx-static-assets"

				ld := &llamadeployv1.LlamaDeployment{
					ObjectMeta: metav1.ObjectMeta{
						Name:      testName,
						Namespace: testNamespace,
					},
					Spec: llamadeployv1.LlamaDeploymentSpec{
						ProjectId:        testProjectID,
						RepoUrl:          testRepoURL,
						GitRef:           testGitRef,
						StaticAssetsPath: "frontend/dist",
					},
				}
				Expect(k8sClient.Create(ctx, ld)).To(Succeed())
				defer cleanupResource(testName)

				_, err := controllerReconciler.Reconcile(ctx, reconcile.Request{NamespacedName: types.NamespacedName{Name: testName, Namespace: testNamespace}})
				Expect(err).NotTo(HaveOccurred())
				CompleteBuild(ctx, controllerReconciler, ld)

				// Verify ConfigMap was created with nginx configuration
				configMap := GetConfigMapEventually(ctx, testName+"-nginx-config", testNamespace)
				Expect(configMap.Data).To(HaveKey("nginx.conf"))
				nginxConf := configMap.Data["nginx.conf"]
				// Should alias static assets path and include try_files fallback to @python_upstream
				Expect(nginxConf).To(ContainSubstring("location /deployments/" + testName + "/ui {"))
				Expect(nginxConf).To(ContainSubstring("alias /opt/app/frontend/dist/;"))
				Expect(nginxConf).To(ContainSubstring("try_files $uri $uri/ /index.html @python_upstream;"))
				// Everything else proxies to python app
				Expect(nginxConf).To(ContainSubstring("location / { proxy_pass http://127.0.0.1:8080;"))
				// Named upstream for fallback proxies to 8081
				Expect(nginxConf).To(ContainSubstring("location @python_upstream { proxy_pass http://127.0.0.1:8081;"))

				// Verify deployment uses ConfigMap mount
				deployment := GetDeploymentEventually(ctx, testName, testNamespace)
				var nginx corev1.Container
				for _, c := range deployment.Spec.Template.Spec.Containers {
					if c.Name == "file-server" {
						nginx = c
						break
					}
				}
				Expect(nginx.Name).To(Equal("file-server"))
				Expect(nginx.Command).To(Equal([]string{"nginx", "-g", "daemon off;"}))
				Expect(nginx.VolumeMounts).To(ContainElement(corev1.VolumeMount{
					Name:      "nginx-config",
					MountPath: "/etc/nginx/nginx.conf",
					SubPath:   "nginx.conf",
				}))

				// Verify nginx-config volume is mounted
				var nginxConfigVolumeFound bool
				for _, vol := range deployment.Spec.Template.Spec.Volumes {
					if vol.Name == "nginx-config" && vol.ConfigMap != nil &&
						vol.ConfigMap.Name == testName+"-nginx-config" {
						nginxConfigVolumeFound = true
						break
					}
				}
				Expect(nginxConfigVolumeFound).To(BeTrue(), "nginx-config volume should be present")
			})

			It("should proxy UI base when StaticAssetsPath is not set", func() {
				testName := "test-nginx-proxy-ui"

				ld := &llamadeployv1.LlamaDeployment{
					ObjectMeta: metav1.ObjectMeta{
						Name:      testName,
						Namespace: testNamespace,
					},
					Spec: llamadeployv1.LlamaDeploymentSpec{
						ProjectId: testProjectID,
						RepoUrl:   testRepoURL,
						GitRef:    testGitRef,
						// StaticAssetsPath unset
					},
				}
				Expect(k8sClient.Create(ctx, ld)).To(Succeed())
				defer cleanupResource(testName)

				_, err := controllerReconciler.Reconcile(ctx, reconcile.Request{NamespacedName: types.NamespacedName{Name: testName, Namespace: testNamespace}})
				Expect(err).NotTo(HaveOccurred())
				CompleteBuild(ctx, controllerReconciler, ld)

				// Verify ConfigMap was created with nginx configuration
				configMap := GetConfigMapEventually(ctx, testName+"-nginx-config", testNamespace)
				Expect(configMap.Data).To(HaveKey("nginx.conf"))
				nginxConf := configMap.Data["nginx.conf"]
				// Should not include an alias when assets path is unset
				Expect(strings.Contains(nginxConf, "alias /opt/app/")).To(BeFalse())
				// UI base should proxy directly to python app
				Expect(nginxConf).To(ContainSubstring("location /deployments/" + testName + "/ui { proxy_pass http://127.0.0.1:8080;"))
				// Everything else proxies to python app, and named upstream to 8081 exists
				Expect(nginxConf).To(ContainSubstring("location / { proxy_pass http://127.0.0.1:8080;"))
				Expect(nginxConf).To(ContainSubstring("location @python_upstream { proxy_pass http://127.0.0.1:8081;"))

				// Verify deployment uses ConfigMap mount
				deployment := GetDeploymentEventually(ctx, testName, testNamespace)
				var nginx corev1.Container
				for _, c := range deployment.Spec.Template.Spec.Containers {
					if c.Name == "file-server" {
						nginx = c
						break
					}
				}
				Expect(nginx.Name).To(Equal("file-server"))
				Expect(nginx.Command).To(Equal([]string{"nginx", "-g", "daemon off;"}))
				Expect(nginx.VolumeMounts).To(ContainElement(corev1.VolumeMount{
					Name:      "nginx-config",
					MountPath: "/etc/nginx/nginx.conf",
					SubPath:   "nginx.conf",
				}))
			})
		})

		Describe("OOM regression tests", func() {
			It("should not regenerate auth token on schema version migration", func() {
				testName := "test-no-token-regen"

				By("Creating a LlamaDeployment and reconciling")
				llamaDeploy := NewLlama(testName, testNamespace, testProjectID, testRepoURL, WithGitRef(testGitRef))
				Expect(k8sClient.Create(ctx, llamaDeploy)).To(Succeed())
				defer CleanupLlama(ctx, testName, testNamespace)

				_, err := controllerReconciler.Reconcile(ctx, reconcile.Request{
					NamespacedName: types.NamespacedName{Name: testName, Namespace: testNamespace},
				})
				Expect(err).NotTo(HaveOccurred())
				CompleteBuild(ctx, controllerReconciler, llamaDeploy)

				By("Recording the auth token after initial reconciliation")
				Expect(k8sClient.Get(ctx, types.NamespacedName{Name: testName, Namespace: testNamespace}, llamaDeploy)).To(Succeed())
				Expect(llamaDeploy.Status.AuthToken).NotTo(BeEmpty())
				originalToken := llamaDeploy.Status.AuthToken

				By("Lowering the schema version to simulate an operator upgrade")
				llamaDeploy.Status.SchemaVersion = "0"
				Expect(k8sClient.Status().Update(ctx, llamaDeploy)).To(Succeed())

				By("Reconciling after schema version change")
				_, err = controllerReconciler.Reconcile(ctx, reconcile.Request{
					NamespacedName: types.NamespacedName{Name: testName, Namespace: testNamespace},
				})
				Expect(err).NotTo(HaveOccurred())

				By("Verifying the auth token was NOT regenerated")
				Expect(k8sClient.Get(ctx, types.NamespacedName{Name: testName, Namespace: testNamespace}, llamaDeploy)).To(Succeed())
				Expect(llamaDeploy.Status.AuthToken).To(Equal(originalToken))
				Expect(llamaDeploy.Status.SchemaVersion).To(Equal(CurrentSchemaVersion))
			})

			It("should advance schema version for terminal (RolloutFailed) deployments", func() {
				testName := "test-terminal-schema-advance"
				os.Setenv(EnvRolloutTimeoutSeconds, "30")
				DeferCleanup(os.Unsetenv, EnvRolloutTimeoutSeconds)

				By("Creating a LlamaDeployment, timing it out to reach RolloutFailed")
				llamaDeploy := NewLlama(testName, testNamespace, testProjectID, testRepoURL, WithGitRef(testGitRef))
				Expect(k8sClient.Create(ctx, llamaDeploy)).To(Succeed())
				defer CleanupLlama(ctx, testName, testNamespace)

				_, err := controllerReconciler.Reconcile(ctx, reconcile.Request{
					NamespacedName: types.NamespacedName{Name: testName, Namespace: testNamespace},
				})
				Expect(err).NotTo(HaveOccurred())
				CompleteBuild(ctx, controllerReconciler, llamaDeploy)

				// Set available replicas so timeout picks RolloutFailed
				SetDeploymentAvailableReplicas(ctx, testName, testNamespace, 1)

				Expect(k8sClient.Get(ctx, types.NamespacedName{Name: testName, Namespace: testNamespace}, llamaDeploy)).To(Succeed())
				past := metav1.NewTime(time.Now().Add(-60 * time.Second))
				llamaDeploy.Status.RolloutStartedAt = &past
				Expect(k8sClient.Status().Update(ctx, llamaDeploy)).To(Succeed())

				_, err = controllerReconciler.Reconcile(ctx, reconcile.Request{
					NamespacedName: types.NamespacedName{Name: testName, Namespace: testNamespace},
				})
				Expect(err).NotTo(HaveOccurred())

				By("Verifying we're in RolloutFailed state")
				Expect(k8sClient.Get(ctx, types.NamespacedName{Name: testName, Namespace: testNamespace}, llamaDeploy)).To(Succeed())
				Expect(llamaDeploy.Status.Phase).To(Equal("RolloutFailed"))
				Expect(llamaDeploy.Status.FailedRolloutGeneration).To(Equal(llamaDeploy.Generation))

				By("Lowering schema version to simulate an operator upgrade")
				llamaDeploy.Status.SchemaVersion = "0"
				Expect(k8sClient.Status().Update(ctx, llamaDeploy)).To(Succeed())

				By("Reconciling — should advance schema version even though deployment is terminal")
				_, err = controllerReconciler.Reconcile(ctx, reconcile.Request{
					NamespacedName: types.NamespacedName{Name: testName, Namespace: testNamespace},
				})
				Expect(err).NotTo(HaveOccurred())

				By("Verifying schema version was advanced to current")
				Expect(k8sClient.Get(ctx, types.NamespacedName{Name: testName, Namespace: testNamespace}, llamaDeploy)).To(Succeed())
				Expect(llamaDeploy.Status.SchemaVersion).To(Equal(CurrentSchemaVersion))
				Expect(llamaDeploy.Status.Phase).To(Equal("RolloutFailed"))

				By("Reconciling again — should be a no-op, not an infinite loop")
				Expect(k8sClient.Get(ctx, types.NamespacedName{Name: testName, Namespace: testNamespace}, llamaDeploy)).To(Succeed())
				tokenBefore := llamaDeploy.Status.AuthToken

				_, err = controllerReconciler.Reconcile(ctx, reconcile.Request{
					NamespacedName: types.NamespacedName{Name: testName, Namespace: testNamespace},
				})
				Expect(err).NotTo(HaveOccurred())

				Expect(k8sClient.Get(ctx, types.NamespacedName{Name: testName, Namespace: testNamespace}, llamaDeploy)).To(Succeed())
				Expect(llamaDeploy.Status.AuthToken).To(Equal(tokenBefore))
				Expect(llamaDeploy.Status.SchemaVersion).To(Equal(CurrentSchemaVersion))
			})

			It("should advance schema version for terminal (BuildFailed) deployments", func() {
				testName := "test-buildfailed-schema-advance"

				By("Creating a LlamaDeployment and reconciling to get initial status")
				llamaDeploy := NewLlama(testName, testNamespace, testProjectID, testRepoURL, WithGitRef(testGitRef))
				Expect(k8sClient.Create(ctx, llamaDeploy)).To(Succeed())
				defer CleanupLlama(ctx, testName, testNamespace)

				_, err := controllerReconciler.Reconcile(ctx, reconcile.Request{
					NamespacedName: types.NamespacedName{Name: testName, Namespace: testNamespace},
				})
				Expect(err).NotTo(HaveOccurred())

				By("Manually setting status to BuildFailed to simulate a failed build")
				Expect(k8sClient.Get(ctx, types.NamespacedName{Name: testName, Namespace: testNamespace}, llamaDeploy)).To(Succeed())
				llamaDeploy.Status.Phase = PhaseBuildFailed
				llamaDeploy.Status.FailedRolloutGeneration = llamaDeploy.Generation
				llamaDeploy.Status.BuildStatus = "Failed"
				Expect(k8sClient.Status().Update(ctx, llamaDeploy)).To(Succeed())

				By("Lowering schema version to simulate an operator upgrade")
				Expect(k8sClient.Get(ctx, types.NamespacedName{Name: testName, Namespace: testNamespace}, llamaDeploy)).To(Succeed())
				llamaDeploy.Status.SchemaVersion = "0"
				Expect(k8sClient.Status().Update(ctx, llamaDeploy)).To(Succeed())

				By("Reconciling — should advance schema version even though deployment build failed")
				_, err = controllerReconciler.Reconcile(ctx, reconcile.Request{
					NamespacedName: types.NamespacedName{Name: testName, Namespace: testNamespace},
				})
				Expect(err).NotTo(HaveOccurred())

				By("Verifying schema version was advanced and phase remains BuildFailed")
				Expect(k8sClient.Get(ctx, types.NamespacedName{Name: testName, Namespace: testNamespace}, llamaDeploy)).To(Succeed())
				Expect(llamaDeploy.Status.SchemaVersion).To(Equal(CurrentSchemaVersion))
				Expect(llamaDeploy.Status.Phase).To(Equal(PhaseBuildFailed))

				By("Reconciling again — should be a no-op, not an infinite loop")
				tokenBefore := llamaDeploy.Status.AuthToken

				_, err = controllerReconciler.Reconcile(ctx, reconcile.Request{
					NamespacedName: types.NamespacedName{Name: testName, Namespace: testNamespace},
				})
				Expect(err).NotTo(HaveOccurred())

				Expect(k8sClient.Get(ctx, types.NamespacedName{Name: testName, Namespace: testNamespace}, llamaDeploy)).To(Succeed())
				Expect(llamaDeploy.Status.AuthToken).To(Equal(tokenBefore))
				Expect(llamaDeploy.Status.SchemaVersion).To(Equal(CurrentSchemaVersion))
				Expect(llamaDeploy.Status.Phase).To(Equal(PhaseBuildFailed))
			})
		})

		Describe("Suspended mode", func() {
			It("should set replicas to 0 and phase to Suspended when spec.suspended is true", func() {
				testName := "test-suspended-set-zero"
				ld := &llamadeployv1.LlamaDeployment{
					ObjectMeta: metav1.ObjectMeta{Name: testName, Namespace: testNamespace},
					Spec:       llamadeployv1.LlamaDeploymentSpec{ProjectId: testProjectID, RepoUrl: testRepoURL, GitRef: testGitRef, Suspended: true},
				}
				Expect(k8sClient.Create(ctx, ld)).To(Succeed())
				defer CleanupLlama(ctx, testName, testNamespace)

				// Suspended deployments skip builds entirely, so a single
				// reconcile creates the Deployment (with 0 replicas) directly.
				_, err := controllerReconciler.Reconcile(ctx, reconcile.Request{NamespacedName: types.NamespacedName{Name: testName, Namespace: testNamespace}})
				Expect(err).NotTo(HaveOccurred())

				dep := GetDeploymentEventually(ctx, testName, testNamespace)
				Expect(*dep.Spec.Replicas).To(Equal(int32(0)))

				ld = &llamadeployv1.LlamaDeployment{}
				Expect(k8sClient.Get(ctx, types.NamespacedName{Name: testName, Namespace: testNamespace}, ld)).To(Succeed())
				Expect(ld.Status.Phase).To(Equal(PhaseSuspended))
			})

			It("should restore replicas to 1 when suspended is set to false", func() {
				testName := "test-suspended-restore"
				ld := &llamadeployv1.LlamaDeployment{
					ObjectMeta: metav1.ObjectMeta{Name: testName, Namespace: testNamespace},
					Spec:       llamadeployv1.LlamaDeploymentSpec{ProjectId: testProjectID, RepoUrl: testRepoURL, GitRef: testGitRef, Suspended: true},
				}
				Expect(k8sClient.Create(ctx, ld)).To(Succeed())
				defer CleanupLlama(ctx, testName, testNamespace)

				// Suspended deployments skip builds — single reconcile is enough.
				_, err := controllerReconciler.Reconcile(ctx, reconcile.Request{NamespacedName: types.NamespacedName{Name: testName, Namespace: testNamespace}})
				Expect(err).NotTo(HaveOccurred())

				dep := GetDeploymentEventually(ctx, testName, testNamespace)
				Expect(*dep.Spec.Replicas).To(Equal(int32(0)))

				// Unsuspend — this triggers a build since the deployment has never built.
				Expect(k8sClient.Get(ctx, types.NamespacedName{Name: testName, Namespace: testNamespace}, ld)).To(Succeed())
				ld.Spec.Suspended = false
				Expect(k8sClient.Update(ctx, ld)).To(Succeed())

				_, err = controllerReconciler.Reconcile(ctx, reconcile.Request{NamespacedName: types.NamespacedName{Name: testName, Namespace: testNamespace}})
				Expect(err).NotTo(HaveOccurred())
				CompleteBuild(ctx, controllerReconciler, ld)

				dep = GetDeploymentEventually(ctx, testName, testNamespace)
				Expect(*dep.Spec.Replicas).To(Equal(int32(1)))

				ld = &llamadeployv1.LlamaDeployment{}
				Expect(k8sClient.Get(ctx, types.NamespacedName{Name: testName, Namespace: testNamespace}, ld)).To(Succeed())
				Expect(ld.Status.Phase).To(Equal(PhasePending))
			})

			It("should not set rolloutStartedAt when suspended", func() {
				testName := "test-suspended-no-rollout"
				ld := &llamadeployv1.LlamaDeployment{
					ObjectMeta: metav1.ObjectMeta{Name: testName, Namespace: testNamespace},
					Spec:       llamadeployv1.LlamaDeploymentSpec{ProjectId: testProjectID, RepoUrl: testRepoURL, GitRef: testGitRef, Suspended: true},
				}
				Expect(k8sClient.Create(ctx, ld)).To(Succeed())
				defer CleanupLlama(ctx, testName, testNamespace)

				// Suspended deployments skip builds — single reconcile is enough.
				_, err := controllerReconciler.Reconcile(ctx, reconcile.Request{NamespacedName: types.NamespacedName{Name: testName, Namespace: testNamespace}})
				Expect(err).NotTo(HaveOccurred())

				ld = &llamadeployv1.LlamaDeployment{}
				Expect(k8sClient.Get(ctx, types.NamespacedName{Name: testName, Namespace: testNamespace}, ld)).To(Succeed())
				Expect(ld.Status.RolloutStartedAt).To(BeNil())
			})

			It("should default to 1 replica when suspended field is not set", func() {
				testName := "test-suspended-default"
				ld := &llamadeployv1.LlamaDeployment{
					ObjectMeta: metav1.ObjectMeta{Name: testName, Namespace: testNamespace},
					Spec:       llamadeployv1.LlamaDeploymentSpec{ProjectId: testProjectID, RepoUrl: testRepoURL, GitRef: testGitRef},
				}
				Expect(k8sClient.Create(ctx, ld)).To(Succeed())
				defer CleanupLlama(ctx, testName, testNamespace)

				_, err := controllerReconciler.Reconcile(ctx, reconcile.Request{NamespacedName: types.NamespacedName{Name: testName, Namespace: testNamespace}})
				Expect(err).NotTo(HaveOccurred())
				CompleteBuild(ctx, controllerReconciler, ld)

				dep := GetDeploymentEventually(ctx, testName, testNamespace)
				Expect(*dep.Spec.Replicas).To(Equal(int32(1)))
			})
		})

		Describe("Max concurrent rollouts gate", func() {
			It("should requeue when rollout limit is reached", func() {
				// Create a deployment that is already rolling out
				rollingName := "test-rolling-dep"
				ld1 := NewLlama(rollingName, testNamespace, testProjectID, testRepoURL, WithGitRef(testGitRef))
				Expect(k8sClient.Create(ctx, ld1)).To(Succeed())
				defer CleanupLlama(ctx, rollingName, testNamespace)

				// Reconcile it to create resources
				_, err := controllerReconciler.Reconcile(ctx, reconcile.Request{NamespacedName: types.NamespacedName{Name: rollingName, Namespace: testNamespace}})
				Expect(err).NotTo(HaveOccurred())

				// Set its phase to RollingOut
				Expect(k8sClient.Get(ctx, types.NamespacedName{Name: rollingName, Namespace: testNamespace}, ld1)).To(Succeed())
				ld1.Status.Phase = PhaseRollingOut
				Expect(k8sClient.Status().Update(ctx, ld1)).To(Succeed())

				// Now create a new deployment that needs a full reconcile, with limit=1
				gatedName := "test-gated-dep"
				ld2 := NewLlama(gatedName, testNamespace, testProjectID, testRepoURL, WithGitRef(testGitRef))
				Expect(k8sClient.Create(ctx, ld2)).To(Succeed())
				defer CleanupLlama(ctx, gatedName, testNamespace)

				gatedReconciler := NewTestReconciler(WithMaxConcurrentRollouts(1))

				result, err := gatedReconciler.Reconcile(ctx, reconcile.Request{NamespacedName: types.NamespacedName{Name: gatedName, Namespace: testNamespace}})
				Expect(err).NotTo(HaveOccurred())
				Expect(result.RequeueAfter).To(BeNumerically(">=", 10*time.Second))
				Expect(result.RequeueAfter).To(BeNumerically("<=", 20*time.Second))

				// Verify the gated deployment did NOT get PhasePending
				Expect(k8sClient.Get(ctx, types.NamespacedName{Name: gatedName, Namespace: testNamespace}, ld2)).To(Succeed())
				Expect(ld2.Status.Phase).NotTo(Equal(PhasePending))
			})

			It("should requeue when another deployment is in Pending phase", func() {
				// PhasePending also indicates an active rollout and should count toward the limit
				pendingName := "test-pending-dep"
				ld1 := NewLlama(pendingName, testNamespace, testProjectID, testRepoURL, WithGitRef(testGitRef))
				Expect(k8sClient.Create(ctx, ld1)).To(Succeed())
				defer CleanupLlama(ctx, pendingName, testNamespace)

				// Reconcile to create resources
				_, err := controllerReconciler.Reconcile(ctx, reconcile.Request{NamespacedName: types.NamespacedName{Name: pendingName, Namespace: testNamespace}})
				Expect(err).NotTo(HaveOccurred())

				// Set its phase to Pending (active rollout, pods not ready yet)
				Expect(k8sClient.Get(ctx, types.NamespacedName{Name: pendingName, Namespace: testNamespace}, ld1)).To(Succeed())
				ld1.Status.Phase = PhasePending
				Expect(k8sClient.Status().Update(ctx, ld1)).To(Succeed())

				// Try to reconcile a new deployment with limit=1
				gatedName := "test-gated-by-pending"
				ld2 := NewLlama(gatedName, testNamespace, testProjectID, testRepoURL, WithGitRef(testGitRef))
				Expect(k8sClient.Create(ctx, ld2)).To(Succeed())
				defer CleanupLlama(ctx, gatedName, testNamespace)

				gatedReconciler := NewTestReconciler(WithMaxConcurrentRollouts(1))

				result, err := gatedReconciler.Reconcile(ctx, reconcile.Request{NamespacedName: types.NamespacedName{Name: gatedName, Namespace: testNamespace}})
				Expect(err).NotTo(HaveOccurred())
				Expect(result.RequeueAfter).To(BeNumerically(">=", 10*time.Second))
				Expect(result.RequeueAfter).To(BeNumerically("<=", 20*time.Second))
			})

			It("should requeue when another deployment is in Building phase", func() {
				// PhaseBuilding also indicates an active rollout and should count toward the limit
				buildingName := "test-building-dep"
				ld1 := NewLlama(buildingName, testNamespace, testProjectID, testRepoURL, WithGitRef(testGitRef))
				Expect(k8sClient.Create(ctx, ld1)).To(Succeed())
				defer CleanupLlama(ctx, buildingName, testNamespace)

				// Reconcile to create resources
				_, err := controllerReconciler.Reconcile(ctx, reconcile.Request{NamespacedName: types.NamespacedName{Name: buildingName, Namespace: testNamespace}})
				Expect(err).NotTo(HaveOccurred())

				// Set its phase to Building (build job in progress)
				Expect(k8sClient.Get(ctx, types.NamespacedName{Name: buildingName, Namespace: testNamespace}, ld1)).To(Succeed())
				ld1.Status.Phase = PhaseBuilding
				Expect(k8sClient.Status().Update(ctx, ld1)).To(Succeed())

				// Try to reconcile a new deployment with limit=1
				gatedName := "test-gated-by-building"
				ld2 := NewLlama(gatedName, testNamespace, testProjectID, testRepoURL, WithGitRef(testGitRef))
				Expect(k8sClient.Create(ctx, ld2)).To(Succeed())
				defer CleanupLlama(ctx, gatedName, testNamespace)

				gatedReconciler := NewTestReconciler(WithMaxConcurrentRollouts(1))

				result, err := gatedReconciler.Reconcile(ctx, reconcile.Request{NamespacedName: types.NamespacedName{Name: gatedName, Namespace: testNamespace}})
				Expect(err).NotTo(HaveOccurred())
				Expect(result.RequeueAfter).To(BeNumerically(">=", 10*time.Second))
				Expect(result.RequeueAfter).To(BeNumerically("<=", 20*time.Second))
			})

			It("should allow rollout when under limit", func() {
				testName := "test-under-limit"
				ld := NewLlama(testName, testNamespace, testProjectID, testRepoURL, WithGitRef(testGitRef))
				Expect(k8sClient.Create(ctx, ld)).To(Succeed())
				defer CleanupLlama(ctx, testName, testNamespace)

				gatedReconciler := NewTestReconciler(WithMaxConcurrentRollouts(5))

				_, err := gatedReconciler.Reconcile(ctx, reconcile.Request{NamespacedName: types.NamespacedName{Name: testName, Namespace: testNamespace}})
				Expect(err).NotTo(HaveOccurred())
				CompleteBuild(ctx, gatedReconciler, ld)

				// Should have proceeded to create resources (deployment exists)
				_ = GetDeploymentEventually(ctx, testName, testNamespace)
			})

			It("should bypass gate when limit is 0", func() {
				testName := "test-limit-zero"
				ld := NewLlama(testName, testNamespace, testProjectID, testRepoURL, WithGitRef(testGitRef))
				Expect(k8sClient.Create(ctx, ld)).To(Succeed())
				defer CleanupLlama(ctx, testName, testNamespace)

				// limit=0 means unlimited
				_, err := controllerReconciler.Reconcile(ctx, reconcile.Request{NamespacedName: types.NamespacedName{Name: testName, Namespace: testNamespace}})
				Expect(err).NotTo(HaveOccurred())
				CompleteBuild(ctx, controllerReconciler, ld)

				// Should have proceeded to create resources (deployment exists)
				_ = GetDeploymentEventually(ctx, testName, testNamespace)
			})

			It("should not gate status-only reconciles", func() {
				// Create a deployment already in RollingOut to fill the limit
				rollingName := "test-rolling-status"
				ld1 := NewLlama(rollingName, testNamespace, testProjectID, testRepoURL, WithGitRef(testGitRef))
				Expect(k8sClient.Create(ctx, ld1)).To(Succeed())
				defer CleanupLlama(ctx, rollingName, testNamespace)

				_, err := controllerReconciler.Reconcile(ctx, reconcile.Request{NamespacedName: types.NamespacedName{Name: rollingName, Namespace: testNamespace}})
				Expect(err).NotTo(HaveOccurred())

				// Set phase to RollingOut
				Expect(k8sClient.Get(ctx, types.NamespacedName{Name: rollingName, Namespace: testNamespace}, ld1)).To(Succeed())
				ld1.Status.Phase = PhaseRollingOut
				Expect(k8sClient.Status().Update(ctx, ld1)).To(Succeed())

				// Create another deployment that is already reconciled (not needing full reconcile)
				statusName := "test-status-only"
				ld2 := NewLlama(statusName, testNamespace, testProjectID, testRepoURL, WithGitRef(testGitRef))
				Expect(k8sClient.Create(ctx, ld2)).To(Succeed())
				defer CleanupLlama(ctx, statusName, testNamespace)

				gatedReconciler := NewTestReconciler(WithMaxConcurrentRollouts(1))

				// First reconcile to initialize (will be gated since ld1 is rolling)
				// But we want to test status-only, so reconcile with unlimited first
				_, err = controllerReconciler.Reconcile(ctx, reconcile.Request{NamespacedName: types.NamespacedName{Name: statusName, Namespace: testNamespace}})
				Expect(err).NotTo(HaveOccurred())
				CompleteBuild(ctx, controllerReconciler, ld2)

				// Now reconcile again - this time it's a status-only reconcile (generation hasn't changed)
				result, err := gatedReconciler.Reconcile(ctx, reconcile.Request{NamespacedName: types.NamespacedName{Name: statusName, Namespace: testNamespace}})
				Expect(err).NotTo(HaveOccurred())
				// Status-only reconcile should NOT be gated (requeue would be 10-20s if gated)
				// It may have a rollout timeout requeue (~1800s) which is fine - just not the gate jitter range
				if result.RequeueAfter > 0 {
					isGateRequeue := result.RequeueAfter >= 10*time.Second && result.RequeueAfter <= 20*time.Second
					Expect(isGateRequeue).To(BeFalse(), "status-only reconcile should not be gated")
				}
			})
		})

		Describe("Max deployments gate", func() {
			It("should requeue when deployment limit is reached", func() {
				// Create a deployment that is already running
				runningName := "test-maxdep-running"
				ld1 := NewLlama(runningName, testNamespace, testProjectID, testRepoURL, WithGitRef(testGitRef))
				Expect(k8sClient.Create(ctx, ld1)).To(Succeed())
				defer CleanupLlama(ctx, runningName, testNamespace)

				// Reconcile it to create resources
				_, err := controllerReconciler.Reconcile(ctx, reconcile.Request{NamespacedName: types.NamespacedName{Name: runningName, Namespace: testNamespace}})
				Expect(err).NotTo(HaveOccurred())

				// Set its phase to Running
				Expect(k8sClient.Get(ctx, types.NamespacedName{Name: runningName, Namespace: testNamespace}, ld1)).To(Succeed())
				ld1.Status.Phase = PhaseRunning
				Expect(k8sClient.Status().Update(ctx, ld1)).To(Succeed())

				// Now create a new deployment that needs a full reconcile, with limit=1
				gatedName := "test-maxdep-gated"
				ld2 := NewLlama(gatedName, testNamespace, testProjectID, testRepoURL, WithGitRef(testGitRef))
				Expect(k8sClient.Create(ctx, ld2)).To(Succeed())
				defer CleanupLlama(ctx, gatedName, testNamespace)

				gatedReconciler := NewTestReconciler(WithMaxDeployments(1))

				result, err := gatedReconciler.Reconcile(ctx, reconcile.Request{NamespacedName: types.NamespacedName{Name: gatedName, Namespace: testNamespace}})
				Expect(err).NotTo(HaveOccurred())
				Expect(result.RequeueAfter).To(Equal(5 * time.Minute))

				// Verify the gated deployment did NOT get PhasePending
				Expect(k8sClient.Get(ctx, types.NamespacedName{Name: gatedName, Namespace: testNamespace}, ld2)).To(Succeed())
				Expect(ld2.Status.Phase).NotTo(Equal(PhasePending))
			})

			It("should allow deployment when under limit", func() {
				testName := "test-maxdep-under-limit"
				ld := NewLlama(testName, testNamespace, testProjectID, testRepoURL, WithGitRef(testGitRef))
				Expect(k8sClient.Create(ctx, ld)).To(Succeed())
				defer CleanupLlama(ctx, testName, testNamespace)

				gatedReconciler := NewTestReconciler(WithMaxDeployments(5))

				_, err := gatedReconciler.Reconcile(ctx, reconcile.Request{NamespacedName: types.NamespacedName{Name: testName, Namespace: testNamespace}})
				CompleteBuild(ctx, gatedReconciler, ld)
				Expect(err).NotTo(HaveOccurred())

				// Should have proceeded to create resources (deployment exists)
				_ = GetDeploymentEventually(ctx, testName, testNamespace)
			})

			It("should bypass gate when limit is 0", func() {
				testName := "test-maxdep-limit-zero"
				ld := NewLlama(testName, testNamespace, testProjectID, testRepoURL, WithGitRef(testGitRef))
				Expect(k8sClient.Create(ctx, ld)).To(Succeed())
				defer CleanupLlama(ctx, testName, testNamespace)

				// limit=0 means unlimited (controllerReconciler has MaxDeployments=0)
				_, err := controllerReconciler.Reconcile(ctx, reconcile.Request{NamespacedName: types.NamespacedName{Name: testName, Namespace: testNamespace}})
				CompleteBuild(ctx, controllerReconciler, ld)
				Expect(err).NotTo(HaveOccurred())

				// Should have proceeded to create resources (deployment exists)
				_ = GetDeploymentEventually(ctx, testName, testNamespace)
			})

			It("should not gate status-only reconciles", func() {
				// Create a deployment already in Running to fill the limit
				runningName := "test-maxdep-running-status"
				ld1 := NewLlama(runningName, testNamespace, testProjectID, testRepoURL, WithGitRef(testGitRef))
				Expect(k8sClient.Create(ctx, ld1)).To(Succeed())
				defer CleanupLlama(ctx, runningName, testNamespace)

				_, err := controllerReconciler.Reconcile(ctx, reconcile.Request{NamespacedName: types.NamespacedName{Name: runningName, Namespace: testNamespace}})
				Expect(err).NotTo(HaveOccurred())

				// Set phase to Running
				Expect(k8sClient.Get(ctx, types.NamespacedName{Name: runningName, Namespace: testNamespace}, ld1)).To(Succeed())
				ld1.Status.Phase = PhaseRunning
				Expect(k8sClient.Status().Update(ctx, ld1)).To(Succeed())

				// Create another deployment that is already reconciled (not needing full reconcile)
				statusName := "test-maxdep-status-only"
				ld2 := NewLlama(statusName, testNamespace, testProjectID, testRepoURL, WithGitRef(testGitRef))
				Expect(k8sClient.Create(ctx, ld2)).To(Succeed())
				defer CleanupLlama(ctx, statusName, testNamespace)

				gatedReconciler := NewTestReconciler(WithMaxDeployments(1))

				// First reconcile to initialize with unlimited reconciler
				_, err = controllerReconciler.Reconcile(ctx, reconcile.Request{NamespacedName: types.NamespacedName{Name: statusName, Namespace: testNamespace}})
				Expect(err).NotTo(HaveOccurred())
				CompleteBuild(ctx, controllerReconciler, ld2)

				// Now reconcile again - this time it's a status-only reconcile (generation hasn't changed)
				result, err := gatedReconciler.Reconcile(ctx, reconcile.Request{NamespacedName: types.NamespacedName{Name: statusName, Namespace: testNamespace}})
				Expect(err).NotTo(HaveOccurred())
				// Status-only reconcile should NOT be gated (requeue would be 5m if gated)
				if result.RequeueAfter > 0 {
					Expect(result.RequeueAfter).NotTo(Equal(5*time.Minute), "status-only reconcile should not be gated")
				}
			})

			It("should not count Suspended deployments toward limit", func() {
				// Create a deployment and set it to Suspended
				suspendedName := "test-maxdep-suspended"
				ld1 := NewLlama(suspendedName, testNamespace, testProjectID, testRepoURL, WithGitRef(testGitRef))
				Expect(k8sClient.Create(ctx, ld1)).To(Succeed())
				defer CleanupLlama(ctx, suspendedName, testNamespace)

				_, err := controllerReconciler.Reconcile(ctx, reconcile.Request{NamespacedName: types.NamespacedName{Name: suspendedName, Namespace: testNamespace}})
				Expect(err).NotTo(HaveOccurred())

				// Set phase to Suspended
				Expect(k8sClient.Get(ctx, types.NamespacedName{Name: suspendedName, Namespace: testNamespace}, ld1)).To(Succeed())
				ld1.Status.Phase = PhaseSuspended
				Expect(k8sClient.Status().Update(ctx, ld1)).To(Succeed())

				// Try to reconcile a new deployment with limit=1
				newName := "test-maxdep-after-suspended"
				ld2 := NewLlama(newName, testNamespace, testProjectID, testRepoURL, WithGitRef(testGitRef))
				Expect(k8sClient.Create(ctx, ld2)).To(Succeed())
				defer CleanupLlama(ctx, newName, testNamespace)

				gatedReconciler := NewTestReconciler(WithMaxDeployments(1))

				_, err = gatedReconciler.Reconcile(ctx, reconcile.Request{NamespacedName: types.NamespacedName{Name: newName, Namespace: testNamespace}})
				CompleteBuild(ctx, gatedReconciler, ld2)
				Expect(err).NotTo(HaveOccurred())

				// Should have proceeded to create resources (Suspended doesn't count)
				_ = GetDeploymentEventually(ctx, newName, testNamespace)
			})

			It("should not count Failed deployments toward limit", func() {
				// Create a deployment and set it to Failed
				failedName := "test-maxdep-failed"
				ld1 := NewLlama(failedName, testNamespace, testProjectID, testRepoURL, WithGitRef(testGitRef))
				Expect(k8sClient.Create(ctx, ld1)).To(Succeed())
				defer CleanupLlama(ctx, failedName, testNamespace)

				_, err := controllerReconciler.Reconcile(ctx, reconcile.Request{NamespacedName: types.NamespacedName{Name: failedName, Namespace: testNamespace}})
				Expect(err).NotTo(HaveOccurred())

				// Set phase to Failed
				Expect(k8sClient.Get(ctx, types.NamespacedName{Name: failedName, Namespace: testNamespace}, ld1)).To(Succeed())
				ld1.Status.Phase = PhaseFailed
				Expect(k8sClient.Status().Update(ctx, ld1)).To(Succeed())

				// Try to reconcile a new deployment with limit=1
				newName := "test-maxdep-after-failed"
				ld2 := NewLlama(newName, testNamespace, testProjectID, testRepoURL, WithGitRef(testGitRef))
				Expect(k8sClient.Create(ctx, ld2)).To(Succeed())
				defer CleanupLlama(ctx, newName, testNamespace)

				gatedReconciler := NewTestReconciler(WithMaxDeployments(1))

				_, err = gatedReconciler.Reconcile(ctx, reconcile.Request{NamespacedName: types.NamespacedName{Name: newName, Namespace: testNamespace}})
				CompleteBuild(ctx, gatedReconciler, ld2)
				Expect(err).NotTo(HaveOccurred())

				// Should have proceeded to create resources (Failed doesn't count)
				_ = GetDeploymentEventually(ctx, newName, testNamespace)
			})

			It("should count RolloutFailed deployments toward limit", func() {
				// Create a deployment and set it to RolloutFailed
				rolloutFailedName := "test-maxdep-rolloutfailed"
				ld1 := NewLlama(rolloutFailedName, testNamespace, testProjectID, testRepoURL, WithGitRef(testGitRef))
				Expect(k8sClient.Create(ctx, ld1)).To(Succeed())
				defer CleanupLlama(ctx, rolloutFailedName, testNamespace)

				_, err := controllerReconciler.Reconcile(ctx, reconcile.Request{NamespacedName: types.NamespacedName{Name: rolloutFailedName, Namespace: testNamespace}})
				Expect(err).NotTo(HaveOccurred())

				// Set phase to RolloutFailed
				Expect(k8sClient.Get(ctx, types.NamespacedName{Name: rolloutFailedName, Namespace: testNamespace}, ld1)).To(Succeed())
				ld1.Status.Phase = PhaseRolloutFailed
				Expect(k8sClient.Status().Update(ctx, ld1)).To(Succeed())

				// Try to reconcile a new deployment with limit=1
				gatedName := "test-maxdep-gated-by-rf"
				ld2 := NewLlama(gatedName, testNamespace, testProjectID, testRepoURL, WithGitRef(testGitRef))
				Expect(k8sClient.Create(ctx, ld2)).To(Succeed())
				defer CleanupLlama(ctx, gatedName, testNamespace)

				gatedReconciler := NewTestReconciler(WithMaxDeployments(1))

				result, err := gatedReconciler.Reconcile(ctx, reconcile.Request{NamespacedName: types.NamespacedName{Name: gatedName, Namespace: testNamespace}})
				Expect(err).NotTo(HaveOccurred())
				Expect(result.RequeueAfter).To(Equal(5 * time.Minute))
			})

			It("safety-net requeue is approximately 5 minutes", func() {
				// Create a deployment that is already running to fill the limit
				runningName := "test-maxdep-safety-running"
				ld1 := NewLlama(runningName, testNamespace, testProjectID, testRepoURL, WithGitRef(testGitRef))
				Expect(k8sClient.Create(ctx, ld1)).To(Succeed())
				defer CleanupLlama(ctx, runningName, testNamespace)

				_, err := controllerReconciler.Reconcile(ctx, reconcile.Request{NamespacedName: types.NamespacedName{Name: runningName, Namespace: testNamespace}})
				Expect(err).NotTo(HaveOccurred())

				// Set phase to Running
				Expect(k8sClient.Get(ctx, types.NamespacedName{Name: runningName, Namespace: testNamespace}, ld1)).To(Succeed())
				ld1.Status.Phase = PhaseRunning
				Expect(k8sClient.Status().Update(ctx, ld1)).To(Succeed())

				// Try to reconcile a new deployment with limit=1
				gatedName := "test-maxdep-safety-gated"
				ld2 := NewLlama(gatedName, testNamespace, testProjectID, testRepoURL, WithGitRef(testGitRef))
				Expect(k8sClient.Create(ctx, ld2)).To(Succeed())
				defer CleanupLlama(ctx, gatedName, testNamespace)

				gatedReconciler := NewTestReconciler(WithMaxDeployments(1))

				result, err := gatedReconciler.Reconcile(ctx, reconcile.Request{NamespacedName: types.NamespacedName{Name: gatedName, Namespace: testNamespace}})
				Expect(err).NotTo(HaveOccurred())
				Expect(result.RequeueAfter).To(Equal(5 * time.Minute))
			})
		})
	})
})
