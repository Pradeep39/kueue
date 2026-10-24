/*
Copyright The Kubernetes Authors.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package apachesparkapplication

import (
	"bytes"
	"context"
	"encoding/json"
	"testing"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/utils/ptr"
	"sigs.k8s.io/controller-runtime/pkg/client"

	kueue "sigs.k8s.io/kueue/apis/kueue/v1beta2"
	sparkv1 "sigs.k8s.io/kueue/pkg/controller/jobs/apachesparkapplication/api/v1"
	"sigs.k8s.io/kueue/pkg/podset"
	utiltesting "sigs.k8s.io/kueue/pkg/util/testing"
)

// daSpec builds a Dynamic-Allocation-enabled spec.
func daSpec(extra map[string]string) sparkv1.ApplicationSpec {
	conf := map[string]string{"spark.dynamicAllocation.enabled": "true"}
	for k, v := range extra {
		conf[k] = v
	}
	return sparkv1.ApplicationSpec{SparkConf: conf}
}

func executorPod(name string, phase corev1.PodPhase, deleting bool) *corev1.Pod {
	pod := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:      name,
			Namespace: "ns",
			Labels: map[string]string{
				appNameLabel: "pi",
				roleLabel:    executorRoleValue,
			},
		},
		Status: corev1.PodStatus{Phase: phase},
	}
	if deleting {
		pod.Finalizers = []string{"test/keep"}
		pod.DeletionTimestamp = ptr.To(metav1.Now())
	}
	return pod
}

func jobInNamespace(spec sparkv1.ApplicationSpec) *SparkApplication {
	return &SparkApplication{SparkApplication: &sparkv1.SparkApplication{
		ObjectMeta: metav1.ObjectMeta{Name: "pi", Namespace: "ns"},
		Spec:       spec,
	}}
}

func TestLiveExecutorCount(t *testing.T) {
	cases := map[string]struct {
		spec sparkv1.ApplicationSpec
		pods []client.Object
		want int32
	}{
		"dynamic allocation off ignores live pods entirely": {
			spec: sparkv1.ApplicationSpec{SparkConf: map[string]string{"spark.executor.instances": "3"}},
			pods: []client.Object{
				executorPod("e0", corev1.PodRunning, false),
				executorPod("e1", corev1.PodRunning, false),
			},
			want: 3,
		},
		"no pods yet falls back to initialExecutors": {
			spec: daSpec(map[string]string{"spark.dynamicAllocation.initialExecutors": "4"}),
			want: 4,
		},
		"no pods yet falls back to minExecutors": {
			spec: daSpec(map[string]string{"spark.dynamicAllocation.minExecutors": "2"}),
			want: 2,
		},
		"initialExecutors beats minExecutors": {
			spec: daSpec(map[string]string{
				"spark.dynamicAllocation.initialExecutors": "5",
				"spark.dynamicAllocation.minExecutors":     "2",
			}),
			want: 5,
		},
		"no dynamic allocation counts at all falls back to the static count": {
			spec: daSpec(map[string]string{"spark.executor.instances": "6"}),
			want: 6,
		},
		"live pods override the configured count": {
			spec: daSpec(map[string]string{"spark.dynamicAllocation.initialExecutors": "1"}),
			pods: []client.Object{
				executorPod("e0", corev1.PodRunning, false),
				executorPod("e1", corev1.PodRunning, false),
				executorPod("e2", corev1.PodRunning, false),
			},
			want: 3,
		},
		// Quota must be reserved as soon as the pod is admitted to the cluster, not once
		// it reaches Running.
		"pending pods count": {
			spec: daSpec(nil),
			pods: []client.Object{
				executorPod("e0", corev1.PodRunning, false),
				executorPod("e1", corev1.PodPending, false),
			},
			want: 2,
		},
		// The containers keep running and occupying node resources until the pod actually
		// terminates, so a pod being deleted still counts.
		"terminating pods still count": {
			spec: daSpec(nil),
			pods: []client.Object{
				executorPod("e0", corev1.PodRunning, false),
				executorPod("e1", corev1.PodRunning, true),
			},
			want: 2,
		},
		"terminal pods do not count": {
			spec: daSpec(nil),
			pods: []client.Object{
				executorPod("e0", corev1.PodRunning, false),
				executorPod("e1", corev1.PodSucceeded, false),
				executorPod("e2", corev1.PodFailed, false),
			},
			want: 1,
		},
		"pods of another application are not counted": {
			spec: daSpec(map[string]string{"spark.dynamicAllocation.initialExecutors": "1"}),
			pods: []client.Object{
				executorPod("e0", corev1.PodRunning, false),
				func() client.Object {
					other := executorPod("other-e0", corev1.PodRunning, false)
					other.Labels[appNameLabel] = "not-pi"
					return other
				}(),
			},
			want: 1,
		},
		"driver pods are not counted as executors": {
			spec: daSpec(map[string]string{"spark.dynamicAllocation.initialExecutors": "1"}),
			pods: []client.Object{
				executorPod("e0", corev1.PodRunning, false),
				func() client.Object {
					driver := executorPod("pi-0-driver", corev1.PodRunning, false)
					driver.Labels[roleLabel] = driverRoleValue
					return driver
				}(),
			},
			want: 1,
		},
		// A reconcile landing while the driver is still creating its initial executors sees a
		// strictly smaller prefix of them. Without the minExecutors floor the derived count is
		// 1, which EnsureWorkloadSlices reads as a scale-down and patches the granted PodSet
		// down to 1 -- dismantling the gang it was just admitted with.
		"live count below minExecutors is raised to the floor": {
			spec: daSpec(map[string]string{
				"spark.dynamicAllocation.minExecutors": "3",
				"spark.dynamicAllocation.maxExecutors": "30",
			}),
			pods: []client.Object{executorPod("e0", corev1.PodRunning, false)},
			want: 3,
		},
		"live count above maxExecutors is capped at the ceiling": {
			spec: daSpec(map[string]string{
				"spark.dynamicAllocation.minExecutors": "1",
				"spark.dynamicAllocation.maxExecutors": "2",
			}),
			pods: []client.Object{
				executorPod("e0", corev1.PodRunning, false),
				executorPod("e1", corev1.PodPending, false),
				executorPod("e2", corev1.PodPending, false),
				executorPod("e3", corev1.PodPending, false),
			},
			want: 2,
		},
		// maxExecutors is applied last, so an inverted configuration can never inflate the
		// count above the declared maximum.
		"minExecutors above maxExecutors never exceeds the maximum": {
			spec: daSpec(map[string]string{
				"spark.dynamicAllocation.minExecutors": "8",
				"spark.dynamicAllocation.maxExecutors": "2",
			}),
			pods: []client.Object{executorPod("e0", corev1.PodRunning, false)},
			want: 2,
		},
		"declared instances below minExecutors reserves the floor before any pods exist": {
			spec: daSpec(map[string]string{
				"spark.executor.instances":             "1",
				"spark.dynamicAllocation.minExecutors": "3",
			}),
			want: 3,
		},
		// The conf keys Spark itself acts on outrank instanceConfig on the Dynamic
		// Allocation path too, and drive the clamp. instanceConfig remains the fallback.
		"sparkConf instances outranks instanceConfig initExecutors": {
			spec: func() sparkv1.ApplicationSpec {
				spec := daSpec(map[string]string{"spark.executor.instances": "9"})
				spec.ApplicationTolerations = &sparkv1.ApplicationTolerations{
					InstanceConfig: &sparkv1.ExecutorInstanceConfig{InitExecutors: 4},
				}
				return spec
			}(),
			want: 9,
		},
		"instanceConfig minExecutors raises a live count below the floor": {
			spec: func() sparkv1.ApplicationSpec {
				spec := daSpec(nil)
				spec.ApplicationTolerations = &sparkv1.ApplicationTolerations{
					InstanceConfig: &sparkv1.ExecutorInstanceConfig{MinExecutors: 3},
				}
				return spec
			}(),
			pods: []client.Object{executorPod("e0", corev1.PodRunning, false)},
			want: 3,
		},
		"instanceConfig maxExecutors caps a live count above the ceiling": {
			spec: func() sparkv1.ApplicationSpec {
				spec := daSpec(nil)
				spec.ApplicationTolerations = &sparkv1.ApplicationTolerations{
					InstanceConfig: &sparkv1.ExecutorInstanceConfig{MaxExecutors: 2},
				}
				return spec
			}(),
			pods: []client.Object{
				executorPod("e0", corev1.PodRunning, false),
				executorPod("e1", corev1.PodRunning, false),
				executorPod("e2", corev1.PodRunning, false),
			},
			want: 2,
		},
		// Dynamic Allocation obeys the sparkConf bounds, so a live count of 3 is raised to
		// the sparkConf floor of 8 rather than capped at instanceConfig's 2.
		"sparkConf bounds take precedence over the instanceConfig equivalents": {
			spec: func() sparkv1.ApplicationSpec {
				spec := daSpec(map[string]string{
					"spark.dynamicAllocation.minExecutors": "8",
					"spark.dynamicAllocation.maxExecutors": "9",
				})
				spec.ApplicationTolerations = &sparkv1.ApplicationTolerations{
					InstanceConfig: &sparkv1.ExecutorInstanceConfig{MinExecutors: 1, MaxExecutors: 2},
				}
				return spec
			}(),
			pods: []client.Object{
				executorPod("e0", corev1.PodRunning, false),
				executorPod("e1", corev1.PodRunning, false),
				executorPod("e2", corev1.PodRunning, false),
			},
			want: 8,
		},
		"declared instances above the dynamic allocation counts wins": {
			spec: daSpec(map[string]string{
				"spark.executor.instances":                 "6",
				"spark.dynamicAllocation.initialExecutors": "2",
				"spark.dynamicAllocation.minExecutors":     "1",
			}),
			want: 6,
		},
	}

	for name, tc := range cases {
		t.Run(name, func(t *testing.T) {
			c := utiltesting.NewClientBuilder(sparkv1.AddToScheme).WithObjects(tc.pods...).Build()

			got, err := jobInNamespace(tc.spec).liveExecutorCount(context.Background(), c)
			if err != nil {
				t.Fatalf("liveExecutorCount() returned unexpected error: %v", err)
			}
			if got != tc.want {
				t.Errorf("liveExecutorCount() = %d, want %d", got, tc.want)
			}
		})
	}
}

// TestLiveExecutorCountIsCachedPerWrapper guards the invariant that makes PodSets() stable
// within one reconcile: two calls must agree even if the live pod set changes in between,
// otherwise the equivalence check against the stored Workload produces spurious
// "not equivalent" verdicts and self-inflicted workload-slice churn.
func TestLiveExecutorCountIsCachedPerWrapper(t *testing.T) {
	ctx := context.Background()
	c := utiltesting.NewClientBuilder(sparkv1.AddToScheme).
		WithObjects(executorPod("e0", corev1.PodRunning, false)).
		Build()

	job := jobInNamespace(daSpec(nil))

	first, err := job.liveExecutorCount(ctx, c)
	if err != nil {
		t.Fatalf("first liveExecutorCount() returned unexpected error: %v", err)
	}

	// Dynamic Allocation scales up underneath us.
	if err := c.Create(ctx, executorPod("e1", corev1.PodRunning, false)); err != nil {
		t.Fatalf("failed to create the second executor pod: %v", err)
	}

	second, err := job.liveExecutorCount(ctx, c)
	if err != nil {
		t.Fatalf("second liveExecutorCount() returned unexpected error: %v", err)
	}
	if first != second {
		t.Errorf("liveExecutorCount() changed within one wrapper: %d then %d", first, second)
	}

	// A fresh wrapper, as NewJob() produces per reconcile, must observe the new count.
	fresh, err := jobInNamespace(daSpec(nil)).liveExecutorCount(ctx, c)
	if err != nil {
		t.Fatalf("fresh liveExecutorCount() returned unexpected error: %v", err)
	}
	if fresh != 2 {
		t.Errorf("a fresh wrapper saw %d executors, want 2", fresh)
	}
}

// TestLiveExecutorCountWithoutClient covers the webhook path, which builds PodSet templates
// with a nil client purely to inspect their metadata.
func TestLiveExecutorCountWithoutClient(t *testing.T) {
	got, err := jobInNamespace(daSpec(map[string]string{
		"spark.dynamicAllocation.initialExecutors": "3",
	})).liveExecutorCount(context.Background(), nil)
	if err != nil {
		t.Fatalf("liveExecutorCount() returned unexpected error: %v", err)
	}
	if got != 3 {
		t.Errorf("liveExecutorCount() = %d, want the initial estimate 3", got)
	}
}

func TestIsTrackedExecutorPod(t *testing.T) {
	cases := map[string]struct {
		mutate func(*corev1.Pod)
		want   bool
	}{
		"executor pod with the app label": {want: true},
		"driver pod": {
			mutate: func(p *corev1.Pod) { p.Labels[roleLabel] = driverRoleValue },
		},
		"executor pod without the app label": {
			mutate: func(p *corev1.Pod) { delete(p.Labels, appNameLabel) },
		},
		"pod with no labels at all": {
			mutate: func(p *corev1.Pod) { p.Labels = nil },
		},
	}

	for name, tc := range cases {
		t.Run(name, func(t *testing.T) {
			pod := executorPod("e0", corev1.PodRunning, false)
			if tc.mutate != nil {
				tc.mutate(pod)
			}
			if got := isTrackedExecutorPod(pod); got != tc.want {
				t.Errorf("isTrackedExecutorPod() = %v, want %v", got, tc.want)
			}
		})
	}

	t.Run("non-pod object", func(t *testing.T) {
		if isTrackedExecutorPod(&sparkv1.SparkApplication{}) {
			t.Error("isTrackedExecutorPod() = true for a non-Pod object, want false")
		}
	})
}

// TestEnsureTemplateSpecNeverMarshalsNullContainers is a regression guard.
//
// corev1.PodSpec.Containers carries no omitempty, so a bare &corev1.PodTemplateSpec{}
// marshals to `"containers": null`. The CRD schema declares containers as `type: array`,
// so the API server rejects the object outright with:
//
//	spec.executorSpec.podTemplateSpec.spec.containers: Invalid value: "null":
//	... in body must be of type array: "null"
//
// This bites whenever an application omits driverSpec/executorSpec, which is the common
// case, both when the webhook gates an elastic job and when RunWithPodSetsInfo injects
// pod set info on admission.
func TestEnsureTemplateSpecNeverMarshalsNullContainers(t *testing.T) {
	cases := map[string]struct {
		spec sparkv1.ApplicationSpec
		role sparkRole
	}{
		"executor template absent entirely": {role: roleExecutor},
		"driver template absent entirely":   {role: roleDriver},
		"executor wrapper present, template nil": {
			spec: sparkv1.ApplicationSpec{ExecutorSpec: &sparkv1.BaseApplicationTemplateSpec{}},
			role: roleExecutor,
		},
		"template present but containers nil": {
			spec: sparkv1.ApplicationSpec{
				ExecutorSpec: &sparkv1.BaseApplicationTemplateSpec{
					PodTemplateSpec: &corev1.PodTemplateSpec{},
				},
			},
			role: roleExecutor,
		},
	}

	for name, tc := range cases {
		t.Run(name, func(t *testing.T) {
			job := jobInNamespace(tc.spec)
			template := job.ensureTemplateSpec(tc.role)

			if template.Spec.Containers == nil {
				t.Fatal("Containers is nil, which marshals to null and fails CRD validation")
			}

			// Assert on the wire form, since that is what the API server validates.
			encoded, err := json.Marshal(job.SparkApplication)
			if err != nil {
				t.Fatalf("failed to marshal the application: %v", err)
			}
			if bytes.Contains(encoded, []byte(`"containers":null`)) {
				t.Errorf("marshalled application contains a null containers list:\n%s", encoded)
			}
		})
	}
}

// TestRestorePodSetsInfoPreservesElasticSchedulingGate pins the elastic-preemption
// deadlock for this integration. It is the same defect the Kubeflow SparkApplication
// integration had, and it deadlocks preemption identically.
//
// stopJob applies Suspend() and RestorePodSetsInfo() in a single patch. PodSets() builds
// a template for quota math that does not carry the ElasticJobSchedulingGate, so
// GetPodSetsInfoFromWorkload -> podset.FromPodSet always yields an empty SchedulingGates
// list. Restoring that list wipes the gate from the executor template, and
// validateElasticJob rejects the resulting update:
//
//	an elastic job must carry the kueue.x-k8s.io/elastic-job scheduling gate on its
//	executor pod template
//
// Nothing re-adds it, since mapachesparkapplication is registered for CREATE only while
// vapachesparkapplication runs on CREATE and UPDATE. The suspend therefore never lands,
// the evicted slice keeps its quota reservation, and the preemptor starves.
//
// Negative control: restore the SchedulingGates assignment in RestorePodSetsInfo and this
// test fails with the gate wiped to empty.
func TestRestorePodSetsInfoPreservesElasticSchedulingGate(t *testing.T) {
	elasticGate := corev1.PodSchedulingGate{Name: kueue.ElasticJobSchedulingGate}

	job := wrap(daSpec(nil))
	// Mirror what the mutating webhook does at CREATE.
	executor := job.ensureTemplateSpec(roleExecutor)
	executor.Spec.SchedulingGates = []corev1.PodSchedulingGate{elasticGate}
	job.ensureTemplateSpec(roleDriver)

	// What stopJob actually passes: derived from the Workload's PodSets, which carry no gates.
	job.RestorePodSetsInfo(context.Background(), []podset.PodSetInfo{{}, {}})

	got := templateSpec(job.SparkApplication, roleExecutor).Spec.SchedulingGates
	if len(got) != 1 || got[0].Name != kueue.ElasticJobSchedulingGate {
		t.Errorf("executor SchedulingGates after restore = %v, want [%s]\n"+
			"the gate must survive, or the webhook rejects the suspend patch and preemption deadlocks",
			got, kueue.ElasticJobSchedulingGate)
	}
}

// TestStopJobAndReadmitPatchesPassElasticValidation closes the loop that the existing
// gate test leaves open.
//
// TestRestorePodSetsInfoPreservesElasticSchedulingGate asserts the gate survives in
// memory. That is necessary but not what actually failed in production: the failure was
// the *API server* rejecting the patch stopJob produces, because validateElasticJob
// requires the ElasticJobSchedulingGate on the executor template. So assert the real
// invariant — the object each mutation leaves behind must still pass that validator.
//
// This matters most for this integration. The Kubeflow path has end-to-end cluster
// evidence; the Apache integration is not deployed anywhere, so these unit assertions are
// the only evidence its equivalent fix is correct.
//
// The PodSetInfos are derived from the job's own PodSets() rather than written by hand,
// because that is where the production staleness comes from: PodSets() reads the CR, so
// once an admission has written the Workload name onto the CR, every later slice's
// recorded template carries it, and the merge sees that stale value against the current
// slice's. A fixture that restores from empty maps wipes the CR and cannot reproduce it.
//
// Negative control, verified: reinstate the SchedulingGates assignment in
// RestorePodSetsInfo and the suspend subtest fails with the real validator error
// ("an elastic job must carry the kueue.x-k8s.io/elastic-job scheduling gate ...").
//
// The re-admission subtest has NO verified negative control. Bypassing podset.Merge in
// RunWithPodSetsInfo — the mutation that broke the Kubeflow integration the same way —
// does not make it fail, so on this path it is a barrier against future drift rather than
// a demonstrated regression guard. Do not read it as proof that this integration is
// susceptible to that bug; the evidence is that it is not.
func TestStopJobAndReadmitPatchesPassElasticValidation(t *testing.T) {
	elasticGate := corev1.PodSchedulingGate{Name: kueue.ElasticJobSchedulingGate}
	clnt := utiltesting.NewClientBuilder().Build()

	// newGatedApp mirrors what the mutating webhook leaves on the object at CREATE.
	newGatedApp := func() *SparkApplication {
		job := wrap(daSpec(nil))
		job.ensureTemplateSpec(roleDriver)
		job.ensureTemplateSpec(roleExecutor).Spec.SchedulingGates = []corev1.PodSchedulingGate{elasticGate}
		if errs := validateElasticJob(job.SparkApplication); len(errs) > 0 {
			t.Fatalf("fixture is already invalid before any mutation: %v", errs)
		}
		return job
	}

	// fromPodSets reproduces getPodSetsInfoFromStatus: the Workload's recorded PodSet
	// templates, plus the annotations Kueue stamps for the admission in progress.
	fromPodSets := func(job *SparkApplication, sliceName string) []podset.PodSetInfo {
		podSets, err := job.PodSets(t.Context(), clnt)
		if err != nil {
			t.Fatalf("PodSets() = %v", err)
		}
		info := make([]podset.PodSetInfo, 0, len(podSets))
		for i := range podSets {
			psi := podset.FromPodSet(&podSets[i])
			if psi.Annotations == nil {
				psi.Annotations = map[string]string{}
			}
			if sliceName != "" {
				psi.Annotations[kueue.WorkloadAnnotation] = sliceName
				psi.Annotations[kueue.WorkloadSliceNameAnnotation] = "app-root-slice"
			}
			info = append(info, psi)
		}
		return info
	}

	t.Run("the patch stopJob produces still validates", func(t *testing.T) {
		job := newGatedApp()

		// stopJob: RestorePodSetsInfo from the Workload's PodSets, then Suspend, in one patch.
		job.RestorePodSetsInfo(t.Context(), fromPodSets(job, ""))
		job.Suspend()

		if errs := validateElasticJob(job.SparkApplication); len(errs) > 0 {
			t.Errorf("validateElasticJob() after stopJob = %v, want no errors\n"+
				"the API server would reject the suspend patch and preemption would never stop the job", errs)
		}
	})

	t.Run("the patch re-admission produces still validates", func(t *testing.T) {
		job := newGatedApp()

		// First admission bakes the current Workload name onto the CR.
		if err := job.RunWithPodSetsInfo(t.Context(), clnt, fromPodSets(job, "app-slice-1")); err != nil {
			t.Fatalf("first admission: RunWithPodSetsInfo() = %v, want nil", err)
		}

		// Preemption. The restore carries the CR's own annotations back, stale name included.
		job.RestorePodSetsInfo(t.Context(), fromPodSets(job, ""))
		job.Suspend()

		// Resume under a NEW slice. The stale name must not conflict with the current one.
		if err := job.RunWithPodSetsInfo(t.Context(), clnt, fromPodSets(job, "app-slice-2")); err != nil {
			t.Fatalf("re-admission under a new slice: RunWithPodSetsInfo() = %v, want nil\n"+
				"a permanent error here leaves the job suspended for ever", err)
		}

		if errs := validateElasticJob(job.SparkApplication); len(errs) > 0 {
			t.Errorf("validateElasticJob() after re-admission = %v, want no errors", errs)
		}
	})
}
