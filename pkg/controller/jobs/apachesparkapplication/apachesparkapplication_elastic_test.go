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

	sparkv1 "sigs.k8s.io/kueue/pkg/controller/jobs/apachesparkapplication/api/v1"
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
