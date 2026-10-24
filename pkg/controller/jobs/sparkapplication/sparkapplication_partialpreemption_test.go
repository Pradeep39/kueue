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

package sparkapplication

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/google/go-cmp/cmp"
	sparkv1beta2 "github.com/kubeflow/spark-operator/v2/api/v1beta2"
	sparkcommon "github.com/kubeflow/spark-operator/v2/pkg/common"
	corev1 "k8s.io/api/core/v1"
	policyv1 "k8s.io/api/policy/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/component-base/featuregate"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/interceptor"

	kueue "sigs.k8s.io/kueue/apis/kueue/v1beta2"
	"sigs.k8s.io/kueue/pkg/constants"
	"sigs.k8s.io/kueue/pkg/features"
	utiltesting "sigs.k8s.io/kueue/pkg/util/testing"
	sparkapplicationtesting "sigs.k8s.io/kueue/pkg/util/testingjobs/sparkapplication"
	"sigs.k8s.io/kueue/pkg/workloadslicing"
)

var partialPreemptionGates = map[featuregate.Feature]bool{
	features.ElasticJobsViaWorkloadSlices:                    true,
	features.ElasticJobsViaWorkloadSlicesForSparkApplication: true,
	features.SparkPartialPreemption:                          true,
}

var reclaimBaseTime = time.Date(2026, 10, 1, 0, 0, 0, 0, time.UTC)

// ppPod builds an executor (or, with role driver, a driver) Pod for app "app" in "ns".
type ppPod struct {
	name     string
	role     string
	cost     string // pod-deletion-cost annotation; "" means absent
	node     string
	ageSec   int // seconds after reclaimBaseTime it was created; larger is newer
	gated    bool
	phase    corev1.PodPhase
	deleting bool
}

func (p ppPod) obj() *corev1.Pod {
	role := p.role
	if role == "" {
		role = sparkcommon.SparkRoleExecutor
	}
	phase := p.phase
	if phase == "" {
		phase = corev1.PodRunning
	}
	pod := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:              p.name,
			Namespace:         "ns",
			CreationTimestamp: metav1.NewTime(reclaimBaseTime.Add(time.Duration(p.ageSec) * time.Second)),
			Labels: map[string]string{
				sparkcommon.LabelSparkAppName: "app",
				sparkcommon.LabelSparkRole:    role,
			},
			// Keeps a deleted Pod around with a DeletionTimestamp, as a real
			// terminating Pod would be during its grace period.
			Finalizers: []string{"kueue.x-k8s.io/test-keep-around"},
		},
		Spec:   corev1.PodSpec{NodeName: p.node},
		Status: corev1.PodStatus{Phase: phase},
	}
	if p.cost != "" {
		pod.Annotations = map[string]string{constants.PodDeletionCostAnnotation: p.cost}
	}
	if p.gated {
		pod.Spec.SchedulingGates = []corev1.PodSchedulingGate{{Name: kueue.ElasticJobSchedulingGate}}
	}
	if p.deleting {
		now := metav1.Now()
		pod.DeletionTimestamp = &now
	}
	return pod
}

func elasticApp() *sparkapplicationtesting.SparkApplicationWrapper {
	return sparkapplicationtesting.MakeSparkApplication("app", "ns").
		Annotation(workloadslicing.EnabledAnnotationKey, workloadslicing.EnabledAnnotationValue)
}

func withSparkConf(app *sparkv1beta2.SparkApplication, conf map[string]string) *sparkv1beta2.SparkApplication {
	app.Spec.SparkConf = conf
	return app
}

// withRunningExecutors marks app's driver as running and records n RUNNING executors in
// status.executorState, which is where numExecutors takes a Dynamic Allocation count from.
func withRunningExecutors(app *sparkv1beta2.SparkApplication, n int) *sparkv1beta2.SparkApplication {
	app.Status.AppState.State = sparkv1beta2.ApplicationStateRunning
	app.Status.SubmissionAttempts = 1
	app.Status.ExecutorState = make(map[string]sparkv1beta2.ExecutorState, n)
	for i := range n {
		app.Status.ExecutorState[fmt.Sprintf("app-exec-%d", i+1)] = sparkv1beta2.ExecutorStateRunning
	}
	return app
}

func TestPartialPreemptionFloor(t *testing.T) {
	da := func(minExecutors *int32) *sparkv1beta2.DynamicAllocation {
		return &sparkv1beta2.DynamicAllocation{Enabled: true, MinExecutors: minExecutors}
	}
	cases := map[string]struct {
		app          *sparkv1beta2.SparkApplication
		featureGates map[featuregate.Feature]bool
		wantFloor    int32
		wantOK       bool
	}{
		"property set": {
			app:       withSparkConf(elasticApp().ExecutorInstances(5).Obj(), map[string]string{sparkConfPreemptionMinExecutors: "2"}),
			wantFloor: 2,
			wantOK:    true,
		},
		"property 0 is honoured": {
			app:       withSparkConf(elasticApp().ExecutorInstances(5).Obj(), map[string]string{sparkConfPreemptionMinExecutors: "0"}),
			wantFloor: 0,
			wantOK:    true,
		},
		"property wins over Dynamic Allocation minExecutors": {
			app: withRunningExecutors(withSparkConf(elasticApp().DynamicAllocation(da(new(int32(3)))).Obj(),
				map[string]string{sparkConfPreemptionMinExecutors: "1"}), 6),
			wantFloor: 1,
			wantOK:    true,
		},
		"Dynamic Allocation with minExecutors unset is raised to 1": {
			app:       withRunningExecutors(elasticApp().DynamicAllocation(da(nil)).Obj(), 6),
			wantFloor: 1,
			wantOK:    true,
		},
		"Dynamic Allocation with minExecutors 0 is raised to 1": {
			app:       withRunningExecutors(elasticApp().DynamicAllocation(da(new(int32(0)))).Obj(), 6),
			wantFloor: 1,
			wantOK:    true,
		},
		"Dynamic Allocation minExecutors 3": {
			app:       withRunningExecutors(elasticApp().DynamicAllocation(da(new(int32(3)))).Obj(), 6),
			wantFloor: 3,
			wantOK:    true,
		},
		"static uses the executor count": {
			app:       elasticApp().ExecutorInstances(4).Obj(),
			wantFloor: 4,
			wantOK:    true,
		},
		"property above the count is capped at the count": {
			app:       withSparkConf(elasticApp().ExecutorInstances(3).Obj(), map[string]string{sparkConfPreemptionMinExecutors: "10"}),
			wantFloor: 3,
			wantOK:    true,
		},
		"capped at the executors status.executorState tracks": {
			app: withRunningExecutors(withSparkConf(elasticApp().DynamicAllocation(da(nil)).Obj(),
				map[string]string{sparkConfPreemptionMinExecutors: "5"}), 2),
			wantFloor: 2,
			wantOK:    true,
		},
		"before the driver reports executors, capped at the initial count": {
			app: withSparkConf(elasticApp().ExecutorInstances(2).DynamicAllocation(da(nil)).Obj(),
				map[string]string{sparkConfPreemptionMinExecutors: "5"}),
			wantFloor: 2,
			wantOK:    true,
		},
		"malformed property falls back to the static count": {
			app:       withSparkConf(elasticApp().ExecutorInstances(4).Obj(), map[string]string{sparkConfPreemptionMinExecutors: "two"}),
			wantFloor: 4,
			wantOK:    true,
		},
		"negative property falls back to Dynamic Allocation": {
			app: withRunningExecutors(withSparkConf(elasticApp().DynamicAllocation(da(new(int32(2)))).Obj(),
				map[string]string{sparkConfPreemptionMinExecutors: "-1"}), 6),
			wantFloor: 2,
			wantOK:    true,
		},
		"gate off": {
			app:          withSparkConf(elasticApp().ExecutorInstances(5).Obj(), map[string]string{sparkConfPreemptionMinExecutors: "2"}),
			featureGates: map[featuregate.Feature]bool{features.ElasticJobsViaWorkloadSlices: true},
		},
		"not elastic": {
			app: withSparkConf(sparkapplicationtesting.MakeSparkApplication("app", "ns").ExecutorInstances(5).Obj(),
				map[string]string{sparkConfPreemptionMinExecutors: "2"}),
		},
	}
	for name, tc := range cases {
		t.Run(name, func(t *testing.T) {
			gates := tc.featureGates
			if gates == nil {
				gates = partialPreemptionGates
			}
			features.SetFeatureGatesDuringTest(t, gates)
			job := fromObject(tc.app)
			podSet, floor, ok := job.PartialPreemptionFloor()
			if ok != tc.wantOK {
				t.Fatalf("PartialPreemptionFloor() ok = %v, want %v", ok, tc.wantOK)
			}
			if !ok {
				return
			}
			if podSet != executorPodSetName {
				t.Errorf("PartialPreemptionFloor() podSet = %q, want %q", podSet, executorPodSetName)
			}
			if floor != tc.wantFloor {
				t.Errorf("PartialPreemptionFloor() floor = %d, want %d", floor, tc.wantFloor)
			}
		})
	}
}

func TestPartialPreemptionMarkedPods(t *testing.T) {
	allKinds := []ppPod{
		{name: "neg", cost: "-100"},
		{name: "neg-pending", cost: "-1", phase: corev1.PodPending},
		{name: "zero", cost: "0"},
		{name: "positive", cost: "5"},
		{name: "missing"},
		{name: "unparsable", cost: "abc"},
		{name: "gated", cost: "-100", gated: true, phase: corev1.PodPending},
		{name: "succeeded", cost: "-100", phase: corev1.PodSucceeded},
		{name: "failed", cost: "-100", phase: corev1.PodFailed},
		{name: "deleting", cost: "-100", deleting: true},
		{name: "driver", role: sparkcommon.SparkRoleDriver, cost: "-100"},
	}
	cases := map[string]struct {
		app          *sparkv1beta2.SparkApplication
		featureGates map[featuregate.Feature]bool
		wantMarked   int32
		wantOK       bool
	}{
		"static elastic app counts only ungated live non-deleting executors with cost < 0": {
			app:        elasticApp().ExecutorInstances(4).Obj(),
			wantMarked: 2,
			wantOK:     true,
		},
		"dynamic allocation elastic app": {
			app:        elasticApp().DynamicAllocation(&sparkv1beta2.DynamicAllocation{Enabled: true}).Obj(),
			wantMarked: 2,
			wantOK:     true,
		},
		"gate off is not observed": {
			app:          elasticApp().ExecutorInstances(4).Obj(),
			featureGates: map[featuregate.Feature]bool{features.ElasticJobsViaWorkloadSlices: true},
		},
		"not elastic is not observed": {
			app: sparkapplicationtesting.MakeSparkApplication("app", "ns").ExecutorInstances(4).Obj(),
		},
	}
	for name, tc := range cases {
		t.Run(name, func(t *testing.T) {
			gates := tc.featureGates
			if gates == nil {
				gates = partialPreemptionGates
			}
			features.SetFeatureGatesDuringTest(t, gates)
			objs := make([]client.Object, 0, len(allKinds))
			for _, p := range allKinds {
				objs = append(objs, p.obj())
			}
			builder := utiltesting.NewClientBuilder().WithObjects(objs...)
			c := builder.Build()
			if err := SetupIndexes(t.Context(), utiltesting.AsIndexer(builder)); err != nil {
				t.Fatalf("failed to setup indexes: %v", err)
			}
			marked, ok, err := fromObject(tc.app).PartialPreemptionMarkedPods(t.Context(), c)
			if err != nil {
				t.Fatalf("PartialPreemptionMarkedPods() returned an unexpected error: %v", err)
			}
			if ok != tc.wantOK || marked != tc.wantMarked {
				t.Errorf("PartialPreemptionMarkedPods() = (%d, %v), want (%d, %v)", marked, ok, tc.wantMarked, tc.wantOK)
			}
		})
	}
}

func TestReclaimPods(t *testing.T) {
	cases := map[string]struct {
		app          *sparkv1beta2.SparkApplication
		featureGates map[featuregate.Feature]bool
		pods         []ppPod
		podSet       kueue.PodSetReference
		keep         int32
		wantDone     bool
		wantErr      bool
		wantDeleted  []string // Pods that end up with a DeletionTimestamp, including pre-existing ones
	}{
		"orders by cost, then unbound first, then newest first": {
			app: withSparkConf(elasticApp().ExecutorInstances(6).Obj(), map[string]string{sparkConfPreemptionMinExecutors: "1"}),
			pods: []ppPod{
				{name: "a-cost5", cost: "5", ageSec: 100},
				{name: "b-old-bound", node: "n1", ageSec: 1},
				{name: "c-new-bound", node: "n1", ageSec: 50},
				{name: "d-unbound", ageSec: 0},
				{name: "e-neg", cost: "-10", node: "n1", ageSec: 0},
				{name: "f-unparsable", cost: "x", node: "n1", ageSec: 10},
			},
			keep:        2,
			wantDeleted: []string{"c-new-bound", "d-unbound", "e-neg", "f-unparsable"},
		},
		"name breaks remaining ties": {
			app: withSparkConf(elasticApp().ExecutorInstances(3).Obj(), map[string]string{sparkConfPreemptionMinExecutors: "1"}),
			pods: []ppPod{
				{name: "z", node: "n1"},
				{name: "y", node: "n1"},
				{name: "x", node: "n1"},
			},
			keep:        2,
			wantDeleted: []string{"x"},
		},
		"never deletes the driver or gated Pods": {
			app: withSparkConf(elasticApp().ExecutorInstances(4).Obj(), map[string]string{sparkConfPreemptionMinExecutors: "0"}),
			pods: []ppPod{
				{name: "driver", role: sparkcommon.SparkRoleDriver, cost: "-100"},
				{name: "gated1", cost: "-100", gated: true, phase: corev1.PodPending},
				{name: "gated2", gated: true, phase: corev1.PodPending},
				{name: "e1", node: "n1"},
				{name: "e2", node: "n1"},
			},
			keep:        0,
			wantDeleted: []string{"e1", "e2"},
		},
		"never below the floor": {
			app: withSparkConf(elasticApp().ExecutorInstances(4).Obj(), map[string]string{sparkConfPreemptionMinExecutors: "3"}),
			pods: []ppPod{
				{name: "e1", node: "n1"},
				{name: "e2", node: "n1"},
				{name: "e3", node: "n1"},
				{name: "e4", node: "n1"},
			},
			keep:        1,
			wantDeleted: []string{"e1"},
		},
		"Dynamic Allocation floor is at least 1": {
			app: elasticApp().DynamicAllocation(&sparkv1beta2.DynamicAllocation{Enabled: true}).Obj(),
			pods: []ppPod{
				{name: "e1", node: "n1"},
				{name: "e2", node: "n1"},
			},
			keep:        0,
			wantDeleted: []string{"e1"},
		},
		"deletions in flight count as holding: nothing more is deleted": {
			app: withSparkConf(elasticApp().ExecutorInstances(4).Obj(), map[string]string{sparkConfPreemptionMinExecutors: "1"}),
			pods: []ppPod{
				{name: "e1", node: "n1", deleting: true},
				{name: "e2", node: "n1", deleting: true},
				{name: "e3", node: "n1"},
				{name: "e4", node: "n1"},
			},
			keep:        2,
			wantDeleted: []string{"e1", "e2"},
		},
		"deletes only the remainder beyond those in flight": {
			app: withSparkConf(elasticApp().ExecutorInstances(4).Obj(), map[string]string{sparkConfPreemptionMinExecutors: "1"}),
			pods: []ppPod{
				{name: "e1", node: "n1", deleting: true},
				{name: "e2", node: "n1"},
				{name: "e3", node: "n1"},
				{name: "e4", node: "n1"},
			},
			keep:        2,
			wantDeleted: []string{"e1", "e2"},
		},
		"done when holding <= keep, ignoring gated and terminal Pods": {
			app: withSparkConf(elasticApp().ExecutorInstances(4).Obj(), map[string]string{sparkConfPreemptionMinExecutors: "1"}),
			pods: []ppPod{
				{name: "e1", node: "n1"},
				{name: "e2", node: "n1"},
				{name: "gated", gated: true, phase: corev1.PodPending},
				{name: "done", node: "n1", phase: corev1.PodSucceeded},
				{name: "failed", node: "n1", phase: corev1.PodFailed},
			},
			keep:     2,
			wantDone: true,
		},
		"not done while a deleted Pod still holds its resources": {
			app: withSparkConf(elasticApp().ExecutorInstances(4).Obj(), map[string]string{sparkConfPreemptionMinExecutors: "1"}),
			pods: []ppPod{
				{name: "e1", node: "n1", deleting: true},
				{name: "e2", node: "n1"},
				{name: "e3", node: "n1"},
			},
			keep:        2,
			wantDeleted: []string{"e1"},
		},
		"other PodSet is rejected": {
			app:     withSparkConf(elasticApp().ExecutorInstances(4).Obj(), map[string]string{sparkConfPreemptionMinExecutors: "1"}),
			pods:    []ppPod{{name: "e1"}, {name: "e2"}},
			podSet:  driverPodSetName,
			wantErr: true,
		},
		"gate off is rejected": {
			app:          withSparkConf(elasticApp().ExecutorInstances(4).Obj(), map[string]string{sparkConfPreemptionMinExecutors: "1"}),
			featureGates: map[featuregate.Feature]bool{features.ElasticJobsViaWorkloadSlices: true},
			pods:         []ppPod{{name: "e1"}, {name: "e2"}},
			wantErr:      true,
		},
	}
	for name, tc := range cases {
		t.Run(name, func(t *testing.T) {
			gates := tc.featureGates
			if gates == nil {
				gates = partialPreemptionGates
			}
			features.SetFeatureGatesDuringTest(t, gates)
			objs := make([]client.Object, 0, len(tc.pods))
			for _, p := range tc.pods {
				objs = append(objs, p.obj())
			}
			c := utiltesting.NewClientBuilder().WithObjects(objs...).Build()
			podSet := tc.podSet
			if podSet == "" {
				podSet = executorPodSetName
			}

			done, err := fromObject(tc.app).ReclaimPods(t.Context(), c, podSet, tc.keep, nil)
			if (err != nil) != tc.wantErr {
				t.Fatalf("ReclaimPods() error = %v, wantErr %v", err, tc.wantErr)
			}
			if done != tc.wantDone {
				t.Errorf("ReclaimPods() done = %v, want %v", done, tc.wantDone)
			}
			if diff := cmp.Diff(tc.wantDeleted, deletedPods(t, c)); diff != "" {
				t.Errorf("deleted Pods mismatch (-want +got):\n%s", diff)
			}
			if tc.wantErr {
				return
			}

			// Level-based: a second call, with the first call's deletions still in
			// flight, deletes nothing more.
			if _, err := fromObject(tc.app).ReclaimPods(t.Context(), c, podSet, tc.keep, nil); err != nil {
				t.Fatalf("second ReclaimPods() returned an unexpected error: %v", err)
			}
			if diff := cmp.Diff(tc.wantDeleted, deletedPods(t, c)); diff != "" {
				t.Errorf("second ReclaimPods() deleted more Pods (-want +got):\n%s", diff)
			}
		})
	}
}

// TestReclaimPodsCompletes walks a reclaim to completion: once the deleted Pods are gone,
// ReclaimPods reports done.
func TestReclaimPodsCompletes(t *testing.T) {
	features.SetFeatureGatesDuringTest(t, partialPreemptionGates)
	objs := []client.Object{
		ppPod{name: "e1", node: "n1"}.obj(),
		ppPod{name: "e2", node: "n1"}.obj(),
		ppPod{name: "e3", node: "n1", cost: "-5"}.obj(),
	}
	c := utiltesting.NewClientBuilder().WithObjects(objs...).Build()
	job := fromObject(withSparkConf(elasticApp().ExecutorInstances(3).Obj(), map[string]string{sparkConfPreemptionMinExecutors: "1"}))

	done, err := job.ReclaimPods(t.Context(), c, executorPodSetName, 2, nil)
	if err != nil || done {
		t.Fatalf("first ReclaimPods() = (%v, %v), want (false, nil)", done, err)
	}
	if diff := cmp.Diff([]string{"e3"}, deletedPods(t, c)); diff != "" {
		t.Fatalf("deleted Pods mismatch (-want +got):\n%s", diff)
	}
	// The kubelet finishes the Pod: it goes terminal and its finalizer is released.
	pod := &corev1.Pod{}
	if err := c.Get(t.Context(), client.ObjectKey{Namespace: "ns", Name: "e3"}, pod); err != nil {
		t.Fatalf("Get() failed: %v", err)
	}
	pod.Finalizers = nil
	if err := c.Update(t.Context(), pod); err != nil {
		t.Fatalf("Update() failed: %v", err)
	}
	done, err = job.ReclaimPods(t.Context(), c, executorPodSetName, 2, nil)
	if err != nil || !done {
		t.Fatalf("second ReclaimPods() = (%v, %v), want (true, nil)", done, err)
	}
}

// TestReclaimPodsGracePeriod checks that Pods are evicted with the reclaim's grace period,
// rounded up to whole seconds, and with none when the reclaim has none.
func TestReclaimPodsGracePeriod(t *testing.T) {
	cases := map[string]struct {
		grace *time.Duration
		want  *int64
	}{
		"no grace period: the Pod's own applies": {},
		"whole seconds":                          {grace: new(45 * time.Second), want: new(int64(45))},
		"sub-second remainder rounds up":         {grace: new(1500 * time.Millisecond), want: new(int64(2))},
		"zero":                                   {grace: new(time.Duration(0)), want: new(int64(0))},
	}
	for name, tc := range cases {
		t.Run(name, func(t *testing.T) {
			features.SetFeatureGatesDuringTest(t, partialPreemptionGates)
			var got []*int64
			c := utiltesting.NewClientBuilder().
				WithObjects(ppPod{name: "e1", node: "n1"}.obj(), ppPod{name: "e2", node: "n1"}.obj()).
				WithInterceptorFuncs(interceptor.Funcs{SubResourceCreate: func(ctx context.Context, c client.Client, sub string, obj, subObj client.Object, opts ...client.SubResourceCreateOption) error {
					if ev, ok := subObj.(*policyv1.Eviction); ok && sub == "eviction" {
						var grace *int64
						if ev.DeleteOptions != nil {
							grace = ev.DeleteOptions.GracePeriodSeconds
						}
						got = append(got, grace)
					}
					return c.SubResource(sub).Create(ctx, obj, subObj, opts...)
				}}).Build()
			job := fromObject(withSparkConf(elasticApp().ExecutorInstances(2).Obj(), map[string]string{sparkConfPreemptionMinExecutors: "1"}))
			if _, err := job.ReclaimPods(t.Context(), c, executorPodSetName, 1, tc.grace); err != nil {
				t.Fatalf("ReclaimPods() error = %v", err)
			}
			if diff := cmp.Diff([]*int64{tc.want}, got); diff != "" {
				t.Errorf("eviction grace period (-want +got):\n%s", diff)
			}
		})
	}
}

// TestReclaimPodsEviction checks how executor Pods are removed: through the Eviction API, never
// deleted directly; a refused eviction (a PodDisruptionBudget) is retried later rather than
// failing the reclaim; and a forbidden eviction (a ClusterRole without pods/eviction) falls back
// to a delete with the same grace period.
func TestReclaimPodsEviction(t *testing.T) {
	podsGR := schema.GroupResource{Resource: "pods"}
	cases := map[string]struct {
		evictErr      error
		wantEvictions int
		wantDeletes   []*int64
		wantRemoved   int
		wantErr       bool
	}{
		"evicted, not deleted directly": {
			wantEvictions: 1, wantRemoved: 1,
		},
		"refused by a disruption budget: no error, nothing removed, retried later": {
			evictErr:      apierrors.NewTooManyRequests("Cannot evict pod as it would violate the pod's disruption budget.", 10),
			wantEvictions: 1,
		},
		"forbidden: deleted instead, with the reclaim's grace period": {
			evictErr:      apierrors.NewForbidden(podsGR, "e1", errors.New("pods/eviction is not allowed")),
			wantEvictions: 1, wantDeletes: []*int64{new(int64(45))}, wantRemoved: 1,
		},
		"other errors are returned": {
			evictErr:      apierrors.NewInternalError(errors.New("boom")),
			wantEvictions: 1, wantErr: true,
		},
	}
	for name, tc := range cases {
		t.Run(name, func(t *testing.T) {
			features.SetFeatureGatesDuringTest(t, partialPreemptionGates)
			evictions := 0
			var deletes []*int64
			c := utiltesting.NewClientBuilder().
				WithObjects(ppPod{name: "e1", node: "n1"}.obj(), ppPod{name: "e2", node: "n1"}.obj()).
				WithInterceptorFuncs(interceptor.Funcs{
					SubResourceCreate: func(ctx context.Context, c client.Client, sub string, obj, subObj client.Object, opts ...client.SubResourceCreateOption) error {
						if sub == "eviction" {
							evictions++
							if tc.evictErr != nil {
								return tc.evictErr
							}
						}
						return c.SubResource(sub).Create(ctx, obj, subObj, opts...)
					},
					Delete: func(ctx context.Context, c client.WithWatch, obj client.Object, opts ...client.DeleteOption) error {
						o := &client.DeleteOptions{}
						o.ApplyOptions(opts)
						deletes = append(deletes, o.GracePeriodSeconds)
						return c.Delete(ctx, obj, opts...)
					},
				}).Build()
			done, err := fromObject(withSparkConf(elasticApp().ExecutorInstances(2).Obj(), map[string]string{sparkConfPreemptionMinExecutors: "1"})).ReclaimPods(t.Context(), c, executorPodSetName, 1, new(45*time.Second))
			if (err != nil) != tc.wantErr {
				t.Fatalf("ReclaimPods() error = %v, wantErr %v", err, tc.wantErr)
			}
			if done {
				t.Errorf("ReclaimPods() done = true, want false while a Pod is still being removed")
			}
			if evictions != tc.wantEvictions {
				t.Errorf("evictions = %d, want %d", evictions, tc.wantEvictions)
			}
			if diff := cmp.Diff(tc.wantDeletes, deletes); diff != "" {
				t.Errorf("direct deletes (-want +got):\n%s", diff)
			}
			if got := len(deletedPods(t, c)); got != tc.wantRemoved {
				t.Errorf("Pods being removed = %d, want %d", got, tc.wantRemoved)
			}
		})
	}
}

func TestPartialPreemptionTerminationGracePeriod(t *testing.T) {
	cases := map[string]struct {
		template  *corev1.PodTemplateSpec
		conf      map[string]string
		want      time.Duration
		wantFound bool
	}{
		"neither set": {},
		"template": {
			template: &corev1.PodTemplateSpec{Spec: corev1.PodSpec{TerminationGracePeriodSeconds: new(int64(90))}},
			want:     90 * time.Second, wantFound: true,
		},
		"Spark property": {
			conf: map[string]string{sparkConfExecutorTerminationGracePeriod: "2m"},
			want: 2 * time.Minute, wantFound: true,
		},
		"template wins over the Spark property": {
			template: &corev1.PodTemplateSpec{Spec: corev1.PodSpec{TerminationGracePeriodSeconds: new(int64(90))}},
			conf:     map[string]string{sparkConfExecutorTerminationGracePeriod: "2m"},
			want:     90 * time.Second, wantFound: true,
		},
		"malformed Spark property is ignored": {
			conf: map[string]string{sparkConfExecutorTerminationGracePeriod: "soon"},
		},
		"template without a grace period": {
			template: &corev1.PodTemplateSpec{},
		},
	}
	for name, tc := range cases {
		t.Run(name, func(t *testing.T) {
			w := elasticApp().ExecutorInstances(3)
			if tc.template != nil {
				w = w.ExecutorTemplate(tc.template)
			}
			got, found := fromObject(withSparkConf(w.Obj(), tc.conf)).PartialPreemptionTerminationGracePeriod()
			if got != tc.want || found != tc.wantFound {
				t.Errorf("PartialPreemptionTerminationGracePeriod() = (%v, %v), want (%v, %v)", got, found, tc.want, tc.wantFound)
			}
		})
	}
}

func deletedPods(t *testing.T, c client.Client) []string {
	t.Helper()
	pods := &corev1.PodList{}
	if err := c.List(t.Context(), pods); err != nil {
		t.Fatalf("List() failed: %v", err)
	}
	var got []string
	for i := range pods.Items {
		if pods.Items[i].DeletionTimestamp != nil {
			got = append(got, pods.Items[i].Name)
		}
	}
	slices.Sort(got)
	return got
}

func TestSparkTimeSeconds(t *testing.T) {
	cases := map[string]struct {
		in     string
		want   int64
		wantOK bool
	}{
		"bare number is seconds": {in: "45", want: 45, wantOK: true},
		"seconds suffix":         {in: "60s", want: 60, wantOK: true},
		"minutes m":              {in: "2m", want: 120, wantOK: true},
		"minutes min":            {in: "3min", want: 180, wantOK: true},
		"hours":                  {in: "1h", want: 3600, wantOK: true},
		"days":                   {in: "1d", want: 86400, wantOK: true},
		"milliseconds truncate":  {in: "1500ms", want: 1, wantOK: true},
		"upper case and spaces":  {in: " 90S ", want: 90, wantOK: true},
		"negative":               {in: "-5s", wantOK: false},
		"garbage":                {in: "soon", wantOK: false},
		"empty":                  {in: "", wantOK: false},
	}
	for name, tc := range cases {
		t.Run(name, func(t *testing.T) {
			got, ok := sparkTimeSeconds(tc.in)
			if ok != tc.wantOK || (ok && got != tc.want) {
				t.Errorf("sparkTimeSeconds(%q) = %d, %v; want %d, %v", tc.in, got, ok, tc.want, tc.wantOK)
			}
		})
	}
}

// The floor property only takes effect on the elastic path; on an application without the
// elastic-job annotation the webhook warns (never rejects) that it will be ignored.
func TestPartialPreemptionWarnings(t *testing.T) {
	const floor = "3"
	static := func(annotate bool) *sparkapplicationtesting.SparkApplicationWrapper {
		w := sparkapplicationtesting.MakeSparkApplication("app", "ns").Queue("local-queue").ExecutorInstances(11)
		if annotate {
			w = w.Annotation(workloadslicing.EnabledAnnotationKey, workloadslicing.EnabledAnnotationValue)
		}
		return w
	}
	withFloor := func(w *sparkapplicationtesting.SparkApplicationWrapper) *sparkv1beta2.SparkApplication {
		return withSparkConf(w.Obj(), map[string]string{sparkConfPreemptionMinExecutors: floor})
	}
	cases := map[string]struct {
		gates    map[featuregate.Feature]bool
		old, job *sparkv1beta2.SparkApplication
		wantWarn bool
	}{
		"floor without the elastic annotation warns":   {gates: partialPreemptionGates, job: withFloor(static(false)), wantWarn: true},
		"floor with the elastic annotation is silent":  {gates: partialPreemptionGates, job: withFloor(static(true))},
		"no floor is silent":                           {gates: partialPreemptionGates, job: static(false).Obj()},
		"gate off is silent":                           {gates: map[featuregate.Feature]bool{features.SparkPartialPreemption: false}, job: withFloor(static(false))},
		"not managed by Kueue is silent":               {gates: partialPreemptionGates, job: withSparkConf(sparkapplicationtesting.MakeSparkApplication("app", "ns").ExecutorInstances(11).Obj(), map[string]string{sparkConfPreemptionMinExecutors: floor})},
		"update adding the floor warns":                {gates: partialPreemptionGates, old: static(false).Obj(), job: withFloor(static(false)), wantWarn: true},
		"update leaving the floor unchanged is silent": {gates: partialPreemptionGates, old: withFloor(static(false)), job: withFloor(static(false))},
	}
	for name, tc := range cases {
		t.Run(name, func(t *testing.T) {
			features.SetFeatureGatesDuringTest(t, tc.gates)
			w := &SparkApplicationWebhook{}
			var warns []string
			if tc.old == nil {
				ctx, _ := utiltesting.ContextWithLog(t)
				got, _ := w.ValidateCreate(ctx, tc.job)
				warns = got
			} else {
				warns = w.partialPreemptionWarnings(tc.old, tc.job)
			}
			if got := len(warns) > 0; got != tc.wantWarn {
				t.Fatalf("warnings = %q, want a warning: %v", warns, tc.wantWarn)
			}
			if tc.wantWarn && !strings.Contains(warns[0], workloadslicing.EnabledAnnotationKey) {
				t.Errorf("warning %q does not name the annotation", warns[0])
			}
		})
	}
}
