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
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/google/go-cmp/cmp"
	sparkv1beta2 "github.com/kubeflow/spark-operator/v2/api/v1beta2"
	sparkcommon "github.com/kubeflow/spark-operator/v2/pkg/common"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
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
	features.ElasticJobsViaWorkloadSlices: true,
	features.SparkPartialPreemption:       true,
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

func TestPartialPreemptionFloor(t *testing.T) {
	da := func(minExecutors *int32) *sparkv1beta2.DynamicAllocation {
		return &sparkv1beta2.DynamicAllocation{Enabled: true, MinExecutors: minExecutors}
	}
	cases := map[string]struct {
		app          *sparkv1beta2.SparkApplication
		featureGates map[featuregate.Feature]bool
		liveCount    *int32 // as PodSets would have cached it; nil uses the client-less estimate
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
			app: withSparkConf(elasticApp().DynamicAllocation(da(new(int32(3)))).Obj(),
				map[string]string{sparkConfPreemptionMinExecutors: "1"}),
			liveCount: new(int32(6)),
			wantFloor: 1,
			wantOK:    true,
		},
		"Dynamic Allocation with minExecutors unset is raised to 1": {
			app:       elasticApp().DynamicAllocation(da(nil)).Obj(),
			liveCount: new(int32(6)),
			wantFloor: 1,
			wantOK:    true,
		},
		"Dynamic Allocation with minExecutors 0 is raised to 1": {
			app:       elasticApp().DynamicAllocation(da(new(int32(0)))).Obj(),
			liveCount: new(int32(6)),
			wantFloor: 1,
			wantOK:    true,
		},
		"Dynamic Allocation minExecutors 3": {
			app:       elasticApp().DynamicAllocation(da(new(int32(3)))).Obj(),
			liveCount: new(int32(6)),
			wantFloor: 3,
			wantOK:    true,
		},
		"Dynamic Allocation minExecutors from sparkConf": {
			app: withSparkConf(elasticApp().Obj(), map[string]string{
				"spark.dynamicAllocation.enabled":      "true",
				"spark.dynamicAllocation.minExecutors": "4",
			}),
			liveCount: new(int32(6)),
			wantFloor: 4,
			wantOK:    true,
		},
		"static uses the executor count": {
			app:       elasticApp().ExecutorInstances(4).Obj(),
			wantFloor: 4,
			wantOK:    true,
		},
		"static uses spark.executor.instances from sparkConf": {
			app: func() *sparkv1beta2.SparkApplication {
				app := elasticApp().Obj()
				app.Spec.Executor.Instances = nil
				return withSparkConf(app, map[string]string{"spark.executor.instances": "7"})
			}(),
			wantFloor: 7,
			wantOK:    true,
		},
		"property above the count is capped at the count": {
			app:       withSparkConf(elasticApp().ExecutorInstances(3).Obj(), map[string]string{sparkConfPreemptionMinExecutors: "10"}),
			wantFloor: 3,
			wantOK:    true,
		},
		"capped at the live count PodSets observed": {
			app:       elasticApp().DynamicAllocation(da(new(int32(3)))).Obj(),
			liveCount: new(int32(2)),
			wantFloor: 2,
			wantOK:    true,
		},
		"malformed property falls back to the static count": {
			app:       withSparkConf(elasticApp().ExecutorInstances(4).Obj(), map[string]string{sparkConfPreemptionMinExecutors: "two"}),
			wantFloor: 4,
			wantOK:    true,
		},
		"negative property falls back to Dynamic Allocation": {
			app: withSparkConf(elasticApp().DynamicAllocation(da(new(int32(2)))).Obj(),
				map[string]string{sparkConfPreemptionMinExecutors: "-1"}),
			liveCount: new(int32(6)),
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
			job.cachedLiveExecutorCount = tc.liveCount
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
		nilClient    bool
		wantMarked   int32
		wantOK       bool
	}{
		"static elastic app counts only ungated live non-deleting executors with cost < 0": {
			app:        elasticApp().ExecutorInstances(4).Obj(),
			wantMarked: 2,
			wantOK:     true,
		},
		"dynamic allocation elastic app shares the live-count listing": {
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
		"no client is not observed": {
			app:       elasticApp().ExecutorInstances(4).Obj(),
			nilClient: true,
		},
	}
	for name, tc := range cases {
		t.Run(name, func(t *testing.T) {
			gates := tc.featureGates
			if gates == nil {
				gates = partialPreemptionGates
			}
			features.SetFeatureGatesDuringTest(t, gates)
			var c client.Client
			if !tc.nilClient {
				objs := make([]client.Object, 0, len(allKinds))
				for _, p := range allKinds {
					objs = append(objs, p.obj())
				}
				builder := utiltesting.NewClientBuilder().WithObjects(objs...)
				c = builder.Build()
				if err := SetupIndexes(t.Context(), utiltesting.AsIndexer(builder)); err != nil {
					t.Fatalf("failed to setup indexes: %v", err)
				}
			}
			job := fromObject(tc.app)
			if _, err := job.PodSets(t.Context(), c); err != nil {
				t.Fatalf("PodSets() returned an unexpected error: %v", err)
			}
			marked, ok := job.PartialPreemptionMarkedPods()
			if ok != tc.wantOK || marked != tc.wantMarked {
				t.Errorf("PartialPreemptionMarkedPods() = (%d, %v), want (%d, %v)", marked, ok, tc.wantMarked, tc.wantOK)
			}
		})
	}
}

func TestPodSetsListsExecutorPodsOnce(t *testing.T) {
	features.SetFeatureGatesDuringTest(t, partialPreemptionGates)
	lists := 0
	builder := utiltesting.NewClientBuilder().
		WithObjects(ppPod{name: "e1", cost: "-1"}.obj(), ppPod{name: "e2"}.obj()).
		WithInterceptorFuncs(interceptor.Funcs{
			List: func(ctx context.Context, cl client.WithWatch, list client.ObjectList, opts ...client.ListOption) error {
				if _, ok := list.(*corev1.PodList); ok {
					lo := (&client.ListOptions{}).ApplyOptions(opts)
					if lo.LabelSelector != nil && strings.Contains(lo.LabelSelector.String(), sparkcommon.LabelSparkRole+"="+sparkcommon.SparkRoleExecutor) {
						lists++
					}
				}
				return cl.List(ctx, list, opts...)
			},
		})
	c := builder.Build()
	if err := SetupIndexes(t.Context(), utiltesting.AsIndexer(builder)); err != nil {
		t.Fatalf("failed to setup indexes: %v", err)
	}
	job := fromObject(elasticApp().DynamicAllocation(&sparkv1beta2.DynamicAllocation{Enabled: true}).Obj())
	for range 2 {
		if _, err := job.PodSets(t.Context(), c); err != nil {
			t.Fatalf("PodSets() returned an unexpected error: %v", err)
		}
	}
	if lists != 1 {
		t.Errorf("executor Pods listed %d times, want 1", lists)
	}
	if marked, ok := job.PartialPreemptionMarkedPods(); !ok || marked != 1 {
		t.Errorf("PartialPreemptionMarkedPods() = (%d, %v), want (1, true)", marked, ok)
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

			done, err := fromObject(tc.app).ReclaimPods(t.Context(), c, podSet, tc.keep)
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
			if _, err := fromObject(tc.app).ReclaimPods(t.Context(), c, podSet, tc.keep); err != nil {
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

	done, err := job.ReclaimPods(t.Context(), c, executorPodSetName, 2)
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
	done, err = job.ReclaimPods(t.Context(), c, executorPodSetName, 2)
	if err != nil || !done {
		t.Fatalf("second ReclaimPods() = (%v, %v), want (true, nil)", done, err)
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

// The migration-round interval defaults to 15s and the per-block attempt count follows it: 90s of
// retries divided by the interval, capped at 9 and never below Spark's 3. Explicit values are kept.
func TestDecommissionRetryDefaults(t *testing.T) {
	cases := map[string]struct {
		conf                       map[string]string
		wantInterval, wantFailures string
	}{
		"neither set: 90/15 = 6":                               {wantInterval: "15s", wantFailures: "6"},
		"interval set: attempts follow":                        {conf: map[string]string{sparkConfDecommissionReattemptInterval: "10s"}, wantInterval: "10s", wantFailures: "9"},
		"interval in ms: capped at 9":                          {conf: map[string]string{sparkConfDecommissionReattemptInterval: "500ms"}, wantInterval: "500ms", wantFailures: "9"},
		"20s interval: 90/20 rounds up":                        {conf: map[string]string{sparkConfDecommissionReattemptInterval: "20s"}, wantInterval: "20s", wantFailures: "5"},
		"long interval: never below 3":                         {conf: map[string]string{sparkConfDecommissionReattemptInterval: "2m"}, wantInterval: "2m", wantFailures: "3"},
		"attempts set: kept":                                   {conf: map[string]string{sparkConfDecommissionMaxFailures: "7"}, wantInterval: "15s", wantFailures: "7"},
		"unparsable interval: kept, attempts from the default": {conf: map[string]string{sparkConfDecommissionReattemptInterval: "soon"}, wantInterval: "soon", wantFailures: "6"},
	}
	for name, tc := range cases {
		t.Run(name, func(t *testing.T) {
			app := fromObject(withSparkConf(elasticApp().Obj(), tc.conf))
			if app.Spec.SparkConf == nil {
				app.Spec.SparkConf = map[string]string{}
			}
			applyDecommissionDefaults(app)
			if got := app.Spec.SparkConf[sparkConfDecommissionReattemptInterval]; got != tc.wantInterval {
				t.Errorf("interval = %q, want %q", got, tc.wantInterval)
			}
			if got := app.Spec.SparkConf[sparkConfDecommissionMaxFailures]; got != tc.wantFailures {
				t.Errorf("max failures = %q, want %q", got, tc.wantFailures)
			}
		})
	}
}
