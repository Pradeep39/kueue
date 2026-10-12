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

package preemption

import (
	"context"
	"fmt"
	"maps"
	"strconv"
	"testing"
	"time"

	"github.com/google/go-cmp/cmp"
	corev1 "k8s.io/api/core/v1"
	apimeta "k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	clocktesting "k8s.io/utils/clock/testing"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/interceptor"

	kueue "sigs.k8s.io/kueue/apis/kueue/v1beta2"
	schdcache "sigs.k8s.io/kueue/pkg/cache/scheduler"
	"sigs.k8s.io/kueue/pkg/constants"
	controllerconstants "sigs.k8s.io/kueue/pkg/controller/constants"
	"sigs.k8s.io/kueue/pkg/features"
	"sigs.k8s.io/kueue/pkg/resources"
	"sigs.k8s.io/kueue/pkg/scheduler/flavorassigner"
	preemptexpectations "sigs.k8s.io/kueue/pkg/scheduler/preemption/expectations"
	"sigs.k8s.io/kueue/pkg/scheduler/preemption/policy"
	utiltesting "sigs.k8s.io/kueue/pkg/util/testing"
	utiltestingapi "sigs.k8s.io/kueue/pkg/util/testing/v1beta2"
	"sigs.k8s.io/kueue/pkg/workload"
	workloadevict "sigs.k8s.io/kueue/pkg/workload/evict"
)

const partialPodSet kueue.PodSetReference = "executor"

// partialVictimSpec describes an admitted workload for the partial-preemption tests. Every Pod
// of its executor PodSet requests podMilli milli-CPU (1000 when unset).
type partialVictimSpec struct {
	name     string
	cq       string
	pods     int32
	floor    int32
	marked   int32
	priority int32
	podMilli int64
	// age orders admissions, in minutes: a larger age was admitted earlier.
	age int
	// inFlightTarget, when set, is an outstanding partial preemption's target.
	inFlightTarget *int32
	// undeclared victims carry no partial-preemption annotations.
	undeclared  bool
	annotations map[string]string
	evicted     bool
}

func (v partialVictimSpec) workload(now time.Time) *kueue.Workload {
	milli := v.podMilli
	if milli == 0 {
		milli = 1000
	}
	w := utiltestingapi.MakeWorkload(v.name, "").
		UID(types.UID(v.name)).
		Priority(v.priority).
		PodSets(*utiltestingapi.MakePodSet(partialPodSet, int(v.pods)).Request(corev1.ResourceCPU, fmt.Sprintf("%dm", milli)).Obj()).
		ReserveQuotaAt(
			utiltestingapi.MakeAdmission(kueue.ClusterQueueReference(v.cq)).
				PodSets(utiltestingapi.MakePodSetAssignment(partialPodSet).
					Assignment(corev1.ResourceCPU, "default", fmt.Sprintf("%dm", milli*int64(v.pods))).
					Count(v.pods).
					Obj()).
				Obj(),
			now.Add(-time.Minute-time.Duration(v.age)*time.Minute),
		)
	if !v.undeclared {
		w.Annotation(constants.PartialPreemptionPodSetAnnotation, string(partialPodSet)).
			Annotation(constants.PartialPreemptionMinCountAnnotation, strconv.Itoa(int(v.floor)))
		if v.marked > 0 {
			w.Annotation(constants.PartialPreemptionMarkedPodsAnnotation, strconv.Itoa(int(v.marked)))
		}
		if v.inFlightTarget != nil {
			w.Annotation(constants.PartialPreemptionTargetAnnotation, strconv.Itoa(int(*v.inFlightTarget)))
		}
	}
	for k, val := range v.annotations {
		w.Annotation(k, val)
	}
	if v.evicted {
		w.Condition(metav1.Condition{Type: kueue.WorkloadEvicted, Status: metav1.ConditionTrue, Reason: kueue.WorkloadEvictedByPreemption})
	}
	return w.Obj()
}

type partialCase struct {
	cqs      []*kueue.ClusterQueue
	victims  []partialVictimSpec
	preempt  partialVictimSpec // the preemptor; its pods are the request
	cohort   kueue.CohortReference
	disabled bool // SparkPartialPreemption off
	// cqDisabled turns partial preemption off in the preemptor's ClusterQueue snapshot.
	cqDisabled bool
}

func partialCQ(name string, cpu string, cohort kueue.CohortReference) *kueue.ClusterQueue {
	w := utiltestingapi.MakeClusterQueue(name).
		ResourceGroup(*utiltestingapi.MakeFlavorQuotas("default").Resource(corev1.ResourceCPU, cpu).Obj()).
		Preemption(kueue.ClusterQueuePreemption{
			WithinClusterQueue:  kueue.PreemptionPolicyLowerPriority,
			ReclaimWithinCohort: kueue.PreemptionPolicyAny,
		})
	if cohort != "" {
		w.Cohort(cohort)
	}
	return w.Obj()
}

// runPartial plans a PartialOnly preemption and returns the Pods each victim gives up and
// the targets themselves. It fails the test unless the snapshot's usage is restored exactly.
func runPartial(t *testing.T, tc partialCase) (map[string]int32, []*Target) {
	t.Helper()
	features.SetFeatureGateDuringTest(t, features.SparkPartialPreemption, !tc.disabled)
	ctx, log := utiltesting.ContextWithLog(t)
	now := time.Now().Truncate(time.Second)
	var objs []kueue.Workload
	for _, v := range tc.victims {
		objs = append(objs, *v.workload(now))
	}
	cl := utiltesting.NewClientBuilder().WithLists(&kueue.WorkloadList{Items: objs}).Build()
	cqCache := schdcache.New(cl)
	cqCache.AddOrUpdateResourceFlavor(log, utiltestingapi.MakeResourceFlavor("default").Obj())
	for _, cq := range tc.cqs {
		if err := cqCache.AddClusterQueue(ctx, cq); err != nil {
			t.Fatalf("Couldn't add ClusterQueue to cache: %v", err)
		}
	}
	snapshot, err := cqCache.Snapshot(ctx)
	if err != nil {
		t.Fatalf("unexpected error while building snapshot: %v", err)
	}
	if tc.cqDisabled {
		snapshot.ClusterQueue(kueue.ClusterQueueReference(tc.preempt.cq)).PartialPreemption.Enabled = false
	}
	usageBefore := snapshotUsage(snapshot, tc.cohort)

	in := tc.preempt
	milli := in.podMilli
	if milli == 0 {
		milli = 1000
	}
	incoming := utiltestingapi.MakeWorkload(in.name, "").
		UID(types.UID(in.name)).
		Priority(in.priority).
		Annotations(in.annotations).
		PodSets(*utiltestingapi.MakePodSet(partialPodSet, int(in.pods)).Request(corev1.ResourceCPU, fmt.Sprintf("%dm", milli)).Obj()).
		Obj()
	wlInfo := workload.NewInfo(log, incoming)
	wlInfo.ClusterQueue = kueue.ClusterQueueReference(in.cq)
	assignment := flavorassigner.Assignment{PodSets: []flavorassigner.PodSetAssignment{{
		Name:    partialPodSet,
		Flavors: flavorassigner.ResourceAssignment{corev1.ResourceCPU: {Name: "default", Mode: flavorassigner.Preempt}},
		Count:   in.pods,
	}}}

	preemptor := New(cl, workload.Ordering{}, &utiltesting.EventRecorder{}, nil, false, clocktesting.NewFakeClock(now), nil, preemptexpectations.New(), nil)
	strategies := preemptor.GetPreemptionStrategyIteratorWithMode(ctx, *wlInfo, snapshot, assignment, PartialOnly)
	targets := preemptor.GetTargetsWithStrategy(ctx, strategies)

	if diff := cmp.Diff(usageBefore, snapshotUsage(snapshot, tc.cohort)); diff != "" {
		t.Errorf("Snapshot usage not restored (-before,+after):\n%s", diff)
	}
	got := map[string]int32{}
	for _, target := range targets {
		if target.Reclaim == nil {
			t.Errorf("Whole-workload target %s from PartialOnly", target.WorkloadInfo.Obj.Name)
			continue
		}
		if _, dup := got[target.WorkloadInfo.Obj.Name]; dup {
			t.Errorf("Victim %s targeted twice", target.WorkloadInfo.Obj.Name)
		}
		got[target.WorkloadInfo.Obj.Name] = target.Reclaim.Count
	}
	return got, targets
}

func snapshotUsage(snapshot *schdcache.Snapshot, cohort kueue.CohortReference) map[string]resources.FlavorResourceQuantities {
	out := map[string]resources.FlavorResourceQuantities{}
	for name, cq := range snapshot.ClusterQueues() {
		out[string(name)] = maps.Clone(cq.ResourceNode.Usage)
	}
	if cohort != "" {
		out["cohort/"+string(cohort)] = maps.Clone(snapshot.Cohort(cohort).ResourceNode.Usage)
	}
	return out
}

func TestPartialPreemptionPlanning(t *testing.T) {
	low := func(name string, pods, floor int32, age int) partialVictimSpec {
		return partialVictimSpec{name: name, cq: "cq", pods: pods, floor: floor, age: age}
	}
	high := func(pods int32) partialVictimSpec {
		return partialVictimSpec{name: "in", cq: "cq", pods: pods, priority: 100}
	}
	cq := func(cpu string) []*kueue.ClusterQueue { return []*kueue.ClusterQueue{partialCQ("cq", cpu, "")} }

	cases := map[string]struct {
		tc   partialCase
		want map[string]int32
	}{
		"N victims and a shortfall of N: each gives up exactly one": {
			tc: partialCase{
				cqs:     cq("12"),
				victims: []partialVictimSpec{low("a", 3, 1, 4), low("b", 3, 1, 3), low("c", 3, 1, 2), low("d", 3, 1, 1)},
				preempt: high(4),
			},
			want: map[string]int32{"a": 1, "b": 1, "c": 1, "d": 1},
		},
		"no victim gives a second Pod before every victim with headroom gave one": {
			tc: partialCase{
				cqs:     cq("12"),
				victims: []partialVictimSpec{low("a", 5, 1, 3), low("b", 4, 1, 2), low("c", 3, 1, 1)},
				preempt: high(5),
			},
			want: map[string]int32{"a": 2, "b": 2, "c": 1},
		},
		"floors are respected": {
			tc: partialCase{
				cqs:     cq("8"),
				victims: []partialVictimSpec{low("a", 4, 3, 2), low("b", 4, 2, 1)},
				preempt: high(3),
			},
			want: map[string]int32{"a": 1, "b": 2},
		},
		"nothing when even the floors cannot fit the preemptor": {
			tc: partialCase{
				cqs:     cq("8"),
				victims: []partialVictimSpec{low("a", 4, 3, 2), low("b", 4, 2, 1)},
				preempt: high(4),
			},
			want: map[string]int32{},
		},
		"marked Pods are swept first, across victims": {
			tc: partialCase{
				cqs: cq("12"),
				victims: []partialVictimSpec{
					{name: "a", cq: "cq", pods: 4, floor: 1, marked: 2, age: 3},
					{name: "b", cq: "cq", pods: 4, floor: 1, marked: 1, age: 2},
					{name: "c", cq: "cq", pods: 4, floor: 1, age: 1},
				},
				preempt: high(3),
			},
			want: map[string]int32{"a": 2, "b": 1},
		},
		"the lower tier is exhausted before a higher one is touched": {
			tc: partialCase{
				cqs: cq("10"),
				victims: []partialVictimSpec{
					{name: "a", cq: "cq", pods: 3, floor: 1, age: 3},
					{name: "b", cq: "cq", pods: 3, floor: 1, age: 2},
					{name: "c", cq: "cq", pods: 4, floor: 1, priority: 5, age: 1},
				},
				preempt: high(5),
			},
			want: map[string]int32{"a": 2, "b": 2, "c": 1},
		},
		"fill-back returns the Pods that turned out not to be needed": {
			tc: partialCase{
				cqs: cq("9"),
				victims: []partialVictimSpec{
					{name: "a", cq: "cq", pods: 3, floor: 1, podMilli: 2000, age: 2},
					{name: "b", cq: "cq", pods: 3, floor: 1, age: 1},
				},
				preempt: partialVictimSpec{name: "in", cq: "cq", pods: 1, priority: 100, podMilli: 2000},
			},
			want: map[string]int32{"a": 1},
		},
		"an outstanding reclaim is reused and its victim is not picked again": {
			tc: partialCase{
				cqs: cq("8"),
				victims: []partialVictimSpec{
					{name: "a", cq: "cq", pods: 4, floor: 1, inFlightTarget: new(int32(3)), age: 2},
					{name: "b", cq: "cq", pods: 4, floor: 1, age: 1},
				},
				preempt: high(2),
			},
			want: map[string]int32{"a": 1, "b": 1},
		},
		"an outstanding reclaim alone is enough": {
			tc: partialCase{
				cqs: cq("8"),
				victims: []partialVictimSpec{
					{name: "a", cq: "cq", pods: 4, floor: 1, inFlightTarget: new(int32(2)), age: 1},
					{name: "b", cq: "cq", pods: 4, floor: 1, age: 2},
				},
				preempt: high(2),
			},
			want: map[string]int32{"a": 2},
		},
		"workloads that do not declare partial preemption are never partially preempted": {
			tc: partialCase{
				cqs: cq("8"),
				victims: []partialVictimSpec{
					{name: "a", cq: "cq", pods: 4, undeclared: true, age: 1},
					{name: "b", cq: "cq", pods: 4, floor: 1, age: 2},
				},
				preempt: high(4),
			},
			want: map[string]int32{},
		},
		"evicted workloads are not partially preempted": {
			tc: partialCase{
				cqs: cq("8"),
				victims: []partialVictimSpec{
					{name: "a", cq: "cq", pods: 4, floor: 1, evicted: true, age: 1},
					{name: "b", cq: "cq", pods: 4, floor: 1, age: 2},
				},
				preempt: high(2),
			},
			want: map[string]int32{"b": 2},
		},
		"the preemptor's own chain is not partially preempted": {
			tc: partialCase{
				cqs: cq("8"),
				victims: []partialVictimSpec{
					{name: "a", cq: "cq", pods: 4, floor: 1, priority: 0, age: 1},
					{name: "b", cq: "cq", pods: 4, floor: 1, age: 2},
				},
				preempt: partialVictimSpec{name: "a-2", cq: "cq", pods: 2, priority: 100, annotations: map[string]string{
					constants.WorkloadSliceReplacementForAnnotation: "/a",
					kueue.WorkloadSliceNameAnnotation:               "a",
				}},
			},
			want: map[string]int32{"b": 2},
		},
		"cohort reclaim stops once the lender is back within nominal": {
			tc: partialCase{
				cohort: "co",
				cqs:    []*kueue.ClusterQueue{partialCQ("cq-a", "4", "co"), partialCQ("cq-b", "4", "co")},
				victims: []partialVictimSpec{
					{name: "z", cq: "cq-a", pods: 2, undeclared: true, priority: 100},
					{name: "x", cq: "cq-b", pods: 3, floor: 0, age: 2},
					{name: "y", cq: "cq-b", pods: 3, floor: 0, age: 1},
				},
				preempt: partialVictimSpec{name: "in", cq: "cq-a", pods: 4, priority: 100},
			},
			want: map[string]int32{},
		},
		"cohort reclaim takes from the borrowing ClusterQueue": {
			tc: partialCase{
				cohort: "co",
				cqs:    []*kueue.ClusterQueue{partialCQ("cq-a", "4", "co"), partialCQ("cq-b", "4", "co")},
				victims: []partialVictimSpec{
					{name: "z", cq: "cq-a", pods: 2, undeclared: true, priority: 100},
					{name: "x", cq: "cq-b", pods: 3, floor: 0, age: 2},
					{name: "y", cq: "cq-b", pods: 3, floor: 0, age: 1},
				},
				preempt: partialVictimSpec{name: "in", cq: "cq-a", pods: 2, priority: 100},
			},
			want: map[string]int32{"x": 1, "y": 1},
		},
		"control for the disabled cases: the same setup reclaims when enabled": {
			tc: partialCase{
				cqs:     cq("6"),
				victims: []partialVictimSpec{low("a", 3, 1, 2), low("b", 3, 1, 1)},
				preempt: high(2),
			},
			want: map[string]int32{"a": 1, "b": 1},
		},
		"nothing when the ClusterQueue does not enable partial preemption": {
			tc: partialCase{
				cqs:        cq("6"),
				victims:    []partialVictimSpec{low("a", 3, 1, 2), low("b", 3, 1, 1)},
				preempt:    high(2),
				cqDisabled: true,
			},
			want: map[string]int32{},
		},
		"nothing with the feature gate off": {
			tc: partialCase{
				cqs:      cq("6"),
				victims:  []partialVictimSpec{low("a", 3, 1, 2), low("b", 3, 1, 1)},
				preempt:  high(2),
				disabled: true,
			},
			want: map[string]int32{},
		},
	}
	for name, tc := range cases {
		t.Run(name, func(t *testing.T) {
			got, targets := runPartial(t, tc.tc)
			if diff := cmp.Diff(tc.want, got); diff != "" {
				t.Errorf("Unexpected reclaims (-want,+got):\n%s", diff)
			}
			for _, target := range targets {
				if target.WorkloadCq == nil || target.WorkloadCq.Name != target.WorkloadInfo.ClusterQueue {
					t.Errorf("Target %s has WorkloadCq %v", target.WorkloadInfo.Obj.Name, target.WorkloadCq)
				}
				wantReason := kueue.InClusterQueueReason
				if target.WorkloadInfo.ClusterQueue != kueue.ClusterQueueReference(tc.tc.preempt.cq) {
					wantReason = kueue.InCohortReclamationReason
				}
				if target.Reason != wantReason {
					t.Errorf("Target %s has reason %q, want %q", target.WorkloadInfo.Obj.Name, target.Reason, wantReason)
				}
			}
		})
	}
}

// Superseded slices contribute no usage: taking Pods from one would free nothing.
func TestPartialPreemptionExcludesSupersededSlices(t *testing.T) {
	features.SetFeatureGateDuringTest(t, features.ElasticJobsViaWorkloadSlices, true)
	elastic := map[string]string{constants.ElasticJobAnnotation: "true"}
	got, _ := runPartial(t, partialCase{
		cqs: []*kueue.ClusterQueue{partialCQ("cq", "12", "")},
		victims: []partialVictimSpec{
			{name: "v", cq: "cq", pods: 4, floor: 1, age: 3, annotations: elastic},
			{name: "v-2", cq: "cq", pods: 6, floor: 1, age: 2, annotations: map[string]string{
				constants.ElasticJobAnnotation:                  "true",
				constants.WorkloadSliceReplacementForAnnotation: "/v",
				kueue.WorkloadSliceNameAnnotation:               "v",
			}},
			{name: "w", cq: "cq", pods: 6, floor: 1, age: 1},
		},
		preempt: partialVictimSpec{name: "in", cq: "cq", pods: 4, priority: 100},
	})
	if _, found := got["v"]; found {
		t.Errorf("Superseded slice v was partially preempted: %v", got)
	}
	if diff := cmp.Diff(map[string]int32{"v-2": 2, "w": 2}, got); diff != "" {
		t.Errorf("Unexpected reclaims (-want,+got):\n%s", diff)
	}
}

// Whole preemption must ignore the partial-preemption declarations entirely.
func TestWholeOnlyIgnoresPartialPreemption(t *testing.T) {
	features.SetFeatureGateDuringTest(t, features.SparkPartialPreemption, true)
	ctx, log := utiltesting.ContextWithLog(t)
	now := time.Now().Truncate(time.Second)
	victim := partialVictimSpec{name: "a", cq: "cq", pods: 4, floor: 1}.workload(now)
	cl := utiltesting.NewClientBuilder().WithObjects(victim).Build()
	cqCache := schdcache.New(cl)
	cqCache.AddOrUpdateResourceFlavor(log, utiltestingapi.MakeResourceFlavor("default").Obj())
	if err := cqCache.AddClusterQueue(ctx, partialCQ("cq", "4", "")); err != nil {
		t.Fatal(err)
	}
	snapshot, err := cqCache.Snapshot(ctx)
	if err != nil {
		t.Fatal(err)
	}
	incoming := utiltestingapi.MakeWorkload("in", "").Priority(100).
		PodSets(*utiltestingapi.MakePodSet(partialPodSet, 1).Request(corev1.ResourceCPU, "1").Obj()).Obj()
	wlInfo := workload.NewInfo(log, incoming)
	wlInfo.ClusterQueue = "cq"
	assignment := flavorassigner.Assignment{PodSets: []flavorassigner.PodSetAssignment{{
		Name: partialPodSet, Count: 1,
		Flavors: flavorassigner.ResourceAssignment{corev1.ResourceCPU: {Name: "default", Mode: flavorassigner.Preempt}},
	}}}
	preemptor := New(cl, workload.Ordering{}, &utiltesting.EventRecorder{}, nil, false, clocktesting.NewFakeClock(now), nil, preemptexpectations.New(), nil)
	targets := preemptor.GetTargetsWithStrategy(ctx, preemptor.GetPreemptionStrategyIteratorWithMode(ctx, *wlInfo, snapshot, assignment, WholeOnly))
	if len(targets) != 1 || targets[0].Reclaim != nil {
		t.Fatalf("Want one whole target, got %v", targets)
	}
}

func TestPartialPodSetUsageRoundTrip(t *testing.T) {
	_, log := utiltesting.ContextWithLog(t)
	now := time.Now()
	wl := workload.NewInfo(log, partialVictimSpec{name: "a", cq: "cq", pods: 3, floor: 1, podMilli: 333}.workload(now))
	got := schdcache.PartialPodSetUsage(wl, partialPodSet, 2)
	fr := resources.FlavorResource{Flavor: "default", Resource: corev1.ResourceCPU}
	if want := resources.NewAmount(666); !got.Quota.Assigned[fr].Equal(want) {
		t.Errorf("PartialPodSetUsage = %v, want %v", got.Quota.Assigned[fr], want)
	}
	if got := schdcache.PartialPodSetUsage(wl, "driver", 1); len(got.Quota.Assigned) != 0 {
		t.Errorf("PartialPodSetUsage of a missing PodSet = %v, want empty", got.Quota.Assigned)
	}
}

func TestIssuePartialPreemption(t *testing.T) {
	for _, useMergePatch := range []bool{false, true} {
		t.Run(fmt.Sprintf("WorkloadRequestUseMergePatch=%t", useMergePatch), func(t *testing.T) {
			features.SetFeatureGateDuringTest(t, features.WorkloadRequestUseMergePatch, useMergePatch)
			features.SetFeatureGateDuringTest(t, features.SparkPartialPreemption, true)
			ctx, log := utiltesting.ContextWithLog(t)
			now := time.Now().Truncate(time.Second)
			victim := partialVictimSpec{name: "a", cq: "cq", pods: 5, floor: 1}.workload(now)
			victim.ResourceVersion = ""
			var evictions, patches int
			cl := utiltesting.NewClientBuilder().
				WithObjects(victim).
				WithStatusSubresource(&kueue.Workload{}).
				WithInterceptorFuncs(interceptor.Funcs{
					Patch: func(ctx context.Context, c client.WithWatch, obj client.Object, patch client.Patch, opts ...client.PatchOption) error {
						patches++
						return c.Patch(ctx, obj, patch, opts...)
					},
					SubResourcePatch: func(ctx context.Context, c client.Client, subResourceName string, obj client.Object, patch client.Patch, opts ...client.SubResourcePatchOption) error {
						patches++
						if err := c.SubResource(subResourceName).Patch(ctx, obj, patch, opts...); err != nil {
							return err
						}
						if wl, ok := obj.(*kueue.Workload); ok && workloadevict.IsEvicted(wl) {
							evictions++
						}
						return nil
					},
					SubResourceApply: func(ctx context.Context, c client.Client, subResourceName string, applyConf runtime.ApplyConfiguration, opts ...client.SubResourceApplyOption) error {
						patches++
						return utiltesting.TreatSSAAsStrategicMergeForApplyConfiguration(ctx, c, subResourceName, applyConf, opts...)
					},
				}).
				Build()
			cq := partialCQ("cq", "5", "")
			cq.Annotations = map[string]string{constants.PartialPreemptionTimeoutAnnotation: "90s"}
			cqCache := schdcache.New(cl)
			cqCache.AddOrUpdateResourceFlavor(log, utiltestingapi.MakeResourceFlavor("default").Obj())
			if err := cqCache.AddClusterQueue(ctx, cq); err != nil {
				t.Fatal(err)
			}
			snapshot, err := cqCache.Snapshot(ctx)
			if err != nil {
				t.Fatal(err)
			}
			var stored kueue.Workload
			if err := cl.Get(ctx, client.ObjectKeyFromObject(victim), &stored); err != nil {
				t.Fatal(err)
			}
			incoming := utiltestingapi.MakeWorkload("in", "").UID("in").Priority(100).
				Label(controllerconstants.JobUIDLabel, "job-in").Obj()
			wlInfo := workload.NewInfo(log, incoming)
			wlInfo.ClusterQueue = "cq"
			targetInfo := workload.NewInfo(log, &stored)
			targetInfo.ClusterQueue = "cq"
			targets := []*Target{{
				WorkloadInfo: targetInfo,
				Reason:       kueue.InClusterQueueReason,
				WorkloadCq:   snapshot.ClusterQueue("cq"),
				Reclaim:      &policy.PartialReclaim{PodSet: partialPodSet, Count: 2},
			}}
			recorder := &utiltesting.EventRecorder{}
			store := preemptexpectations.New()
			preemptor := New(cl, workload.Ordering{}, recorder, nil, false, clocktesting.NewFakeClock(now), nil, store, nil)

			preempted, failed, err := preemptor.IssuePreemptions(ctx, cqCache, wlInfo, targets, snapshot.ClusterQueue("cq"))
			if err != nil || preempted != 1 || failed != 0 {
				t.Fatalf("IssuePreemptions = (%d, %d, %v), want (1, 0, nil)", preempted, failed, err)
			}
			var got kueue.Workload
			if err := cl.Get(ctx, client.ObjectKeyFromObject(victim), &got); err != nil {
				t.Fatal(err)
			}
			wantAnn := map[string]string{
				constants.PartialPreemptionTargetAnnotation:       "3",
				constants.PartialPreemptionDeadlineAnnotation:     now.Add(90 * time.Second).UTC().Format(time.RFC3339),
				constants.PartialPreemptionRestoreCountAnnotation: "5",
			}
			for k, want := range wantAnn {
				if got.Annotations[k] != want {
					t.Errorf("Annotation %s = %q, want %q", k, got.Annotations[k], want)
				}
			}
			cond := apimeta.FindStatusCondition(got.Status.Conditions, constants.PartiallyPreemptedCondition)
			if cond == nil || cond.Status != metav1.ConditionTrue || cond.Reason != constants.PartialPreemptionIssuedReason {
				t.Errorf("Unexpected PartiallyPreempted condition %+v", cond)
			}
			if workloadevict.IsEvicted(&got) || evictions != 0 || !workload.HasQuotaReservation(&got) {
				t.Error("The victim of a partial preemption was evicted")
			}
			if apimeta.FindStatusCondition(got.Status.Conditions, kueue.WorkloadPreempted) != nil {
				t.Error("The victim of a partial preemption has a Preempted condition")
			}
			reasons := map[string]int{}
			for _, e := range recorder.RecordedEvents {
				reasons[e.Reason]++
			}
			if diff := cmp.Diff(map[string]int{"PartiallyPreempted": 1, "PartiallyPreemptedWorkload": 1}, reasons); diff != "" {
				t.Errorf("Unexpected events (-want,+got):\n%s", diff)
			}
			if store.Satisfied(log, client.ObjectKeyFromObject(victim)) {
				t.Error("The preemption expectation should be pending until the target is observed")
			}

			// The same stale targets are skipped while the expectation is pending.
			patchesAfterFirst := patches
			if preempted, _, err := preemptor.IssuePreemptions(ctx, cqCache, wlInfo, targets, snapshot.ClusterQueue("cq")); err != nil || preempted != 1 {
				t.Fatalf("Second IssuePreemptions = (%d, %v)", preempted, err)
			}
			if patches != patchesAfterFirst {
				t.Errorf("Second IssuePreemptions patched %d times", patches-patchesAfterFirst)
			}

			// Once the target is observed, the reclaim counts as ongoing and the expectation is met.
			observed := workload.NewInfo(log, &got)
			observed.ClusterQueue = "cq"
			targets[0].WorkloadInfo = observed
			if preempted, _, err := preemptor.IssuePreemptions(ctx, cqCache, wlInfo, targets, snapshot.ClusterQueue("cq")); err != nil || preempted != 1 {
				t.Fatalf("Third IssuePreemptions = (%d, %v)", preempted, err)
			}
			if patches != patchesAfterFirst {
				t.Errorf("IssuePreemptions on an ongoing partial preemption patched %d times", patches-patchesAfterFirst)
			}
			if !store.Satisfied(log, client.ObjectKeyFromObject(victim)) {
				t.Error("The preemption expectation should be met once the target is observed")
			}
		})
	}
}

func TestIssuePartialPreemptionKeepsRestoreCount(t *testing.T) {
	features.SetFeatureGateDuringTest(t, features.SparkPartialPreemption, true)
	ctx, log := utiltesting.ContextWithLog(t)
	now := time.Now().Truncate(time.Second)
	victim := partialVictimSpec{name: "a", cq: "cq", pods: 4, floor: 1, annotations: map[string]string{
		constants.PartialPreemptionRestoreCountAnnotation: "6",
	}}.workload(now)
	victim.ResourceVersion = ""
	cl := utiltesting.NewClientBuilder().WithObjects(victim).WithStatusSubresource(&kueue.Workload{}).
		WithInterceptorFuncs(interceptor.Funcs{SubResourceApply: func(ctx context.Context, c client.Client, subResourceName string, applyConf runtime.ApplyConfiguration, opts ...client.SubResourceApplyOption) error {
			return utiltesting.TreatSSAAsStrategicMergeForApplyConfiguration(ctx, c, subResourceName, applyConf, opts...)
		}}).Build()
	var stored kueue.Workload
	if err := cl.Get(ctx, client.ObjectKeyFromObject(victim), &stored); err != nil {
		t.Fatal(err)
	}
	cqCache := schdcache.New(cl)
	cqCache.AddOrUpdateResourceFlavor(log, utiltestingapi.MakeResourceFlavor("default").Obj())
	if err := cqCache.AddClusterQueue(ctx, partialCQ("cq", "4", "")); err != nil {
		t.Fatal(err)
	}
	snapshot, err := cqCache.Snapshot(ctx)
	if err != nil {
		t.Fatal(err)
	}
	preemptor := New(cl, workload.Ordering{}, &utiltesting.EventRecorder{}, nil, false, clocktesting.NewFakeClock(now), nil, preemptexpectations.New(), nil)
	targetInfo := workload.NewInfo(log, &stored)
	targetInfo.ClusterQueue = "cq"
	in := workload.NewInfo(log, utiltestingapi.MakeWorkload("in", "").Obj())
	if _, _, err := preemptor.IssuePreemptions(ctx, cqCache, in, []*Target{{
		WorkloadInfo: targetInfo,
		Reason:       kueue.InClusterQueueReason,
		WorkloadCq:   snapshot.ClusterQueue("cq"),
		Reclaim:      &policy.PartialReclaim{PodSet: partialPodSet, Count: 1},
	}}, snapshot.ClusterQueue("cq")); err != nil {
		t.Fatal(err)
	}
	var got kueue.Workload
	if err := cl.Get(ctx, client.ObjectKeyFromObject(victim), &got); err != nil {
		t.Fatal(err)
	}
	if got.Annotations[constants.PartialPreemptionRestoreCountAnnotation] != "6" || got.Annotations[constants.PartialPreemptionTargetAnnotation] != "3" {
		t.Errorf("Unexpected annotations %v", got.Annotations)
	}
	if got.Annotations[constants.PartialPreemptionDeadlineAnnotation] != now.Add(90*time.Second).UTC().Format(time.RFC3339) {
		t.Errorf("Deadline %q does not use the default timeout", got.Annotations[constants.PartialPreemptionDeadlineAnnotation])
	}
}

// A partial victim recorded earlier in the cycle frees only its reclaimed Pods. Crediting its
// whole usage would let a second workload fit into quota the victim still holds (SC-1).
func TestPreemptedTargetsCreditOnlyReclaimedPods(t *testing.T) {
	features.SetFeatureGateDuringTest(t, features.SparkPartialPreemption, true)
	ctx, log := utiltesting.ContextWithLog(t)
	now := time.Now().Truncate(time.Second)
	a := partialVictimSpec{name: "a", cq: "cq", pods: 4, floor: 1}.workload(now)
	b := partialVictimSpec{name: "b", cq: "cq", pods: 4, floor: 1}.workload(now)
	cl := utiltesting.NewClientBuilder().WithObjects(a, b).Build()
	cqCache := schdcache.New(cl)
	cqCache.AddOrUpdateResourceFlavor(log, utiltestingapi.MakeResourceFlavor("default").Obj())
	if err := cqCache.AddClusterQueue(ctx, partialCQ("cq", "8", "")); err != nil {
		t.Fatal(err)
	}
	snapshot, err := cqCache.Snapshot(ctx)
	if err != nil {
		t.Fatal(err)
	}
	cq := snapshot.ClusterQueue("cq")
	aInfo := cq.Workloads[workload.Key(a)]
	bInfo := cq.Workloads[workload.Key(b)]
	before := snapshotUsage(snapshot, "")
	fr := resources.FlavorResource{Flavor: "default", Resource: corev1.ResourceCPU}
	need := func(milli int64) workload.Usage {
		return workload.Usage{Quota: workload.ResourceUsage{Assigned: resources.FlavorResourceQuantities{fr: resources.NewAmount(milli)}}}
	}
	partialA := &Target{WorkloadInfo: aInfo, Reclaim: &policy.PartialReclaim{PodSet: partialPodSet, Count: 1}}

	preempted := PreemptedTargets{}
	preempted.Insert([]*Target{partialA})

	// fits(): the usage-only simulation.
	revert := preempted.SimulateUsageRemoval(snapshot)
	if got := cq.Fits(need(1000)); got != schdcache.FitsCheckOk {
		t.Errorf("One reclaimed Pod should fit one Pod, got %v", got)
	}
	if got := cq.Fits(need(2000)); got == schdcache.FitsCheckOk {
		t.Error("A partial victim was credited more than its reclaimed Pod")
	}
	revert()
	if diff := cmp.Diff(before, snapshotUsage(snapshot, "")); diff != "" {
		t.Errorf("SimulateUsageRemoval did not restore usage (-before,+after):\n%s", diff)
	}

	// The same through MergeWithTargets, as fits() combines recorded and new targets.
	merged := preempted.MergeWithTargets([]*Target{{WorkloadInfo: bInfo, Reclaim: &policy.PartialReclaim{PodSet: partialPodSet, Count: 2}}})
	revert = merged.SimulateUsageRemoval(snapshot)
	if got := cq.Fits(need(3000)); got != schdcache.FitsCheckOk {
		t.Errorf("Three reclaimed Pods should fit three Pods, got %v", got)
	}
	if got := cq.Fits(need(4000)); got == schdcache.FitsCheckOk {
		t.Error("Merged partial victims were over-credited")
	}
	revert()
	if len(preempted) != 1 {
		t.Errorf("MergeWithTargets mutated the receiver: %v", preempted)
	}

	// simulateOtherPreemptions(): partial victims stay in their ClusterQueue.
	revert = preempted.SimulateRemoval(ctx, log, snapshot)
	if cq.Workloads[workload.Key(a)] != aInfo {
		t.Error("A partial victim was removed from its ClusterQueue")
	}
	if got := cq.Fits(need(2000)); got == schdcache.FitsCheckOk {
		t.Error("SimulateRemoval over-credited a partial victim")
	}
	revert()
	if diff := cmp.Diff(before, snapshotUsage(snapshot, "")); diff != "" {
		t.Errorf("SimulateRemoval did not restore usage (-before,+after):\n%s", diff)
	}

	// A whole target is still credited whole.
	whole := PreemptedTargets{}
	whole.Insert([]*Target{{WorkloadInfo: bInfo}})
	revert = whole.SimulateUsageRemoval(snapshot)
	if got := cq.Fits(need(4000)); got != schdcache.FitsCheckOk {
		t.Errorf("A whole victim should free all its Pods, got %v", got)
	}
	revert()
}
