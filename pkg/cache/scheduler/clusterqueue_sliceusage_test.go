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

package scheduler

import (
	"testing"
	"time"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	kueue "sigs.k8s.io/kueue/apis/kueue/v1beta2"
	"sigs.k8s.io/kueue/pkg/constants"
	controllerconsts "sigs.k8s.io/kueue/pkg/controller/constants"
	"sigs.k8s.io/kueue/pkg/features"
	utiltesting "sigs.k8s.io/kueue/pkg/util/testing"
	utiltestingapi "sigs.k8s.io/kueue/pkg/util/testing/v1beta2"
	"sigs.k8s.io/kueue/pkg/workload"
)

var sliceBaseTime = time.Date(2026, 8, 8, 12, 0, 0, 0, time.UTC)

// sliceWorkload builds an admitted elastic-job workload slice belonging to the chain rooted
// at chainRoot, with count executors of 512Mi totalling memory. ageSeconds orders slices
// within a chain deterministically; replaces, when set, names the slice this one supersedes.
func sliceWorkload(name, chainRoot string, ageSeconds int, count int32, memory, replaces string) *kueue.Workload {
	return sliceWorkloadForJob(name, chainRoot, "job-uid-1", ageSeconds, count, memory, replaces)
}

// sliceWorkloadForJob is sliceWorkload with an explicit owning-job UID, for the case where
// a recreated job's slices inherit the previous instance's chain-root name.
func sliceWorkloadForJob(name, chainRoot, jobUID string, ageSeconds int, count int32, memory, replaces string) *kueue.Workload {
	wl := utiltestingapi.MakeWorkload(name, "batch-ns").
		Label(controllerconsts.JobUIDLabel, jobUID).
		Annotation(constants.ElasticJobAnnotation, elasticJobAnnotationValue).
		Annotation(kueue.WorkloadSliceNameAnnotation, chainRoot).
		Creation(sliceBaseTime.Add(time.Duration(ageSeconds) * time.Second)).
		PodSets(*utiltestingapi.MakePodSet("executor", int(count)).
			Request(corev1.ResourceMemory, "512Mi").Obj())
	if replaces != "" {
		wl = wl.Annotation(constants.WorkloadSliceReplacementForAnnotation, "batch-ns/"+replaces)
	}
	return wl.
		ReserveQuotaAt(utiltestingapi.MakeAdmission("batch-queue").
			PodSets(utiltestingapi.MakePodSetAssignment("executor").
				Assignment(corev1.ResourceMemory, "default", memory).
				Count(count).
				Obj()).
			Obj(), sliceBaseTime).
		Condition(metav1.Condition{Type: kueue.WorkloadAdmitted, Status: metav1.ConditionTrue}).
		Obj()
}

// TestClusterQueueSliceReplacementUsageAccounting exercises the ordering and transition cases
// of the superseded-slice accounting added to fix the flavorsUsage overcommit. Each case
// asserts the ClusterQueue's admitted usage after a sequence of cache operations.
func TestClusterQueueSliceReplacementUsageAccounting(t *testing.T) {
	features.SetFeatureGateDuringTest(t, features.ElasticJobsViaWorkloadSlices, true)

	// 4Gi = 8 executors, 5Gi = 10, 6Gi = 12.
	const (
		oldName = "slice-old"
		newName = "slice-new"
	)

	type op struct {
		add    *kueue.Workload
		delete string
	}

	cases := map[string]struct {
		ops       []op
		wantUsage string
	}{
		"replacement added after the slice it replaces: only the replacement counts": {
			ops: []op{
				{add: sliceWorkload(oldName, oldName, 0, 8, "4Gi", "")},
				{add: sliceWorkload(newName, oldName, 1, 10, "5Gi", oldName)},
			},
			wantUsage: "5Gi",
		},
		"replacement added BEFORE the slice it replaces: the late arrival stays uncounted": {
			// Cache adds are event-driven and can arrive in either order.
			ops: []op{
				{add: sliceWorkload(newName, oldName, 1, 10, "5Gi", oldName)},
				{add: sliceWorkload(oldName, oldName, 0, 8, "4Gi", "")},
			},
			wantUsage: "5Gi",
		},
		"superseded slice finally Finished and deleted: usage unchanged": {
			ops: []op{
				{add: sliceWorkload(oldName, oldName, 0, 8, "4Gi", "")},
				{add: sliceWorkload(newName, oldName, 1, 10, "5Gi", oldName)},
				{delete: oldName},
			},
			wantUsage: "5Gi",
		},
		"superseded slice re-added after deletion stays uncounted": {
			// The old slice remains admitted in the API until its Finish lands, so an
			// unrelated update event can re-add it. It must not resume being counted.
			ops: []op{
				{add: sliceWorkload(oldName, oldName, 0, 8, "4Gi", "")},
				{add: sliceWorkload(newName, oldName, 1, 10, "5Gi", oldName)},
				{delete: oldName},
				{add: sliceWorkload(oldName, oldName, 0, 8, "4Gi", "")},
			},
			wantUsage: "5Gi",
		},
		"replacement deleted before the slice it replaced: predecessor resumes counting": {
			// The admission patch failed and the new slice was rolled out of the cache.
			// The old slice is still legitimately admitted, so its usage must come back.
			ops: []op{
				{add: sliceWorkload(oldName, oldName, 0, 8, "4Gi", "")},
				{add: sliceWorkload(newName, oldName, 1, 10, "5Gi", oldName)},
				{delete: newName},
			},
			wantUsage: "4Gi",
		},
		"superseded slice updated in place stays uncounted": {
			// addOrUpdateWorkload deletes then re-adds; the superseded state must survive.
			ops: []op{
				{add: sliceWorkload(oldName, oldName, 0, 8, "4Gi", "")},
				{add: sliceWorkload(newName, oldName, 1, 10, "5Gi", oldName)},
				{add: sliceWorkload(oldName, oldName, 0, 8, "4Gi", "")},
			},
			wantUsage: "5Gi",
		},
		"chain of three slices: only the newest counts": {
			ops: []op{
				{add: sliceWorkload("slice-a", "slice-a", 0, 4, "2Gi", "")},
				{add: sliceWorkload("slice-b", "slice-a", 1, 8, "4Gi", "slice-a")},
				{add: sliceWorkload("slice-c", "slice-a", 2, 10, "5Gi", "slice-b")},
			},
			wantUsage: "5Gi",
		},
		"chain of three, middle slice deleted: newest still the only one counted": {
			ops: []op{
				{add: sliceWorkload("slice-a", "slice-a", 0, 4, "2Gi", "")},
				{add: sliceWorkload("slice-b", "slice-a", 1, 8, "4Gi", "slice-a")},
				{add: sliceWorkload("slice-c", "slice-a", 2, 10, "5Gi", "slice-b")},
				{delete: "slice-b"},
			},
			wantUsage: "5Gi",
		},
		"two replacements race for the same predecessor, one deleted: predecessor stays uncounted": {
			ops: []op{
				{add: sliceWorkload(oldName, oldName, 0, 8, "4Gi", "")},
				{add: sliceWorkload("slice-new-1", oldName, 2, 10, "5Gi", oldName)},
				{add: sliceWorkload("slice-new-2", oldName, 1, 2, "1Gi", oldName)},
				{delete: "slice-new-2"},
			},
			// slice-new-1 still supersedes the old slice, so only its 5Gi counts.
			wantUsage: "5Gi",
		},
		"same chain root reused by a recreated job: both instances counted": {
			// A deleted-and-recreated job's slices can inherit the previous instance's
			// chain-root name. They are independent jobs, so both must be counted; folding
			// the job UID into the chain key is what keeps them apart.
			ops: []op{
				{add: sliceWorkloadForJob("old-instance", "shared-root", "job-uid-A", 0, 4, "2Gi", "")},
				{add: sliceWorkloadForJob("new-instance", "shared-root", "job-uid-B", 1, 4, "2Gi", "")},
			},
			// 2Gi per instance; both count because they are different jobs.
			wantUsage: "4Gi",
		},
		"non-slice workloads are unaffected": {
			ops: []op{
				{add: sliceWorkload("plain-one", "plain-one", 0, 4, "2Gi", "")},
				{add: sliceWorkload("plain-two", "plain-two", 0, 4, "2Gi", "")},
			},
			wantUsage: "4Gi",
		},
	}

	for name, tc := range cases {
		t.Run(name, func(t *testing.T) {
			cq := utiltestingapi.MakeClusterQueue("batch-queue").
				ResourceGroup(
					*utiltestingapi.MakeFlavorQuotas("default").
						Resource(corev1.ResourceMemory, "6Gi").
						Obj(),
				).
				Obj()

			ctx, log := utiltesting.ContextWithLog(t)
			cache := New(utiltesting.NewFakeClient())
			if err := cache.AddClusterQueue(ctx, cq); err != nil {
				t.Fatalf("Adding ClusterQueue: %v", err)
			}

			for i, o := range tc.ops {
				switch {
				case o.add != nil:
					if added := cache.AddOrUpdateWorkload(log, o.add); !added {
						t.Fatalf("op %d: workload %s was not added", i, workload.Key(o.add))
					}
				case o.delete != "":
					if err := cache.DeleteWorkload(log, workload.NewReference("batch-ns", o.delete)); err != nil {
						t.Fatalf("op %d: deleting workload %s: %v", i, o.delete, err)
					}
				}
			}

			stats, err := cache.Usage(cq)
			if err != nil {
				t.Fatalf("Couldn't get usage: %v", err)
			}
			want := resource.MustParse(tc.wantUsage)
			got := stats.AdmittedResources[0].Resources[0].Total
			if got.Cmp(want) != 0 {
				t.Errorf("Admitted usage = %s, want %s", got.String(), want.String())
			}
			// Reservation and admitted usage are updated through the same funnel
			// (updateWorkloadUsage), so both must agree.
			if gotRes := stats.ReservedResources[0].Resources[0].Total; gotRes.Cmp(want) != 0 {
				t.Errorf("Reserved usage = %s, want %s", gotRes.String(), want.String())
			}
			nominal := cq.Spec.ResourceGroups[0].Flavors[0].Resources[0].NominalQuota
			if got.Cmp(nominal) > 0 {
				t.Errorf("usage %s exceeds nominalQuota %s", got.String(), nominal.String())
			}
		})
	}
}

// TestClusterQueueSliceReplacementDisabledFeatureGate confirms the superseded-slice
// accounting is inert when ElasticJobsViaWorkloadSlices is off, so non-elastic clusters keep
// the previous behavior even if the annotation is somehow present.
func TestClusterQueueSliceReplacementDisabledFeatureGate(t *testing.T) {
	features.SetFeatureGateDuringTest(t, features.ElasticJobsViaWorkloadSlices, false)

	cq := utiltestingapi.MakeClusterQueue("batch-queue").
		ResourceGroup(
			*utiltestingapi.MakeFlavorQuotas("default").
				Resource(corev1.ResourceMemory, "16Gi").
				Obj(),
		).
		Obj()

	ctx, log := utiltesting.ContextWithLog(t)
	cache := New(utiltesting.NewFakeClient())
	if err := cache.AddClusterQueue(ctx, cq); err != nil {
		t.Fatalf("Adding ClusterQueue: %v", err)
	}
	for _, wl := range []*kueue.Workload{
		sliceWorkload("slice-old", "slice-old", 0, 8, "4Gi", ""),
		sliceWorkload("slice-new", "slice-old", 1, 10, "5Gi", "slice-old"),
	} {
		if added := cache.AddOrUpdateWorkload(log, wl); !added {
			t.Fatalf("Workload %s was not added", workload.Key(wl))
		}
	}

	stats, err := cache.Usage(cq)
	if err != nil {
		t.Fatalf("Couldn't get usage: %v", err)
	}
	want := resource.MustParse("9Gi")
	if got := stats.AdmittedResources[0].Resources[0].Total; got.Cmp(want) != 0 {
		t.Errorf("Admitted usage = %s, want %s (both slices counted with the gate off)",
			got.String(), want.String())
	}
}

// TestClusterQueueUsageFrozenAdmissionCountDoesNotOvercommit pins an invariant that is easy
// to get wrong: a stale, frozen status.admission count does NOT inflate ClusterQueue usage.
//
// It is tempting to conclude otherwise, because the cache builds a workload's usage from
// status.admission (workload.totalRequestsFromAdmission) and an elastic scale-down patches
// only spec.podSets[].Count. But that same function scales the admitted usage back down to
// podSetsCountsAfterReclaim — i.e. the *spec* count — whenever the spec count is lower. So
// the cache effectively charges min(spec, granted), and a granted count left frozen high is
// cosmetic as far as quota accounting is concerned.
//
// The inputs here are the two live workloads captured from a real cluster (als-1-b99b3 and
// als-2-2011a): each requests 3 executors in spec while still granted 5 and 7. Both rows
// below charge the same 4Gi, which is the real demand.
func TestClusterQueueUsageFrozenAdmissionCountDoesNotOvercommit(t *testing.T) {
	features.SetFeatureGateDuringTest(t, features.ElasticJobsViaWorkloadSlices, true)

	// specCount is what each job currently wants; grantedCount is what status.admission
	// still charges. driverMiB accounts for the single 512Mi driver each job holds.
	newSlice := func(name, chainRoot, jobUID string, specCount, grantedCount int32) *kueue.Workload {
		return utiltestingapi.MakeWorkload(name, "batch-ns").
			Label(controllerconsts.JobUIDLabel, jobUID).
			Annotation(constants.ElasticJobAnnotation, elasticJobAnnotationValue).
			Annotation(kueue.WorkloadSliceNameAnnotation, chainRoot).
			Creation(sliceBaseTime).
			PodSets(
				*utiltestingapi.MakePodSet("driver", 1).Request(corev1.ResourceMemory, "512Mi").Obj(),
				*utiltestingapi.MakePodSet("executor", int(specCount)).Request(corev1.ResourceMemory, "512Mi").Obj()).
			ReserveQuotaAt(utiltestingapi.MakeAdmission("batch-queue").
				PodSets(
					utiltestingapi.MakePodSetAssignment("driver").
						Assignment(corev1.ResourceMemory, "default", "512Mi").Count(1).Obj(),
					utiltestingapi.MakePodSetAssignment("executor").
						Assignment(corev1.ResourceMemory, "default",
							resource.NewQuantity(int64(grantedCount)*512*1024*1024, resource.BinarySI).String()).
						Count(grantedCount).Obj()).
				Obj(), sliceBaseTime).
			Condition(metav1.Condition{Type: kueue.WorkloadAdmitted, Status: metav1.ConditionTrue}).
			Obj()
	}

	cases := map[string]struct {
		als1Granted int32
		als2Granted int32
		wantUsage   string
	}{
		"granted counts frozen above spec: still charged at the spec count": {
			als1Granted: 5, // als-1-b99b3: spec 3, granted 5
			als2Granted: 7, // als-2-2011a: spec 3, granted 7
			wantUsage:   "4Gi",
		},
		"granted counts already in step with spec": {
			als1Granted: 3,
			als2Granted: 3,
			wantUsage:   "4Gi",
		},
	}

	for name, tc := range cases {
		t.Run(name, func(t *testing.T) {
			cq := utiltestingapi.MakeClusterQueue("batch-queue").
				ResourceGroup(
					*utiltestingapi.MakeFlavorQuotas("default").
						Resource(corev1.ResourceMemory, "6Gi").
						Obj(),
				).
				Obj()

			ctx, log := utiltesting.ContextWithLog(t)
			cache := New(utiltesting.NewFakeClient())
			if err := cache.AddClusterQueue(ctx, cq); err != nil {
				t.Fatalf("Adding ClusterQueue: %v", err)
			}
			for _, wl := range []*kueue.Workload{
				newSlice("als-1-b99b3", "als-1-7005f", "job-als-1", 3, tc.als1Granted),
				newSlice("als-2-2011a", "als-2-ae646", "job-als-2", 3, tc.als2Granted),
			} {
				if added := cache.AddOrUpdateWorkload(log, wl); !added {
					t.Fatalf("Workload %s was not added", workload.Key(wl))
				}
			}

			stats, err := cache.Usage(cq)
			if err != nil {
				t.Fatalf("Couldn't get usage: %v", err)
			}
			want := resource.MustParse(tc.wantUsage)
			got := stats.AdmittedResources[0].Resources[0].Total
			if got.Cmp(want) != 0 {
				t.Errorf("Admitted usage = %s, want %s", got.String(), want.String())
			}
			nominal := cq.Spec.ResourceGroups[0].Flavors[0].Resources[0].NominalQuota
			if got.Cmp(nominal) > 0 {
				t.Errorf("usage %s exceeds nominalQuota %s", got.String(), nominal.String())
			}
		})
	}
}
