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

	"github.com/google/go-cmp/cmp"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	kueue "sigs.k8s.io/kueue/apis/kueue/v1beta2"
	"sigs.k8s.io/kueue/pkg/constants"
	"sigs.k8s.io/kueue/pkg/features"
	utiltesting "sigs.k8s.io/kueue/pkg/util/testing"
	utiltestingapi "sigs.k8s.io/kueue/pkg/util/testing/v1beta2"
	"sigs.k8s.io/kueue/pkg/workload"
)

// Mirrored from pkg/workloadslicing rather than imported: that package imports
// pkg/cache/scheduler, so importing it here would be an import cycle.
const (
	elasticJobAnnotationValue         = "true"
	workloadSliceReplacementForAnnKey = "kueue.x-k8s.io/workload-slice-replacement-for"
)

// TestClusterQueueUsageWorkloadSliceScaleUpDoubleCount verifies that the scheduler cache
// does not double-count quota during an elastic-job (workload slice) scale-up, which
// otherwise drives ClusterQueue.status.flavorsUsage above the ClusterQueue's nominalQuota.
//
// The two accounting structures disagree on what a scale-up costs:
//
//   - At scheduling time, flavorassigner.Assignment.append() charges only the DELTA
//     between the new and old slice when replaceWorkloadSlice != nil:
//
//     requestAmount -= a.findOldPodSetRequest(psAssignment.Name, resource)
//
//     That delta is added to the SNAPSHOT via cq.AddUsage(), which is correct: the
//     snapshot already holds the old slice's full usage, so old_full + delta = new_full.
//
//   - The resulting PodSetAssignment written to status.admission is NOT delta-adjusted —
//     psAssignment.Count and psAssignment.Requests carry the full new count. Scheduler.admit
//     then calls assumeWorkload → Cache.AddOrUpdateWorkload, and the PERSISTENT cache
//     (clusterQueue.addOrUpdateWorkload → workload.NewInfo → totalRequestsFromAdmission)
//     reads that full ResourceUsage.
//
// Left uncorrected, the persistent cache holds old_full + new_full instead of new_full for
// as long as both slices are admitted, and ClusterQueue.status.flavorsUsage is rendered
// straight from it (ClusterQueueReconciler.updateCqStatusIfChanged ← Cache.Usage ←
// clusterQueue.AdmittedUsage).
//
// The overlap is not instantaneous. The old slice is deliberately excluded from the
// preemption targets (workloadslicing.FindReplacedSliceTarget: it is "evicted rather than
// preempted"), and it is only Finished asynchronously, inside the admit success callback
// (Scheduler.replaceOldWorkloadSlice). If that Finish fails it is logged and never retried
// — recovery is punted to the next job reconcile — so the window can stay open indefinitely.
//
// The fix makes a replaced slice contribute no usage for as long as a cached slice declares
// itself its replacement, so the ledger is correct regardless of when (or whether) the
// old slice's Finish lands.
func TestClusterQueueUsageWorkloadSliceScaleUpDoubleCount(t *testing.T) {
	features.SetFeatureGateDuringTest(t, features.ElasticJobsViaWorkloadSlices, true)

	now := time.Now().Truncate(time.Second)

	// 6Gi nominal quota, no borrowing beyond it, mirroring the reported cluster setup
	// (512Mi executors, so 12 executor slots).
	cq := utiltestingapi.MakeClusterQueue("batch-queue").
		ResourceGroup(
			*utiltestingapi.MakeFlavorQuotas("default").
				Resource(corev1.ResourceMemory, "6Gi").
				Obj(),
		).
		Obj()

	const (
		oldSliceName = "sparkapplication-als-1-7005f"
		newSliceName = "sparkapplication-als-1-7a41b"
	)

	// The admitted old slice: 8 executors x 512Mi = 4Gi.
	oldSlice := utiltestingapi.MakeWorkload(oldSliceName, "batch-ns").
		Annotation(constants.ElasticJobAnnotation, elasticJobAnnotationValue).
		Annotation(kueue.WorkloadSliceNameAnnotation, oldSliceName).
		PodSets(*utiltestingapi.MakePodSet("executor", 8).
			Request(corev1.ResourceMemory, "512Mi").Obj()).
		ReserveQuotaAt(utiltestingapi.MakeAdmission("batch-queue").
			PodSets(utiltestingapi.MakePodSetAssignment("executor").
				Assignment(corev1.ResourceMemory, "default", "4Gi").
				Count(8).
				Obj()).
			Obj(), now).
		Condition(metav1.Condition{Type: kueue.WorkloadAdmitted, Status: metav1.ConditionTrue}).
		Obj()

	// Dynamic Allocation scales 8 -> 10 executors. The replacement slice is admitted with
	// its FULL count (10 x 512Mi = 5Gi) in status.admission, even though the scheduler only
	// charged the 1Gi delta against the snapshot. Note it carries
	// WorkloadSliceReplacementFor pointing at the old slice — the annotation the persistent
	// cache never looks at.
	newSlice := utiltestingapi.MakeWorkload(newSliceName, "batch-ns").
		Annotation(constants.ElasticJobAnnotation, elasticJobAnnotationValue).
		Annotation(kueue.WorkloadSliceNameAnnotation, oldSliceName).
		Annotation(workloadSliceReplacementForAnnKey, string(workload.Key(oldSlice))).
		PodSets(*utiltestingapi.MakePodSet("executor", 10).
			Request(corev1.ResourceMemory, "512Mi").Obj()).
		ReserveQuotaAt(utiltestingapi.MakeAdmission("batch-queue").
			PodSets(utiltestingapi.MakePodSetAssignment("executor").
				Assignment(corev1.ResourceMemory, "default", "5Gi").
				Count(10).
				Obj()).
			Obj(), now).
		Condition(metav1.Condition{Type: kueue.WorkloadAdmitted, Status: metav1.ConditionTrue}).
		Obj()

	ctx, log := utiltesting.ContextWithLog(t)
	cache := New(utiltesting.NewFakeClient())
	if err := cache.AddClusterQueue(ctx, cq); err != nil {
		t.Fatalf("Adding ClusterQueue: %v", err)
	}

	// Both slices are simultaneously present and admitted in the cache. This is exactly the
	// state Scheduler.admit leaves behind between assumeWorkload (synchronous) and the old
	// slice's Finish propagating back (asynchronous, unretried on failure).
	for _, wl := range []*kueue.Workload{oldSlice, newSlice} {
		if added := cache.AddOrUpdateWorkload(log, wl); !added {
			t.Fatalf("Workload %s was not added to the cache", workload.Key(wl))
		}
	}

	stats, err := cache.Usage(cq)
	if err != nil {
		t.Fatalf("Couldn't get usage: %v", err)
	}

	// The new slice supersedes the old one: both describe the same set of executor Pods, so
	// the job's contribution is the new slice's 10 x 512Mi = 5Gi, within the 6Gi nominal.
	// Before the fix this reported 4Gi (old, full) + 5Gi (new, full) = 9Gi — 3Gi ABOVE
	// nominalQuota — because the replaced slice's usage was never netted out.
	wantUsedResources := []kueue.FlavorUsage{{
		Name: "default",
		Resources: []kueue.ResourceUsage{{
			Name:  corev1.ResourceMemory,
			Total: resource.MustParse("5Gi"),
		}},
	}}

	if diff := cmp.Diff(wantUsedResources, stats.AdmittedResources); diff != "" {
		t.Errorf("Unexpected admitted resources (-want,+got):\n%s", diff)
	}

	// Restate the guarantee in the terms the bug manifested in: the ClusterQueue must never
	// report more usage than it has nominal quota. status.flavorsUsage is rendered verbatim
	// from stats.AdmittedResources by ClusterQueueReconciler.updateCqStatusIfChanged.
	nominal := cq.Spec.ResourceGroups[0].Flavors[0].Resources[0].NominalQuota
	if got := stats.AdmittedResources[0].Resources[0].Total; got.Cmp(nominal) > 0 {
		t.Errorf("ClusterQueue.status.flavorsUsage (%s) exceeds nominalQuota (%s) while both "+
			"workload slices of a single job are admitted", got.String(), nominal.String())
	}
}
