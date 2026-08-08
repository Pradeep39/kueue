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

package flavorassigner

import (
	"testing"
	"time"

	corev1 "k8s.io/api/core/v1"

	configapi "sigs.k8s.io/kueue/apis/config/v1beta2"
	kueue "sigs.k8s.io/kueue/apis/kueue/v1beta2"
	schdcache "sigs.k8s.io/kueue/pkg/cache/scheduler"
	"sigs.k8s.io/kueue/pkg/features"
	"sigs.k8s.io/kueue/pkg/resources"
	utiltesting "sigs.k8s.io/kueue/pkg/util/testing"
	utiltestingapi "sigs.k8s.io/kueue/pkg/util/testing/v1beta2"
	"sigs.k8s.io/kueue/pkg/workload"
)

// TestWorkloadSliceAssignmentUsageIsDeltaButAPICountIsFull pins the asymmetry that made the
// ClusterQueue.status.flavorsUsage overcommit possible (see
// TestClusterQueueUsageWorkloadSliceScaleUpDoubleCount in pkg/cache/scheduler).
//
// For a workload-slice scale-up, one Assignment deliberately produces two different numbers:
//
//   - Assignment.Usage.Quota.Assigned — the DELTA (new - old). Correct for the caller in
//     Scheduler.schedule, which does cq.AddUsage(usage) against a SNAPSHOT that already
//     holds the old slice's full usage.
//
//   - Assignment.ToAPI() → PodSetAssignment.Count / .ResourceUsage — the FULL new count.
//     Correct for status.admission, which must describe the whole PodSet (pod gating,
//     ungating and reclaim all read it), not a difference against a predecessor.
//
// Both values are right for their own consumer, so neither is changed by the overcommit fix.
// The hazard is that the persistent cache re-derives usage from the API value via
// totalRequestsFromAdmission — so IT is what must net out the replaced slice. This test
// asserts both numbers off a single Assign() call so the divergence stays visible and
// intentional; if either changes, the cache-side compensation needs revisiting.
func TestWorkloadSliceAssignmentUsageIsDeltaButAPICountIsFull(t *testing.T) {
	features.SetFeatureGateDuringTest(t, features.ElasticJobsViaWorkloadSlices, true)

	ctx, log := utiltesting.ContextWithLog(t)

	resourceFlavors := map[kueue.ResourceFlavorReference]*kueue.ResourceFlavor{
		"default": utiltestingapi.MakeResourceFlavor("default").Obj(),
	}

	cq := *utiltestingapi.MakeClusterQueue("batch-queue").
		ResourceGroup(
			*utiltestingapi.MakeFlavorQuotas("default").
				Resource(corev1.ResourceMemory, "6Gi").
				Obj(),
		).
		Obj()

	// Old slice: 8 executors x 512Mi = 4Gi, already admitted.
	oldSlice := utiltestingapi.MakeWorkload("old-slice", "batch-ns").
		PodSets(*utiltestingapi.MakePodSet("executor", 8).
			Request(corev1.ResourceMemory, "512Mi").Obj()).
		ReserveQuotaAt(utiltestingapi.MakeAdmission("batch-queue").
			PodSets(utiltestingapi.MakePodSetAssignment("executor").
				Assignment(corev1.ResourceMemory, "default", "4Gi").
				Count(8).
				Obj()).
			Obj(), time.Now().Truncate(time.Second)).
		Obj()

	// New slice: Dynamic Allocation scaled 8 -> 10 executors, so 10 x 512Mi = 5Gi.
	newSlice := utiltestingapi.MakeWorkload("new-slice", "batch-ns").
		PodSets(*utiltestingapi.MakePodSet("executor", 10).
			Request(corev1.ResourceMemory, "512Mi").Obj()).
		Obj()

	cache := schdcache.New(utiltesting.NewFakeClient())
	if err := cache.AddClusterQueue(ctx, &cq); err != nil {
		t.Fatalf("Failed to add CQ to cache: %v", err)
	}
	for _, rf := range resourceFlavors {
		cache.AddOrUpdateResourceFlavor(log, rf)
	}
	snapshot, err := cache.Snapshot(ctx)
	if err != nil {
		t.Fatalf("unexpected error while building snapshot: %v", err)
	}
	cqSnapshot := snapshot.ClusterQueue(kueue.ClusterQueueReference(cq.Name))

	// The scale-up path: replaceWorkloadSlice is the old slice being superseded.
	assigner := New(
		workload.NewInfo(newSlice),
		cqSnapshot,
		resourceFlavors,
		false,
		&testOracle{},
		workload.NewInfo(oldSlice),
		configapi.QuotaCheckBlockUndeclared,
		resources.NewResourceFormatter(),
	)
	assignment := assigner.Assign(ctx, nil)

	if mode := assignment.RepresentativeMode(); mode != Fit {
		t.Fatalf("RepresentativeMode() = %v, want %v", mode, Fit)
	}

	fr := resources.FlavorResource{Flavor: "default", Resource: corev1.ResourceMemory}

	// Half one: the snapshot-facing number is the 1Gi delta (5Gi - 4Gi), NOT 5Gi.
	// 1Gi expressed in bytes, the unit resources.Amount stores memory in.
	wantDelta := resources.NewAmount(1 << 30)
	if got := assignment.Usage.Quota.Assigned[fr]; !got.Equal(wantDelta) {
		t.Errorf("Assignment.Usage.Quota.Assigned[%v] = %v, want %v (the new-minus-old delta)",
			fr, got, wantDelta)
	}

	// Half two: the API-facing number from the SAME assignment is the full 10 / 5Gi.
	api := assignment.ToAPI(log)
	if len(api) != 1 {
		t.Fatalf("ToAPI() returned %d PodSetAssignments, want 1", len(api))
	}
	if got := api[0].Count; got == nil || *got != 10 {
		t.Errorf("PodSetAssignment.Count = %v, want 10 (the full new count, not the delta of 2)", got)
	}
	gotAPIUsage := api[0].ResourceUsage[corev1.ResourceMemory]
	if gotAPIUsage.String() != "5Gi" {
		t.Errorf("PodSetAssignment.ResourceUsage[memory] = %v, want 5Gi (the full new request, not the 1Gi delta)",
			gotAPIUsage.String())
	}

	// Restate the consequence: the persistent cache reads the API number, so adding the new
	// slice while the old one is still admitted charges 4Gi + 5Gi = 9Gi against a 6Gi
	// nominalQuota, rather than the 4Gi + 1Gi = 5Gi the snapshot correctly computes.
	t.Logf("Snapshot is charged the %v delta; status.admission carries the full %v. "+
		"The persistent cache reads the latter and never nets out the old slice's 4Gi.",
		wantDelta, gotAPIUsage.String())
}
