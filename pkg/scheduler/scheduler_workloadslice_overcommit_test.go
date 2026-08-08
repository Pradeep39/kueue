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
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/interceptor"

	kueue "sigs.k8s.io/kueue/apis/kueue/v1beta2"
	qcache "sigs.k8s.io/kueue/pkg/cache/queue"
	schdcache "sigs.k8s.io/kueue/pkg/cache/scheduler"
	"sigs.k8s.io/kueue/pkg/constants"
	"sigs.k8s.io/kueue/pkg/features"
	preemptexpectations "sigs.k8s.io/kueue/pkg/scheduler/preemption/expectations"
	"sigs.k8s.io/kueue/pkg/util/routine"
	utiltesting "sigs.k8s.io/kueue/pkg/util/testing"
	utiltestingapi "sigs.k8s.io/kueue/pkg/util/testing/v1beta2"
	"sigs.k8s.io/kueue/pkg/workload"
	"sigs.k8s.io/kueue/pkg/workloadslicing"
)

// TestScheduleWorkloadSliceScaleUpDoesNotOvercommit drives a full scheduling cycle for an
// elastic-job scale-up and asserts the ClusterQueue's reported usage afterwards.
//
// This is the end-to-end form of the bug the accounting fix addresses. A scale-up admits a
// replacement slice whose status.admission carries the FULL new executor count, while the
// scheduler only charged the delta against its snapshot. The superseded slice is Finished
// asynchronously and, critically, Scheduler.replaceOldWorkloadSlice only logs a Finish
// failure — it never retries. The test forces exactly that failure so both slices stay
// admitted, which is the state a real cluster sits in when the overcommit is observed.
//
// Without the fix, the ClusterQueue reports 4Gi (old, full) + 5Gi (new, full) = 9Gi against
// a 6Gi nominalQuota. With it, only the chain's latest slice is counted: 5Gi.
func TestScheduleWorkloadSliceScaleUpDoesNotOvercommit(t *testing.T) {
	features.SetFeatureGateDuringTest(t, features.ElasticJobsViaWorkloadSlices, true)

	now := time.Now().Truncate(time.Second)
	ctx, log := utiltesting.ContextWithLog(t)

	const (
		oldSliceName = "als-1-slice-1"
		newSliceName = "als-1-slice-2"
	)

	flavor := utiltestingapi.MakeResourceFlavor("default").Obj()
	cq := utiltestingapi.MakeClusterQueue("batch-queue").
		ResourceGroup(
			*utiltestingapi.MakeFlavorQuotas("default").
				Resource(corev1.ResourceMemory, "6Gi").
				Obj(),
		).
		Obj()
	lq := utiltestingapi.MakeLocalQueue("batch-lq", "batch-ns").ClusterQueue("batch-queue").Obj()

	// The admitted slice: 8 executors x 512Mi = 4Gi. Annotated the way jobframework's
	// prepareWorkloadSlice annotates a chain root.
	oldSlice := utiltestingapi.MakeWorkload(oldSliceName, "batch-ns").
		Queue("batch-lq").
		Annotation(workloadslicing.EnabledAnnotationKey, workloadslicing.EnabledAnnotationValue).
		Annotation(kueue.WorkloadSliceNameAnnotation, oldSliceName).
		Creation(now).
		PodSets(*utiltestingapi.MakePodSet("executor", 8).
			Request(corev1.ResourceMemory, "512Mi").Obj()).
		ReserveQuotaAt(utiltestingapi.MakeAdmission("batch-queue").
			PodSets(utiltestingapi.MakePodSetAssignment("executor").
				Assignment(corev1.ResourceMemory, "default", "4Gi").
				Count(8).
				Obj()).
			Obj(), now).
		Condition(metav1.Condition{
			Type: kueue.WorkloadQuotaReserved, Status: metav1.ConditionTrue,
			Reason: kueue.WorkloadQuotaReserved, LastTransitionTime: metav1.NewTime(now),
		}).
		Condition(metav1.Condition{
			Type: kueue.WorkloadAdmitted, Status: metav1.ConditionTrue,
			Reason: kueue.WorkloadAdmitted, LastTransitionTime: metav1.NewTime(now),
		}).
		Obj()

	// Dynamic Allocation scaled to 10 executors; the pending replacement awaits admission.
	newSlice := utiltestingapi.MakeWorkload(newSliceName, "batch-ns").
		Queue("batch-lq").
		Annotation(workloadslicing.EnabledAnnotationKey, workloadslicing.EnabledAnnotationValue).
		Annotation(kueue.WorkloadSliceNameAnnotation, oldSliceName).
		Annotation(constants.WorkloadSliceReplacementForAnnotation, "batch-ns/"+oldSliceName).
		Creation(now.Add(time.Second)).
		PodSets(*utiltestingapi.MakePodSet("executor", 10).
			Request(corev1.ResourceMemory, "512Mi").Obj()).
		Obj()

	// Fail every status patch aimed at the superseded slice, so its Finish never lands and
	// it stays admitted — the unretried failure path in Scheduler.replaceOldWorkloadSlice.
	errFinishFailed := errors.New("simulated failure finishing the superseded slice")
	cl := utiltesting.NewClientBuilder().
		WithLists(
			&kueue.WorkloadList{Items: []kueue.Workload{*oldSlice, *newSlice}},
			&kueue.LocalQueueList{Items: []kueue.LocalQueue{*lq}},
		).
		WithObjects(utiltesting.MakeNamespaceWrapper("batch-ns").Obj()).
		WithStatusSubresource(&kueue.Workload{}).
		WithInterceptorFuncs(interceptor.Funcs{
			SubResourcePatch: func(ctx context.Context, c client.Client, subResourceName string, obj client.Object, patch client.Patch, opts ...client.SubResourcePatchOption) error {
				if wl, ok := obj.(*kueue.Workload); ok && wl.Name == oldSliceName && subResourceName == "status" {
					return errFinishFailed
				}
				return utiltesting.TreatSSAAsStrategicMerge(ctx, c, subResourceName, obj, patch, opts...)
			},
		}).
		Build()

	cqCache := schdcache.New(cl)
	qManager := qcache.NewManagerForUnitTests(cl, cqCache)
	cqCache.AddOrUpdateResourceFlavor(log, flavor)
	if err := cqCache.AddClusterQueue(ctx, cq); err != nil {
		t.Fatalf("Inserting ClusterQueue in cache: %v", err)
	}
	if err := qManager.AddClusterQueue(ctx, cq); err != nil {
		t.Fatalf("Inserting ClusterQueue in manager: %v", err)
	}
	if err := qManager.AddLocalQueue(ctx, lq); err != nil {
		t.Fatalf("Inserting LocalQueue in manager: %v", err)
	}

	scheduler := New(qManager, cqCache, cl, &utiltesting.EventRecorder{},
		WithPreemptionExpectations(preemptexpectations.New()))
	wg := sync.WaitGroup{}
	scheduler.setAdmissionRoutineWrapper(routine.NewWrapper(
		func() { wg.Add(1) },
		func() { wg.Done() },
	))

	schedCtx, cancel := context.WithTimeout(ctx, queueingTimeout)
	defer cancel()
	go qManager.CleanUpOnContext(schedCtx)
	scheduler.schedule(schedCtx)
	wg.Wait()

	// Both slices must still be admitted for this to be testing what it claims to.
	snapshot, err := cqCache.Snapshot(ctx)
	if err != nil {
		t.Fatalf("unexpected error while building snapshot: %v", err)
	}
	cqSnapshot := snapshot.ClusterQueue("batch-queue")
	for _, name := range []workload.Reference{
		workload.NewReference("batch-ns", oldSliceName),
		workload.NewReference("batch-ns", newSliceName),
	} {
		if _, ok := cqSnapshot.Workloads[name]; !ok {
			t.Fatalf("Workload %s is not in the cache; the scale-up overlap this test needs did not happen", name)
		}
	}

	stats, err := cqCache.Usage(cq)
	if err != nil {
		t.Fatalf("Couldn't get usage: %v", err)
	}
	want := resource.MustParse("5Gi")
	got := stats.AdmittedResources[0].Resources[0].Total
	if got.Cmp(want) != 0 {
		t.Errorf("ClusterQueue admitted usage = %s, want %s (only the chain's latest slice counts)",
			got.String(), want.String())
	}
	nominal := cq.Spec.ResourceGroups[0].Flavors[0].Resources[0].NominalQuota
	if got.Cmp(nominal) > 0 {
		t.Errorf("ClusterQueue.status.flavorsUsage (%s) exceeds nominalQuota (%s)",
			got.String(), nominal.String())
	}
}
