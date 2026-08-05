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

package jobframework

import (
	"context"
	"testing"
	"time"

	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	apimeta "k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"
	testingclock "k8s.io/utils/clock/testing"
	"sigs.k8s.io/controller-runtime/pkg/client"

	kueue "sigs.k8s.io/kueue/apis/kueue/v1beta2"
	"sigs.k8s.io/kueue/pkg/controller/core/indexer"
	"sigs.k8s.io/kueue/pkg/podset"
	utiltesting "sigs.k8s.io/kueue/pkg/util/testing"
	utiltestingapi "sigs.k8s.io/kueue/pkg/util/testing/v1beta2"
	"sigs.k8s.io/kueue/pkg/workload"
	"sigs.k8s.io/kueue/pkg/workloadslicing"
)

// stubGenericJob is a minimal GenericJob implementation for exercising
// prepareWorkloadSlice() directly, without going through a full Reconcile().
type stubGenericJob struct {
	object client.Object
	gvk    schema.GroupVersionKind
}

func (j *stubGenericJob) Object() client.Object                         { return j.object }
func (j *stubGenericJob) IsSuspended() bool                             { return false }
func (j *stubGenericJob) Suspend()                                      {}
func (j *stubGenericJob) IsActive() bool                                { return true }
func (j *stubGenericJob) GVK() schema.GroupVersionKind                  { return j.gvk }
func (j *stubGenericJob) Finished(context.Context) (string, bool, bool) { return "", false, false }
func (j *stubGenericJob) PodSets(context.Context, client.Client) ([]kueue.PodSet, error) {
	return nil, nil
}
func (j *stubGenericJob) PodsReady(context.Context, client.Client) bool { return true }
func (j *stubGenericJob) RunWithPodSetsInfo(context.Context, client.Client, []podset.PodSetInfo) error {
	return nil
}
func (j *stubGenericJob) RestorePodSetsInfo(context.Context, []podset.PodSetInfo) bool { return false }

var _ GenericJob = (*stubGenericJob)(nil)

// TestPrepareWorkloadSliceToleratesCacheLagBetweenNormalizeAndRelist reproduces a race
// observed live: EnsureWorkloadSlices' own not-finished-workloads List (moments earlier
// in the same reconcile) can legitimately see one admitted slice plus one pending
// replacement — a normal, expected shape while a scale-up-on-admitted replacement is in
// flight. prepareWorkloadSlice's own List, a few lines later in the same reconcile, can
// observe that same pair. Before the fix, prepareWorkloadSlice treated any count > 1 as
// fatal and returned "unexpected workload-slices count: 2", even though the rest of the
// slicing machinery (EnsureWorkloadSlices' own default: branch, via
// NormalizeActiveSlices) already has a deterministic way to pick the surviving slice
// out of exactly this shape.
func TestPrepareWorkloadSliceToleratesCacheLagBetweenNormalizeAndRelist(t *testing.T) {
	now := time.Now()
	fakeClock := testingclock.NewFakeClock(now)
	testGVK := batchv1.SchemeGroupVersion.WithKind("Job")
	testJob := &batchv1.Job{
		ObjectMeta: metav1.ObjectMeta{Name: "job", Namespace: "ns", UID: types.UID("job-uid")},
	}

	admittedSlice := utiltestingapi.MakeWorkload("wl-admitted", "ns").
		OwnerReference(testGVK, testJob.Name, string(testJob.UID)).
		ReserveQuotaAt(utiltestingapi.MakeAdmission("cq").PodSets(
			utiltestingapi.MakePodSetAssignment(kueue.DefaultPodSetName).Assignment(corev1.ResourceCPU, "default", "1").Obj(),
		).Obj(), now).
		Creation(now.Add(-time.Minute)).
		PodSets(*utiltestingapi.MakePodSet(kueue.DefaultPodSetName, 2).Request(corev1.ResourceCPU, "1").Obj()).
		Obj()

	pendingReplacement := utiltestingapi.MakeWorkload("wl-pending", "ns").
		OwnerReference(testGVK, testJob.Name, string(testJob.UID)).
		Annotation(workloadslicing.WorkloadSliceReplacementFor, "ns/wl-admitted").
		Creation(now).
		PodSets(*utiltestingapi.MakePodSet(kueue.DefaultPodSetName, 3).Request(corev1.ResourceCPU, "1").Obj()).
		Obj()

	cl := utiltesting.NewClientBuilder(batchv1.AddToScheme, kueue.AddToScheme).
		WithObjects(admittedSlice, pendingReplacement).
		WithStatusSubresource(&kueue.Workload{}).
		WithIndex(&kueue.Workload{}, indexer.OwnerReferenceIndexKey(testGVK), indexer.WorkloadOwnerIndexFunc(testGVK)).
		Build()

	job := &stubGenericJob{object: testJob, gvk: testGVK}
	newSlice := utiltestingapi.MakeWorkload("wl-new", "ns").
		OwnerReference(testGVK, testJob.Name, string(testJob.UID)).
		PodSets(*utiltestingapi.MakePodSet(kueue.DefaultPodSetName, 4).Request(corev1.ResourceCPU, "1").Obj()).
		Obj()

	ctx, _ := utiltesting.ContextWithLog(t)
	if err := prepareWorkloadSlice(ctx, cl, fakeClock, job, newSlice); err != nil {
		t.Fatalf("prepareWorkloadSlice() returned an unexpected error: %v", err)
	}

	// The new slice must link to the pending replacement's chain, since that's the
	// slice NormalizeActiveSlices selected as the surviving one to replace.
	wantReplacementFor := string(workload.Key(pendingReplacement))
	if got := newSlice.Annotations[workloadslicing.WorkloadSliceReplacementFor]; got != wantReplacementFor {
		t.Errorf("WorkloadSliceReplacementFor annotation = %q, want %q", got, wantReplacementFor)
	}

	// The admitted slice is kept un-finished by normalization (it still holds the quota
	// reservation the pending replacement is waiting to take over) — normalization only
	// finishes slices that are neither the selected one nor the admitted one.
	gotAdmitted := &kueue.Workload{}
	if err := cl.Get(ctx, client.ObjectKeyFromObject(admittedSlice), gotAdmitted); err != nil {
		t.Fatalf("failed to get admitted slice: %v", err)
	}
	if apimeta.IsStatusConditionTrue(gotAdmitted.Status.Conditions, kueue.WorkloadFinished) {
		t.Errorf("admitted slice wl-admitted should still be un-finished, got conditions: %+v", gotAdmitted.Status.Conditions)
	}
}
