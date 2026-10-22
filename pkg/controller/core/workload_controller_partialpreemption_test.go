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

package core

import (
	"context"
	"testing"
	"time"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/types"
	testingclock "k8s.io/utils/clock/testing"
	"sigs.k8s.io/controller-runtime/pkg/event"

	kueue "sigs.k8s.io/kueue/apis/kueue/v1beta2"
	qcache "sigs.k8s.io/kueue/pkg/cache/queue"
	schdcache "sigs.k8s.io/kueue/pkg/cache/scheduler"
	"sigs.k8s.io/kueue/pkg/constants"
	"sigs.k8s.io/kueue/pkg/features"
	preemptexpectations "sigs.k8s.io/kueue/pkg/scheduler/preemption/expectations"
	utiltesting "sigs.k8s.io/kueue/pkg/util/testing"
	utiltestingapi "sigs.k8s.io/kueue/pkg/util/testing/v1beta2"
)

// partialVictim is an admitted elastic workload whose executor PodSet is granted `granted`
// Pods, optionally with a partial preemption outstanding.
func partialVictim(now time.Time, granted int32, pending bool) *kueue.Workload {
	w := utiltestingapi.MakeWorkload("victim", "ns").
		Queue("lq").
		Active(true).
		Annotation(constants.PartialPreemptionPodSetAnnotation, "executor").
		Annotation(constants.PartialPreemptionMinCountAnnotation, "1").
		PodSets(*utiltestingapi.MakePodSet("executor", 6).Request(corev1.ResourceCPU, "1").Obj()).
		ReserveQuotaAt(utiltestingapi.MakeAdmission("cq").PodSets(
			utiltestingapi.MakePodSetAssignment("executor").Count(granted).Obj(),
		).Obj(), now).
		AdmittedAt(true, now)
	if pending {
		w = w.Annotation(constants.PartialPreemptionTargetAnnotation, "3")
	}
	return w.Obj()
}

// A partially preempted victim stays admitted, so the paths that observe a whole preemption
// never fire for it; the update that carries the scheduler's target must observe it.
func TestUpdateObservesPartialPreemption(t *testing.T) {
	cases := map[string]struct {
		gate          bool
		pending       bool
		wantSatisfied bool
	}{
		"target visible, gate on":  {gate: true, pending: true, wantSatisfied: true},
		"target visible, gate off": {gate: false, pending: true, wantSatisfied: false},
		"no target":                {gate: true, pending: false, wantSatisfied: false},
	}
	for name, tc := range cases {
		t.Run(name, func(t *testing.T) {
			features.SetFeatureGateDuringTest(t, features.SparkPartialPreemption, tc.gate)
			now := time.Now().Truncate(time.Second)
			oldWl := partialVictim(now, 6, false)
			newWl := partialVictim(now, 6, tc.pending)

			cl := utiltesting.NewClientBuilder().Build()
			cqCache := schdcache.New(cl)
			qManager := qcache.NewManagerForUnitTests(cl, cqCache, qcache.WithClock(testingclock.NewFakeClock(now)))
			store := preemptexpectations.New()
			reconciler := NewWorkloadReconciler(cl, qManager, cqCache, &utiltesting.EventRecorder{}, WithPreemptionExpectations(store))
			ctx, log := utiltesting.ContextWithLog(t)
			setupClusterQueue(ctx, t, cl, qManager, cqCache, utiltestingapi.MakeClusterQueue("cq").Obj(), false)
			setupLocalQueue(ctx, t, cl, qManager, utiltestingapi.MakeLocalQueue("lq", "ns").ClusterQueue("cq").Obj(), false)

			key := types.NamespacedName{Name: newWl.Name, Namespace: newWl.Namespace}
			store.ExpectUIDs(log, key, []types.UID{newWl.UID})
			reconciler.handleUpdate(ctx, event.TypedUpdateEvent[*kueue.Workload]{ObjectOld: oldWl, ObjectNew: newWl})
			if got := store.Satisfied(log, key); got != tc.wantSatisfied {
				t.Errorf("expectation satisfied = %v, want %v", got, tc.wantSatisfied)
			}
		})
	}
}

// A completed partial preemption lowers only the grant. The preemptor waiting on it is
// inadmissible and must be requeued to use the quota freed.
func TestUpdateRequeuesWhenPartialPreemptionLowersGrant(t *testing.T) {
	cases := map[string]struct {
		gate        bool
		grantBefore int32
		grantAfter  int32
		wantMoved   int
	}{
		"grant lowered, gate on":  {gate: true, grantBefore: 6, grantAfter: 3, wantMoved: 1},
		"grant lowered, gate off": {gate: false, grantBefore: 6, grantAfter: 3, wantMoved: 0},
		"grant unchanged":         {gate: true, grantBefore: 6, grantAfter: 6, wantMoved: 0},
	}
	for name, tc := range cases {
		t.Run(name, func(t *testing.T) {
			features.SetFeatureGateDuringTest(t, features.SparkPartialPreemption, tc.gate)
			now := time.Now().Truncate(time.Second)
			ctx, log := utiltesting.ContextWithLog(t)
			oldWl := partialVictim(now, tc.grantBefore, true)
			newWl := partialVictim(now, tc.grantAfter, false)
			// The victim exists before its LocalQueue is added, as an admitted workload does
			// when the queue manager starts, so the manager knows which queue it belongs to.
			cl := utiltesting.NewClientBuilder().WithObjects(utiltesting.MakeNamespace("ns"), oldWl.DeepCopy()).Build()
			cqCache := schdcache.New(cl)
			qManager, requeuer := qcache.NewManagerForUnitTestsWithRequeuer(cl, nil,
				qcache.WithPreemptionExpectations(preemptexpectations.New()))
			if err := qManager.AddClusterQueue(ctx, utiltestingapi.MakeClusterQueue("cq").Obj()); err != nil {
				t.Fatal(err)
			}
			if err := qManager.AddLocalQueue(ctx, utiltestingapi.MakeLocalQueue("lq", "ns").ClusterQueue("cq").Obj()); err != nil {
				t.Fatal(err)
			}
			if err := cqCache.AddClusterQueue(ctx, utiltestingapi.MakeClusterQueue("cq").Obj()); err != nil {
				t.Fatal(err)
			}
			reconciler := NewWorkloadReconciler(cl, qManager, cqCache, &utiltesting.EventRecorder{})
			requeuer.ProcessRequeues(ctx) // drain the notifications queue setup produced

			// The preemptor: pending, popped, and parked as inadmissible.
			preemptor := utiltestingapi.MakeWorkload("preemptor", "ns").Queue("lq").Request(corev1.ResourceCPU, "2").Obj()
			if err := cl.Create(ctx, preemptor); err != nil {
				t.Fatal(err)
			}
			if err := qManager.AddOrUpdateWorkload(ctx, log, preemptor); err != nil {
				t.Fatal(err)
			}
			headsCtx, cancel := context.WithTimeout(ctx, time.Second)
			defer cancel()
			go qManager.CleanUpOnContext(headsCtx)
			heads := qManager.Heads(headsCtx)
			if len(heads) != 1 {
				t.Fatalf("expected the preemptor at the head, got %d heads", len(heads))
			}
			qManager.RequeueWorkload(ctx, &heads[0].Info, qcache.RequeueReasonNoFit, qcache.QuotaReservedReasonWaitingForQuota)
			if moved := requeuer.ProcessRequeues(ctx); moved != 0 {
				t.Fatalf("workloads moved before the victim's update = %d, want 0", moved)
			}
			if got := len(qManager.DumpInadmissible()["cq"]); got != 1 {
				t.Fatalf("expected the preemptor parked as inadmissible, got %v", qManager.DumpInadmissible())
			}

			cqCache.AddOrUpdateWorkload(ctx, log, oldWl)
			reconciler.handleUpdate(ctx, event.TypedUpdateEvent[*kueue.Workload]{ObjectOld: oldWl, ObjectNew: newWl})
			if moved := requeuer.ProcessRequeues(ctx); moved != tc.wantMoved {
				t.Errorf("workloads requeued = %d, want %d", moved, tc.wantMoved)
			}
		})
	}
}
