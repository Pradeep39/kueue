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
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/go-logr/logr"
	"github.com/google/go-cmp/cmp"
	corev1 "k8s.io/api/core/v1"
	apimeta "k8s.io/apimachinery/pkg/api/meta"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/component-base/featuregate"
	testingclock "k8s.io/utils/clock/testing"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/interceptor"

	kueue "sigs.k8s.io/kueue/apis/kueue/v1beta2"
	qcache "sigs.k8s.io/kueue/pkg/cache/queue"
	schdcache "sigs.k8s.io/kueue/pkg/cache/scheduler"
	"sigs.k8s.io/kueue/pkg/constants"
	"sigs.k8s.io/kueue/pkg/features"
	"sigs.k8s.io/kueue/pkg/scheduler/assignment"
	"sigs.k8s.io/kueue/pkg/scheduler/flavorassigner"
	"sigs.k8s.io/kueue/pkg/scheduler/preemption"
	preemptexpectations "sigs.k8s.io/kueue/pkg/scheduler/preemption/expectations"
	"sigs.k8s.io/kueue/pkg/scheduler/preemption/policy"
	"sigs.k8s.io/kueue/pkg/util/routine"
	utiltesting "sigs.k8s.io/kueue/pkg/util/testing"
	utiltestingapi "sigs.k8s.io/kueue/pkg/util/testing/v1beta2"
	"sigs.k8s.io/kueue/pkg/workload"
	"sigs.k8s.io/kueue/pkg/workloadslicing"
)

// fakePlan returns a plan whose CanFit is fit.
func fakePlan(fit bool) assignment.Plan {
	a := flavorassigner.Assignment{PodSets: []flavorassigner.PodSetAssignment{{Name: "main"}}}
	if fit {
		a.SetRepresentativeMode(flavorassigner.Fit)
	} else {
		a.SetRepresentativeMode(flavorassigner.Preempt)
	}
	return assignment.Plan{Assignment: &a}
}

func TestAttemptLadder(t *testing.T) {
	full := func(m preemption.Mode) planAttempt { return planAttempt{mode: m} }
	red := func(m preemption.Mode) planAttempt { return planAttempt{reduced: true, mode: m} }
	const (
		none    = preemption.NoPreemption
		partial = preemption.PartialOnly
		whole   = preemption.WholeOnly
		undecl  = preemption.WholeOnlyUndeclared
	)
	reducible := func(w *utiltestingapi.WorkloadWrapper) *utiltestingapi.WorkloadWrapper {
		return w.PodSets(*utiltestingapi.MakePodSet("main", 4).SetMinimumCount(1).Request(corev1.ResourceCPU, "1").Obj())
	}
	growBack := func(w *utiltestingapi.WorkloadWrapper) *utiltestingapi.WorkloadWrapper {
		return w.Annotation(constants.PartialPreemptionPodSetAnnotation, "main").
			Annotation(constants.PartialPreemptionMinCountAnnotation, "1").
			Annotation(constants.PartialPreemptionRestoreCountAnnotation, "4")
	}
	inFlightCQ := func() *schdcache.ClusterQueueSnapshot {
		victim := utiltestingapi.MakeWorkload("victim", "default").
			Annotation(constants.PartialPreemptionTargetAnnotation, "2").Obj()
		return &schdcache.ClusterQueueSnapshot{
			PartialPreemption: schdcache.PartialPreemptionSettings{Enabled: true},
			Workloads:         map[workload.Reference]*workload.Info{workload.Key(victim): workload.NewInfo(logr.Discard(), victim)},
		}
	}

	cases := map[string]struct {
		gate         bool
		cqEnabled    bool
		fallbackWait bool
		inFlightCQ   bool
		wl           func(*utiltestingapi.WorkloadWrapper) *utiltestingapi.WorkloadWrapper
		// fitsIn are the attempts that would yield a plan that can fit.
		fitsIn []planAttempt

		wantTried []planAttempt
		wantFit   bool
		wantNote  string
	}{
		"gate off: today's ladder, whole preemption first": {
			wl:        reducible,
			fitsIn:    []planAttempt{full(none), red(none), red(whole)},
			wantTried: []planAttempt{full(whole), red(whole)},
			wantFit:   true,
		},
		"gate off, no reduction: whole preemption only": {
			wantTried: []planAttempt{full(whole)},
		},
		"gate on but ClusterQueue not enabled: unchanged": {
			gate:      true,
			wl:        reducible,
			wantTried: []planAttempt{full(whole), red(whole)},
		},
		"free fit": {
			gate: true, cqEnabled: true, wl: reducible,
			fitsIn:    []planAttempt{full(none), full(partial), full(whole)},
			wantTried: []planAttempt{full(none)},
			wantFit:   true,
		},
		"reduced free fit beats any preemption": {
			gate: true, cqEnabled: true, wl: reducible,
			fitsIn:    []planAttempt{red(none), full(partial), full(whole)},
			wantTried: []planAttempt{full(none), red(none)},
			wantFit:   true,
		},
		"partial preemption of the full count": {
			gate: true, cqEnabled: true, wl: reducible,
			fitsIn:    []planAttempt{full(partial), red(partial), full(whole)},
			wantTried: []planAttempt{full(none), red(none), full(partial)},
			wantFit:   true,
		},
		"partial preemption of a reduced count beats whole eviction": {
			gate: true, cqEnabled: true, wl: reducible,
			fitsIn:    []planAttempt{red(partial), full(whole)},
			wantTried: []planAttempt{full(none), red(none), full(partial), red(partial)},
			wantFit:   true,
		},
		"fallback Evict: whole preemption after partial cannot fit": {
			gate: true, cqEnabled: true, wl: reducible,
			fitsIn:    []planAttempt{full(whole)},
			wantTried: []planAttempt{full(none), red(none), full(partial), red(partial), full(whole)},
			wantFit:   true,
		},
		"fallback Evict, nothing fits": {
			gate: true, cqEnabled: true,
			wantTried: []planAttempt{full(none), full(partial), full(whole)},
		},
		"fallback Wait: never whole preemption": {
			gate: true, cqEnabled: true, fallbackWait: true, wl: reducible,
			fitsIn:    []planAttempt{full(whole), red(whole)},
			wantTried: []planAttempt{full(none), red(none), full(partial), red(partial)},
			wantNote:  "partial preemption cannot make room and the ClusterQueue's partial-preemption fallback is Wait",
		},
		"scale-up slice: falls back only to victims that did not opt in": {
			gate: true, cqEnabled: true,
			wl: func(w *utiltestingapi.WorkloadWrapper) *utiltestingapi.WorkloadWrapper {
				return reducible(w).Annotation(workloadslicing.WorkloadSliceReplacementFor, "default/predecessor")
			},
			fitsIn:    []planAttempt{full(whole), red(whole)},
			wantTried: []planAttempt{full(none), red(none), full(partial), red(partial), full(undecl), red(undecl)},
			wantNote:  "scaling up a running workload never preempts whole workloads that opted into partial preemption, and nothing else can make room",
		},
		"scale-up slice: evicts a victim that did not opt in": {
			gate: true, cqEnabled: true,
			wl: func(w *utiltestingapi.WorkloadWrapper) *utiltestingapi.WorkloadWrapper {
				return reducible(w).Annotation(workloadslicing.WorkloadSliceReplacementFor, "default/predecessor")
			},
			fitsIn:    []planAttempt{full(undecl)},
			wantTried: []planAttempt{full(none), red(none), full(partial), red(partial), full(undecl)},
			wantFit:   true,
		},
		"scale-up slice: partial preemption still applies": {
			gate: true, cqEnabled: true,
			wl: func(w *utiltestingapi.WorkloadWrapper) *utiltestingapi.WorkloadWrapper {
				return reducible(w).Annotation(workloadslicing.WorkloadSliceReplacementFor, "default/predecessor")
			},
			fitsIn:    []planAttempt{red(partial)},
			wantTried: []planAttempt{full(none), red(none), full(partial), red(partial)},
			wantFit:   true,
		},
		"grow-back: free quota only, never preempts": {
			gate: true, cqEnabled: true,
			wl:        func(w *utiltestingapi.WorkloadWrapper) *utiltestingapi.WorkloadWrapper { return growBack(reducible(w)) },
			fitsIn:    []planAttempt{full(partial), red(partial), full(whole), red(whole)},
			wantTried: []planAttempt{full(none), red(none)},
			wantNote:  "growing back after a partial preemption, so it may only use free quota",
		},
		"grow-back fits into free quota": {
			gate: true, cqEnabled: true, wl: growBack,
			fitsIn:    []planAttempt{full(none)},
			wantTried: []planAttempt{full(none)},
			wantFit:   true,
		},
		"grow-back with the gate off is ordinary": {
			wl:        growBack,
			fitsIn:    []planAttempt{full(whole)},
			wantTried: []planAttempt{full(whole)},
			wantFit:   true,
		},
		"partial preemption in flight holds back the Evict fallback": {
			gate: true, cqEnabled: true, inFlightCQ: true,
			fitsIn:    []planAttempt{full(whole)},
			wantTried: []planAttempt{full(none), full(partial)},
			wantNote:  "partial preemption cannot make room yet; whole-workload preemption waits for the partial preemptions in progress to complete",
		},
	}
	for name, tc := range cases {
		t.Run(name, func(t *testing.T) {
			features.SetFeatureGatesDuringTest(t, map[featuregate.Feature]bool{
				features.SparkPartialPreemption: tc.gate,
				features.PartialAdmission:       true,
			})
			w := utiltestingapi.MakeWorkload("wl", "default").
				PodSets(*utiltestingapi.MakePodSet("main", 4).Request(corev1.ResourceCPU, "1").Obj())
			if tc.wl != nil {
				w = tc.wl(w)
			}
			cq := &schdcache.ClusterQueueSnapshot{PartialPreemption: schdcache.PartialPreemptionSettings{
				Enabled: tc.cqEnabled, FallbackWait: tc.fallbackWait,
			}}
			if tc.inFlightCQ {
				cq = inFlightCQ()
			}
			ladder := planLadder(workload.NewInfo(logr.Discard(), w.Obj()), cq)

			fits := func(a planAttempt) bool {
				for _, f := range tc.fitsIn {
					if f == a {
						return true
					}
				}
				return false
			}
			var tried []planAttempt
			plan, fit := ladder.run(
				func(m preemption.Mode) assignment.Plan {
					tried = append(tried, full(m))
					return fakePlan(fits(full(m)))
				},
				func(m preemption.Mode) (assignment.Plan, bool) {
					tried = append(tried, red(m))
					if fits(red(m)) {
						return fakePlan(true), true
					}
					return assignment.Plan{}, false
				},
			)
			if diff := cmp.Diff(tc.wantTried, tried, cmp.AllowUnexported(planAttempt{})); diff != "" {
				t.Errorf("Unexpected attempts (-want,+got):\n%s", diff)
			}
			if fit != tc.wantFit || plan.CanFit() != tc.wantFit {
				t.Errorf("fit = %v, plan.CanFit() = %v, want %v", fit, plan.CanFit(), tc.wantFit)
			}
			note := ""
			if !fit {
				note = ladder.noFitNote()
			}
			if note != tc.wantNote {
				t.Errorf("noFitNote = %q, want %q", note, tc.wantNote)
			}
		})
	}
}

func TestAttemptLadderStopsAtPlanError(t *testing.T) {
	ladder := newAttemptLadder(true, preemption.NoPreemption, preemption.PartialOnly)
	calls := 0
	plan, fit := ladder.run(
		func(preemption.Mode) assignment.Plan {
			calls++
			p := fakePlan(false)
			p.Error = context.Canceled
			return p
		},
		func(preemption.Mode) (assignment.Plan, bool) {
			calls++
			return fakePlan(true), true
		},
	)
	if fit || plan.Error == nil || calls != 1 {
		t.Errorf("fit = %v, err = %v, calls = %d; want the first plan's error after one call", fit, plan.Error, calls)
	}
}

func TestEntryMarkPartialPreemptionOutcome(t *testing.T) {
	partial := &preemption.Target{Reclaim: &policy.PartialReclaim{PodSet: "main", Count: 1}}
	whole := &preemption.Target{}
	cases := map[string]struct {
		targets     []*preemption.Target
		wantMessage string
	}{
		"all partial": {
			targets:     []*preemption.Target{partial, partial},
			wantMessage: "fits with preemption. Pending the partial preemption of 2 workload(s)",
		},
		"mixed": {
			targets:     []*preemption.Target{partial, whole},
			wantMessage: "fits with preemption. Pending the preemption of 2 workload(s)",
		},
		"all whole": {
			targets:     []*preemption.Target{whole, whole},
			wantMessage: "fits with preemption. Pending the preemption of 2 workload(s)",
		},
	}
	for name, tc := range cases {
		t.Run(name, func(t *testing.T) {
			e := entry{inadmissibleMsg: "fits with preemption"}
			e.markPreemptionOutcomeOf(len(tc.targets), 0, allPartialTargets(tc.targets))
			if e.inadmissibleMsg != tc.wantMessage {
				t.Errorf("message = %q, want %q", e.inadmissibleMsg, tc.wantMessage)
			}
			if e.requeueReason != qcache.RequeueReasonPendingPreemption {
				t.Errorf("requeueReason = %q, want %q", e.requeueReason, qcache.RequeueReasonPendingPreemption)
			}
		})
	}
}

// runPartialPreemptionCycle runs one scheduling cycle over a single ClusterQueue "pp"
// (10 CPU, preempting lower priorities within the queue) and returns the client.
func runPartialPreemptionCycle(t *testing.T, cqAnnotations map[string]string, workloads []kueue.Workload) client.Client {
	t.Helper()
	ctx, log := utiltesting.ContextWithLog(t)
	cqw := utiltestingapi.MakeClusterQueue("pp").
		NamespaceSelector(&metav1.LabelSelector{}).
		Preemption(kueue.ClusterQueuePreemption{WithinClusterQueue: kueue.PreemptionPolicyLowerPriority}).
		ResourceGroup(*utiltestingapi.MakeFlavorQuotas("default").Resource(corev1.ResourceCPU, "10").Obj())
	for k, v := range cqAnnotations {
		cqw = cqw.Annotation(k, v)
	}
	cq := cqw.Obj()
	lq := utiltestingapi.MakeLocalQueue("pp", "default").ClusterQueue("pp").Obj()
	cl := utiltesting.NewClientBuilder().
		WithLists(&kueue.WorkloadList{Items: workloads}, &kueue.LocalQueueList{Items: []kueue.LocalQueue{*lq}}).
		WithObjects(utiltesting.MakeNamespaceWrapper("default").Obj()).
		WithStatusSubresource(&kueue.Workload{}).
		WithInterceptorFuncs(interceptor.Funcs{SubResourceApply: utiltesting.TreatSSAAsStrategicMergeForApplyConfiguration}).
		Build()
	cqCache := schdcache.New(cl)
	qManager := qcache.NewManagerForUnitTests(cl, cqCache)
	if err := qManager.AddLocalQueue(ctx, lq); err != nil {
		t.Fatal(err)
	}
	cqCache.AddOrUpdateResourceFlavor(log, utiltestingapi.MakeResourceFlavor("default").Obj())
	if err := cqCache.AddClusterQueue(ctx, cq); err != nil {
		t.Fatal(err)
	}
	if err := qManager.AddClusterQueue(ctx, cq); err != nil {
		t.Fatal(err)
	}
	fakeClock := testingclock.NewFakeClock(time.Now().Truncate(time.Second))
	s := New(qManager, cqCache, cl, &utiltesting.EventRecorder{},
		WithClock(t, fakeClock), WithPreemptionExpectations(preemptexpectations.New()))
	wg := sync.WaitGroup{}
	s.setAdmissionRoutineWrapper(routine.NewWrapper(func() { wg.Add(1) }, func() { wg.Done() }))
	ctx, cancel := context.WithTimeout(ctx, queueingTimeout)
	defer cancel()
	go qManager.CleanUpOnContext(ctx)
	s.schedule(ctx)
	wg.Wait()
	return cl
}

func getWorkload(t *testing.T, cl client.Client, name string) *kueue.Workload {
	t.Helper()
	wl := &kueue.Workload{}
	if err := cl.Get(context.Background(), client.ObjectKey{Namespace: "default", Name: name}, wl); err != nil {
		t.Fatal(err)
	}
	return wl
}

func TestSchedulePartialPreemption(t *testing.T) {
	now := time.Now().Truncate(time.Second)
	admittedAt := func(name string, priority int32, cpu string, count int32) *utiltestingapi.WorkloadWrapper {
		return utiltestingapi.MakeWorkload(name, "default").Queue("pp").Priority(priority).
			PodSets(*utiltestingapi.MakePodSet("main", int(count)).Request(corev1.ResourceCPU, cpu).Obj()).
			ReserveQuotaAt(utiltestingapi.MakeAdmission("pp").
				PodSets(utiltestingapi.MakePodSetAssignment("main").
					Assignment(corev1.ResourceCPU, "default", resourceTimes(cpu, count)).Count(count).Obj()).Obj(), now).
			AdmittedAt(true, now)
	}
	// low fills the ClusterQueue; high can only be admitted by preempting it.
	low := admittedAt("low", 0, "10", 1).Obj()
	high := utiltestingapi.MakeWorkload("high", "default").Queue("pp").Priority(100).
		PodSets(*utiltestingapi.MakePodSet("main", 1).Request(corev1.ResourceCPU, "5").Obj()).Obj()

	cases := map[string]struct {
		gate          bool
		cqAnnotations map[string]string
		workloads     []kueue.Workload
		// wantEvicted are the workloads expected to carry an Evicted condition.
		wantEvicted []string
		// wantAdmitted are the workloads expected to hold a quota reservation.
		wantAdmitted []string
		// wantPending maps pending workloads to a substring of their QuotaReserved message.
		wantPending map[string]string
	}{
		"gate off, fallback Wait annotation ignored: whole preemption as today": {
			cqAnnotations: map[string]string{constants.PartialPreemptionFallbackAnnotation: constants.PartialPreemptionFallbackWait},
			workloads:     []kueue.Workload{*low.DeepCopy(), *high.DeepCopy()},
			wantEvicted:   []string{"low"},
			wantAdmitted:  []string{"low"},
			wantPending:   map[string]string{"high": "Pending the preemption of 1 workload(s)"},
		},
		"fallback Evict: whole preemption once partial preemption cannot make room": {
			gate:         true,
			workloads:    []kueue.Workload{*low.DeepCopy(), *high.DeepCopy()},
			wantEvicted:  []string{"low"},
			wantAdmitted: []string{"low"},
			wantPending:  map[string]string{"high": "Pending the preemption of 1 workload(s)"},
		},
		"fallback Wait: the preemptor stays pending and nothing is evicted": {
			gate:          true,
			cqAnnotations: map[string]string{constants.PartialPreemptionFallbackAnnotation: constants.PartialPreemptionFallbackWait},
			workloads:     []kueue.Workload{*low.DeepCopy(), *high.DeepCopy()},
			wantAdmitted:  []string{"low"},
			wantPending:   map[string]string{"high": "partial-preemption fallback is Wait"},
		},
		"a partial preemption in flight holds back whole eviction": {
			gate: true,
			workloads: []kueue.Workload{
				*admittedAt("low", 0, "1", 10).
					Annotation(constants.PartialPreemptionPodSetAnnotation, "main").
					Annotation(constants.PartialPreemptionMinCountAnnotation, "2").
					Annotation(constants.PartialPreemptionTargetAnnotation, "8").Obj(),
				*high.DeepCopy(),
			},
			wantAdmitted: []string{"low"},
			wantPending:  map[string]string{"high": "partial preemptions in progress"},
		},
		"grow-back never preempts": {
			gate: true,
			workloads: []kueue.Workload{
				*low.DeepCopy(),
				*utiltestingapi.MakeWorkload("grow", "default").Queue("pp").Priority(100).
					Annotation(constants.PartialPreemptionPodSetAnnotation, "main").
					Annotation(constants.PartialPreemptionMinCountAnnotation, "1").
					Annotation(constants.PartialPreemptionRestoreCountAnnotation, "4").
					PodSets(*utiltestingapi.MakePodSet("main", 4).Request(corev1.ResourceCPU, "1").Obj()).Obj(),
			},
			wantAdmitted: []string{"low"},
			wantPending:  map[string]string{"grow": "growing back after a partial preemption"},
		},
		"replacement slice waits for the partial preemption of its predecessor": {
			gate: true,
			workloads: []kueue.Workload{
				*admittedAt("app-1", 0, "1", 4).
					Annotation(constants.PartialPreemptionPodSetAnnotation, "main").
					Annotation(constants.PartialPreemptionMinCountAnnotation, "1").
					Annotation(constants.PartialPreemptionTargetAnnotation, "3").Obj(),
				*utiltestingapi.MakeWorkload("app-2", "default").Queue("pp").
					Annotation(workloadslicing.WorkloadSliceReplacementFor, "default/app-1").
					Annotation(constants.PartialPreemptionPodSetAnnotation, "main").
					Annotation(constants.PartialPreemptionMinCountAnnotation, "1").
					PodSets(*utiltestingapi.MakePodSet("main", 5).Request(corev1.ResourceCPU, "1").Obj()).Obj(),
			},
			wantAdmitted: []string{"app-1"},
			wantPending:  map[string]string{"app-2": "Waiting for the partial preemption of the replaced workload slice default/app-1"},
		},
		"replacement slice with no partial preemption outstanding is admitted": {
			gate: true,
			workloads: []kueue.Workload{
				*admittedAt("app-1", 0, "1", 4).
					Annotation(constants.PartialPreemptionPodSetAnnotation, "main").
					Annotation(constants.PartialPreemptionMinCountAnnotation, "1").Obj(),
				*utiltestingapi.MakeWorkload("app-2", "default").Queue("pp").
					Annotation(workloadslicing.WorkloadSliceReplacementFor, "default/app-1").
					Annotation(constants.PartialPreemptionPodSetAnnotation, "main").
					Annotation(constants.PartialPreemptionMinCountAnnotation, "1").
					PodSets(*utiltestingapi.MakePodSet("main", 5).Request(corev1.ResourceCPU, "1").Obj()).Obj(),
			},
			wantAdmitted: []string{"app-1", "app-2"},
		},
	}
	for name, tc := range cases {
		t.Run(name, func(t *testing.T) {
			features.SetFeatureGatesDuringTest(t, map[featuregate.Feature]bool{
				features.SparkPartialPreemption:       tc.gate,
				features.ElasticJobsViaWorkloadSlices: true,
			})
			cl := runPartialPreemptionCycle(t, tc.cqAnnotations, tc.workloads)
			for _, wl := range tc.workloads {
				got := getWorkload(t, cl, wl.Name)
				evicted := apimeta.IsStatusConditionTrue(got.Status.Conditions, kueue.WorkloadEvicted)
				if want := contains(tc.wantEvicted, wl.Name); evicted != want {
					t.Errorf("%s: evicted = %v, want %v", wl.Name, evicted, want)
				}
				reserved := workload.HasQuotaReservation(got)
				if want := contains(tc.wantAdmitted, wl.Name); reserved != want {
					t.Errorf("%s: quota reserved = %v, want %v (conditions %+v)", wl.Name, reserved, want, got.Status.Conditions)
				}
				if wantMsg, pending := tc.wantPending[wl.Name]; pending {
					cond := apimeta.FindStatusCondition(got.Status.Conditions, kueue.WorkloadQuotaReserved)
					if cond == nil || !strings.Contains(cond.Message, wantMsg) {
						t.Errorf("%s: QuotaReserved condition %+v, want a message containing %q", wl.Name, cond, wantMsg)
					}
				}
			}
		})
	}
}

func contains(s []string, v string) bool {
	for _, x := range s {
		if x == v {
			return true
		}
	}
	return false
}

// resourceTimes formats count × qty CPU for an admission.
func resourceTimes(qty string, count int32) string {
	q := resource.MustParse(qty)
	q.Mul(int64(count))
	return q.String()
}
