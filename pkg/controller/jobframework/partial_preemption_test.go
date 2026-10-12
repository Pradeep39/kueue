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

package jobframework_test

import (
	"context"
	"strconv"
	"testing"
	"time"

	"github.com/google/go-cmp/cmp"
	"go.uber.org/mock/gomock"
	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	apimeta "k8s.io/apimachinery/pkg/api/meta"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	testingclock "k8s.io/utils/clock/testing"
	controllerruntime "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/interceptor"

	kueue "sigs.k8s.io/kueue/apis/kueue/v1beta2"
	mocks "sigs.k8s.io/kueue/internal/mocks/controller/jobframework"
	kueueconstants "sigs.k8s.io/kueue/pkg/constants"
	"sigs.k8s.io/kueue/pkg/controller/core/indexer"
	. "sigs.k8s.io/kueue/pkg/controller/jobframework"
	"sigs.k8s.io/kueue/pkg/features"
	utiltesting "sigs.k8s.io/kueue/pkg/util/testing"
	utiltestingapi "sigs.k8s.io/kueue/pkg/util/testing/v1beta2"
	testingjob "sigs.k8s.io/kueue/pkg/util/testingjobs/job"
	"sigs.k8s.io/kueue/pkg/workload"
	"sigs.k8s.io/kueue/pkg/workloadslicing"
)

const (
	ppDriver   kueue.PodSetReference = "driver"
	ppExecutor kueue.PodSetReference = "executor"
	ppTipName                        = "job-wl-tip"
)

// ppJob is a fake elastic job implementing JobWithPartialPreemption. It models its executor
// Pods: running counts the ones that still hold node resources (including those being
// deleted), and ReclaimPods only finishes stopping them after stopAfterCalls calls.
type ppJob struct {
	*mocks.MockGenericJob

	floorPodSet kueue.PodSetReference
	floor       int32
	floorOK     bool
	marked      int32
	markedOK    bool

	running        int32
	stopAfterCalls int
	reclaimCalls   []int32
	// observed records the stored executor grant, and the executors still holding
	// resources, each time ReclaimPods is called.
	observed []ppObservation
}

type ppObservation struct{ grant, running int32 }

var _ JobWithPartialPreemption = (*ppJob)(nil)

func (j *ppJob) PartialPreemptionFloor() (kueue.PodSetReference, int32, bool) {
	return j.floorPodSet, j.floor, j.floorOK
}

func (j *ppJob) PartialPreemptionMarkedPods() (int32, bool) { return j.marked, j.markedOK }

func (j *ppJob) ReclaimPods(ctx context.Context, c client.Client, podSet kueue.PodSetReference, keep int32) (bool, error) {
	j.reclaimCalls = append(j.reclaimCalls, keep)
	wl := &kueue.Workload{}
	if err := c.Get(ctx, client.ObjectKey{Namespace: "ns", Name: ppTipName}, wl); err == nil {
		g, _ := workload.GrantedCount(wl, podSet)
		j.observed = append(j.observed, ppObservation{grant: g, running: j.running})
	}
	if len(j.reclaimCalls) >= j.stopAfterCalls && j.running > keep {
		j.running = keep
	}
	return j.running <= keep, nil
}

var ppGVK = batchv1.SchemeGroupVersion.WithKind("Job")

type ppEnv struct {
	t        *testing.T
	ctx      context.Context
	cl       client.Client
	rec      *utiltesting.EventRecorder
	r        *JobReconciler
	clock    *testingclock.FakeClock
	job      *ppJob
	podSets  []kueue.PodSet
	patches  int
	finished bool
}

func newPPEnv(t *testing.T, now time.Time, executors int32, wls ...*kueue.Workload) *ppEnv {
	t.Helper()
	ctx, _ := utiltesting.ContextWithLog(t)
	env := &ppEnv{t: t, ctx: ctx, clock: testingclock.NewFakeClock(now)}
	jobObj := testingjob.MakeJob("job", "ns").Queue("lq").UID("job-uid").
		SetAnnotation(workloadslicing.EnabledAnnotationKey, workloadslicing.EnabledAnnotationValue).Obj()
	env.podSets = []kueue.PodSet{
		*utiltestingapi.MakePodSet(ppDriver, 1).Request(corev1.ResourceCPU, "1").Obj(),
		*utiltestingapi.MakePodSet(ppExecutor, int(executors)).Request(corev1.ResourceCPU, "1").Obj(),
	}
	mgj := mocks.NewMockGenericJob(gomock.NewController(t))
	mgj.EXPECT().Object().Return(jobObj).AnyTimes()
	mgj.EXPECT().GVK().Return(ppGVK).AnyTimes()
	mgj.EXPECT().IsSuspended().Return(false).AnyTimes()
	mgj.EXPECT().IsActive().Return(true).AnyTimes()
	mgj.EXPECT().Suspend().AnyTimes()
	mgj.EXPECT().RestorePodSetsInfo(gomock.Any(), gomock.Any()).Return(true).AnyTimes()
	mgj.EXPECT().Finished(gomock.Any()).DoAndReturn(func(context.Context) (string, bool, bool) {
		return "", true, env.finished
	}).AnyTimes()
	mgj.EXPECT().PodSets(gomock.Any(), gomock.Any()).DoAndReturn(func(context.Context, client.Client) ([]kueue.PodSet, error) {
		out := make([]kueue.PodSet, len(env.podSets))
		for i := range env.podSets {
			out[i] = *env.podSets[i].DeepCopy()
		}
		return out, nil
	}).AnyTimes()
	env.job = &ppJob{MockGenericJob: mgj, floorPodSet: ppExecutor, floor: 1, floorOK: true, marked: 0, markedOK: true, running: 4, stopAfterCalls: 1}

	objs := []client.Object{jobObj, &corev1.Namespace{ObjectMeta: metav1.ObjectMeta{Name: "ns"}}}
	for _, wl := range wls {
		objs = append(objs, wl)
	}
	env.cl = utiltesting.NewClientBuilder().
		WithObjects(objs...).
		WithStatusSubresource(&kueue.Workload{}).
		WithIndex(&kueue.Workload{}, indexer.OwnerReferenceIndexKey(ppGVK), indexer.WorkloadOwnerIndexFunc(ppGVK)).
		WithIndex(&kueue.Workload{}, indexer.WorkloadSliceNameKey, indexer.IndexWorkloadSliceName).
		WithInterceptorFuncs(interceptor.Funcs{
			// Server-side apply merges conditions by type; the fake's default does not.
			SubResourceApply: utiltesting.TreatSSAAsStrategicMergeForApplyConfiguration,
			Patch: func(ctx context.Context, c client.WithWatch, obj client.Object, patch client.Patch, opts ...client.PatchOption) error {
				if _, isWl := obj.(*kueue.Workload); isWl {
					env.patches++
				}
				return c.Patch(ctx, obj, patch, opts...)
			},
		}).
		Build()
	env.rec = &utiltesting.EventRecorder{}
	env.r = NewReconciler(env.cl, env.rec, WithClock(env.clock))
	return env
}

func (e *ppEnv) reconcile() controllerruntime.Result {
	e.t.Helper()
	res, err := e.r.ReconcileGenericJob(e.ctx, controllerruntime.Request{NamespacedName: client.ObjectKey{Namespace: "ns", Name: "job"}}, e.job)
	if err != nil {
		e.t.Fatalf("ReconcileGenericJob() error: %v", err)
	}
	return res
}

func (e *ppEnv) get(name string) *kueue.Workload {
	e.t.Helper()
	wl := &kueue.Workload{}
	if err := e.cl.Get(e.ctx, client.ObjectKey{Namespace: "ns", Name: name}, wl); err != nil {
		e.t.Fatalf("getting workload %s: %v", name, err)
	}
	return wl
}

func (e *ppEnv) workloads() []kueue.Workload {
	e.t.Helper()
	list := &kueue.WorkloadList{}
	if err := e.cl.List(e.ctx, list, client.InNamespace("ns")); err != nil {
		e.t.Fatal(err)
	}
	return list.Items
}

// assertSC2 is EQM's SC-2 in the shape a partial preemption can break it: every executor Pod
// that still holds node resources has a grant, at every point the job observed and now.
func (e *ppEnv) assertSC2() {
	e.t.Helper()
	for i, obs := range e.job.observed {
		if obs.grant < obs.running {
			e.t.Errorf("SC-2: grant was %d while %d executors still ran (ReclaimPods call %d)", obs.grant, obs.running, i)
		}
	}
	if g, ok := workload.GrantedCount(e.get(ppTipName), ppExecutor); ok && g < e.job.running {
		e.t.Errorf("SC-2: grant is %d while %d executors still run", g, e.job.running)
	}
}

// makePPTip builds the admitted tip slice: 4 executors granted, 4 requested.
func makePPTip(now time.Time) *utiltestingapi.WorkloadWrapper {
	return utiltestingapi.MakeWorkload(ppTipName, "ns").
		Creation(now.Add(-time.Hour)).
		ControllerReference(ppGVK, "job", "job-uid").
		Annotation(workloadslicing.EnabledAnnotationKey, workloadslicing.EnabledAnnotationValue).
		Annotation(kueue.WorkloadSliceNameAnnotation, ppTipName).
		Annotation(kueueconstants.PartialPreemptionPodSetAnnotation, string(ppExecutor)).
		Annotation(kueueconstants.PartialPreemptionMinCountAnnotation, "1").
		Annotation(kueueconstants.PartialPreemptionMarkedPodsAnnotation, "0").
		Queue("lq").
		PodSets(
			*utiltestingapi.MakePodSet(ppDriver, 1).Request(corev1.ResourceCPU, "1").Obj(),
			*utiltestingapi.MakePodSet(ppExecutor, 4).Request(corev1.ResourceCPU, "1").Obj(),
		).
		ReserveQuotaAt(utiltestingapi.MakeAdmission("cq").PodSets(
			utiltestingapi.MakePodSetAssignment(ppDriver).Assignment(corev1.ResourceCPU, "default", "1").Count(1).Obj(),
			utiltestingapi.MakePodSetAssignment(ppExecutor).Assignment(corev1.ResourceCPU, "default", "4").Count(4).Obj(),
		).Obj(), now.Add(-time.Hour)).
		AdmittedAt(true, now.Add(-time.Hour))
}

// withTarget marks w as the victim of a partial preemption down to target.
func withTarget(w *utiltestingapi.WorkloadWrapper, target int32, deadline, issued time.Time) *utiltestingapi.WorkloadWrapper {
	return w.Annotation(kueueconstants.PartialPreemptionTargetAnnotation, strconv.Itoa(int(target))).
		Annotation(kueueconstants.PartialPreemptionDeadlineAnnotation, deadline.UTC().Format(time.RFC3339)).
		Condition(metav1.Condition{
			Type:               kueueconstants.PartiallyPreemptedCondition,
			Status:             metav1.ConditionTrue,
			Reason:             kueueconstants.PartialPreemptionIssuedReason,
			Message:            "issued by test",
			LastTransitionTime: metav1.NewTime(issued),
		})
}

func enablePPGates(t *testing.T, on bool) {
	features.SetFeatureGateDuringTest(t, features.ElasticJobsViaWorkloadSlices, true)
	features.SetFeatureGateDuringTest(t, features.SparkPartialPreemption, on)
}

func grantOf(t *testing.T, wl *kueue.Workload) (int32, string) {
	t.Helper()
	g, _ := workload.GrantedCount(wl, ppExecutor)
	for _, psa := range wl.Status.Admission.PodSetAssignments {
		if psa.Name == ppExecutor {
			q := psa.ResourceUsage[corev1.ResourceCPU]
			return g, q.String()
		}
	}
	return g, ""
}

func execSpecCount(wl *kueue.Workload) int32 {
	return workload.ExtractPodSetCountsFromWorkload(wl)[ppExecutor]
}

func TestPartialPreemptionReclaimOrchestration(t *testing.T) {
	now := time.Now().Truncate(time.Second)
	type want struct {
		requeueAfter  time.Duration
		reclaimCalls  []int32
		grant         int32
		cpu           string
		targetCleared bool
		condStatus    metav1.ConditionStatus
		condReason    string
		eventType     string
		eventReason   string
		restoreCount  string
	}
	cases := map[string]struct {
		gateOff        bool
		wl             *kueue.Workload
		stopAfterCalls int
		jobFinished    bool
		floor          int32
		want           want
	}{
		"pods not stopped yet: grant untouched, requeue at the poll interval": {
			wl:             withTarget(makePPTip(now), 2, now.Add(time.Minute), now).Obj(),
			stopAfterCalls: 2,
			want: want{
				requeueAfter: 5 * time.Second, reclaimCalls: []int32{2}, grant: 4, cpu: "4",
				condStatus: metav1.ConditionTrue, condReason: kueueconstants.PartialPreemptionIssuedReason, restoreCount: "4",
			},
		},
		"pods not stopped, deadline close: requeue at the deadline": {
			wl:             withTarget(makePPTip(now), 2, now.Add(3*time.Second), now).Obj(),
			stopAfterCalls: 2,
			want: want{
				requeueAfter: 3 * time.Second, reclaimCalls: []int32{2}, grant: 4, cpu: "4",
				condStatus: metav1.ConditionTrue, condReason: kueueconstants.PartialPreemptionIssuedReason, restoreCount: "4",
			},
		},
		"pods stopped: grant lowered to the target, spec untouched, target cleared": {
			wl:             withTarget(makePPTip(now), 2, now.Add(time.Minute), now).Obj(),
			stopAfterCalls: 1,
			want: want{
				reclaimCalls: []int32{2}, grant: 2, cpu: "2", targetCleared: true,
				condStatus: metav1.ConditionFalse, condReason: kueueconstants.PartialPreemptionCompletedReason,
				eventType: corev1.EventTypeNormal, eventReason: ReasonPartialPreemptionComplete, restoreCount: "4",
			},
		},
		"deadline passed with pods still running: grant lowered anyway, timed out": {
			wl:             withTarget(makePPTip(now), 2, now.Add(-time.Second), now.Add(-time.Minute)).Obj(),
			stopAfterCalls: 99,
			want: want{
				reclaimCalls: []int32{2}, grant: 2, cpu: "2", targetCleared: true,
				condStatus: metav1.ConditionFalse, condReason: kueueconstants.PartialPreemptionTimedOutReason,
				eventType: corev1.EventTypeWarning, eventReason: ReasonPartialPreemptionTimedOut, restoreCount: "4",
			},
		},
		"target below the floor is raised to the floor": {
			wl:             withTarget(makePPTip(now), 1, now.Add(time.Minute), now).Obj(),
			stopAfterCalls: 1,
			floor:          3,
			want: want{
				reclaimCalls: []int32{3}, grant: 3, cpu: "3", targetCleared: true,
				condStatus: metav1.ConditionFalse, condReason: kueueconstants.PartialPreemptionCompletedReason,
				eventType: corev1.EventTypeNormal, eventReason: ReasonPartialPreemptionComplete, restoreCount: "4",
			},
		},
		"grant already at the target (stale pass): cleaned up without lowering or reporting again": {
			wl:             withTarget(makePPTip(now), 4, now.Add(time.Minute), now).Obj(),
			stopAfterCalls: 1,
			want: want{
				reclaimCalls: []int32{4}, grant: 4, cpu: "4", targetCleared: true,
				condStatus: metav1.ConditionFalse, condReason: kueueconstants.PartialPreemptionCompletedReason,
			},
		},
		"evicted mid-reclaim: abandoned without touching the grant": {
			wl:             withTarget(makePPTip(now), 2, now.Add(time.Minute), now).EvictedAt(now).Obj(),
			stopAfterCalls: 1,
			want: want{
				grant: 4, cpu: "4", targetCleared: true,
				condStatus: metav1.ConditionFalse, condReason: kueueconstants.PartialPreemptionAbandonedReason,
				eventType: corev1.EventTypeNormal, eventReason: ReasonPartialPreemptionAbandoned,
			},
		},
		"workload finished mid-reclaim: abandoned without touching the grant": {
			wl:             withTarget(makePPTip(now), 2, now.Add(time.Minute), now).FinishedAt(now).Obj(),
			stopAfterCalls: 1,
			want: want{
				grant: 4, cpu: "4", targetCleared: true,
				condStatus: metav1.ConditionFalse, condReason: kueueconstants.PartialPreemptionAbandonedReason,
				eventType: corev1.EventTypeNormal, eventReason: ReasonPartialPreemptionAbandoned,
			},
		},
		"job finished mid-reclaim: abandoned without touching the grant": {
			wl:             withTarget(makePPTip(now), 2, now.Add(time.Minute), now).Obj(),
			stopAfterCalls: 1,
			jobFinished:    true,
			want: want{
				grant: 4, cpu: "4", targetCleared: true,
				condStatus: metav1.ConditionFalse, condReason: kueueconstants.PartialPreemptionAbandonedReason,
				eventType: corev1.EventTypeNormal, eventReason: ReasonPartialPreemptionAbandoned,
			},
		},
		"gate off: the target is not acted on": {
			gateOff:        true,
			wl:             withTarget(makePPTip(now), 2, now.Add(-time.Second), now.Add(-time.Minute)).Obj(),
			stopAfterCalls: 1,
			want: want{
				grant: 4, cpu: "4",
				condStatus: metav1.ConditionTrue, condReason: kueueconstants.PartialPreemptionIssuedReason,
			},
		},
	}
	for name, tc := range cases {
		t.Run(name, func(t *testing.T) {
			enablePPGates(t, !tc.gateOff)
			env := newPPEnv(t, now, 4, tc.wl)
			env.job.stopAfterCalls = tc.stopAfterCalls
			env.finished = tc.jobFinished
			if tc.floor != 0 {
				env.job.floor = tc.floor
			}
			res := env.reconcile()

			if res.RequeueAfter != tc.want.requeueAfter {
				t.Errorf("RequeueAfter = %v, want %v", res.RequeueAfter, tc.want.requeueAfter)
			}
			if diff := cmp.Diff(tc.want.reclaimCalls, env.job.reclaimCalls); diff != "" {
				t.Errorf("ReclaimPods keep arguments (-want,+got):\n%s", diff)
			}
			got := env.get(ppTipName)
			g, cpu := grantOf(t, got)
			gotCPU, wantCPU := resource.MustParse(cpu), resource.MustParse(tc.want.cpu)
			if g != tc.want.grant || gotCPU.Cmp(wantCPU) != 0 {
				t.Errorf("executor grant = %d (cpu %s), want %d (cpu %s)", g, cpu, tc.want.grant, tc.want.cpu)
			}
			if c := execSpecCount(got); c != 4 {
				t.Errorf("executor spec count = %d, want 4 (the request must not change)", c)
			}
			_, hasTarget := got.Annotations[kueueconstants.PartialPreemptionTargetAnnotation]
			_, hasDeadline := got.Annotations[kueueconstants.PartialPreemptionDeadlineAnnotation]
			if cleared := !hasTarget && !hasDeadline; cleared != tc.want.targetCleared {
				t.Errorf("target and deadline cleared = %v, want %v (annotations %v)", cleared, tc.want.targetCleared, got.Annotations)
			}
			cond := apimeta.FindStatusCondition(got.Status.Conditions, kueueconstants.PartiallyPreemptedCondition)
			if cond == nil || cond.Status != tc.want.condStatus || cond.Reason != tc.want.condReason {
				t.Errorf("PartiallyPreempted condition = %+v, want %s/%s", cond, tc.want.condStatus, tc.want.condReason)
			}
			if got := got.Annotations[kueueconstants.PartialPreemptionRestoreCountAnnotation]; got != tc.want.restoreCount {
				t.Errorf("restore count = %q, want %q", got, tc.want.restoreCount)
			}
			var gotEvent *utiltesting.EventRecord
			for i := range env.rec.RecordedEvents {
				if ev := &env.rec.RecordedEvents[i]; ev.Reason == ReasonPartialPreemptionComplete ||
					ev.Reason == ReasonPartialPreemptionTimedOut || ev.Reason == ReasonPartialPreemptionAbandoned {
					gotEvent = ev
				}
			}
			switch {
			case tc.want.eventReason == "" && gotEvent != nil:
				t.Errorf("unexpected event %+v", *gotEvent)
			case tc.want.eventReason != "" && (gotEvent == nil || gotEvent.Reason != tc.want.eventReason || gotEvent.EventType != tc.want.eventType):
				t.Errorf("event = %+v, want %s %s", gotEvent, tc.want.eventType, tc.want.eventReason)
			}
			if tc.want.condReason != kueueconstants.PartialPreemptionTimedOutReason {
				env.assertSC2()
			}
		})
	}
}

// TestPartialPreemptionSC2AcrossReconciles walks a reclaim through several reconciles while the
// job's own count drops as its Pods are deleted, which is exactly what would make the slice
// logic lower the grant early if it ran mid-reclaim. SC-2 must hold throughout, and the grant
// must fall only once the Pods have stopped.
func TestPartialPreemptionSC2AcrossReconciles(t *testing.T) {
	enablePPGates(t, true)
	now := time.Now().Truncate(time.Second)
	env := newPPEnv(t, now, 4, withTarget(makePPTip(now), 2, now.Add(time.Minute), now).Obj())
	env.job.stopAfterCalls = 3
	// The live count no longer includes the Pods being deleted.
	env.podSets[1].Count = 2

	for i := range 2 {
		res := env.reconcile()
		env.assertSC2()
		if g, _ := grantOf(t, env.get(ppTipName)); g != 4 {
			t.Fatalf("reconcile %d: grant lowered to %d before the reclaimed pods stopped", i, g)
		}
		if res.RequeueAfter == 0 {
			t.Errorf("reconcile %d: expected a requeue while pods are stopping", i)
		}
	}
	env.reconcile()
	env.assertSC2()
	if g, _ := grantOf(t, env.get(ppTipName)); g != 2 {
		t.Fatalf("grant after the pods stopped = %d, want 2", g)
	}
}

// TestPartialPreemptionGrowBack pins §5.5: once the grant is lowered, the next reconcile sees
// request > grant and creates a scale-up slice, carrying the restore count, both for a job whose
// count follows its live Pods and for one with a fixed count.
func TestPartialPreemptionGrowBack(t *testing.T) {
	now := time.Now().Truncate(time.Second)
	cases := map[string]struct {
		// countAfter is the job's executor count once the reclaim is done.
		countAfter int32
		// reconciles is how many reconciles after the reclaim it takes to create the probe.
		reconciles int
	}{
		// jobframework sees only the count, so a static application and a live count whose
		// replacements were recreated gated are the same case: request == spec > grant.
		"fixed count, or live count with replacements recreated gated": {countAfter: 4, reconciles: 1},
		"live count, the driver also wants more":                       {countAfter: 6, reconciles: 1},
		"live count, replacements not yet recreated":                   {countAfter: 3, reconciles: 2},
		"live count, replacements recreated beyond old":                {countAfter: 5, reconciles: 1},
	}
	for name, tc := range cases {
		t.Run(name, func(t *testing.T) {
			enablePPGates(t, true)
			env := newPPEnv(t, now, 4, withTarget(makePPTip(now), 2, now.Add(time.Minute), now).Obj())
			env.reconcile()
			if g, _ := grantOf(t, env.get(ppTipName)); g != 2 {
				t.Fatalf("grant after reclaim = %d, want 2", g)
			}
			env.podSets[1].Count = tc.countAfter
			for range tc.reconciles {
				env.reconcile()
			}
			var probe *kueue.Workload
			for _, wl := range env.workloads() {
				if wl.Name != ppTipName {
					probe = &wl
				}
			}
			if probe == nil {
				t.Fatalf("no grow-back slice was created")
			}
			if got := workloadslicing.ReplacementForKey(probe); got == nil || string(*got) != "ns/"+ppTipName {
				t.Errorf("grow-back slice replaces %v, want ns/%s", got, ppTipName)
			}
			if got := execSpecCount(probe); got != tc.countAfter {
				t.Errorf("grow-back slice executor count = %d, want %d", got, tc.countAfter)
			}
			if got := probe.Annotations[kueueconstants.PartialPreemptionRestoreCountAnnotation]; got != "4" {
				t.Errorf("grow-back slice restore count = %q, want \"4\"", got)
			}
			if got := probe.Annotations[kueueconstants.PartialPreemptionPodSetAnnotation]; got != string(ppExecutor) {
				t.Errorf("grow-back slice reclaimable podset = %q, want %q", got, ppExecutor)
			}
			if !workload.IsGrowBack(probe) {
				t.Errorf("workload.IsGrowBack(probe) = false, want true")
			}
		})
	}
}

func TestPartialPreemptionDeclaration(t *testing.T) {
	now := time.Now().Truncate(time.Second)
	cases := map[string]struct {
		gateOff     bool
		wl          *kueue.Workload
		floorOK     bool
		floor       int32
		marked      int32
		markedOK    bool
		want        map[string]string
		wantPatches int
	}{
		"declaration added to an admitted slice that has none": {
			wl: makePPTip(now).Annotations(map[string]string{
				workloadslicing.EnabledAnnotationKey: workloadslicing.EnabledAnnotationValue,
				kueue.WorkloadSliceNameAnnotation:    ppTipName,
			}).Obj(),
			floorOK: true, floor: 2, marked: 1, markedOK: true,
			want: map[string]string{
				kueueconstants.PartialPreemptionPodSetAnnotation:     string(ppExecutor),
				kueueconstants.PartialPreemptionMinCountAnnotation:   "2",
				kueueconstants.PartialPreemptionMarkedPodsAnnotation: "1",
			},
			wantPatches: 1,
		},
		"unchanged declaration is not rewritten": {
			wl:      makePPTip(now).Obj(),
			floorOK: true, floor: 1, marked: 0, markedOK: true,
			want: map[string]string{
				kueueconstants.PartialPreemptionPodSetAnnotation:     string(ppExecutor),
				kueueconstants.PartialPreemptionMinCountAnnotation:   "1",
				kueueconstants.PartialPreemptionMarkedPodsAnnotation: "0",
			},
		},
		"changed floor and marked count are updated": {
			wl:      makePPTip(now).Obj(),
			floorOK: true, floor: 3, marked: 2, markedOK: true,
			want: map[string]string{
				kueueconstants.PartialPreemptionPodSetAnnotation:     string(ppExecutor),
				kueueconstants.PartialPreemptionMinCountAnnotation:   "3",
				kueueconstants.PartialPreemptionMarkedPodsAnnotation: "2",
			},
			wantPatches: 1,
		},
		"unobserved marked count keeps the last one": {
			wl:      makePPTip(now).Annotation(kueueconstants.PartialPreemptionMarkedPodsAnnotation, "2").Obj(),
			floorOK: true, floor: 1, marked: 0, markedOK: false,
			want: map[string]string{
				kueueconstants.PartialPreemptionPodSetAnnotation:     string(ppExecutor),
				kueueconstants.PartialPreemptionMinCountAnnotation:   "1",
				kueueconstants.PartialPreemptionMarkedPodsAnnotation: "2",
			},
		},
		"no floor removes the declaration": {
			wl:          makePPTip(now).Obj(),
			floorOK:     false,
			want:        map[string]string{},
			wantPatches: 1,
		},
		"restore count dropped once the grant is back": {
			wl:      makePPTip(now).Annotation(kueueconstants.PartialPreemptionRestoreCountAnnotation, "4").Obj(),
			floorOK: true, floor: 1, marked: 0, markedOK: true,
			want: map[string]string{
				kueueconstants.PartialPreemptionPodSetAnnotation:     string(ppExecutor),
				kueueconstants.PartialPreemptionMinCountAnnotation:   "1",
				kueueconstants.PartialPreemptionMarkedPodsAnnotation: "0",
			},
			wantPatches: 1,
		},
		"restore count kept while the grant is still below it": {
			wl:      makePPTip(now).Annotation(kueueconstants.PartialPreemptionRestoreCountAnnotation, "6").Obj(),
			floorOK: true, floor: 1, marked: 0, markedOK: true,
			want: map[string]string{
				kueueconstants.PartialPreemptionPodSetAnnotation:       string(ppExecutor),
				kueueconstants.PartialPreemptionMinCountAnnotation:     "1",
				kueueconstants.PartialPreemptionMarkedPodsAnnotation:   "0",
				kueueconstants.PartialPreemptionRestoreCountAnnotation: "6",
			},
		},
		"gate off: nothing is written": {
			gateOff: true,
			wl: makePPTip(now).Annotations(map[string]string{
				workloadslicing.EnabledAnnotationKey: workloadslicing.EnabledAnnotationValue,
				kueue.WorkloadSliceNameAnnotation:    ppTipName,
			}).Obj(),
			floorOK: true, floor: 2, marked: 1, markedOK: true,
			want: map[string]string{},
		},
	}
	ppKeys := []string{
		kueueconstants.PartialPreemptionPodSetAnnotation,
		kueueconstants.PartialPreemptionMinCountAnnotation,
		kueueconstants.PartialPreemptionMarkedPodsAnnotation,
		kueueconstants.PartialPreemptionRestoreCountAnnotation,
		kueueconstants.PartialPreemptionTargetAnnotation,
	}
	for name, tc := range cases {
		t.Run(name, func(t *testing.T) {
			enablePPGates(t, !tc.gateOff)
			env := newPPEnv(t, now, 4, tc.wl)
			env.job.floorOK, env.job.floor, env.job.marked, env.job.markedOK = tc.floorOK, tc.floor, tc.marked, tc.markedOK
			env.reconcile()
			got := map[string]string{}
			ann := env.get(ppTipName).Annotations
			for _, k := range ppKeys {
				if v, has := ann[k]; has {
					got[k] = v
				}
			}
			if diff := cmp.Diff(tc.want, got); diff != "" {
				t.Errorf("partial preemption annotations (-want,+got):\n%s", diff)
			}
			if env.patches != tc.wantPatches {
				t.Errorf("workload patches = %d, want %d", env.patches, tc.wantPatches)
			}
			if len(env.workloads()) != 1 {
				t.Errorf("expected no new slice, got %d workloads", len(env.workloads()))
			}
		})
	}
}

func TestConstructWorkloadDeclaresPartialPreemption(t *testing.T) {
	for _, gateOn := range []bool{true, false} {
		t.Run("gate "+strconv.FormatBool(gateOn), func(t *testing.T) {
			enablePPGates(t, gateOn)
			env := newPPEnv(t, time.Now(), 4)
			env.job.floor, env.job.marked = 2, 1
			wl, err := ConstructWorkload(env.ctx, env.cl, env.job, nil, nil)
			if err != nil {
				t.Fatal(err)
			}
			want := map[string]string{}
			if gateOn {
				want = map[string]string{
					kueueconstants.PartialPreemptionPodSetAnnotation:     string(ppExecutor),
					kueueconstants.PartialPreemptionMinCountAnnotation:   "2",
					kueueconstants.PartialPreemptionMarkedPodsAnnotation: "1",
				}
			}
			got := map[string]string{}
			for _, k := range []string{kueueconstants.PartialPreemptionPodSetAnnotation, kueueconstants.PartialPreemptionMinCountAnnotation, kueueconstants.PartialPreemptionMarkedPodsAnnotation} {
				if v, has := wl.Annotations[k]; has {
					got[k] = v
				}
			}
			if diff := cmp.Diff(want, got); diff != "" {
				t.Errorf("annotations (-want,+got):\n%s", diff)
			}
		})
	}
}

// TestPartialPreemptionRestoreCountCarriedToPendingReplacement covers a scale-up slice that was
// already pending when its predecessor was partially preempted: it was created without the
// restore count, so the sync carries it over, or it could preempt its own preemptor.
func TestPartialPreemptionRestoreCountCarriedToPendingReplacement(t *testing.T) {
	enablePPGates(t, true)
	now := time.Now().Truncate(time.Second)
	tip := makePPTip(now).Annotation(kueueconstants.PartialPreemptionRestoreCountAnnotation, "4").Obj()
	tip.Status.Admission.PodSetAssignments[1].Count = new(int32(2))
	tip.Status.Admission.PodSetAssignments[1].ResourceUsage = corev1.ResourceList{corev1.ResourceCPU: resource.MustParse("2")}
	pending := utiltestingapi.MakeWorkload("job-wl-probe", "ns").
		Creation(now).
		ControllerReference(ppGVK, "job", "job-uid").
		Annotation(workloadslicing.EnabledAnnotationKey, workloadslicing.EnabledAnnotationValue).
		Annotation(kueue.WorkloadSliceNameAnnotation, ppTipName).
		Annotation(workloadslicing.WorkloadSliceReplacementFor, "ns/"+ppTipName).
		Queue("lq").
		PodSets(
			*utiltestingapi.MakePodSet(ppDriver, 1).Request(corev1.ResourceCPU, "1").Obj(),
			*utiltestingapi.MakePodSet(ppExecutor, 6).Request(corev1.ResourceCPU, "1").Obj(),
		).Obj()
	env := newPPEnv(t, now, 6, tip, pending)
	env.reconcile()
	if got := env.get("job-wl-probe").Annotations[kueueconstants.PartialPreemptionRestoreCountAnnotation]; got != "4" {
		t.Errorf("pending replacement restore count = %q, want \"4\"", got)
	}
	if got := env.get(ppTipName).Annotations[kueueconstants.PartialPreemptionRestoreCountAnnotation]; got != "4" {
		t.Errorf("tip restore count = %q, want \"4\" (its grant is still below it)", got)
	}
}
