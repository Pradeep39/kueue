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
	"fmt"
	"math"
	"testing"
	"time"

	sparkappv1beta2 "github.com/kubeflow/spark-operator/v2/api/v1beta2"
	sparkcommon "github.com/kubeflow/spark-operator/v2/pkg/common"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/event"

	kueue "sigs.k8s.io/kueue/apis/kueue/v1beta2"
	"sigs.k8s.io/kueue/pkg/controller/jobframework"
	"sigs.k8s.io/kueue/pkg/features"
	utiltesting "sigs.k8s.io/kueue/pkg/util/testing"
	utiltestingapi "sigs.k8s.io/kueue/pkg/util/testing/v1beta2"
	sparkapplicationtesting "sigs.k8s.io/kueue/pkg/util/testingjobs/sparkapplication"
	"sigs.k8s.io/kueue/pkg/workloadslicing"
)

// untypedReader stands in for the informer cache's untyped SparkApplications.
type untypedReader map[client.ObjectKey]*unstructured.Unstructured

func (r untypedReader) Get(_ context.Context, key client.ObjectKey, obj client.Object, _ ...client.GetOption) error {
	u, ok := obj.(*unstructured.Unstructured)
	stored := r[key]
	if !ok || stored == nil {
		return apierrors.NewNotFound(schema.GroupResource{Group: gvk.Group, Resource: "sparkapplications"}, key.Name)
	}
	stored.DeepCopyInto(u)
	return nil
}

func (r untypedReader) List(context.Context, client.ObjectList, ...client.ListOption) error {
	return nil
}

// useDeclared makes the operator declare fields for app, in an untyped copy at resourceVersion rv.
func useDeclared(t *testing.T, app *sparkappv1beta2.SparkApplication, rv string, fields map[string]int64) {
	t.Helper()
	u := untypedSparkApplication()
	u.SetNamespace(app.Namespace)
	u.SetName(app.Name)
	u.SetResourceVersion(rv)
	for field, v := range fields {
		_ = unstructured.SetNestedField(u.Object, v, "status", field)
	}
	previous := declaredReader
	declaredReader = untypedReader{client.ObjectKeyFromObject(app): u}
	t.Cleanup(func() { declaredReader = previous })
}

func noDeclared(t *testing.T) {
	t.Helper()
	previous := declaredReader
	declaredReader = nil
	t.Cleanup(func() { declaredReader = previous })
}

type podOption func(*corev1.Pod)

func ready(p *corev1.Pod) {
	p.Status.Conditions = append(p.Status.Conditions, corev1.PodCondition{Type: corev1.PodReady, Status: corev1.ConditionTrue})
}

func deleting(p *corev1.Pod) {
	p.DeletionTimestamp = &metav1.Time{}
	p.Finalizers = []string{"test"} // the fake client keeps an object being deleted only with a finalizer
}

func otherSubmission(p *corev1.Pod) {
	p.Labels[sparkcommon.LabelSubmissionID] = "sub-0"
}

func execPod(name string, phase corev1.PodPhase, opts ...podOption) *corev1.Pod {
	p := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{Namespace: "ns", Name: name, Labels: map[string]string{
			sparkcommon.LabelSparkAppName: "sparkapp",
			sparkcommon.LabelSparkRole:    sparkcommon.SparkRoleExecutor,
			sparkcommon.LabelSubmissionID: "sub-1",
		}},
		Status: corev1.PodStatus{Phase: phase},
	}
	for _, opt := range opts {
		opt(p)
	}
	return p
}

func runningSparkApp() *sparkappv1beta2.SparkApplication {
	app := sparkapplicationtesting.MakeSparkApplication("sparkapp", "ns").
		ExecutorInstances(2).
		AppState(sparkappv1beta2.ApplicationStateRunning).
		Obj()
	app.ResourceVersion = "7"
	app.Status.SubmissionID = "sub-1"
	return app
}

// podClient serves pods. A pod marked as being deleted is created with its finalizer and then
// deleted, since the fake client sets the deletion timestamp only on Delete.
func podClient(pods ...*corev1.Pod) client.Client {
	b := utiltesting.NewClientBuilder()
	var toDelete []*corev1.Pod
	for _, p := range pods {
		p = p.DeepCopy()
		if p.DeletionTimestamp != nil {
			p.DeletionTimestamp = nil
			toDelete = append(toDelete, p)
		}
		b = b.WithObjects(p)
	}
	c := b.Build()
	for _, p := range toDelete {
		_ = c.Delete(context.Background(), p)
	}
	return c
}

func TestTrimUntyped(t *testing.T) {
	u := untypedSparkApplication()
	u.SetNamespace("ns")
	u.SetName("sparkapp")
	u.SetUID("uid")
	u.SetResourceVersion("7")
	u.SetManagedFields([]metav1.ManagedFieldsEntry{{Manager: "m"}})
	_ = unstructured.SetNestedField(u.Object, "a big spec", "spec", "mainApplicationFile")
	_ = unstructured.SetNestedField(u.Object, int64(4), "status", "activeExecutors")
	_ = unstructured.SetNestedField(u.Object, int64(3), "status", "readyExecutors")
	_ = unstructured.SetNestedField(u.Object, int64(9), "status", "maxExecutorId")
	_ = unstructured.SetNestedField(u.Object, "RUNNING", "status", "applicationState", "state")

	out, err := trimUntyped(u)
	if err != nil {
		t.Fatal(err)
	}
	got := out.(*unstructured.Unstructured)
	if got.GetName() != "sparkapp" || got.GetNamespace() != "ns" || got.GetUID() != "uid" || got.GetResourceVersion() != "7" {
		t.Errorf("identity not kept: %v", got.Object["metadata"])
	}
	if _, found := got.Object["spec"]; found {
		t.Errorf("spec kept: %v", got.Object["spec"])
	}
	status, _, _ := unstructured.NestedMap(got.Object, "status")
	if len(status) != 3 || status["activeExecutors"] != int64(4) || status["readyExecutors"] != int64(3) || status["maxExecutorId"] != int64(9) {
		t.Errorf("status = %v, want only the three declared fields", status)
	}

	// A typed SparkApplication only loses its managed fields
	typed := runningSparkApp()
	typed.ManagedFields = []metav1.ManagedFieldsEntry{{Manager: "m"}}
	out, err = trimUntyped(typed)
	if err != nil {
		t.Fatal(err)
	}
	if got := out.(*sparkappv1beta2.SparkApplication); got.ManagedFields != nil || got.Status.AppState.State != sparkappv1beta2.ApplicationStateRunning {
		t.Errorf("typed object changed beyond its managed fields: %+v", got)
	}
}

func TestPodsReadyCountsReadyExecutors(t *testing.T) {
	podsReady := func(status metav1.ConditionStatus, reason string) *kueue.Workload {
		return utiltestingapi.MakeWorkload("wl", "ns").
			Condition(metav1.Condition{Type: kueue.WorkloadPodsReady, Status: status, Reason: reason}).
			Obj()
	}
	grantedExecutors := func(n int32) *kueue.Workload {
		now := time.Now()
		return utiltestingapi.MakeWorkload("wl", "ns").
			PodSets(*utiltestingapi.MakePodSet(driverPodSetName, 1).Obj(), *utiltestingapi.MakePodSet(executorPodSetName, 3).Obj()).
			ReserveQuotaAt(utiltestingapi.MakeAdmission("cq").PodSets(
				utiltestingapi.MakePodSetAssignment(driverPodSetName).Count(1).Obj(),
				utiltestingapi.MakePodSetAssignment(executorPodSetName).Count(n).Obj(),
			).Obj(), now).
			AdmittedAt(true, now).
			Obj()
	}
	replacementSlice := utiltestingapi.MakeWorkload("wl-2", "ns").
		Annotation(workloadslicing.WorkloadSliceReplacementFor, "ns/wl-1").
		Obj()
	readyPods := func(n int) []*corev1.Pod {
		pods := make([]*corev1.Pod, 0, n)
		for i := range n {
			pods = append(pods, execPod(fmt.Sprintf("e%d", i+1), corev1.PodRunning, ready))
		}
		return pods
	}
	initialThree := &sparkappv1beta2.DynamicAllocation{Enabled: true, MinExecutors: new(int32(1)), InitialExecutors: new(int32(3))}
	testCases := map[string]struct {
		dynamicAllocation *sparkappv1beta2.DynamicAllocation
		sparkConf         map[string]string
		workload          *kueue.Workload
		declaredRV        string
		declared          map[string]int64
		pods              []*corev1.Pod
		executorState     map[string]sparkappv1beta2.ExecutorState
		nilClient         bool
		want              bool
	}{
		"declared ready executors reach the instances": {
			declaredRV: "7",
			declared:   map[string]int64{"readyExecutors": 2},
			want:       true,
		},
		"declared ready executors below the instances, though Pods are ready": {
			declaredRV: "7",
			declared:   map[string]int64{"readyExecutors": 1},
			pods:       []*corev1.Pod{execPod("e1", corev1.PodRunning, ready), execPod("e2", corev1.PodRunning, ready)},
			want:       false,
		},
		"declared ready executors out of range: counted from the Pods": {
			declaredRV: "7",
			declared:   map[string]int64{"readyExecutors": math.MaxInt32 + 1},
			pods:       []*corev1.Pod{execPod("e1", corev1.PodRunning, ready), execPod("e2", corev1.PodRunning, ready)},
			want:       true,
		},
		"declared ready executors negative: counted from the Pods": {
			declaredRV: "7",
			declared:   map[string]int64{"readyExecutors": -1},
			pods:       []*corev1.Pod{execPod("e1", corev1.PodRunning, ready), execPod("e2", corev1.PodRunning, ready)},
			want:       true,
		},
		"declared counts at another resourceVersion: counted from the Pods": {
			declaredRV: "6",
			declared:   map[string]int64{"readyExecutors": 0},
			pods:       []*corev1.Pod{execPod("e1", corev1.PodRunning, ready), execPod("e2", corev1.PodRunning, ready)},
			want:       true,
		},
		"not declared, e.g. an older operator: ready Pods reach the instances": {
			pods: []*corev1.Pod{execPod("e1", corev1.PodRunning, ready), execPod("e2", corev1.PodRunning, ready)},
			want: true,
		},
		"completed, terminating, unready and other submissions' executors don't count": {
			pods: []*corev1.Pod{
				execPod("e1", corev1.PodRunning, ready),
				execPod("e2", corev1.PodSucceeded, ready),
				execPod("e3", corev1.PodRunning, ready, deleting),
				execPod("e4", corev1.PodRunning),
				execPod("e5", corev1.PodRunning, ready, otherSubmission),
			},
			want: false,
		},
		"completed entries of status.executorState don't count": {
			executorState: map[string]sparkappv1beta2.ExecutorState{
				"e1": sparkappv1beta2.ExecutorStateCompleted,
				"e2": sparkappv1beta2.ExecutorStateCompleted,
			},
			want: false,
		},
		"dynamic allocation, not ready yet: fewer than the initial executors": {
			dynamicAllocation: initialThree,
			pods:              readyPods(2),
			want:              false,
		},
		"dynamic allocation, not ready yet: the initial executors": {
			dynamicAllocation: initialThree,
			pods:              readyPods(3),
			want:              true,
		},
		"dynamic allocation, not ready yet: the instances are the initial executors": {
			dynamicAllocation: &sparkappv1beta2.DynamicAllocation{Enabled: true, MinExecutors: new(int32(1))},
			pods:              readyPods(1),
			want:              false,
		},
		"dynamic allocation, not ready yet: granted fewer than the initial executors": {
			dynamicAllocation: initialThree,
			workload:          grantedExecutors(2),
			pods:              readyPods(2),
			want:              true,
		},
		"dynamic allocation in sparkConf, not ready yet: its initialExecutors": {
			sparkConf: map[string]string{
				"spark.dynamicAllocation.enabled":          "true",
				"spark.dynamicAllocation.initialExecutors": "3",
			},
			pods: readyPods(2),
			want: false,
		},
		"dynamic allocation in sparkConf, started: its minExecutors": {
			sparkConf: map[string]string{
				"spark.dynamicAllocation.enabled":          "true",
				"spark.dynamicAllocation.minExecutors":     "1",
				"spark.dynamicAllocation.initialExecutors": "3",
			},
			workload: podsReady(metav1.ConditionTrue, kueue.WorkloadStarted),
			pods:     readyPods(1),
			want:     true,
		},
		"dynamic allocation, started: minExecutors is the floor": {
			dynamicAllocation: initialThree,
			workload:          podsReady(metav1.ConditionTrue, kueue.WorkloadStarted),
			pods:              readyPods(1),
			want:              true,
		},
		"dynamic allocation, started: below minExecutors": {
			dynamicAllocation: initialThree,
			workload:          podsReady(metav1.ConditionTrue, kueue.WorkloadStarted),
			want:              false,
		},
		"dynamic allocation, waiting for recovery: minExecutors is the floor": {
			dynamicAllocation: initialThree,
			workload:          podsReady(metav1.ConditionFalse, kueue.WorkloadWaitForRecovery),
			pods:              readyPods(1),
			want:              true,
		},
		"dynamic allocation, a replacement slice: minExecutors is the floor": {
			dynamicAllocation: initialThree,
			workload:          replacementSlice,
			pods:              readyPods(1),
			want:              true,
		},
		"dynamic allocation without minExecutors, started: the driver running is enough": {
			dynamicAllocation: &sparkappv1beta2.DynamicAllocation{Enabled: true},
			workload:          podsReady(metav1.ConditionTrue, kueue.WorkloadStarted),
			want:              true,
		},
		"without a client: RUNNING entries of status.executorState": {
			nilClient: true,
			executorState: map[string]sparkappv1beta2.ExecutorState{
				"e1": sparkappv1beta2.ExecutorStateRunning,
				"e2": sparkappv1beta2.ExecutorStateRunning,
			},
			want: true,
		},
	}
	for name, tc := range testCases {
		t.Run(name, func(t *testing.T) {
			ctx, _ := utiltesting.ContextWithLog(t)
			app := runningSparkApp()
			app.Spec.DynamicAllocation = tc.dynamicAllocation
			app.Spec.SparkConf = tc.sparkConf
			app.Status.ExecutorState = tc.executorState
			if tc.declared != nil {
				useDeclared(t, app, tc.declaredRV, tc.declared)
			} else {
				noDeclared(t)
			}
			var c client.Client
			if !tc.nilClient {
				c = podClient(tc.pods...)
			}
			if got := (*SparkApplication)(app).PodsReady(ctx, c, jobframework.WithWorkload(tc.workload)); got != tc.want {
				t.Errorf("PodsReady() = %v, want %v", got, tc.want)
			}
		})
	}
}

func TestNumExecutorsFromDeclaredActiveExecutors(t *testing.T) {
	features.SetFeatureGateDuringTest(t, features.ElasticJobsViaWorkloadSlicesForSparkApplication, true)
	newApp := func() *sparkappv1beta2.SparkApplication {
		app := sparkapplicationtesting.MakeSparkApplication("sparkapp", "ns").
			Annotation(workloadslicing.EnabledAnnotationKey, workloadslicing.EnabledAnnotationValue).
			ExecutorInstances(2).
			DynamicAllocation(&sparkappv1beta2.DynamicAllocation{Enabled: true, MaxExecutors: new(int32(8))}).
			AppState(sparkappv1beta2.ApplicationStateRunning).
			Obj()
		app.ResourceVersion = "7"
		app.Status.SubmissionID = "sub-1"
		return app
	}
	// Four executors were created; one has gone, so the driver is past its ramp-up
	pods := func() []*corev1.Pod {
		ps := []*corev1.Pod{execPod("e2", corev1.PodRunning), execPod("e3", corev1.PodRunning), execPod("e4", corev1.PodPending)}
		for i, p := range ps {
			p.Labels[sparkcommon.LabelSparkExecutorID] = []string{"2", "3", "4"}[i]
		}
		return ps
	}
	testCases := map[string]struct {
		declaredRV string
		declared   map[string]int64
		want       int32
	}{
		"declared active executors at the application's resourceVersion": {
			declaredRV: "7",
			declared:   map[string]int64{"activeExecutors": 5},
			want:       5,
		},
		"declared active executors at another resourceVersion: counted from the Pods": {
			declaredRV: "6",
			declared:   map[string]int64{"activeExecutors": 5},
			want:       3,
		},
		"not declared: counted from the Pods": {
			want: 3,
		},
	}
	for name, tc := range testCases {
		t.Run(name, func(t *testing.T) {
			ctx, _ := utiltesting.ContextWithLog(t)
			app := newApp()
			if tc.declared != nil {
				useDeclared(t, app, tc.declaredRV, tc.declared)
			} else {
				noDeclared(t)
			}
			got, err := (*SparkApplication)(app).numExecutors(ctx, podClient(pods()...))
			if err != nil {
				t.Fatal(err)
			}
			if got != tc.want {
				t.Errorf("numExecutors() = %d, want %d", got, tc.want)
			}
		})
	}
}

func TestExecutorPodCountsAdd(t *testing.T) {
	withID := func(id string) podOption {
		return func(p *corev1.Pod) { p.Labels[sparkcommon.LabelSparkExecutorID] = id }
	}
	testCases := map[string]struct {
		pod  *corev1.Pod
		want executorPodCounts
	}{
		"pending":                            {pod: execPod("e1", corev1.PodPending, withID("1")), want: executorPodCounts{active: 1, maxID: 1}},
		"running, not ready":                 {pod: execPod("e2", corev1.PodRunning, withID("2")), want: executorPodCounts{active: 1, maxID: 2}},
		"running and ready":                  {pod: execPod("e3", corev1.PodRunning, ready, withID("3")), want: executorPodCounts{active: 1, ready: 1, maxID: 3}},
		"running, ready, being deleted":      {pod: execPod("e4", corev1.PodRunning, ready, deleting, withID("4")), want: executorPodCounts{active: 1, maxID: 4}},
		"succeeded with the ready condition": {pod: execPod("e5", corev1.PodSucceeded, ready, withID("5")), want: executorPodCounts{maxID: 5}},
		"failed":                             {pod: execPod("e6", corev1.PodFailed, withID("6")), want: executorPodCounts{maxID: 6}},
		"without an executor ID":             {pod: execPod("e7", corev1.PodRunning), want: executorPodCounts{active: 1}},
		"with an invalid executor ID":        {pod: execPod("e8", corev1.PodRunning, withID("x")), want: executorPodCounts{active: 1}},
	}
	for name, tc := range testCases {
		t.Run(name, func(t *testing.T) {
			var got executorPodCounts
			got.add(tc.pod)
			if got != tc.want {
				t.Errorf("add() = %+v, want %+v", got, tc.want)
			}
		})
	}
}

func TestCreatedExecutorsTakesTheHighestExecutorID(t *testing.T) {
	ctx, _ := utiltesting.ContextWithLog(t)
	app := runningSparkApp()
	pod := execPod("e3", corev1.PodRunning)
	pod.Labels[sparkcommon.LabelSparkExecutorID] = "3"
	job := (*SparkApplication)(app)
	counts, err := job.countExecutorPods(ctx, podClient(pod))
	if err != nil {
		t.Fatal(err)
	}

	// At another resourceVersion too: the highest ID only grows
	useDeclared(t, app, "6", map[string]int64{"maxExecutorId": 1200})
	if got := job.createdExecutors(job.declaredExecutors(ctx), counts); got != 1200 {
		t.Errorf("createdExecutors() = %d, want 1200", got)
	}
	// A lower declared ID doesn't lower it
	useDeclared(t, app, "7", map[string]int64{"maxExecutorId": 1})
	if got := job.createdExecutors(job.declaredExecutors(ctx), counts); got != 3 {
		t.Errorf("createdExecutors() = %d, want 3", got)
	}
	// Nor does a lower recorded one, and a higher recorded one counts
	app.Annotations = map[string]string{maxExecutorIDAnnotation: "9", maxExecutorIDSubmissionAnnotation: app.Status.SubmissionID}
	if got := job.createdExecutors(job.declaredExecutors(ctx), counts); got != 9 {
		t.Errorf("createdExecutors() with a recorded ID = %d, want 9", got)
	}
}

func TestExecutorPodChanged(t *testing.T) {
	base := execPod("e1", corev1.PodRunning)
	readyPod := execPod("e1", corev1.PodRunning, ready)
	deletingPod := execPod("e1", corev1.PodRunning, deleting)
	succeeded := execPod("e1", corev1.PodSucceeded)
	relabelled := execPod("e1", corev1.PodRunning)
	relabelled.Labels["other"] = "x"
	testCases := map[string]struct {
		old, updated *corev1.Pod
		want         bool
	}{
		"phase change":            {old: base, updated: succeeded, want: true},
		"becomes ready":           {old: base, updated: readyPod, want: true},
		"becomes unready":         {old: readyPod, updated: base, want: true},
		"starts being deleted":    {old: base, updated: deletingPod, want: true},
		"unrelated metadata only": {old: base, updated: relabelled, want: false},
	}
	for name, tc := range testCases {
		t.Run(name, func(t *testing.T) {
			if got := executorPodChanged(event.UpdateEvent{ObjectOld: tc.old, ObjectNew: tc.updated}); got != tc.want {
				t.Errorf("executorPodChanged() = %v, want %v", got, tc.want)
			}
		})
	}
}
