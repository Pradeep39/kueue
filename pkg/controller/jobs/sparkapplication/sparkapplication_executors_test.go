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
	"testing"
	"time"

	sparkappv1beta2 "github.com/kubeflow/spark-operator/v2/api/v1beta2"
	sparkcommon "github.com/kubeflow/spark-operator/v2/pkg/common"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/event"

	kueue "sigs.k8s.io/kueue/apis/kueue/v1beta2"
	utiltesting "sigs.k8s.io/kueue/pkg/util/testing"
	utiltestingapi "sigs.k8s.io/kueue/pkg/util/testing/v1beta2"
	sparkapplicationtesting "sigs.k8s.io/kueue/pkg/util/testingjobs/sparkapplication"
)

// stubReader stands in for the informer cache: untyped SparkApplications by key, and executor Pods.
type stubReader struct {
	untyped map[client.ObjectKey]*unstructured.Unstructured
	pods    []corev1.Pod
}

func (r *stubReader) Get(_ context.Context, key client.ObjectKey, obj client.Object, _ ...client.GetOption) error {
	u, ok := obj.(*unstructured.Unstructured)
	stored := r.untyped[key]
	if !ok || stored == nil {
		return apierrors.NewNotFound(schema.GroupResource{Group: gvk.Group, Resource: "sparkapplications"}, key.Name)
	}
	stored.DeepCopyInto(u)
	return nil
}

func (r *stubReader) List(_ context.Context, list client.ObjectList, opts ...client.ListOption) error {
	podList, ok := list.(*corev1.PodList)
	if !ok {
		return nil
	}
	lo := (&client.ListOptions{}).ApplyOptions(opts)
	for _, pod := range r.pods {
		if lo.Namespace != "" && pod.Namespace != lo.Namespace {
			continue
		}
		if lo.LabelSelector != nil && !lo.LabelSelector.Matches(labels.Set(pod.Labels)) {
			continue
		}
		podList.Items = append(podList.Items, pod)
	}
	return nil
}

// useExecutorReader makes r the executor reader for the test.
func useExecutorReader(t *testing.T, r client.Reader) {
	t.Helper()
	previous := executorReader
	executorReader = r
	t.Cleanup(func() { executorReader = previous })
}

// declared returns the untyped copy of app with the given status fields, at resourceVersion rv.
func declared(app *sparkappv1beta2.SparkApplication, rv string, fields map[string]int64) *unstructured.Unstructured {
	u := untypedSparkApplication()
	u.SetNamespace(app.Namespace)
	u.SetName(app.Name)
	u.SetResourceVersion(rv)
	for field, v := range fields {
		_ = unstructured.SetNestedField(u.Object, v, "status", field)
	}
	return u
}

func readerFor(app *sparkappv1beta2.SparkApplication, u *unstructured.Unstructured, pods ...corev1.Pod) *stubReader {
	r := &stubReader{untyped: map[client.ObjectKey]*unstructured.Unstructured{}, pods: pods}
	if u != nil {
		r.untyped[client.ObjectKey{Namespace: app.Namespace, Name: app.Name}] = u
	}
	return r
}

type podOption func(*corev1.Pod)

func ready(p *corev1.Pod) {
	p.Status.Conditions = append(p.Status.Conditions, corev1.PodCondition{Type: corev1.PodReady, Status: corev1.ConditionTrue})
}

func deleting(p *corev1.Pod) {
	p.DeletionTimestamp = &metav1.Time{}
}

func submission(id string) podOption {
	return func(p *corev1.Pod) { p.Labels[sparkcommon.LabelSubmissionID] = id }
}

func execPod(app, name string, phase corev1.PodPhase, opts ...podOption) corev1.Pod {
	p := corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{Namespace: "ns", Name: name, Labels: map[string]string{
			sparkcommon.LabelSparkAppName: app,
			sparkcommon.LabelSparkRole:    sparkcommon.SparkRoleExecutor,
			sparkcommon.LabelSubmissionID: "sub-1",
		}},
		Status: corev1.PodStatus{Phase: phase},
	}
	for _, opt := range opts {
		opt(&p)
	}
	return p
}

func runningApp() *sparkappv1beta2.SparkApplication {
	app := sparkapplicationtesting.MakeSparkApplication("sparkapp", "ns").
		ExecutorInstances(2).
		AppState(sparkappv1beta2.ApplicationStateRunning).
		Obj()
	app.ResourceVersion = "7"
	app.Status.SubmissionID = "sub-1"
	return app
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
	want := map[string]any{"activeExecutors": int64(4), "readyExecutors": int64(3), "maxExecutorId": int64(9)}
	if len(status) != len(want) || status["activeExecutors"] != want["activeExecutors"] ||
		status["readyExecutors"] != want["readyExecutors"] || status["maxExecutorId"] != want["maxExecutorId"] {
		t.Errorf("status = %v, want %v", status, want)
	}

	// A typed SparkApplication only loses its managed fields
	typed := runningApp()
	typed.ManagedFields = []metav1.ManagedFieldsEntry{{Manager: "m"}}
	out, err = trimUntyped(typed)
	if err != nil {
		t.Fatal(err)
	}
	if got := out.(*sparkappv1beta2.SparkApplication); got.ManagedFields != nil || got.Status.AppState.State != sparkappv1beta2.ApplicationStateRunning {
		t.Errorf("typed object changed beyond its managed fields: %+v", got)
	}
}

func TestExecutors(t *testing.T) {
	pods := []corev1.Pod{
		execPod("sparkapp", "exec-1", corev1.PodRunning, ready),
		execPod("sparkapp", "exec-2", corev1.PodRunning),
		execPod("sparkapp", "exec-3", corev1.PodRunning, ready, deleting),
		execPod("sparkapp", "exec-4", corev1.PodPending),
		execPod("sparkapp", "exec-5", corev1.PodSucceeded, ready),
		execPod("sparkapp", "exec-6", corev1.PodRunning, ready, submission("sub-0")),
		execPod("other", "other-exec-1", corev1.PodRunning, ready),
	}
	testCases := map[string]struct {
		untyped    func(app *sparkappv1beta2.SparkApplication) *unstructured.Unstructured
		pods       []corev1.Pod
		noReader   bool
		executors  map[string]sparkappv1beta2.ExecutorState
		wantCounts executorCounts
	}{
		"declared counts at the application's resourceVersion": {
			untyped: func(app *sparkappv1beta2.SparkApplication) *unstructured.Unstructured {
				return declared(app, "7", map[string]int64{"activeExecutors": 9, "readyExecutors": 8})
			},
			pods:       pods,
			wantCounts: executorCounts{active: 9, ready: 8, exist: true},
		},
		"declared counts at another resourceVersion: counted from the Pods": {
			untyped: func(app *sparkappv1beta2.SparkApplication) *unstructured.Unstructured {
				return declared(app, "6", map[string]int64{"activeExecutors": 9, "readyExecutors": 8})
			},
			pods:       pods,
			wantCounts: executorCounts{active: 4, ready: 1, exist: true},
		},
		"no declared counts, e.g. an older operator: counted from the Pods": {
			untyped: func(app *sparkappv1beta2.SparkApplication) *unstructured.Unstructured {
				return declared(app, "7", nil)
			},
			pods:       pods,
			wantCounts: executorCounts{active: 4, ready: 1, exist: true},
		},
		"only activeExecutors declared: readiness counted from the Pods": {
			untyped: func(app *sparkappv1beta2.SparkApplication) *unstructured.Unstructured {
				return declared(app, "7", map[string]int64{"activeExecutors": 9})
			},
			pods:       pods,
			wantCounts: executorCounts{active: 9, ready: 1, exist: true},
		},
		"no executor Pods": {
			untyped: func(app *sparkappv1beta2.SparkApplication) *unstructured.Unstructured {
				return declared(app, "7", nil)
			},
			wantCounts: executorCounts{},
		},
		"no reader: counted from status.executorState": {
			noReader: true,
			executors: map[string]sparkappv1beta2.ExecutorState{
				"exec-1": sparkappv1beta2.ExecutorStateRunning,
				"exec-2": sparkappv1beta2.ExecutorStatePending,
				"exec-3": sparkappv1beta2.ExecutorStateCompleted,
			},
			wantCounts: executorCounts{active: 2, ready: 2, exist: true},
		},
	}
	for name, tc := range testCases {
		t.Run(name, func(t *testing.T) {
			ctx, _ := utiltesting.ContextWithLog(t)
			app := runningApp()
			app.Status.ExecutorState = tc.executors
			if tc.noReader {
				useExecutorReader(t, nil)
			} else {
				var u *unstructured.Unstructured
				if tc.untyped != nil {
					u = tc.untyped(app)
				}
				useExecutorReader(t, readerFor(app, u, tc.pods...))
			}
			if got := (*SparkApplication)(app).executors(ctx); got != tc.wantCounts {
				t.Errorf("executors() = %+v, want %+v", got, tc.wantCounts)
			}
		})
	}
}

func TestPodsReadyFromReadyExecutors(t *testing.T) {
	testCases := map[string]struct {
		dynamicAllocation *sparkappv1beta2.DynamicAllocation
		state             sparkappv1beta2.ApplicationStateType
		declaredReady     *int64
		pods              []corev1.Pod
		granted           *int32
		want              bool
	}{
		"declared ready executors reach the instances": {
			declaredReady: new(int64(2)),
			want:          true,
		},
		"declared ready executors below the instances": {
			declaredReady: new(int64(1)),
			want:          false,
		},
		"ready Pods reach the instances": {
			pods: []corev1.Pod{
				execPod("sparkapp", "exec-1", corev1.PodRunning, ready),
				execPod("sparkapp", "exec-2", corev1.PodRunning, ready),
			},
			want: true,
		},
		"completed, terminating and unready executors don't count": {
			pods: []corev1.Pod{
				execPod("sparkapp", "exec-1", corev1.PodRunning, ready),
				execPod("sparkapp", "exec-2", corev1.PodSucceeded, ready),
				execPod("sparkapp", "exec-3", corev1.PodRunning, ready, deleting),
				execPod("sparkapp", "exec-4", corev1.PodRunning),
			},
			want: false,
		},
		"dynamic allocation without minExecutors: the driver running is enough": {
			dynamicAllocation: &sparkappv1beta2.DynamicAllocation{Enabled: true},
			want:              true,
		},
		"dynamic allocation: minExecutors is the floor": {
			dynamicAllocation: &sparkappv1beta2.DynamicAllocation{Enabled: true, MinExecutors: new(int32(1))},
			pods:              []corev1.Pod{execPod("sparkapp", "exec-1", corev1.PodRunning, ready)},
			want:              true,
		},
		"the grant caps the floor": {
			declaredReady: new(int64(1)),
			granted:       new(int32(1)),
			want:          true,
		},
		"driver not running": {
			state:         sparkappv1beta2.ApplicationStateSubmitted,
			declaredReady: new(int64(2)),
			want:          false,
		},
	}
	for name, tc := range testCases {
		t.Run(name, func(t *testing.T) {
			ctx, _ := utiltesting.ContextWithLog(t)
			app := runningApp()
			app.Spec.DynamicAllocation = tc.dynamicAllocation
			if tc.state != "" {
				app.Status.AppState.State = tc.state
			}
			fields := map[string]int64{}
			if tc.declaredReady != nil {
				fields["activeExecutors"] = *tc.declaredReady
				fields["readyExecutors"] = *tc.declaredReady
			}
			useExecutorReader(t, readerFor(app, declared(app, "7", fields), tc.pods...))
			job := (*SparkApplication)(app)
			var got bool
			if tc.granted != nil {
				wl := utiltestingapi.MakeWorkload("wl", "ns").
					ReserveQuotaAt(utiltestingapi.MakeAdmission("cq").PodSets(
						kueue.PodSetAssignment{Name: driverPodSetName, Count: new(int32(1))},
						kueue.PodSetAssignment{Name: executorPodSetName, Count: tc.granted},
					).Obj(), time.Now()).Obj()
				got = job.PodsReadyForWorkload(ctx, nil, wl)
			} else {
				got = job.PodsReady(ctx, nil)
			}
			if got != tc.want {
				t.Errorf("PodsReady = %v, want %v", got, tc.want)
			}
		})
	}
}

func TestExecutorSequenceFromDeclaredMaxExecutorID(t *testing.T) {
	app := runningApp()
	app.Status.SubmissionAttempts = 1
	app.Status.ExecutorState = map[string]sparkappv1beta2.ExecutorState{"exec-1": sparkappv1beta2.ExecutorStateRunning}
	// At another resourceVersion too: the highest ID only grows
	useExecutorReader(t, readerFor(app, declared(app, "6", map[string]int64{"maxExecutorId": 7})))
	if got, want := (*SparkApplication)(app).GetWorkloadNameExtraPart(), "0_1_7"; got != want {
		t.Errorf("GetWorkloadNameExtraPart() = %q, want %q", got, want)
	}
	// The tracked executors win when the declared ID is lower, so the sequence never goes back
	useExecutorReader(t, readerFor(app, declared(app, "6", map[string]int64{"maxExecutorId": 0})))
	if got, want := (*SparkApplication)(app).GetWorkloadNameExtraPart(), "0_1_1"; got != want {
		t.Errorf("GetWorkloadNameExtraPart() = %q, want %q", got, want)
	}
}

func TestExecutorPodChanged(t *testing.T) {
	base := execPod("sparkapp", "exec-1", corev1.PodRunning)
	readyPod := execPod("sparkapp", "exec-1", corev1.PodRunning, ready)
	deletingPod := execPod("sparkapp", "exec-1", corev1.PodRunning, deleting)
	succeeded := execPod("sparkapp", "exec-1", corev1.PodSucceeded)
	relabelled := execPod("sparkapp", "exec-1", corev1.PodRunning)
	relabelled.Labels["other"] = "x"
	testCases := map[string]struct {
		old, updated corev1.Pod
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
			if got := executorPodChanged(event.UpdateEvent{ObjectOld: &tc.old, ObjectNew: &tc.updated}); got != tc.want {
				t.Errorf("executorPodChanged() = %v, want %v", got, tc.want)
			}
		})
	}
}

func TestExecutorPodToSparkApplication(t *testing.T) {
	pod := execPod("sparkapp", "exec-1", corev1.PodRunning)
	if got := executorPodToSparkApplication(context.Background(), &pod); len(got) != 1 || got[0].Name != "sparkapp" || got[0].Namespace != "ns" {
		t.Errorf("executorPodToSparkApplication() = %v", got)
	}
	driver := execPod("sparkapp", "driver", corev1.PodRunning)
	driver.Labels[sparkcommon.LabelSparkRole] = sparkcommon.SparkRoleDriver
	if got := executorPodToSparkApplication(context.Background(), &driver); len(got) != 0 {
		t.Errorf("a driver Pod mapped to %v", got)
	}
}
