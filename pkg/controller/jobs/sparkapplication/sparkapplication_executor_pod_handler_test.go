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

	sparkcommon "github.com/kubeflow/spark-operator/v2/pkg/common"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/util/workqueue"
	testingclock "k8s.io/utils/clock/testing"
	"sigs.k8s.io/controller-runtime/pkg/event"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"
)

func trackedExecutorPod(name string) *corev1.Pod {
	return &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:      name,
			Namespace: "ns",
			Labels: map[string]string{
				sparkcommon.LabelSparkAppName: "app",
				sparkcommon.LabelSparkRole:    sparkcommon.SparkRoleExecutor,
			},
		},
	}
}

func TestExecutorPodHandlerDebounce(t *testing.T) {
	fakeClock := testingclock.NewFakeClock(time.Now())
	h := newExecutorPodHandler()
	h.clock = fakeClock

	q := workqueue.NewTypedRateLimitingQueue(workqueue.DefaultTypedControllerRateLimiter[reconcile.Request]())
	defer q.ShutDown()

	// A burst of 3 events for the same SparkApplication, each within the
	// debounce window of the previous one, must coalesce into a single
	// enqueued reconcile once the burst goes quiet.
	h.Create(context.Background(), event.CreateEvent{Object: trackedExecutorPod("e1")}, q)
	fakeClock.Step(2 * time.Second)
	h.Update(context.Background(), event.UpdateEvent{ObjectOld: trackedExecutorPod("e2"), ObjectNew: trackedExecutorPod("e2")}, q)
	fakeClock.Step(2 * time.Second)
	h.Delete(context.Background(), event.DeleteEvent{Object: trackedExecutorPod("e3")}, q)

	if got := q.Len(); got != 0 {
		t.Fatalf("expected no reconcile enqueued mid-burst, queue length = %d", got)
	}

	// Advance past the debounce window measured from the last event.
	fakeClock.Step(executorPodDebounce)

	if got := q.Len(); got != 1 {
		t.Fatalf("expected exactly one reconcile enqueued after the burst went quiet, got %d", got)
	}

	item, _ := q.Get()
	want := reconcile.Request{NamespacedName: types.NamespacedName{Namespace: "ns", Name: "app"}}
	if item != want {
		t.Errorf("enqueued request = %v, want %v", item, want)
	}
}

func TestExecutorPodHandlerFlushesOnContinuousChurn(t *testing.T) {
	fakeClock := testingclock.NewFakeClock(time.Now())
	h := newExecutorPodHandler()
	h.clock = fakeClock

	q := workqueue.NewTypedRateLimitingQueue(workqueue.DefaultTypedControllerRateLimiter[reconcile.Request]())
	defer q.ShutDown()

	// A continuous stream of events, each landing well inside the debounce window of
	// the previous one, must not be able to push the enqueue out forever: once the
	// burst has run for executorPodMaxWait, it must flush even though events keep
	// arriving faster than the debounce window.
	step := executorPodDebounce / 2
	elapsed := time.Duration(0)
	i := 0
	for elapsed < executorPodMaxWait {
		h.Update(context.Background(), event.UpdateEvent{
			ObjectOld: trackedExecutorPod("e"), ObjectNew: trackedExecutorPod("e"),
		}, q)
		if got := q.Len(); got != 0 {
			t.Fatalf("expected no reconcile enqueued while the burst is still within maxWait (elapsed=%s), queue length = %d", elapsed, got)
		}
		fakeClock.Step(step)
		elapsed += step
		i++
	}

	// This next event lands after the burst has been running for at least
	// executorPodMaxWait: it must flush immediately instead of resetting again.
	h.Update(context.Background(), event.UpdateEvent{
		ObjectOld: trackedExecutorPod("e"), ObjectNew: trackedExecutorPod("e"),
	}, q)

	if got := q.Len(); got != 1 {
		t.Fatalf("expected the continuous churn to force a flush after maxWait, queue length = %d", got)
	}
}

func TestExecutorPodHandlerIgnoresUntrackedPods(t *testing.T) {
	fakeClock := testingclock.NewFakeClock(time.Now())
	h := newExecutorPodHandler()
	h.clock = fakeClock

	q := workqueue.NewTypedRateLimitingQueue(workqueue.DefaultTypedControllerRateLimiter[reconcile.Request]())
	defer q.ShutDown()

	untracked := &corev1.Pod{ObjectMeta: metav1.ObjectMeta{Name: "not-spark", Namespace: "ns"}}
	h.Create(context.Background(), event.CreateEvent{Object: untracked}, q)
	fakeClock.Step(executorPodDebounce)

	if got := q.Len(); got != 0 {
		t.Fatalf("expected no reconcile enqueued for an untracked pod, got queue length = %d", got)
	}
}

func TestExecutorPodPredicate(t *testing.T) {
	pred := executorPodPredicate{}

	tests := map[string]struct {
		obj  *corev1.Pod
		want bool
	}{
		"tracked executor pod": {
			obj:  trackedExecutorPod("e1"),
			want: true,
		},
		"driver pod": {
			obj: &corev1.Pod{
				ObjectMeta: metav1.ObjectMeta{
					Labels: map[string]string{
						sparkcommon.LabelSparkAppName: "app",
						sparkcommon.LabelSparkRole:    sparkcommon.SparkRoleDriver,
					},
				},
			},
			want: false,
		},
		"executor pod missing the app-name label": {
			obj: &corev1.Pod{
				ObjectMeta: metav1.ObjectMeta{
					Labels: map[string]string{
						sparkcommon.LabelSparkRole: sparkcommon.SparkRoleExecutor,
					},
				},
			},
			want: false,
		},
	}

	for name, tc := range tests {
		t.Run(name, func(t *testing.T) {
			if got := pred.Create(event.CreateEvent{Object: tc.obj}); got != tc.want {
				t.Errorf("Create() = %v, want %v", got, tc.want)
			}
			if got := pred.Delete(event.DeleteEvent{Object: tc.obj}); got != tc.want {
				t.Errorf("Delete() = %v, want %v", got, tc.want)
			}
		})
	}
}
