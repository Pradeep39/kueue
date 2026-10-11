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
		// Tracked so the job is reconciled when an evicted application's last Pod goes.
		"driver pod": {
			obj: &corev1.Pod{
				ObjectMeta: metav1.ObjectMeta{
					Labels: map[string]string{
						sparkcommon.LabelSparkAppName: "app",
						sparkcommon.LabelSparkRole:    sparkcommon.SparkRoleDriver,
					},
				},
			},
			want: true,
		},
		"driver pod missing the app-name label": {
			obj: &corev1.Pod{
				ObjectMeta: metav1.ObjectMeta{
					Labels: map[string]string{
						sparkcommon.LabelSparkRole: sparkcommon.SparkRoleDriver,
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

func trackedDriverPod(name string) *corev1.Pod {
	pod := trackedExecutorPod(name)
	pod.Labels[sparkcommon.LabelSparkRole] = sparkcommon.SparkRoleDriver
	return pod
}

func drain(t *testing.T, q workqueue.TypedRateLimitingInterface[reconcile.Request]) {
	t.Helper()
	for q.Len() > 0 {
		item, _ := q.Get()
		q.Done(item)
	}
}

// TestExecutorPodHandlerTeardownFlushesImmediately pins the teardown fast path. Once the
// driver goes away, the reconcile that releases an evicted Workload's quota must not wait
// out the debounce: debouncing it held quota ~8s past the Pods' exit under preemption.
func TestExecutorPodHandlerTeardownFlushesImmediately(t *testing.T) {
	ctx := context.Background()
	for name, goAway := range map[string]func(*executorPodHandler, workqueue.TypedRateLimitingInterface[reconcile.Request]){
		"driver deleted": func(h *executorPodHandler, q workqueue.TypedRateLimitingInterface[reconcile.Request]) {
			h.Delete(ctx, event.DeleteEvent{Object: trackedDriverPod("d")}, q)
		},
		"driver terminating": func(h *executorPodHandler, q workqueue.TypedRateLimitingInterface[reconcile.Request]) {
			d := trackedDriverPod("d")
			d.DeletionTimestamp = &metav1.Time{Time: time.Now()}
			h.Update(ctx, event.UpdateEvent{ObjectOld: trackedDriverPod("d"), ObjectNew: d}, q)
		},
		"driver terminated": func(h *executorPodHandler, q workqueue.TypedRateLimitingInterface[reconcile.Request]) {
			d := trackedDriverPod("d")
			d.Status.Phase = corev1.PodSucceeded
			h.Update(ctx, event.UpdateEvent{ObjectOld: trackedDriverPod("d"), ObjectNew: d}, q)
		},
	} {
		t.Run(name, func(t *testing.T) {
			fakeClock := testingclock.NewFakeClock(time.Now())
			h := newExecutorPodHandler()
			h.clock = fakeClock
			q := workqueue.NewTypedRateLimitingQueue(workqueue.DefaultTypedControllerRateLimiter[reconcile.Request]())
			defer q.ShutDown()

			// A pending debounced burst is superseded, not left to fire later.
			h.Update(ctx, event.UpdateEvent{ObjectOld: trackedExecutorPod("e1"), ObjectNew: trackedExecutorPod("e1")}, q)
			goAway(h, q)
			if got := q.Len(); got != 1 {
				t.Fatalf("queue length right after the driver went away = %d, want 1", got)
			}
			drain(t, q)

			// Every later event for the application also goes straight to the queue.
			h.Delete(ctx, event.DeleteEvent{Object: trackedExecutorPod("e2")}, q)
			if got := q.Len(); got != 1 {
				t.Fatalf("queue length right after an executor deletion during teardown = %d, want 1", got)
			}
			drain(t, q)

			// The superseded timer must not fire a stray reconcile later.
			fakeClock.Step(executorPodDebounce)
			if got := q.Len(); got != 0 {
				t.Errorf("queue length after the old debounce window = %d, want 0", got)
			}
		})
	}
}

func TestExecutorPodHandlerTeardownEnds(t *testing.T) {
	ctx := context.Background()
	for name, tc := range map[string]struct {
		after func(*executorPodHandler, *testingclock.FakeClock, workqueue.TypedRateLimitingInterface[reconcile.Request])
	}{
		// A resubmission brings a new driver, and scaling under it is debounced again.
		"a new driver": {after: func(h *executorPodHandler, _ *testingclock.FakeClock, q workqueue.TypedRateLimitingInterface[reconcile.Request]) {
			h.Create(ctx, event.CreateEvent{Object: trackedDriverPod("d2")}, q)
		}},
		// An application deleted outright must not stay in teardown forever.
		"the teardown TTL": {after: func(_ *executorPodHandler, c *testingclock.FakeClock, _ workqueue.TypedRateLimitingInterface[reconcile.Request]) {
			c.Step(teardownTTL + time.Second)
		}},
	} {
		t.Run(name, func(t *testing.T) {
			fakeClock := testingclock.NewFakeClock(time.Now())
			h := newExecutorPodHandler()
			h.clock = fakeClock
			q := workqueue.NewTypedRateLimitingQueue(workqueue.DefaultTypedControllerRateLimiter[reconcile.Request]())
			defer q.ShutDown()

			h.Delete(ctx, event.DeleteEvent{Object: trackedDriverPod("d")}, q)
			tc.after(h, fakeClock, q)
			drain(t, q)

			h.Delete(ctx, event.DeleteEvent{Object: trackedExecutorPod("e")}, q)
			if got := q.Len(); got != 0 {
				t.Fatalf("an executor deletion after teardown ended was enqueued immediately (queue length %d); want it debounced", got)
			}
			fakeClock.Step(executorPodDebounce)
			if got := q.Len(); got != 1 {
				t.Errorf("queue length after the debounce window = %d, want 1", got)
			}
		})
	}
}

// TestExecutorPodHandlerScaleDownStaysDebounced: with the driver alive, Dynamic Allocation
// deleting executors is ordinary scaling and keeps the debounce.
func TestExecutorPodHandlerScaleDownStaysDebounced(t *testing.T) {
	ctx := context.Background()
	fakeClock := testingclock.NewFakeClock(time.Now())
	h := newExecutorPodHandler()
	h.clock = fakeClock
	q := workqueue.NewTypedRateLimitingQueue(workqueue.DefaultTypedControllerRateLimiter[reconcile.Request]())
	defer q.ShutDown()

	h.Update(ctx, event.UpdateEvent{ObjectOld: trackedDriverPod("d"), ObjectNew: trackedDriverPod("d")}, q)
	for _, e := range []string{"e1", "e2", "e3"} {
		h.Delete(ctx, event.DeleteEvent{Object: trackedExecutorPod(e)}, q)
	}
	if got := q.Len(); got != 0 {
		t.Fatalf("scale-down deletions under a live driver enqueued immediately (queue length %d)", got)
	}
	fakeClock.Step(executorPodDebounce)
	if got := q.Len(); got != 1 {
		t.Errorf("queue length after the debounce window = %d, want 1", got)
	}
}
