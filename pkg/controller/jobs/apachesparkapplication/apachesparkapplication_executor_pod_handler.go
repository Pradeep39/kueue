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

package apachesparkapplication

import (
	"context"
	"sync"
	"time"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/util/workqueue"
	"k8s.io/utils/clock"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/event"
	"sigs.k8s.io/controller-runtime/pkg/predicate"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"
)

// executorPodDebounce is how long executor Pod events for a given SparkApplication must go
// quiet before a reconcile is enqueued for it. Dynamic Allocation creates and deletes
// executor Pods in bursts, and PodSets() re-derives the live executor count from the API
// server on every reconcile, so reconciling per Pod event would repeat that List and re-run
// workload-slice bookkeeping once per event in the burst rather than once per burst.
const executorPodDebounce = 5 * time.Second

// executorPodMaxWait caps how long a continuous stream of executor Pod events for one
// SparkApplication can keep pushing the reconcile out. A trailing-edge debounce with no
// ceiling can be reset forever: a long-running Dynamic Allocation app sees routine Pod
// status churn that can land more often than every executorPodDebounce, so the quiet window
// might never occur, starving PodSets() re-derivation and leaving usage accounting stuck at
// a stale count. Once a burst has run for executorPodMaxWait the next event flushes
// immediately instead of resetting the timer again.
const executorPodMaxWait = 30 * time.Second

// executorPodPredicate filters Pod events down to Spark executor Pods carrying the
// operator's owning-application label. Driver Pods and non-Spark Pods are dropped before
// they reach the handler.
type executorPodPredicate struct{}

var _ predicate.Predicate = executorPodPredicate{}

func (executorPodPredicate) Create(e event.CreateEvent) bool { return isTrackedExecutorPod(e.Object) }
func (executorPodPredicate) Update(e event.UpdateEvent) bool {
	return isTrackedExecutorPod(e.ObjectOld) || isTrackedExecutorPod(e.ObjectNew)
}
func (executorPodPredicate) Delete(e event.DeleteEvent) bool { return isTrackedExecutorPod(e.Object) }
func (executorPodPredicate) Generic(event.GenericEvent) bool { return false }

func isTrackedExecutorPod(obj client.Object) bool {
	pod, ok := obj.(*corev1.Pod)
	if !ok {
		return false
	}
	return pod.Labels[roleLabel] == executorRoleValue && pod.Labels[appNameLabel] != ""
}

// executorPodHandler enqueues a reconcile for the owning SparkApplication once executor Pod
// events for it have gone quiet for executorPodDebounce.
//
// Executor Pods carry no OwnerReference to the SparkApplication - Spark's driver creates
// them - so an Owns()-based watch cannot be used. This relies instead on the
// spark.operator/spark-app-name label, which the operator injects into every driver and
// executor Pod via spark.kubernetes.{driver,executor}.label.*, including Pods that Dynamic
// Allocation creates long after admission. It is the same label PodLabelSelector() and
// liveExecutorCount() depend on.
//
// The handler never writes to the SparkApplication: it only decides when to ask the job
// reconciler to re-derive PodSets(), which reads the live count straight from the API
// server. Per-key timers (reset on each new event for that key, guarded by mu) coalesce a
// burst without touching the workqueue's delay heap, where staggered AddAfter calls would
// each fire independently and defeat the coalescing.
type executorPodHandler struct {
	clock    clock.WithTickerAndDelayedExecution
	debounce time.Duration
	maxWait  time.Duration

	mu          sync.Mutex
	timers      map[types.NamespacedName]clock.Timer
	burstStarts map[types.NamespacedName]time.Time
}

func newExecutorPodHandler() *executorPodHandler {
	return &executorPodHandler{
		clock:       clock.RealClock{},
		debounce:    executorPodDebounce,
		maxWait:     executorPodMaxWait,
		timers:      make(map[types.NamespacedName]clock.Timer),
		burstStarts: make(map[types.NamespacedName]time.Time),
	}
}

func (h *executorPodHandler) Create(_ context.Context, e event.CreateEvent, q workqueue.TypedRateLimitingInterface[reconcile.Request]) {
	h.schedule(e.Object, q)
}

func (h *executorPodHandler) Update(_ context.Context, e event.UpdateEvent, q workqueue.TypedRateLimitingInterface[reconcile.Request]) {
	h.schedule(e.ObjectNew, q)
}

func (h *executorPodHandler) Delete(_ context.Context, e event.DeleteEvent, q workqueue.TypedRateLimitingInterface[reconcile.Request]) {
	h.schedule(e.Object, q)
}

func (h *executorPodHandler) Generic(context.Context, event.GenericEvent, workqueue.TypedRateLimitingInterface[reconcile.Request]) {
}

// schedule (re)starts the debounce timer for obj's owning SparkApplication. Repeated calls
// for the same key within the window keep pushing the enqueue out; the request reaches the
// queue once no further event arrives for a full window - unless the burst has already run
// for executorPodMaxWait, in which case it flushes immediately.
func (h *executorPodHandler) schedule(obj client.Object, q workqueue.TypedRateLimitingInterface[reconcile.Request]) {
	pod, ok := obj.(*corev1.Pod)
	if !ok {
		return
	}
	appName := pod.Labels[appNameLabel]
	if appName == "" {
		return
	}
	key := types.NamespacedName{Namespace: pod.Namespace, Name: appName}
	req := reconcile.Request{NamespacedName: key}

	h.mu.Lock()
	defer h.mu.Unlock()

	now := h.clock.Now()
	if t, ok := h.timers[key]; ok {
		if now.Sub(h.burstStarts[key]) >= h.maxWait {
			t.Stop()
			delete(h.timers, key)
			delete(h.burstStarts, key)
			q.Add(req)
			return
		}
		t.Reset(h.debounce)
		return
	}

	h.burstStarts[key] = now
	h.timers[key] = h.clock.AfterFunc(h.debounce, func() {
		q.Add(req)
		h.mu.Lock()
		delete(h.timers, key)
		delete(h.burstStarts, key)
		h.mu.Unlock()
	})
}
