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
	"sync"
	"time"

	sparkcommon "github.com/kubeflow/spark-operator/v2/pkg/common"
	sparkutil "github.com/kubeflow/spark-operator/v2/pkg/util"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/util/workqueue"
	"k8s.io/utils/clock"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/event"
	"sigs.k8s.io/controller-runtime/pkg/predicate"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"
)

// executorPodDebounce is how long executor Pod events for a given SparkApplication must
// go quiet before a reconcile is enqueued for it. Dynamic Allocation creates/deletes
// executor Pods in rapid bursts (observed: multiple live-count changes within ~300ms in
// production), and PodSets() re-derives the live executor count from the API server on
// every reconcile — so reconciling on every individual Pod event would just repeat that
// list call and re-run workload-slice bookkeeping once per event in the burst instead of
// once for the whole burst.
const executorPodDebounce = 5 * time.Second

// executorPodPredicate filters Pod events down to Spark executor Pods that carry the
// Spark Operator's admission-webhook-assigned owning-application label. Driver Pods and
// any non-Spark Pods are ignored before they ever reach the handler.
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
	return sparkutil.IsExecutorPod(pod) && pod.Labels[sparkcommon.LabelSparkAppName] != ""
}

// executorPodHandler enqueues a reconcile for the owning SparkApplication once executor
// Pod events for it have gone quiet for executorPodDebounce.
//
// Executor Pods aren't linked to the SparkApplication via OwnerReference (they're owned by
// the driver Pod, which Spark itself creates), so the standard Owns()-based watch can't be
// used here. Instead this relies on the sparkoperator.k8s.io/app-name label, which the
// Spark Operator's admission webhook stamps onto every driver/executor Pod, including ones
// Dynamic Allocation creates well after the SparkApplication was admitted — the same label
// PodLabelSelector() and liveExecutorCount() already depend on.
//
// Unlike the reverted ExecutorInstancesReconciler, this handler never writes to the
// SparkApplication: it only decides when to ask the job reconciler to re-derive PodSets(),
// which reads the live executor count straight from the API server. The debounce exists
// purely to coalesce a Dynamic Allocation burst into a single reconcile instead of one per
// Pod event; per-key timers (reset on every new event for that key, guarded by mu) achieve
// that without touching the workqueue's delay heap, where staggered AddAfter calls from a
// burst would each fire independently and defeat the coalescing.
type executorPodHandler struct {
	clock    clock.WithTickerAndDelayedExecution
	debounce time.Duration

	mu     sync.Mutex
	timers map[types.NamespacedName]clock.Timer
}

func newExecutorPodHandler() *executorPodHandler {
	return &executorPodHandler{
		clock:    clock.RealClock{},
		debounce: executorPodDebounce,
		timers:   make(map[types.NamespacedName]clock.Timer),
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

// schedule (re)starts the debounce timer for obj's owning SparkApplication. Repeated
// calls for the same key within the debounce window keep pushing the enqueue out; the
// reconcile.Request only reaches the queue once no further event arrives for that key
// for a full debounce window.
func (h *executorPodHandler) schedule(obj client.Object, q workqueue.TypedRateLimitingInterface[reconcile.Request]) {
	pod, ok := obj.(*corev1.Pod)
	if !ok {
		return
	}
	appName := pod.Labels[sparkcommon.LabelSparkAppName]
	if appName == "" {
		return
	}
	key := types.NamespacedName{Namespace: pod.Namespace, Name: appName}
	req := reconcile.Request{NamespacedName: key}

	h.mu.Lock()
	defer h.mu.Unlock()

	if t, ok := h.timers[key]; ok {
		t.Reset(h.debounce)
		return
	}
	h.timers[key] = h.clock.AfterFunc(h.debounce, func() {
		q.Add(req)
		h.mu.Lock()
		delete(h.timers, key)
		h.mu.Unlock()
	})
}
