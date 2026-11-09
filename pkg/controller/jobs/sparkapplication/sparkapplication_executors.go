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

	sparkv1beta2 "github.com/kubeflow/spark-operator/v2/api/v1beta2"
	sparkcommon "github.com/kubeflow/spark-operator/v2/pkg/common"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/client-go/tools/events"
	"k8s.io/utils/ptr"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/builder"
	ctrlcache "sigs.k8s.io/controller-runtime/pkg/cache"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/event"
	"sigs.k8s.io/controller-runtime/pkg/handler"
	"sigs.k8s.io/controller-runtime/pkg/predicate"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"

	"sigs.k8s.io/kueue/pkg/controller/jobframework"
	utilpod "sigs.k8s.io/kueue/pkg/util/pod"
)

// The Spark Operator may declare the executors of a SparkApplication in its status:
// status.activeExecutors (pending or running executor Pods), status.readyExecutors (running,
// ready and not being deleted) and status.maxExecutorId. Kueue's vendored operator API may not
// have these fields, and decoding into it drops unknown fields, so they are read from an untyped
// copy of the SparkApplication, which works with any operator version. An operator that doesn't
// declare them leaves them unset, and the executors are then counted from their Pods.

// executorReader reads the untyped SparkApplications and the executor Pods from the manager's
// informer cache. It is set when the reconciler is created; without it (e.g. in the webhook-only
// tests), the executors are counted from status.executorState as before.
var executorReader client.Reader

var newGenericReconciler = jobframework.NewGenericReconcilerFactory(NewJob, watchExecutorPods)

// NewReconciler creates the SparkApplication reconciler, and keeps the manager's informer cache,
// which the job framework passes as the field indexer, to read the executors from.
var NewReconciler jobframework.ReconcilerFactory = func(ctx context.Context, c client.Client, indexer client.FieldIndexer, record events.EventRecorder, opts ...jobframework.Option) (jobframework.JobReconcilerInterface, error) {
	if informerCache, ok := indexer.(ctrlcache.Cache); ok {
		executorReader = informerCache
	}
	return newGenericReconciler(ctx, c, indexer, record, opts...)
}

func untypedSparkApplication() *unstructured.Unstructured {
	u := &unstructured.Unstructured{}
	u.SetGroupVersionKind(gvk)
	return u
}

// The status fields the operator may declare the executors in.
const (
	activeExecutorsField = "activeExecutors"
	readyExecutorsField  = "readyExecutors"
	maxExecutorIDField   = "maxExecutorId"
)

// declaredStatusFields are the status fields kept in the untyped copy.
var declaredStatusFields = []string{activeExecutorsField, readyExecutorsField, maxExecutorIDField}

// ConfigureCache trims the untyped SparkApplications in the manager's cache to their identity and
// the declared executor fields, so the untyped copy costs a few fields per application. The
// transform applies to the GVK, so the typed SparkApplications only lose their managed fields, as
// with the cache's default transform.
func ConfigureCache(o *ctrl.Options) {
	if o.Cache.ByObject == nil {
		o.Cache.ByObject = make(map[client.Object]ctrlcache.ByObject)
	}
	o.Cache.ByObject[untypedSparkApplication()] = ctrlcache.ByObject{Transform: trimUntyped}
}

func trimUntyped(in any) (any, error) {
	u, ok := in.(*unstructured.Unstructured)
	if !ok {
		return ctrlcache.TransformStripManagedFields()(in)
	}
	trimmed := untypedSparkApplication()
	trimmed.SetNamespace(u.GetNamespace())
	trimmed.SetName(u.GetName())
	trimmed.SetUID(u.GetUID())
	trimmed.SetResourceVersion(u.GetResourceVersion())
	copyDeclaredStatus(trimmed, u)
	return trimmed, nil
}

// copyDeclaredStatus copies the declared executor fields that src has to dst.
func copyDeclaredStatus(dst, src *unstructured.Unstructured) {
	for _, field := range declaredStatusFields {
		if v, found, _ := unstructured.NestedFieldNoCopy(src.Object, "status", field); found {
			_ = unstructured.SetNestedField(dst.Object, v, "status", field)
		}
	}
}

// declaredExecutors are the executor counts the operator declares; a nil field is not declared.
type declaredExecutors struct {
	active, ready *int32
	maxID         *int64
	// current is whether the untyped copy is at the same resourceVersion as the application, so
	// that its counts describe the same state. The two copies come from separate informers.
	current bool
}

// currentActive returns the declared active executors, if the copy is current.
func (d declaredExecutors) currentActive() *int32 {
	if !d.current {
		return nil
	}
	return d.active
}

// currentReady returns the declared ready executors, if the copy is current.
func (d declaredExecutors) currentReady() *int32 {
	if !d.current {
		return nil
	}
	return d.ready
}

// highestID returns the declared highest executor ID, or 0. It only grows, so it's used whatever
// the state of the copy.
func (d declaredExecutors) highestID() int64 {
	return ptr.Deref(d.maxID, 0)
}

func (j *SparkApplication) declaredExecutors(ctx context.Context) declaredExecutors {
	if executorReader == nil {
		return declaredExecutors{}
	}
	u := untypedSparkApplication()
	if err := executorReader.Get(ctx, client.ObjectKey{Namespace: j.Namespace, Name: j.Name}, u); err != nil {
		return declaredExecutors{}
	}
	return declaredExecutors{
		active:  toInt32(declaredStatusInt(u, activeExecutorsField)),
		ready:   toInt32(declaredStatusInt(u, readyExecutorsField)),
		maxID:   declaredStatusInt(u, maxExecutorIDField),
		current: u.GetResourceVersion() == j.ResourceVersion,
	}
}

// declaredStatusInt returns the integer status field of u, or nil if it is not set.
func declaredStatusInt(u *unstructured.Unstructured, field string) *int64 {
	v, found, err := unstructured.NestedInt64(u.Object, "status", field)
	if !found || err != nil {
		return nil
	}
	return &v
}

func toInt32(v *int64) *int32 {
	if v == nil {
		return nil
	}
	return ptr.To(int32(*v))
}

// executorCounts are the executors of the current submission.
type executorCounts struct {
	active, ready int32
	// exist is whether the submission has had an executor, in any phase.
	exist bool
}

func (j *SparkApplication) countExecutorPods(ctx context.Context) (executorCounts, error) {
	labels := client.MatchingLabels{
		sparkcommon.LabelSparkAppName: j.Name,
		sparkcommon.LabelSparkRole:    sparkcommon.SparkRoleExecutor,
	}
	if j.Status.SubmissionID != "" {
		labels[sparkcommon.LabelSubmissionID] = j.Status.SubmissionID
	}
	var pods corev1.PodList
	if err := executorReader.List(ctx, &pods, client.InNamespace(j.Namespace), labels, client.UnsafeDisableDeepCopy); err != nil {
		return executorCounts{}, err
	}
	var c executorCounts
	for i := range pods.Items {
		c.add(&pods.Items[i])
	}
	return c, nil
}

func (c *executorCounts) add(pod *corev1.Pod) {
	c.exist = true
	if isActiveExecutor(pod) {
		c.active++
	}
	if isReadyExecutor(pod) {
		c.ready++
	}
}

// isActiveExecutor reports whether the driver keeps the executor: its pod is pending or running.
func isActiveExecutor(pod *corev1.Pod) bool {
	return pod.Status.Phase == corev1.PodPending || pod.Status.Phase == corev1.PodRunning
}

// isReadyExecutor reports whether the executor serves the application: its pod is running and
// ready, and not being deleted.
func isReadyExecutor(pod *corev1.Pod) bool {
	return pod.Status.Phase == corev1.PodRunning && pod.DeletionTimestamp == nil && utilpod.IsReady(pod)
}

// executors returns the active and ready executors of the current submission. Each count is the
// operator's declared one when the untyped copy describes the application's current state, and
// otherwise comes from the executor Pods in the cache. Without the cache, the counts come from
// status.executorState, as before.
func (j *SparkApplication) executors(ctx context.Context) executorCounts {
	d := j.declaredExecutors(ctx)
	active, ready := d.currentActive(), d.currentReady()
	if active != nil && ready != nil {
		return executorCounts{active: *active, ready: *ready, exist: true}
	}
	if executorReader != nil {
		if c, err := j.countExecutorPods(ctx); err == nil {
			if active != nil {
				c.active, c.exist = *active, true
			}
			c.ready = ptr.Deref(ready, c.ready)
			return c
		}
	}
	return j.executorStateCounts()
}

// executorStateCounts counts the executors in status.executorState.
func (j *SparkApplication) executorStateCounts() executorCounts {
	var c executorCounts
	for _, st := range j.Status.ExecutorState {
		switch st {
		case sparkv1beta2.ExecutorStatePending:
			c.active++
		case sparkv1beta2.ExecutorStateRunning:
			c.active++
			c.ready++
		case sparkv1beta2.ExecutorStateCompleted:
			c.ready++
		}
	}
	c.exist = len(j.Status.ExecutorState) > 0
	return c
}

// watchExecutorPods reconciles a SparkApplication when one of its executor Pods is created,
// deleted, changes phase, becomes ready or unready, or starts being deleted, so that counting the
// executors from their Pods follows them.
func watchExecutorPods(b *builder.Builder, _ client.Client) *builder.Builder {
	return b.Watches(&corev1.Pod{}, handler.EnqueueRequestsFromMapFunc(executorPodToSparkApplication),
		builder.WithPredicates(predicate.Funcs{UpdateFunc: executorPodChanged}))
}

func executorPodToSparkApplication(_ context.Context, obj client.Object) []reconcile.Request {
	labels := obj.GetLabels()
	name := labels[sparkcommon.LabelSparkAppName]
	if labels[sparkcommon.LabelSparkRole] != sparkcommon.SparkRoleExecutor || name == "" {
		return nil
	}
	return []reconcile.Request{{NamespacedName: client.ObjectKey{Namespace: obj.GetNamespace(), Name: name}}}
}

func executorPodChanged(e event.UpdateEvent) bool {
	oldPod, isOldPod := e.ObjectOld.(*corev1.Pod)
	newPod, isNewPod := e.ObjectNew.(*corev1.Pod)
	if !isOldPod || !isNewPod {
		return true
	}
	return oldPod.Status.Phase != newPod.Status.Phase ||
		utilpod.IsReady(oldPod) != utilpod.IsReady(newPod) ||
		(oldPod.DeletionTimestamp == nil) != (newPod.DeletionTimestamp == nil)
}
