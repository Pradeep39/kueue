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
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/builder"
	ctrlcache "sigs.k8s.io/controller-runtime/pkg/cache"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/event"
	"sigs.k8s.io/controller-runtime/pkg/handler"
	"sigs.k8s.io/controller-runtime/pkg/predicate"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"

	"sigs.k8s.io/kueue/pkg/controller/jobframework"
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

// declaredStatusFields are the status fields kept in the untyped copy.
var declaredStatusFields = []string{"activeExecutors", "readyExecutors", "maxExecutorId"}

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
	for _, field := range declaredStatusFields {
		if v, found, _ := unstructured.NestedFieldNoCopy(u.Object, "status", field); found {
			_ = unstructured.SetNestedField(trimmed.Object, v, "status", field)
		}
	}
	return trimmed, nil
}

// declaredExecutors are the executor counts the operator declares; a nil field is not declared.
type declaredExecutors struct {
	active, ready *int32
	maxID         *int64
	// current is whether the untyped copy is at the same resourceVersion as the application, so
	// that its counts describe the same state. The two copies come from separate informers.
	current bool
}

func (j *SparkApplication) declaredExecutors(ctx context.Context) declaredExecutors {
	if executorReader == nil {
		return declaredExecutors{}
	}
	u := untypedSparkApplication()
	if err := executorReader.Get(ctx, client.ObjectKey{Namespace: j.Namespace, Name: j.Name}, u); err != nil {
		return declaredExecutors{}
	}
	d := declaredExecutors{current: u.GetResourceVersion() == j.ResourceVersion}
	if v, found, _ := unstructured.NestedInt64(u.Object, "status", "activeExecutors"); found {
		d.active = new(int32(v))
	}
	if v, found, _ := unstructured.NestedInt64(u.Object, "status", "readyExecutors"); found {
		d.ready = new(int32(v))
	}
	if v, found, _ := unstructured.NestedInt64(u.Object, "status", "maxExecutorId"); found {
		d.maxID = new(v)
	}
	return d
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
		pod := &pods.Items[i]
		c.exist = true
		switch pod.Status.Phase {
		case corev1.PodPending:
			c.active++
		case corev1.PodRunning:
			c.active++
			if pod.DeletionTimestamp == nil && podReady(pod) {
				c.ready++
			}
		}
	}
	return c, nil
}

func podReady(pod *corev1.Pod) bool {
	for _, condition := range pod.Status.Conditions {
		if condition.Type == corev1.PodReady {
			return condition.Status == corev1.ConditionTrue
		}
	}
	return false
}

// executors returns the active and ready executors of the current submission. Each count is the
// operator's declared one when the untyped copy describes the application's current state, and
// otherwise comes from the executor Pods in the cache. Without the cache, the counts come from
// status.executorState, as before.
func (j *SparkApplication) executors(ctx context.Context) executorCounts {
	d := j.declaredExecutors(ctx)
	if d.current && d.active != nil && d.ready != nil {
		return executorCounts{active: *d.active, ready: *d.ready, exist: true}
	}
	if executorReader != nil {
		if c, err := j.countExecutorPods(ctx); err == nil {
			if d.current && d.active != nil {
				c.active, c.exist = *d.active, true
			}
			if d.current && d.ready != nil {
				c.ready = *d.ready
			}
			return c
		}
	}
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
		podReady(oldPod) != podReady(newPod) ||
		(oldPod.DeletionTimestamp == nil) != (newPod.DeletionTimestamp == nil)
}
