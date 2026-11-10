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
	"math"
	"strconv"

	sparkcommon "github.com/kubeflow/spark-operator/v2/pkg/common"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/client-go/tools/events"
	"k8s.io/utils/ptr"
	ctrl "sigs.k8s.io/controller-runtime"
	ctrlcache "sigs.k8s.io/controller-runtime/pkg/cache"
	"sigs.k8s.io/controller-runtime/pkg/client"

	"sigs.k8s.io/kueue/pkg/controller/jobframework"
	utilpod "sigs.k8s.io/kueue/pkg/util/pod"
)

// The Spark Operator may declare the executors of a SparkApplication in status.executors
// (kubeflow/spark-operator#3290): the submission they belong to, its executor Pods that are
// active (pending or running, and not terminating), terminating, and ready, and its highest
// executor ID. Kueue's vendored operator API may not have the field, and decoding into it drops
// unknown fields, so it is read from an untyped copy of the SparkApplication, which works with
// any operator version. An operator that doesn't declare it leaves it unset, and the executors
// are then counted from their Pods.

// declaredReader reads the untyped SparkApplications from the manager's informer cache. It is set
// when the reconciler is created; without it, the executors are counted from their Pods.
var declaredReader client.Reader

// NewReconciler creates the SparkApplication reconciler, and keeps the manager's informer cache,
// which the job framework passes as the field indexer, to read the declared executors from.
var NewReconciler jobframework.ReconcilerFactory = func(ctx context.Context, c client.Client, indexer client.FieldIndexer, record events.EventRecorder, opts ...jobframework.Option) (jobframework.JobReconcilerInterface, error) {
	if informerCache, ok := indexer.(ctrlcache.Cache); ok {
		declaredReader = informerCache
	}
	return newGenericReconciler(ctx, c, indexer, record, opts...)
}

func untypedSparkApplication() *unstructured.Unstructured {
	u := &unstructured.Unstructured{}
	u.SetGroupVersionKind(gvk)
	return u
}

// The status field the operator declares the executors in, the only one kept in the untyped
// copy, and its fields.
const (
	executorsField     = "executors"
	submissionIDField  = "submissionID"
	activeField        = "active"
	terminatingField   = "terminating"
	readyField         = "ready"
	maxExecutorIDField = "maxExecutorID"
)

// ConfigureCache trims the untyped SparkApplications in the manager's cache to their identity and
// status.executors, so the untyped copy costs a few fields per application. The
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

// copyDeclaredStatus copies status.executors from src to dst, if src has it.
func copyDeclaredStatus(dst, src *unstructured.Unstructured) {
	if v, found, _ := unstructured.NestedFieldNoCopy(src.Object, "status", executorsField); found {
		_ = unstructured.SetNestedField(dst.Object, v, "status", executorsField)
	}
}

// declaredExecutors are the executor counts the operator declares for the application's current
// submission; a nil field is not declared.
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

// highestID returns the declared highest executor ID, or 0. It only grows within a submission, so
// it's used whatever the state of the copy.
func (d declaredExecutors) highestID() int {
	return int(ptr.Deref(d.maxID, 0))
}

func (j *SparkApplication) declaredExecutors(ctx context.Context) declaredExecutors {
	if declaredReader == nil {
		return declaredExecutors{}
	}
	u := untypedSparkApplication()
	if err := declaredReader.Get(ctx, client.ObjectKey{Namespace: j.Namespace, Name: j.Name}, u); err != nil {
		return declaredExecutors{}
	}
	executors, found, err := unstructured.NestedMap(u.Object, "status", executorsField)
	if !found || err != nil {
		return declaredExecutors{}
	}
	// Counts of another submission are stale.
	if submissionID, _, _ := unstructured.NestedString(executors, submissionIDField); submissionID == "" || submissionID != j.Status.SubmissionID {
		return declaredExecutors{}
	}
	return declaredExecutors{
		// Terminating executors hold resources until they stop, like the active ones.
		active:  toInt32(sumDeclared(declaredInt(executors, activeField), declaredInt(executors, terminatingField))),
		ready:   toInt32(declaredInt(executors, readyField)),
		maxID:   declaredInt(executors, maxExecutorIDField),
		current: u.GetResourceVersion() == j.ResourceVersion,
	}
}

// declaredInt returns the integer field of the declared executors, or nil if it is not set.
func declaredInt(executors map[string]any, field string) *int64 {
	v, found, err := unstructured.NestedInt64(executors, field)
	if !found || err != nil {
		return nil
	}
	return &v
}

// sumDeclared returns a + b, or nil if either is not declared or negative.
func sumDeclared(a, b *int64) *int64 {
	if a == nil || b == nil || *a < 0 || *b < 0 {
		return nil
	}
	return ptr.To(*a + *b)
}

// toInt32 returns v as an int32, or nil if it is not set or out of range, so that a
// malformed count is treated as not declared.
func toInt32(v *int64) *int32 {
	if v == nil || *v < 0 || *v > math.MaxInt32 {
		return nil
	}
	return ptr.To(int32(*v))
}

// executorPodCounts are the executor pods of the current submission.
type executorPodCounts struct {
	// active are pending or running, ready are running and ready and not being deleted.
	active, ready int32
	// maxID is the highest executor ID among them.
	maxID int
}

func (c *executorPodCounts) add(pod *corev1.Pod) {
	if isActiveExecutor(pod) {
		c.active++
	}
	if isReadyExecutor(pod) {
		c.ready++
	}
	c.maxID = max(c.maxID, executorID(pod))
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

// executorID returns the executor ID of the pod from its label, or 0.
func executorID(pod *corev1.Pod) int {
	id, _ := strconv.Atoi(pod.Labels[sparkcommon.LabelSparkExecutorID])
	return id
}
