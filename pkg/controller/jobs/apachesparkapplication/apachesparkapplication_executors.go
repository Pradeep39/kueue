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
	"slices"
	"strconv"

	corev1 "k8s.io/api/core/v1"
	"sigs.k8s.io/controller-runtime/pkg/builder"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/event"
	"sigs.k8s.io/controller-runtime/pkg/handler"
	"sigs.k8s.io/controller-runtime/pkg/predicate"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"

	sparkv1 "sigs.k8s.io/kueue/pkg/controller/jobs/apachesparkapplication/api/v1"
)

// The operator may declare the executors of an application in its status: status.activeExecutors
// (pending or running executor Pods), status.readyExecutors (running, ready and not being deleted)
// and status.maxExecutorId. Each is used when the operator declares it, and otherwise the
// executors are counted from their Pods in the informer cache. Without a client, the executors
// are counted from status.executorState, as before.
//
// The executor Pods are listed by application name, not by attempt: the operator deletes an
// attempt's executors before it starts the next one, and until the next driver starts, the
// executor count is the initial one.

const (
	// executorIDLabel is the label Spark sets on an executor Pod to its executor ID.
	executorIDLabel = "spark-exec-id"

	// maxExecutorIDAnnotation records the highest executor ID of the current attempt counted
	// from its Pods, for an operator that doesn't declare status.maxExecutorId. It never
	// decreases within the attempt, so it survives the deletion of executor Pods.
	maxExecutorIDAnnotation = "kueue.x-k8s.io/spark-max-executor-id"
	// maxExecutorIDAttemptAnnotation records the attempt that maxExecutorIDAnnotation belongs to.
	maxExecutorIDAttemptAnnotation = "kueue.x-k8s.io/spark-max-executor-id-attempt"
)

// driverRunningStates are the states in which the application's driver is running.
var driverRunningStates = []sparkv1.ApplicationStateSummary{
	sparkv1.DriverReady,
	sparkv1.InitializedBelowThresholdExecutors,
	sparkv1.RunningHealthy,
	sparkv1.RunningWithPartialCapacity,
	sparkv1.RunningWithBelowThresholdExecutors,
}

// executorPodCounts are the application's executor Pods.
type executorPodCounts struct {
	// active are pending, including those held by a scheduling gate, or running, including
	// those being deleted; ready are running and ready, and not being deleted.
	active, ready int32
	// exist is whether there is an executor Pod, in any phase.
	exist bool
	// maxID is the highest executor ID among them.
	maxID int64
}

func (j *SparkApplication) countExecutorPods(ctx context.Context, c client.Client) (executorPodCounts, error) {
	pods, err := j.listExecutorPods(ctx, c)
	if err != nil {
		return executorPodCounts{}, err
	}
	var counts executorPodCounts
	for i := range pods {
		pod := &pods[i]
		counts.exist = true
		switch pod.Status.Phase {
		case corev1.PodPending:
			counts.active++
		case corev1.PodRunning:
			counts.active++
			if pod.DeletionTimestamp == nil && podReady(pod) {
				counts.ready++
			}
		}
		counts.maxID = max(counts.maxID, executorID(pod))
	}
	return counts, nil
}

func podReady(pod *corev1.Pod) bool {
	for _, condition := range pod.Status.Conditions {
		if condition.Type == corev1.PodReady {
			return condition.Status == corev1.ConditionTrue
		}
	}
	return false
}

// executorID returns the executor ID of pod from its spark-exec-id label, or the trailing digits
// of its name, or 0.
func executorID(pod *corev1.Pod) int64 {
	if id, err := strconv.ParseInt(pod.Labels[executorIDLabel], 10, 64); err == nil {
		return id
	}
	name := pod.Name
	start := len(name)
	for start > 0 && name[start-1] >= '0' && name[start-1] <= '9' {
		start--
	}
	id, _ := strconv.ParseInt(name[start:], 10, 64)
	return id
}

// activeExecutors returns the executors the driver keeps, pending or running, and whether the
// current attempt has had any: status.activeExecutors when the operator declares it, otherwise
// the executor Pods.
func (j *SparkApplication) activeExecutors(ctx context.Context, c client.Client) (int32, bool, error) {
	if j.Status.ActiveExecutors != nil {
		return *j.Status.ActiveExecutors, true, nil
	}
	if c == nil {
		var count int32
		for _, st := range j.Status.ExecutorState {
			if st == sparkv1.ExecutorStatePending || st == sparkv1.ExecutorStateRunning {
				count++
			}
		}
		return count, len(j.Status.ExecutorState) > 0, nil
	}
	counts, err := j.countExecutorPods(ctx, c)
	if err != nil {
		return 0, false, err
	}
	return counts.active, counts.exist, nil
}

// readyExecutors returns the ready executors: status.readyExecutors when the operator declares
// it, otherwise the executor Pods that are running and ready, and not being deleted.
func (j *SparkApplication) readyExecutors(ctx context.Context, c client.Client) (int32, error) {
	if j.Status.ReadyExecutors != nil {
		return *j.Status.ReadyExecutors, nil
	}
	if c == nil {
		var count int32
		for _, st := range j.Status.ExecutorState {
			if st == sparkv1.ExecutorStateRunning {
				count++
			}
		}
		return count, nil
	}
	counts, err := j.countExecutorPods(ctx, c)
	if err != nil {
		return 0, err
	}
	return counts.ready, nil
}

// minReadyExecutors returns the executors the application needs ready: with Dynamic Allocation,
// its minExecutors, Spark's default of 0 when unset; otherwise its executor instances.
func (j *SparkApplication) minReadyExecutors() (int32, error) {
	if !j.dynamicAllocationEnabled() {
		return j.staticExecutorCount()
	}
	n, _, err := j.dynamicAllocationCount("minExecutors")
	return n, err
}

// podsReady reports whether the driver runs and enough executors are ready. granted, when known,
// caps the expected count at the executors Kueue grants.
func (j *SparkApplication) podsReady(ctx context.Context, c client.Client, granted *int32) (bool, error) {
	if !slices.Contains(driverRunningStates, j.Status.CurrentState.CurrentStateSummary) {
		return false, nil
	}
	expected, err := j.minReadyExecutors()
	if err != nil {
		return false, err
	}
	if granted != nil {
		expected = min(expected, *granted)
	}
	if expected == 0 {
		return true, nil
	}
	ready, err := j.readyExecutors(ctx, c)
	if err != nil {
		return false, err
	}
	return ready >= expected, nil
}

// attemptID returns the ID of the current attempt, or -1 before the first one.
func (j *SparkApplication) attemptID() int64 {
	if a := j.Status.CurrentAttemptSummary; a != nil && a.AttemptInfo != nil {
		return a.AttemptInfo.ID
	}
	return -1
}

// recordedMaxExecutorID returns the highest executor ID recorded in maxExecutorIDAnnotation for
// the current attempt, or 0 if the record belongs to another one.
func (j *SparkApplication) recordedMaxExecutorID() int64 {
	if j.Annotations[maxExecutorIDAttemptAnnotation] != strconv.FormatInt(j.attemptID(), 10) {
		return 0
	}
	id, _ := strconv.ParseInt(j.Annotations[maxExecutorIDAnnotation], 10, 64)
	return id
}

// GetCustomAnnotations records the highest executor ID of the current attempt, counted from its
// executor Pods, in maxExecutorIDAnnotation, for an elastic application whose operator doesn't
// declare status.maxExecutorId. The recorded ID never decreases within the attempt.
func (j *SparkApplication) GetCustomAnnotations(ctx context.Context, c client.Client) (map[string]string, error) {
	if !isAnElasticJob(j.SparkApplication) || j.Status.MaxExecutorID != nil || j.attemptID() < 0 || c == nil {
		return nil, nil
	}
	counts, err := j.countExecutorPods(ctx, c)
	if err != nil {
		return nil, err
	}
	maxID := max(j.recordedMaxExecutorID(), counts.maxID)
	if maxID == 0 {
		return nil, nil
	}
	return map[string]string{
		maxExecutorIDAnnotation:        strconv.FormatInt(maxID, 10),
		maxExecutorIDAttemptAnnotation: strconv.FormatInt(j.attemptID(), 10),
	}, nil
}

// watchExecutorPods reconciles an application when one of its executor Pods is created, deleted,
// changes phase, becomes ready or unready, or starts being deleted, so that counting the
// executors from their Pods follows them.
func watchExecutorPods(b *builder.Builder) *builder.Builder {
	return b.Watches(&corev1.Pod{}, handler.EnqueueRequestsFromMapFunc(executorPodToSparkApplication),
		builder.WithPredicates(predicate.Funcs{UpdateFunc: executorPodChanged}))
}

func executorPodToSparkApplication(_ context.Context, obj client.Object) []reconcile.Request {
	labels := obj.GetLabels()
	name := labels[appNameLabel]
	if labels[roleLabel] != executorRoleValue || name == "" {
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
