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
	"cmp"
	"context"
	"fmt"
	"slices"
	"strconv"
	"time"

	sparkv1beta2 "github.com/kubeflow/spark-operator/v2/api/v1beta2"
	sparkcommon "github.com/kubeflow/spark-operator/v2/pkg/common"
	corev1 "k8s.io/api/core/v1"
	policyv1 "k8s.io/api/policy/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/klog/v2"
	"k8s.io/utils/ptr"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"

	kueue "sigs.k8s.io/kueue/apis/kueue/v1beta2"
	"sigs.k8s.io/kueue/pkg/constants"
	"sigs.k8s.io/kueue/pkg/controller/jobframework"
	"sigs.k8s.io/kueue/pkg/features"
	utilpod "sigs.k8s.io/kueue/pkg/util/pod"
)

// sparkConfPreemptionMinExecutors is the sparkConf key an application sets to choose the
// executor count partial preemption must never reclaim below (docs/design/
// spark-aware-preemption.md §5.2). Kueue reads it but never writes it.
const sparkConfPreemptionMinExecutors = "spark.kueue.preemption.minExecutors"

// sparkConfExecutorTerminationGracePeriod is Spark's executor grace period property. From
// Spark 4.1 it is written onto every executor Pod (BasicExecutorFeatureStep), overriding the
// Pod template's terminationGracePeriodSeconds, and defaults to 30s. Before 4.1 it does not
// exist and the template's value applies. When an application sets only one of the two, the
// webhook copies it to the other, so every Spark version uses it.
const sparkConfExecutorTerminationGracePeriod = "spark.kubernetes.executor.terminationGracePeriodSeconds"

// decommissionSparkConfDefaults are the Spark properties the webhook defaults to "true" for a
// reclaimable application, so deleting an executor Pod triggers graceful decommissioning and
// block migration rather than a plain loss (§5.4).
var decommissionSparkConfDefaults = []string{
	"spark.decommission.enabled",
	"spark.storage.decommission.enabled",
	"spark.storage.decommission.shuffleBlocks.enabled",
	"spark.storage.decommission.rddBlocks.enabled",
}

var _ jobframework.JobWithPartialPreemption = (*SparkApplication)(nil)

// partialPreemptionEnabled reports whether j may declare a reclaimable executor PodSet: the
// gate is on and the application is on the elastic path. Static elastic applications are
// included, since the elastic path is keyed off the annotation, not Dynamic Allocation (§5.1).
func partialPreemptionEnabled(j *SparkApplication) bool {
	return features.Enabled(features.SparkPartialPreemption) && isAnElasticJob(j)
}

// dynamicAllocationEnabled reports whether the application uses Dynamic Allocation, read from
// spec.dynamicAllocation the same way numExecutors reads it.
func (j *SparkApplication) dynamicAllocationEnabled() bool {
	return ptr.Deref(j.Spec.DynamicAllocation, sparkv1beta2.DynamicAllocation{}).Enabled
}

// sparkConfPreemptionMinExecutorsValue reads spark.kueue.preemption.minExecutors. A
// non-integer or negative value is reported as an error.
func (j *SparkApplication) sparkConfPreemptionMinExecutorsValue() (int32, bool, error) {
	raw, ok := j.Spec.SparkConf[sparkConfPreemptionMinExecutors]
	if !ok {
		return 0, false, nil
	}
	n, err := strconv.ParseInt(raw, 10, 32)
	if err != nil {
		return 0, false, fmt.Errorf("%s: %w", sparkConfPreemptionMinExecutors, err)
	}
	if n < 0 {
		return 0, false, fmt.Errorf("%s: must not be negative, got %d", sparkConfPreemptionMinExecutors, n)
	}
	return int32(n), true, nil
}

// resolvedPartialPreemptionFloor resolves the executor floor before capping (§5.2):
// spark.kueue.preemption.minExecutors if set and valid; else, with Dynamic Allocation,
// spec.dynamicAllocation.minExecutors raised to at least 1; else the static executor count.
// A malformed property is treated as unset; the webhook rejects it at create time.
func (j *SparkApplication) resolvedPartialPreemptionFloor() int32 {
	if n, ok, err := j.sparkConfPreemptionMinExecutorsValue(); err == nil && ok {
		return n
	}
	if j.dynamicAllocationEnabled() {
		return max(ptr.Deref(j.Spec.DynamicAllocation.MinExecutors, 0), 1)
	}
	return j.numInitialExecutors()
}

// PartialPreemptionFloor implements jobframework.JobWithPartialPreemption. The floor is capped
// at the executor PodSet's count, which follows status.executorState (numExecutors).
func (j *SparkApplication) PartialPreemptionFloor() (kueue.PodSetReference, int32, bool) {
	if !partialPreemptionEnabled(j) {
		return "", 0, false
	}
	return executorPodSetName, min(j.resolvedPartialPreemptionFloor(), j.numExecutors()), true
}

// PartialPreemptionTerminationGracePeriod implements jobframework.JobWithPartialPreemption: the
// executor grace period the application sets, from the executor Pod template or, failing that,
// spark.kubernetes.executor.terminationGracePeriodSeconds. It bounds Spark's decommission
// hand-off (§5.4): an executor still migrating blocks when it ends is killed, and its
// unmigrated work recomputed.
func (j *SparkApplication) PartialPreemptionTerminationGracePeriod() (time.Duration, bool) {
	if t := j.Spec.Executor.Template; t != nil && t.Spec.TerminationGracePeriodSeconds != nil {
		return time.Duration(*t.Spec.TerminationGracePeriodSeconds) * time.Second, true
	}
	if v, ok := j.Spec.SparkConf[sparkConfExecutorTerminationGracePeriod]; ok {
		if seconds, ok := sparkTimeSeconds(v); ok {
			return time.Duration(seconds) * time.Second, true
		}
	}
	return 0, false
}

// PartialPreemptionMarkedPods implements jobframework.JobWithPartialPreemption: how many of
// the application's ungated, non-terminal, not-being-deleted executor Pods carry a negative
// pod-deletion-cost, i.e. executors Spark itself has chosen to remove.
func (j *SparkApplication) PartialPreemptionMarkedPods(ctx context.Context, c client.Client) (int32, bool, error) {
	if !partialPreemptionEnabled(j) {
		return 0, false, nil
	}
	pods, err := j.listExecutorPods(ctx, c)
	if err != nil {
		return 0, false, err
	}
	var marked int32
	for i := range pods {
		p := &pods[i]
		if holdsExecutorGrant(p) && p.DeletionTimestamp == nil && podDeletionCost(p) < 0 {
			marked++
		}
	}
	return marked, true, nil
}

// listExecutorPods lists the application's executor Pods, uncached.
func (j *SparkApplication) listExecutorPods(ctx context.Context, c client.Client) ([]corev1.Pod, error) {
	podList := &corev1.PodList{}
	if err := c.List(ctx, podList,
		client.InNamespace(j.Namespace),
		client.MatchingLabels{
			sparkcommon.LabelSparkAppName: j.Name,
			sparkcommon.LabelSparkRole:    sparkcommon.SparkRoleExecutor,
		},
	); err != nil {
		return nil, err
	}
	return podList.Items, nil
}

// holdsExecutorGrant reports whether pod holds (or may at any moment take) executor quota:
// it is past the elastic scheduling gate and not terminal. A Pod being deleted still holds
// its node resources until it is terminal or gone, so it counts.
func holdsExecutorGrant(pod *corev1.Pod) bool {
	return !utilpod.HasGate(pod, kueue.ElasticJobSchedulingGate) &&
		pod.Status.Phase != corev1.PodSucceeded && pod.Status.Phase != corev1.PodFailed
}

// podDeletionCost returns the Pod's controller.kubernetes.io/pod-deletion-cost, with a
// missing or unparsable value read as 0, the Kubernetes default.
func podDeletionCost(pod *corev1.Pod) int64 {
	n, err := strconv.ParseInt(pod.Annotations[constants.PodDeletionCostAnnotation], 10, 32)
	if err != nil {
		return 0
	}
	return n
}

// compareReclaimOrder orders Pods the way partial preemption deletes them (§5.4): ascending
// pod-deletion-cost, then unbound before bound, then newest first, then by name.
func compareReclaimOrder(a, b *corev1.Pod) int {
	if c := cmp.Compare(podDeletionCost(a), podDeletionCost(b)); c != 0 {
		return c
	}
	aBound, bBound := a.Spec.NodeName != "", b.Spec.NodeName != ""
	if aBound != bBound {
		if !aBound {
			return -1
		}
		return 1
	}
	if c := b.CreationTimestamp.Compare(a.CreationTimestamp.Time); c != 0 {
		return c
	}
	return cmp.Compare(a.Name, b.Name)
}

// +kubebuilder:rbac:groups="",resources=pods,verbs=get;list;watch;delete
// +kubebuilder:rbac:groups="",resources=pods/eviction,verbs=create

// ReclaimPods implements jobframework.JobWithPartialPreemption.
//
// It is level-based: it counts the executor Pods still holding a grant, including those
// already being deleted, and evicts only enough more to bring that count to keep. Calling
// it again while deletions are in flight evicts nothing more. The driver is never listed,
// gated Pods hold no grant and are never touched, and keep is never below the floor. Pods are
// removed through evictExecutorPod.
func (j *SparkApplication) ReclaimPods(ctx context.Context, c client.Client, podSet kueue.PodSetReference, keep int32, gracePeriod *time.Duration) (bool, error) {
	if podSet != executorPodSetName {
		return false, fmt.Errorf("partial preemption of PodSet %q is not supported, only %q", podSet, executorPodSetName)
	}
	if !partialPreemptionEnabled(j) {
		return false, fmt.Errorf("partial preemption is not enabled for SparkApplication %s/%s", j.Namespace, j.Name)
	}
	keep = max(keep, j.resolvedPartialPreemptionFloor())

	pods, err := j.listExecutorPods(ctx, c)
	if err != nil {
		return false, err
	}
	var holding, deleting int32
	candidates := make([]*corev1.Pod, 0, len(pods))
	for i := range pods {
		p := &pods[i]
		if !holdsExecutorGrant(p) {
			continue
		}
		holding++
		if p.DeletionTimestamp != nil {
			deleting++
			continue
		}
		candidates = append(candidates, p)
	}
	if holding <= keep {
		return true, nil
	}
	toDelete := int(holding - keep - deleting)
	if toDelete <= 0 {
		return false, nil
	}
	slices.SortFunc(candidates, compareReclaimOrder)
	var graceSeconds *int64
	if gracePeriod != nil {
		// Whole seconds, rounded up so a sub-second remainder is never cut.
		graceSeconds = ptr.To(int64((*gracePeriod + time.Second - 1) / time.Second))
	}
	log := ctrl.LoggerFrom(ctx)
	for _, p := range candidates[:min(toDelete, len(candidates))] {
		log.V(3).Info("Evicting executor Pod for partial preemption", "pod", klog.KObj(p), "keep", keep, "holding", holding)
		if err := evictExecutorPod(ctx, c, p, graceSeconds); err != nil {
			return false, err
		}
	}
	return false, nil
}

// evictExecutorPod removes p through the Eviction API rather than a plain delete. Eviction is the
// conventional way for one controller to remove Pods another controller owns, and it honours
// PodDisruptionBudgets. A refusal (HTTP 429, e.g. from a budget) is not an error: the Pod stays,
// and the next ReclaimPods call tries again, while the reclaim timeout still bounds the wait.
// When eviction is forbidden, as with a ClusterRole that predates pods/eviction, it deletes the
// Pod instead, so a reclaim never stalls on RBAC.
func evictExecutorPod(ctx context.Context, c client.Client, p *corev1.Pod, graceSeconds *int64) error {
	eviction := &policyv1.Eviction{ObjectMeta: metav1.ObjectMeta{Name: p.Name, Namespace: p.Namespace}}
	if graceSeconds != nil {
		eviction.DeleteOptions = &metav1.DeleteOptions{GracePeriodSeconds: graceSeconds}
	}
	err := c.SubResource("eviction").Create(ctx, p, eviction)
	log := ctrl.LoggerFrom(ctx)
	switch {
	case err == nil || apierrors.IsNotFound(err):
		return nil
	case apierrors.IsTooManyRequests(err):
		log.V(3).Info("Eviction of executor Pod refused; retrying on the next reconcile", "pod", klog.KObj(p), "reason", err.Error())
		return nil
	case apierrors.IsForbidden(err):
		log.V(2).Info("Eviction of executor Pod forbidden; deleting it instead", "pod", klog.KObj(p), "reason", err.Error())
		var opts []client.DeleteOption
		if graceSeconds != nil {
			opts = append(opts, client.GracePeriodSeconds(*graceSeconds))
		}
		return client.IgnoreNotFound(c.Delete(ctx, p, opts...))
	default:
		return err
	}
}
