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
	"cmp"
	"context"
	"fmt"
	"slices"
	"strconv"
	"strings"
	"time"

	corev1 "k8s.io/api/core/v1"
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
// spark-aware-preemption.md §5.2). Kueue reads it but never writes it. It is the same key the
// Kubeflow SparkApplication integration reads, so an application moves between operators as is.
const sparkConfPreemptionMinExecutors = "spark.kueue.preemption.minExecutors"

// sparkConfExecutorTerminationGracePeriod is Spark's executor grace period property. From Spark
// 4.1 it is written onto every executor Pod, overriding the Pod template's
// terminationGracePeriodSeconds, and defaults to 30s. Before 4.1 it does not exist and the
// template's value applies. When an application sets only one of the two, the webhook copies it
// to the other, so every Spark version uses it.
const sparkConfExecutorTerminationGracePeriod = "spark.kubernetes.executor.terminationGracePeriodSeconds"

// decommissionSparkConfDefaults are the Spark properties the webhook defaults to "true" for a
// reclaimable application, so deleting an executor Pod triggers graceful decommissioning and
// block migration rather than a plain loss (§5.4). They exist since Spark 3.1.
var decommissionSparkConfDefaults = []string{
	"spark.decommission.enabled",
	"spark.storage.decommission.enabled",
	"spark.storage.decommission.shuffleBlocks.enabled",
	"spark.storage.decommission.rddBlocks.enabled",
}

var _ jobframework.JobWithPartialPreemption = (*SparkApplication)(nil)

// partialPreemptionEnabled reports whether j may declare a reclaimable executor PodSet: the gate
// is on and the application is on the elastic path. Static elastic applications are included,
// since the elastic path is keyed off the annotation, not Dynamic Allocation
// (docs/design/spark-aware-preemption.md §5.1).
func partialPreemptionEnabled(j *SparkApplication) bool {
	return features.Enabled(features.SparkPartialPreemption) && isAnElasticJob(j.SparkApplication)
}

// preemptionMinExecutors reads spark.kueue.preemption.minExecutors. A non-integer or negative
// value is reported as an error.
func (j *SparkApplication) preemptionMinExecutors() (int32, bool, error) {
	raw, ok := j.conf(sparkConfPreemptionMinExecutors)
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

// resolvedPartialPreemptionFloor resolves the executor floor before capping (§5.2), as the
// Kubeflow integration does: spark.kueue.preemption.minExecutors if set and valid; else, with
// Dynamic Allocation, minExecutors (sparkConf first, then instanceConfig) raised to at least 1;
// else the static executor count. A malformed property is treated as unset; the webhook rejects
// it at create time.
//
// The floor is then raised to the operator's own threshold, instanceConfig.minExecutors. Once an
// application has started, the operator moves it to RunningWithBelowThresholdExecutors when it
// has fewer ready executors than that, and PodsReady reports false outside RunningHealthy and
// RunningWithPartialCapacity. A reclaim below the threshold would therefore make the Workload
// look not ready, and waitForPodsReady's recovery timeout would then evict it whole.
func (j *SparkApplication) resolvedPartialPreemptionFloor() (int32, error) {
	floor, err := j.unboundedPartialPreemptionFloor()
	if err != nil {
		return 0, err
	}
	if ic := instanceConfig(j.SparkApplication); ic != nil && ic.MinExecutors > floor {
		floor = ic.MinExecutors
	}
	return floor, nil
}

func (j *SparkApplication) unboundedPartialPreemptionFloor() (int32, error) {
	if n, ok, err := j.preemptionMinExecutors(); err == nil && ok {
		return n, nil
	}
	if j.dynamicAllocationEnabled() {
		n, _, _ := j.dynamicAllocationCount("minExecutors")
		return max(n, 1), nil
	}
	return j.staticExecutorCount()
}

// currentExecutorCount is the executor PodSet's count as PodSets() last derived it, or, when
// PodSets() has not run on this wrapper, the client-less estimate it would fall back to.
func (j *SparkApplication) currentExecutorCount() (int32, error) {
	if j.cachedLiveExecutorCount != nil {
		return *j.cachedLiveExecutorCount, nil
	}
	return j.computeLiveExecutorCount(context.Background(), nil)
}

// reclaimable reports whether partial preemption could ever take an executor from the
// application: its resolved floor is below the most executors it can run. For Dynamic
// Allocation that is maxExecutors (unbounded when unset, as in Spark); for a static application
// it is its executor count.
func (j *SparkApplication) reclaimable() bool {
	floor, err := j.resolvedPartialPreemptionFloor()
	if err != nil {
		return false
	}
	if j.dynamicAllocationEnabled() {
		maxExecutors, ok, err := j.dynamicAllocationCount("maxExecutors")
		return err == nil && (!ok || floor < maxExecutors)
	}
	count, err := j.staticExecutorCount()
	return err == nil && floor < count
}

// PartialPreemptionFloor implements jobframework.JobWithPartialPreemption.
func (j *SparkApplication) PartialPreemptionFloor() (kueue.PodSetReference, int32, bool) {
	if !partialPreemptionEnabled(j) {
		return "", 0, false
	}
	floor, err := j.resolvedPartialPreemptionFloor()
	if err != nil {
		return "", 0, false
	}
	count, err := j.currentExecutorCount()
	if err != nil {
		return "", 0, false
	}
	return executorPodSetName, min(floor, count), true
}

// PartialPreemptionTerminationGracePeriod implements jobframework.JobWithPartialPreemption: the
// executor grace period the application sets, from the executor Pod template or, failing that,
// spark.kubernetes.executor.terminationGracePeriodSeconds. It bounds Spark's decommission
// hand-off (§5.4).
func (j *SparkApplication) PartialPreemptionTerminationGracePeriod() (time.Duration, bool) {
	if t := templateSpec(j.SparkApplication, roleExecutor); t != nil && t.Spec.TerminationGracePeriodSeconds != nil {
		return time.Duration(*t.Spec.TerminationGracePeriodSeconds) * time.Second, true
	}
	if v, ok := j.conf(sparkConfExecutorTerminationGracePeriod); ok {
		if seconds, ok := sparkTimeSeconds(v); ok {
			return time.Duration(seconds) * time.Second, true
		}
	}
	return 0, false
}

// PartialPreemptionMarkedPods implements jobframework.JobWithPartialPreemption. The count is
// observed by PodSets().
func (j *SparkApplication) PartialPreemptionMarkedPods() (int32, bool) {
	if j.cachedMarkedExecutorPods == nil {
		return 0, false
	}
	return *j.cachedMarkedExecutorPods, true
}

// observeMarkedExecutorPods records, for PartialPreemptionMarkedPods, how many of the
// application's executor Pods Spark itself has chosen to remove (a negative pod-deletion-cost).
func (j *SparkApplication) observeMarkedExecutorPods(ctx context.Context, c client.Client) error {
	if j.cachedMarkedExecutorPods != nil {
		return nil
	}
	pods, err := j.executorPods(ctx, c)
	if err != nil {
		return err
	}
	var marked int32
	for i := range pods {
		p := &pods[i]
		if holdsExecutorGrant(p) && p.DeletionTimestamp == nil && podDeletionCost(p) < 0 {
			marked++
		}
	}
	j.cachedMarkedExecutorPods = ptr.To(marked)
	return nil
}

// listExecutorPods lists the application's executor Pods, uncached. The operator injects both
// labels into the executor Pod template, so every driver-created executor carries them.
func (j *SparkApplication) listExecutorPods(ctx context.Context, c client.Client) ([]corev1.Pod, error) {
	podList := &corev1.PodList{}
	if err := c.List(ctx, podList,
		client.InNamespace(j.Namespace),
		client.MatchingLabels{
			appNameLabel: j.Name,
			roleLabel:    executorRoleValue,
		},
	); err != nil {
		return nil, err
	}
	return podList.Items, nil
}

// executorPods lists the application's executor Pods once per reconcile, so the live count and
// the marked-Pod count are taken from the same observation.
func (j *SparkApplication) executorPods(ctx context.Context, c client.Client) ([]corev1.Pod, error) {
	if j.cachedExecutorPods != nil {
		return *j.cachedExecutorPods, nil
	}
	pods, err := j.listExecutorPods(ctx, c)
	if err != nil {
		return nil, err
	}
	j.cachedExecutorPods = &pods
	return pods, nil
}

// holdsExecutorGrant reports whether pod holds (or may at any moment take) executor quota: it is
// past the elastic scheduling gate and not terminal. A Pod being deleted still holds its node
// resources until it is terminal or gone, so it counts.
func holdsExecutorGrant(pod *corev1.Pod) bool {
	return !utilpod.HasGate(pod, kueue.ElasticJobSchedulingGate) && isVerifiedLiveExecutor(pod)
}

// podDeletionCost returns the Pod's controller.kubernetes.io/pod-deletion-cost, with a missing or
// unparsable value read as 0, the Kubernetes default.
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

// sparkTimeSeconds parses a Spark time property whose default unit is seconds, the way Spark's
// timeConf(TimeUnit.SECONDS) does: a bare number is seconds, and the suffixes ms, s, m/min, h and
// d are accepted. Sub-second values are truncated.
func sparkTimeSeconds(v string) (int64, bool) {
	v = strings.ToLower(strings.TrimSpace(v))
	units := []struct {
		suffix string
		ms     int64
	}{{"ms", 1}, {"min", 60_000}, {"s", 1_000}, {"m", 60_000}, {"h", 3_600_000}, {"d", 86_400_000}}
	mult := int64(1_000)
	for _, u := range units {
		if strings.HasSuffix(v, u.suffix) {
			v, mult = strings.TrimSuffix(v, u.suffix), u.ms
			break
		}
	}
	n, err := strconv.ParseInt(v, 10, 64)
	if err != nil || n < 0 {
		return 0, false
	}
	return n * mult / 1_000, true
}

// +kubebuilder:rbac:groups="",resources=pods,verbs=get;list;watch;delete

// ReclaimPods implements jobframework.JobWithPartialPreemption.
//
// It is level-based: it counts the executor Pods still holding a grant, including those already
// being deleted, and deletes only enough more to bring that count to keep. Calling it again
// while deletions are in flight deletes nothing more. The driver is never listed, gated Pods hold
// no grant and are never touched, and keep is never below the floor. Pods are deleted with
// gracePeriod, rounded up to whole seconds, when it is not nil.
func (j *SparkApplication) ReclaimPods(ctx context.Context, c client.Client, podSet kueue.PodSetReference, keep int32, gracePeriod *time.Duration) (bool, error) {
	if podSet != executorPodSetName {
		return false, fmt.Errorf("partial preemption of PodSet %q is not supported, only %q", podSet, executorPodSetName)
	}
	if !partialPreemptionEnabled(j) {
		return false, fmt.Errorf("partial preemption is not enabled for SparkApplication %s/%s", j.Namespace, j.Name)
	}
	floor, err := j.resolvedPartialPreemptionFloor()
	if err != nil {
		return false, err
	}
	keep = max(keep, floor)

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
	var opts []client.DeleteOption
	if gracePeriod != nil {
		opts = append(opts, client.GracePeriodSeconds(int64((*gracePeriod+time.Second-1)/time.Second)))
	}
	log := ctrl.LoggerFrom(ctx)
	for _, p := range candidates[:min(toDelete, len(candidates))] {
		log.V(3).Info("Deleting executor Pod for partial preemption", "pod", klog.KObj(p), "keep", keep, "holding", holding)
		if err := c.Delete(ctx, p, opts...); client.IgnoreNotFound(err) != nil {
			return false, err
		}
	}
	return false, nil
}
