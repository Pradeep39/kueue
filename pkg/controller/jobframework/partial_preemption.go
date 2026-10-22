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

package jobframework

import (
	"context"
	"fmt"
	"strconv"
	"time"

	corev1 "k8s.io/api/core/v1"
	apimeta "k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/klog/v2"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"

	kueue "sigs.k8s.io/kueue/apis/kueue/v1beta2"
	"sigs.k8s.io/kueue/pkg/constants"
	"sigs.k8s.io/kueue/pkg/features"
	"sigs.k8s.io/kueue/pkg/util/api"
	clientutil "sigs.k8s.io/kueue/pkg/util/client"
	"sigs.k8s.io/kueue/pkg/workload"
	workloadevict "sigs.k8s.io/kueue/pkg/workload/evict"
	workloadfinish "sigs.k8s.io/kueue/pkg/workload/finish"
	workloadpatching "sigs.k8s.io/kueue/pkg/workload/patching"
	"sigs.k8s.io/kueue/pkg/workloadslicing"
)

// partialPreemptionPollInterval bounds how long an outstanding reclaim waits between checks
// that its Pods have stopped (docs/design/spark-aware-preemption.md §5.4 step 2).
const partialPreemptionPollInterval = 5 * time.Second

// partialPreemptionFieldOwner is the field manager of the PartiallyPreempted condition. The
// scheduler sets it True under this owner and the job reconciler resolves it under the same one.
const partialPreemptionFieldOwner = "kueue-partial-preemption"

// partialPreemptionFor returns job as a JobWithPartialPreemption when partial preemption
// applies to it: the SparkPartialPreemption gate is on, the job is on the elastic path (only
// an elastic Workload's grant may be lowered in place, see validateAdmissionUpdate), and it
// implements the interface.
func partialPreemptionFor(job GenericJob) (JobWithPartialPreemption, bool) {
	if !features.Enabled(features.SparkPartialPreemption) || !WorkloadSliceEnabled(job) {
		return nil, false
	}
	pp, ok := job.(JobWithPartialPreemption)
	return pp, ok
}

// setOrDeleteAnnotation makes wl's annotation key equal to *value, or absent when value is
// nil, and reports whether it changed anything.
func setOrDeleteAnnotation(wl *kueue.Workload, key string, value *string) bool {
	cur, has := wl.Annotations[key]
	if value == nil {
		if !has {
			return false
		}
		delete(wl.Annotations, key)
		return true
	}
	if has && cur == *value {
		return false
	}
	metav1.SetMetaDataAnnotation(&wl.ObjectMeta, key, *value)
	return true
}

func int32String(n int32) *string {
	s := strconv.FormatInt(int64(n), 10)
	return &s
}

// applyPartialPreemptionDeclaration writes the job's partial-preemption declaration (§5.2)
// onto wl in memory, and reports whether anything changed. A job that declares no floor has
// the declaration removed. A marked-Pods count the job has not observed leaves the last one
// in place rather than dropping it.
func applyPartialPreemptionDeclaration(wl *kueue.Workload, pp JobWithPartialPreemption) bool {
	podSet, minCount, ok := pp.PartialPreemptionFloor()
	if !ok || podSet == "" {
		changed := setOrDeleteAnnotation(wl, constants.PartialPreemptionPodSetAnnotation, nil)
		changed = setOrDeleteAnnotation(wl, constants.PartialPreemptionMinCountAnnotation, nil) || changed
		changed = setOrDeleteAnnotation(wl, constants.PartialPreemptionTerminationGracePeriodAnnotation, nil) || changed
		return setOrDeleteAnnotation(wl, constants.PartialPreemptionMarkedPodsAnnotation, nil) || changed
	}
	ps := string(podSet)
	changed := setOrDeleteAnnotation(wl, constants.PartialPreemptionPodSetAnnotation, &ps)
	changed = setOrDeleteAnnotation(wl, constants.PartialPreemptionMinCountAnnotation, int32String(minCount)) || changed
	var grace *string
	if d, declared := pp.PartialPreemptionTerminationGracePeriod(); declared {
		grace = new(d.String())
	}
	changed = setOrDeleteAnnotation(wl, constants.PartialPreemptionTerminationGracePeriodAnnotation, grace) || changed
	if marked, observed := pp.PartialPreemptionMarkedPods(); observed {
		changed = setOrDeleteAnnotation(wl, constants.PartialPreemptionMarkedPodsAnnotation, int32String(marked)) || changed
	}
	return changed
}

// applyRestoreCountHygiene maintains wl's restore count (§5.5) in memory: it is dropped from an
// admitted Workload whose grant has grown back to it, and carried onto a pending replacement of
// predecessor that lacks it. It is left alone while a partial preemption is outstanding, since
// the grant has not been lowered yet.
func applyRestoreCountHygiene(wl, predecessor *kueue.Workload) bool {
	if workload.PartialPreemptionPending(wl) {
		return false
	}
	if restore, has := workload.PartialPreemptionRestoreCount(wl); has {
		spec, declared := workload.PartialPreemption(wl)
		if !declared || !workload.IsAdmitted(wl) {
			return false
		}
		if granted, ok := workload.GrantedCount(wl, spec.PodSet); ok && granted >= restore {
			return setOrDeleteAnnotation(wl, constants.PartialPreemptionRestoreCountAnnotation, nil)
		}
		return false
	}
	if predecessor == nil || workload.HasQuotaReservation(wl) {
		return false
	}
	if key := workloadslicing.ReplacementForKey(wl); key == nil || *key != workload.Key(predecessor) {
		return false
	}
	return carryRestoreCount(wl, predecessor)
}

// carryRestoreCount copies predecessor's restore count onto its replacement slice wl, so the
// grow-back slice is known to be grow-back (§5.5).
func carryRestoreCount(wl, predecessor *kueue.Workload) bool {
	if !features.Enabled(features.SparkPartialPreemption) {
		return false
	}
	restore, has := predecessor.Annotations[constants.PartialPreemptionRestoreCountAnnotation]
	if !has {
		return false
	}
	return setOrDeleteAnnotation(wl, constants.PartialPreemptionRestoreCountAnnotation, &restore)
}

// syncPartialPreemptionAnnotations keeps the declaration and restore count current on wl (the
// slice the job reconciler works with) and on the quota-holding slice it may be waiting
// behind, patching metadata only for a Workload whose annotations actually change.
func (r *JobReconciler) syncPartialPreemptionAnnotations(ctx context.Context, job GenericJob, wl *kueue.Workload) error {
	pp, ok := partialPreemptionFor(job)
	if !ok {
		return nil
	}
	retained, err := workloadslicing.FindLatestActiveWorkload(ctx, r.client, job.Object(), job.GVK())
	if err != nil {
		return err
	}
	targets := []*kueue.Workload{retained}
	if wl != nil && (retained == nil || retained.Name != wl.Name) {
		targets = append(targets, wl)
	}
	for _, target := range targets {
		if target == nil || workloadfinish.IsFinished(target) || !target.DeletionTimestamp.IsZero() {
			continue
		}
		var predecessor *kueue.Workload
		if target != retained {
			predecessor = retained
		}
		if err := clientutil.Patch(ctx, r.client, target, func() (bool, error) {
			changed := applyPartialPreemptionDeclaration(target, pp)
			return applyRestoreCountHygiene(target, predecessor) || changed, nil
		}, clientutil.WithRetryOnConflict()); err != nil {
			return fmt.Errorf("updating partial preemption annotations of %s: %w", klog.KObj(target), err)
		}
	}
	return nil
}

// reconcilePartialPreemption runs the victim side of an outstanding partial preemption
// (docs/design/spark-aware-preemption.md §5.4) for each of the job's Workloads that carries a
// target. It runs before any slice scaling, and stop reports that the caller must return
// result and err without going on to EnsureWorkloadSlices: while the reclaimed Pods are still
// stopping, a scale-down there would lower the grant early, handing their quota to the
// preemptor while they still run (the SC-2 breach).
//
// For an admitted Workload the steps are: delete the Pods (ReclaimPods); wait for them to stop
// or for the deadline; only then lower the grant to the target, leaving spec.podSets alone so
// the next reconcile creates the grow-back slice; then clear the target. A Workload that was
// evicted, finished, or lost its admission mid-reclaim has its target dropped without touching
// its grant: normal release applies to it.
func (r *JobReconciler) reconcilePartialPreemption(ctx context.Context, job GenericJob) (result ctrl.Result, stop bool, err error) {
	pp, ok := partialPreemptionFor(job)
	object := job.Object()
	if !ok || PrebuiltWorkloadNameFor(object) != "" {
		return ctrl.Result{}, false, nil
	}
	list := &kueue.WorkloadList{}
	if err := r.client.List(ctx, list, client.InNamespace(object.GetNamespace()), OwnerReferenceIndexFieldMatcher(job.GVK(), object.GetName())); err != nil {
		return ctrl.Result{}, true, err
	}
	_, _, jobFinished := job.Finished(ctx)
	for i := range list.Items {
		wl := &list.Items[i]
		if !workload.PartialPreemptionPending(wl) {
			continue
		}
		if jobFinished || workloadfinish.IsFinished(wl) || workloadevict.IsEvicted(wl) || !workload.IsAdmitted(wl) || !wl.DeletionTimestamp.IsZero() {
			if err := r.abandonPartialPreemption(ctx, job, wl); err != nil {
				return ctrl.Result{}, true, err
			}
			continue
		}
		return r.executePartialPreemption(ctx, job, pp, wl)
	}
	return ctrl.Result{}, false, nil
}

func (r *JobReconciler) executePartialPreemption(ctx context.Context, job GenericJob, pp JobWithPartialPreemption, wl *kueue.Workload) (ctrl.Result, bool, error) {
	log := ctrl.LoggerFrom(ctx).WithValues("workload", klog.KObj(wl))
	target, _ := workload.PartialPreemptionTarget(wl)

	podSet, floor, hasFloor := pp.PartialPreemptionFloor()
	if spec, declared := workload.PartialPreemption(wl); declared {
		podSet = spec.PodSet
	} else if !hasFloor {
		log.V(2).Info("Partial preemption target on a Workload that declares no reclaimable PodSet; dropping it")
		return ctrl.Result{}, false, r.abandonPartialPreemption(ctx, job, wl)
	}
	// ReclaimPods never goes below the floor, so a target under it could never complete and
	// would only end by timing out with Pods still running. The floor wins.
	if hasFloor && target < floor {
		log.V(2).Info("Partial preemption target is below the floor; reclaiming down to the floor", "target", target, "floor", floor)
		target = floor
	}
	granted, ok := workload.GrantedCount(wl, podSet)
	if !ok {
		return ctrl.Result{}, false, r.abandonPartialPreemption(ctx, job, wl)
	}

	// Record the pre-reclaim grant before anything is taken, so grow-back knows how far to go.
	// Only the first outstanding reclaim sets it; a later one keeps the older, higher count.
	if _, has := workload.PartialPreemptionRestoreCount(wl); !has && granted > target {
		if err := clientutil.Patch(ctx, r.client, wl, func() (bool, error) {
			return setOrDeleteAnnotation(wl, constants.PartialPreemptionRestoreCountAnnotation, int32String(granted)), nil
		}); err != nil {
			return ctrl.Result{}, true, fmt.Errorf("recording partial preemption restore count: %w", err)
		}
	}

	// KEP-975: the target is published on the admission before any Pod is evicted.
	if err := publishReclaimTargetCount(ctx, r.client, wl, podSet, target); err != nil {
		return ctrl.Result{}, true, err
	}

	// Steps 1 and 2: delete the Pods and see whether they have stopped.
	var grace *time.Duration
	if d, ok := workload.PartialPreemptionReclaimGracePeriod(wl); ok {
		grace = &d
	}
	done, err := pp.ReclaimPods(ctx, r.client, podSet, target, grace)
	if err != nil {
		return ctrl.Result{}, true, fmt.Errorf("reclaiming pods of podset %q: %w", podSet, err)
	}
	now := r.clock.Now()
	deadline, hasDeadline := partialPreemptionDeadline(wl)
	timedOut := !done && hasDeadline && !now.Before(deadline)
	if !done && !timedOut {
		requeue := partialPreemptionPollInterval
		if hasDeadline {
			requeue = min(requeue, deadline.Sub(now))
		}
		log.V(3).Info("Waiting for partially preempted pods to stop", "podSet", podSet, "target", target, "requeueAfter", requeue)
		return ctrl.Result{RequeueAfter: requeue}, true, nil
	}

	// A pass that read the Workload before the previous pass's writes landed finds the grant
	// already at the target. Finish the cleanup without lowering again or reporting twice.
	if granted <= target {
		if err := r.resolvePartiallyPreempted(ctx, wl, constants.PartialPreemptionCompletedReason,
			fmt.Sprintf("Reclaimed %s down to %d", podSet, target), now); err != nil {
			return ctrl.Result{}, true, err
		}
		return ctrl.Result{}, true, clearPartialPreemptionTarget(ctx, r.client, wl)
	}

	// Step 3: lower the grant, and only now. spec.podSets is left alone on purpose.
	reason := constants.PartialPreemptionCompletedReason
	eventType, eventReason := corev1.EventTypeNormal, ReasonPartialPreemptionComplete
	message := fmt.Sprintf("Reclaimed %s down to %d; its grant was lowered from %d", podSet, target, granted)
	if timedOut {
		reason = constants.PartialPreemptionTimedOutReason
		eventType, eventReason = corev1.EventTypeWarning, ReasonPartialPreemptionTimedOut
		message = fmt.Sprintf("Pods of %s did not stop by the deadline %s; its grant was lowered from %d to %d anyway",
			podSet, deadline.UTC().Format(time.RFC3339), granted, target)
	}
	// The published target is cleared in the same write that lowers the grant (KEP-975).
	clearTarget := func(wl *kueue.Workload) bool { return workload.SetReclaimTargetCount(wl, podSet, nil) }
	if _, err := workloadslicing.LowerGrantedCounts(ctx, r.client, wl, workload.PodSetsCounts{podSet: target}, clearTarget); err != nil {
		return ctrl.Result{}, true, fmt.Errorf("lowering the grant of a partially preempted workload: %w", err)
	}
	// Resolving the condition is a separate write: the scheduler owns it through its own field
	// manager. If it fails, the next pass finds the grant already at the target and retries.
	if err := r.resolvePartiallyPreempted(ctx, wl, reason, message, now); err != nil {
		return ctrl.Result{}, true, err
	}

	// Step 4: clear the target.
	if err := clearPartialPreemptionTarget(ctx, r.client, wl); err != nil {
		return ctrl.Result{}, true, err
	}
	r.record.Eventf(job.Object(), nil, eventType, eventReason, "PartialPreemption", "Workload %s: %s", workload.Key(wl), message)
	log.V(2).Info("Partial preemption ended", "reason", reason, "podSet", podSet, "target", target, "previousGrant", granted)
	// The Workload update requeues the job; the next pass sees spec > grant and creates the
	// grow-back slice from a fresh read.
	return ctrl.Result{}, true, nil
}

// abandonPartialPreemption drops an outstanding target without touching the grant.
func (r *JobReconciler) abandonPartialPreemption(ctx context.Context, job GenericJob, wl *kueue.Workload) error {
	message := "The workload was evicted, finished or lost its admission before the partial preemption completed"
	if err := r.resolvePartiallyPreempted(ctx, wl, constants.PartialPreemptionAbandonedReason, message, r.clock.Now()); err != nil {
		return client.IgnoreNotFound(err)
	}
	if err := clearPartialPreemptionTarget(ctx, r.client, wl); err != nil {
		return client.IgnoreNotFound(err)
	}
	r.record.Eventf(job.Object(), nil, corev1.EventTypeNormal, ReasonPartialPreemptionAbandoned, "PartialPreemption", "Workload %s: %s", workload.Key(wl), message)
	return nil
}

// publishReclaimTargetCount makes sure the admission carries target as podSet's
// reclaimTargetCount. The scheduler writes it when it issues the target; this covers a reclaim
// that starts before that write lands. A Workload CRD that predates the field prunes it, so a
// field that does not stick is logged and the reclaim goes on without it.
func publishReclaimTargetCount(ctx context.Context, c client.Client, wl *kueue.Workload, podSet kueue.PodSetReference, target int32) error {
	if got, ok := workload.ReclaimTargetCount(wl, podSet); ok && got == target {
		return nil
	}
	if err := workload.UpdateReclaimTargetCount(ctx, c, wl, podSet, &target); err != nil {
		return fmt.Errorf("publishing reclaimTargetCount: %w", err)
	}
	if got, ok := workload.ReclaimTargetCount(wl, podSet); features.Enabled(features.SparkPartialPreemption) &&
		workload.HasQuotaReservation(wl) && (!ok || got != target) {
		ctrl.LoggerFrom(ctx).V(2).Info("reclaimTargetCount was not stored; the Workload CRD may predate it. Reclaiming without it",
			"workload", klog.KObj(wl), "podSet", podSet, "target", target)
	}
	return nil
}

func clearPartialPreemptionTarget(ctx context.Context, c client.Client, wl *kueue.Workload) error {
	if err := workload.UpdateReclaimTargetCount(ctx, c, wl, "", nil); err != nil {
		return fmt.Errorf("clearing reclaimTargetCount: %w", err)
	}
	if err := clientutil.Patch(ctx, c, wl, func() (bool, error) {
		changed := setOrDeleteAnnotation(wl, constants.PartialPreemptionTargetAnnotation, nil)
		changed = setOrDeleteAnnotation(wl, constants.PartialPreemptionReclaimGracePeriodAnnotation, nil) || changed
		return setOrDeleteAnnotation(wl, constants.PartialPreemptionDeadlineAnnotation, nil) || changed, nil
	}, clientutil.WithRetryOnConflict()); err != nil {
		return fmt.Errorf("clearing partial preemption target: %w", err)
	}
	return nil
}

// resolvePartiallyPreempted sets the PartiallyPreempted condition to False with reason. It
// applies the condition alone, forced, under the field manager the scheduler sets it True with,
// so the two writers own the same entry and neither drops the other's update.
func (r *JobReconciler) resolvePartiallyPreempted(ctx context.Context, wl *kueue.Workload, reason, message string, now time.Time) error {
	if cond := apimeta.FindStatusCondition(wl.Status.Conditions, constants.PartiallyPreemptedCondition); cond != nil &&
		cond.Status == metav1.ConditionFalse && cond.Reason == reason {
		return nil
	}
	condition := metav1.Condition{
		Type:               constants.PartiallyPreemptedCondition,
		Status:             metav1.ConditionFalse,
		Reason:             reason,
		Message:            api.TruncateConditionMessage(message),
		ObservedGeneration: wl.Generation,
		LastTransitionTime: metav1.NewTime(now),
	}
	if err := workloadpatching.PatchStatus(ctx, r.client, wl, client.FieldOwner(partialPreemptionFieldOwner), func(wl *kueue.Workload) (bool, error) {
		return apimeta.SetStatusCondition(&wl.Status.Conditions, condition), nil
	}, workloadpatching.WithForceApply(), workloadpatching.WithLooseOnApply()); err != nil {
		return fmt.Errorf("resolving the %s condition: %w", constants.PartiallyPreemptedCondition, err)
	}
	return nil
}

// partialPreemptionDeadline is the deadline annotation, or, when the scheduler wrote none, the
// default timeout (the recorded or default grace period plus the cushion) from when the
// PartiallyPreempted condition was set.
func partialPreemptionDeadline(wl *kueue.Workload) (time.Time, bool) {
	if deadline, ok := workload.PartialPreemptionDeadline(wl); ok {
		return deadline, true
	}
	cond := apimeta.FindStatusCondition(wl.Status.Conditions, constants.PartiallyPreemptedCondition)
	if cond == nil || cond.Status != metav1.ConditionTrue {
		return time.Time{}, false
	}
	grace, ok := workload.PartialPreemptionReclaimGracePeriod(wl)
	if !ok {
		grace = constants.DefaultPartialPreemptionTerminationGracePeriod
	}
	return cond.LastTransitionTime.Add(grace + constants.PartialPreemptionTimeoutCushion), true
}
