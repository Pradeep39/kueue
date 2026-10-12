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

package preemption

import (
	"context"
	"errors"
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
	"sigs.k8s.io/kueue/pkg/util/api"
	clientutil "sigs.k8s.io/kueue/pkg/util/client"
	"sigs.k8s.io/kueue/pkg/workload"
	workloadpatching "sigs.k8s.io/kueue/pkg/workload/patching"
)

// PartialPreemptionFieldOwner is the field manager that sets the PartiallyPreempted condition
// with Server-Side Apply. It is not the admission field owner: an apply by that owner carrying
// only this condition would remove the admission conditions it owns.
const PartialPreemptionFieldOwner = client.FieldOwner(constants.KueueName + "-partial-preemption")

var errNotPartiallyPreemptable = errors.New("workload can no longer be partially preempted")

// issuePartialPreemption writes a partial preemption onto the victim (§5.4): the target, the
// deadline and, unless one is already recorded, the restore count as annotations, then the
// PartiallyPreempted condition. The victim's job reconciler carries it out. If another
// partial preemption is found outstanding when patching, that one is left alone.
func (p *Preemptor) issuePartialPreemption(ctx context.Context, preemptor *workload.Info, target *Target, preemptionMessage string) error {
	log := ctrl.LoggerFrom(ctx)
	wl := target.WorkloadInfo.Obj.DeepCopy()
	now := p.clock.Now()
	timeout := partialPreemptionTimeout(target)

	var newTarget, granted int32
	issued := false
	err := clientutil.Patch(ctx, p.client, wl, func() (bool, error) {
		issued = false
		if workload.PartialPreemptionPending(wl) {
			return false, nil
		}
		spec, ok := workload.PartialPreemption(wl)
		if !ok || spec.PodSet != target.Reclaim.PodSet {
			return false, errNotPartiallyPreemptable
		}
		var admitted bool
		granted, admitted = workload.GrantedCount(wl, spec.PodSet)
		if !admitted {
			return false, errNotPartiallyPreemptable
		}
		newTarget = max(granted-target.Reclaim.Count, spec.MinCount)
		if newTarget >= granted {
			return false, errNotPartiallyPreemptable
		}
		ann := wl.GetAnnotations()
		if ann == nil {
			ann = make(map[string]string, 3)
		}
		ann[constants.PartialPreemptionTargetAnnotation] = strconv.FormatInt(int64(newTarget), 10)
		ann[constants.PartialPreemptionDeadlineAnnotation] = now.Add(timeout).UTC().Format(time.RFC3339)
		if _, has := ann[constants.PartialPreemptionRestoreCountAnnotation]; !has {
			ann[constants.PartialPreemptionRestoreCountAnnotation] = strconv.FormatInt(int64(granted), 10)
		}
		wl.SetAnnotations(ann)
		issued = true
		return true, nil
	}, clientutil.WithRetryOnConflict())
	if err != nil {
		return err
	}
	if !issued {
		log.V(3).Info("Partial preemption already outstanding", "targetWorkload", klog.KObj(wl), "preemptingWorkload", klog.KObj(preemptor.Obj))
		return nil
	}

	reclaimed := granted - newTarget
	message := fmt.Sprintf("Partially preempted %d Pod(s) of PodSet %s, from %d to %d, for workload %s. %s",
		reclaimed, target.Reclaim.PodSet, granted, newTarget, workload.Key(preemptor.Obj), preemptionMessage)
	if err := workloadpatching.PatchStatus(ctx, p.client, wl, PartialPreemptionFieldOwner, func(wl *kueue.Workload) (bool, error) {
		return apimeta.SetStatusCondition(&wl.Status.Conditions, metav1.Condition{
			Type:               constants.PartiallyPreemptedCondition,
			Status:             metav1.ConditionTrue,
			Reason:             constants.PartialPreemptionIssuedReason,
			Message:            api.TruncateConditionMessage(message),
			LastTransitionTime: metav1.NewTime(now),
			ObservedGeneration: wl.Generation,
		}), nil
	}, workloadpatching.WithLooseOnApply(), workloadpatching.WithRetryOnConflict()); err != nil {
		// The target annotation is what the reconciler acts on, so the reclaim is under way.
		log.Error(err, "Failed to set the PartiallyPreempted condition", "targetWorkload", klog.KObj(wl))
	}

	preemptorEffPri, preemptorBase, preemptorBoost := priorityInfo(log, preemptor.Obj)
	targetEffPri, targetBase, targetBoost := priorityInfo(log, target.WorkloadInfo.Obj)
	log.V(3).Info("Partially preempted", "targetWorkload", klog.KObj(wl), "preemptingWorkload", klog.KObj(preemptor.Obj),
		"podSet", target.Reclaim.PodSet, "count", reclaimed, "granted", granted, "partialPreemptionTarget", newTarget,
		"reason", target.Reason, "targetClusterQueue", klog.KRef("", string(target.WorkloadInfo.ClusterQueue)),
		"preemptorEffectivePriority", preemptorEffPri, "targetEffectivePriority", targetEffPri)
	p.recorder.Eventf(target.WorkloadInfo.Obj, nil, corev1.EventTypeNormal, "PartiallyPreempted", "PartiallyPreempted",
		api.TruncateEventMessage(message+fmt.Sprintf("; preemptor effective priority: %d (base: %d, boost: %d); preemptee effective priority: %d (base: %d, boost: %d)",
			preemptorEffPri, preemptorBase, preemptorBoost, targetEffPri, targetBase, targetBoost)))
	p.recorder.Eventf(preemptor.Obj, nil, corev1.EventTypeNormal, "PartiallyPreemptedWorkload", "PartiallyPreemptedWorkload",
		"Partially preempted %d Pod(s) of PodSet %s of workload %s (UID: %s) in ClusterQueue %s",
		reclaimed, target.Reclaim.PodSet, klog.KObj(target.WorkloadInfo.Obj), target.WorkloadInfo.Obj.UID, target.WorkloadInfo.ClusterQueue)
	return nil
}

// partialPreemptionTimeout is the victim ClusterQueue's partial-preemption timeout.
func partialPreemptionTimeout(target *Target) time.Duration {
	if target.WorkloadCq != nil && target.WorkloadCq.PartialPreemption.Timeout > 0 {
		return target.WorkloadCq.PartialPreemption.Timeout
	}
	d, _ := time.ParseDuration(constants.DefaultPartialPreemptionTimeout)
	return d
}
