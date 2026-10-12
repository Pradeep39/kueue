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

package workload

import (
	"strconv"
	"time"

	"k8s.io/utils/ptr"

	kueue "sigs.k8s.io/kueue/apis/kueue/v1beta2"
	"sigs.k8s.io/kueue/pkg/constants"
)

// PartialPreemptionSpec is what a Workload declares about how it may be partially preempted.
type PartialPreemptionSpec struct {
	// PodSet is the only PodSet partial preemption may reduce.
	PodSet kueue.PodSetReference
	// MinCount is the count PodSet must never be reduced below.
	MinCount int32
	// MarkedPods is how many of PodSet's ungated Pods their job has already chosen to remove.
	MarkedPods int32
}

// PartialPreemption returns the Workload's partial-preemption declaration. ok is false when the
// Workload declares none, or declares it malformed: such a Workload is only ever preempted whole.
func PartialPreemption(wl *kueue.Workload) (spec PartialPreemptionSpec, ok bool) {
	ann := wl.GetAnnotations()
	podSet, hasPodSet := ann[constants.PartialPreemptionPodSetAnnotation]
	minStr, hasMin := ann[constants.PartialPreemptionMinCountAnnotation]
	if !hasPodSet || podSet == "" || !hasMin {
		return spec, false
	}
	minCount, err := strconv.ParseInt(minStr, 10, 32)
	if err != nil || minCount < 0 {
		return spec, false
	}
	spec.PodSet = kueue.PodSetReference(podSet)
	spec.MinCount = int32(minCount)
	if s, has := ann[constants.PartialPreemptionMarkedPodsAnnotation]; has {
		if n, err := strconv.ParseInt(s, 10, 32); err == nil && n > 0 {
			spec.MarkedPods = int32(n)
		}
	}
	return spec, true
}

// GrantedCount returns the granted count of the named PodSet, falling back to its spec count
// when the assignment carries none, as ExtractGrantedPodSetCounts does. ok is false when the
// Workload is not admitted or has no such PodSet.
func GrantedCount(wl *kueue.Workload, podSet kueue.PodSetReference) (int32, bool) {
	if wl.Status.Admission == nil {
		return 0, false
	}
	var specCount int32
	for i := range wl.Spec.PodSets {
		if wl.Spec.PodSets[i].Name == podSet {
			specCount = wl.Spec.PodSets[i].Count
			break
		}
	}
	for i := range wl.Status.Admission.PodSetAssignments {
		psa := &wl.Status.Admission.PodSetAssignments[i]
		if psa.Name == podSet {
			return ptr.Deref(psa.Count, specCount), true
		}
	}
	return 0, false
}

// ReclaimableCount is how many Pods of the declared PodSet partial preemption may still take
// from an admitted Workload: its grant above the floor. It is 0 for a Workload that declares no
// partial preemption, is not admitted, or already has a partial preemption outstanding.
func ReclaimableCount(wl *kueue.Workload) int32 {
	spec, ok := PartialPreemption(wl)
	if !ok || PartialPreemptionPending(wl) {
		return 0
	}
	granted, ok := GrantedCount(wl, spec.PodSet)
	if !ok || granted <= spec.MinCount {
		return 0
	}
	return granted - spec.MinCount
}

// PartialPreemptionTarget returns the granted count an outstanding partial preemption must
// reduce the Workload's declared PodSet to.
func PartialPreemptionTarget(wl *kueue.Workload) (int32, bool) {
	s, has := wl.GetAnnotations()[constants.PartialPreemptionTargetAnnotation]
	if !has {
		return 0, false
	}
	n, err := strconv.ParseInt(s, 10, 32)
	if err != nil || n < 0 {
		return 0, false
	}
	return int32(n), true
}

// PartialPreemptionPending reports whether a partial preemption is outstanding on the Workload.
func PartialPreemptionPending(wl *kueue.Workload) bool {
	_, ok := PartialPreemptionTarget(wl)
	return ok
}

// PartialPreemptionDeadline returns when an outstanding partial preemption stops waiting for its
// Pods to stop.
func PartialPreemptionDeadline(wl *kueue.Workload) (time.Time, bool) {
	s, has := wl.GetAnnotations()[constants.PartialPreemptionDeadlineAnnotation]
	if !has {
		return time.Time{}, false
	}
	t, err := time.Parse(time.RFC3339, s)
	if err != nil {
		return time.Time{}, false
	}
	return t, true
}

// PartialPreemptionRestoreCount returns the granted count the Workload's declared PodSet had
// before it was partially preempted. While the grant is below it, scale-up is grow-back.
func PartialPreemptionRestoreCount(wl *kueue.Workload) (int32, bool) {
	s, has := wl.GetAnnotations()[constants.PartialPreemptionRestoreCountAnnotation]
	if !has {
		return 0, false
	}
	n, err := strconv.ParseInt(s, 10, 32)
	if err != nil || n < 0 {
		return 0, false
	}
	return int32(n), true
}

// IsGrowBack reports whether wl, a slice of a partially preempted Workload, is still growing
// back to the count it had before it was partially preempted. Such a slice may only be admitted
// into free quota: it must not preempt, or it could take capacity back from its own preemptor.
func IsGrowBack(wl *kueue.Workload) bool {
	restore, ok := PartialPreemptionRestoreCount(wl)
	if !ok {
		return false
	}
	spec, ok := PartialPreemption(wl)
	if !ok {
		return false
	}
	if granted, admitted := GrantedCount(wl, spec.PodSet); admitted {
		return granted < restore
	}
	// A pending scale-up slice has no grant yet; it is grow-back while the restore count stands.
	return true
}
