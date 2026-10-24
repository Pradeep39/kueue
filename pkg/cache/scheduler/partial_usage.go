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

package scheduler

import (
	corev1 "k8s.io/api/core/v1"

	kueue "sigs.k8s.io/kueue/apis/kueue/v1beta2"
	"sigs.k8s.io/kueue/pkg/resources"
	"sigs.k8s.io/kueue/pkg/workload"
)

// PartialPodSetUsage returns the quota usage that n Pods of wl's podSet account for: the
// PodSet's usage at its current count minus its usage once scaled down by n with
// PodSetResources.ScaledTo, which is what the cache charges after the grant is lowered
// (docs/design/spark-aware-preemption.md §5.6, SC-1).
//
// The result is empty for a PodSet that wl does not have, that has a topology request (TAS
// PodSets are never partially preempted, and ScaledTo does not scale their assignment), or
// when n is not positive. n is capped at the PodSet's count.
func PartialPodSetUsage(wl *workload.Info, podSet kueue.PodSetReference, n int32) workload.Usage {
	usage := workload.Usage{Quota: workload.ResourceUsage{Assigned: resources.FlavorResourceQuantities{}}}
	if wl == nil || n <= 0 {
		return usage
	}
	for i := range wl.TotalRequests {
		ps := &wl.TotalRequests[i]
		if ps.Name != podSet {
			continue
		}
		if ps.TopologyRequest != nil || ps.Count <= 0 || ps.Requests == nil {
			return usage
		}
		scaled := ps.ScaledTo(ps.Count - min(n, ps.Count))
		ps.Requests.ForEach(func(res corev1.ResourceName, q resources.Amount) {
			flv := ps.Flavors[res]
			if flv == "" {
				return
			}
			delta := q.Sub(scaled.Requests.ResourceValue(res))
			if delta.Sign() == 0 {
				return
			}
			fr := resources.FlavorResource{Flavor: flv, Resource: res}
			usage.Quota.Assigned[fr] = usage.Quota.Assigned[fr].Add(delta)
		})
		return usage
	}
	return usage
}

// RemovePartialWorkloadUsage removes from the snapshot the usage of n Pods of wl's podSet,
// as PartialPodSetUsage computes it, leaving wl in its ClusterQueue. It simulates a partial
// preemption. Like RemoveWorkload, it leaves a released workload's usage untouched.
func (s *Snapshot) RemovePartialWorkloadUsage(wl *workload.Info, podSet kueue.PodSetReference, n int32) {
	if s.released.Has(workload.Key(wl.Obj)) {
		return
	}
	s.removeUsage(s.ClusterQueue(wl.ClusterQueue), PartialPodSetUsage(wl, podSet, n))
}

// AddPartialWorkloadUsage adds back what RemovePartialWorkloadUsage removed for the same
// arguments, exactly.
func (s *Snapshot) AddPartialWorkloadUsage(wl *workload.Info, podSet kueue.PodSetReference, n int32) {
	if s.released.Has(workload.Key(wl.Obj)) {
		return
	}
	s.AddUsage(s.ClusterQueue(wl.ClusterQueue), PartialPodSetUsage(wl, podSet, n))
}
