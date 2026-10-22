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
	"maps"
	"slices"

	"github.com/go-logr/logr"

	schdcache "sigs.k8s.io/kueue/pkg/cache/scheduler"
	"sigs.k8s.io/kueue/pkg/scheduler/preemption/policy"
	"sigs.k8s.io/kueue/pkg/workload"
)

type PreemptedWorkloads map[workload.Reference]*workload.Info

func (p PreemptedWorkloads) HasAny(newTargets []*Target) bool {
	for _, target := range newTargets {
		if _, found := p[workload.Key(target.WorkloadInfo.Obj)]; found {
			return true
		}
	}
	return false
}

func (p PreemptedWorkloads) Insert(newTargets []*Target) {
	for _, target := range newTargets {
		p[workload.Key(target.WorkloadInfo.Obj)] = target.WorkloadInfo
	}
}

// MergeWithTargets returns the union of workloads already preempted and
// the new preemption targets, deduplicated by workload key.
// The receiver is not mutated.
func (p PreemptedWorkloads) MergeWithTargets(targets []*Target) PreemptedWorkloads {
	merged := maps.Clone(p)
	if merged == nil {
		merged = make(PreemptedWorkloads, len(targets))
	}
	merged.Insert(targets)
	return merged
}

func (p PreemptedWorkloads) Workloads() []*workload.Info {
	return slices.Collect(maps.Values(p))
}

// PreemptedTarget is a preemption decided earlier in the scheduling cycle: a whole workload,
// or, when Reclaim is set, only Reclaim.Count Pods of its Reclaim.PodSet.
type PreemptedTarget struct {
	WorkloadInfo *workload.Info
	Reclaim      *policy.PartialReclaim
}

// PreemptedTargets is PreemptedWorkloads made aware of partial preemption. Where
// PreemptedWorkloads would credit a partial victim's whole usage, it credits only the Pods
// the partial preemption takes (docs/design/spark-aware-preemption.md §5.6, SC-1).
type PreemptedTargets map[workload.Reference]PreemptedTarget

func (p PreemptedTargets) HasAny(newTargets []*Target) bool {
	for _, target := range newTargets {
		if _, found := p[workload.Key(target.WorkloadInfo.Obj)]; found {
			return true
		}
	}
	return false
}

// Insert records the targets. A workload targeted both partially and whole counts as whole;
// partial preemptions of the same workload add up.
func (p PreemptedTargets) Insert(newTargets []*Target) {
	for _, target := range newTargets {
		insertTarget(p, target)
	}
}

func insertTarget(p PreemptedTargets, target *Target) {
	key := workload.Key(target.WorkloadInfo.Obj)
	entry := PreemptedTarget{WorkloadInfo: target.WorkloadInfo}
	if target.Reclaim != nil {
		entry.Reclaim = &policy.PartialReclaim{PodSet: target.Reclaim.PodSet, Count: target.Reclaim.Count}
	}
	if existing, found := p[key]; found {
		if existing.Reclaim == nil || entry.Reclaim == nil || existing.Reclaim.PodSet != entry.Reclaim.PodSet {
			entry.Reclaim = nil
		} else {
			entry.Reclaim.Count += existing.Reclaim.Count
		}
	}
	p[key] = entry
}

// MergeWithTargets returns the union of the recorded preemptions and the new targets. The
// receiver is not mutated.
func (p PreemptedTargets) MergeWithTargets(targets []*Target) PreemptedTargets {
	merged := make(PreemptedTargets, len(p)+len(targets))
	for key, entry := range p {
		if entry.Reclaim != nil {
			entry.Reclaim = &policy.PartialReclaim{PodSet: entry.Reclaim.PodSet, Count: entry.Reclaim.Count}
		}
		merged[key] = entry
	}
	merged.Insert(targets)
	return merged
}

// Workloads returns every recorded workload, whole or partial.
func (p PreemptedTargets) Workloads() []*workload.Info {
	out := make([]*workload.Info, 0, len(p))
	for _, entry := range p {
		out = append(out, entry.WorkloadInfo)
	}
	return out
}

// WholeWorkloads returns the workloads preempted whole.
func (p PreemptedTargets) WholeWorkloads() []*workload.Info {
	out := make([]*workload.Info, 0, len(p))
	for _, entry := range p {
		if entry.Reclaim == nil {
			out = append(out, entry.WorkloadInfo)
		}
	}
	return out
}

// SimulateUsageRemoval removes the usage the recorded preemptions free from the snapshot,
// leaving the workloads in their ClusterQueues, and returns the undo. It replaces
// snapshot.SimulateWorkloadUsageRemoval(preemptedWorkloads.Workloads()).
func (p PreemptedTargets) SimulateUsageRemoval(snapshot *schdcache.Snapshot) func() {
	revertWhole := snapshot.SimulateWorkloadUsageRemoval(p.WholeWorkloads())
	revertPartial := p.simulatePartialRemoval(snapshot)
	return func() {
		revertPartial()
		revertWhole()
	}
}

// SimulateRemoval is SimulateUsageRemoval for preemptions decided by other workloads: whole
// victims also leave their ClusterQueues and the scheduling simulator, while partial victims
// stay and only free their reclaimed Pods' quota. Their Pods stay in the simulator, which is
// stricter than reality. It replaces snapshot.SimulateWorkloadRemoval and
// snapshot.SimulatePodRemoval over the preempted workloads.
func (p PreemptedTargets) SimulateRemoval(ctx context.Context, log logr.Logger, snapshot *schdcache.Snapshot) func() {
	whole := p.WholeWorkloads()
	revertUsage := snapshot.SimulateWorkloadRemoval(whole)
	revertPods := snapshot.SimulatePodRemoval(ctx, log, whole)
	revertPartial := p.simulatePartialRemoval(snapshot)
	return func() {
		revertPartial()
		revertPods()
		revertUsage()
	}
}

func (p PreemptedTargets) simulatePartialRemoval(snapshot *schdcache.Snapshot) func() {
	var partial []PreemptedTarget
	for _, entry := range p {
		if entry.Reclaim != nil {
			partial = append(partial, entry)
		}
	}
	for _, entry := range partial {
		snapshot.RemovePartialWorkloadUsage(entry.WorkloadInfo, entry.Reclaim.PodSet, entry.Reclaim.Count)
	}
	return func() {
		for _, entry := range partial {
			snapshot.AddPartialWorkloadUsage(entry.WorkloadInfo, entry.Reclaim.PodSet, entry.Reclaim.Count)
		}
	}
}
