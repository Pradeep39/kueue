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
	"cmp"
	"context"
	"iter"
	"slices"
	"time"

	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/sets"
	"k8s.io/klog/v2"
	"sigs.k8s.io/controller-runtime/pkg/log"

	kueuealpha "sigs.k8s.io/kueue/apis/kueue/v1alpha1"
	kueue "sigs.k8s.io/kueue/apis/kueue/v1beta2"
	"sigs.k8s.io/kueue/pkg/constants"
	controllerconstants "sigs.k8s.io/kueue/pkg/controller/constants"
	"sigs.k8s.io/kueue/pkg/features"
	"sigs.k8s.io/kueue/pkg/scheduler/preemption/classical"
	"sigs.k8s.io/kueue/pkg/scheduler/preemption/policy"
	"sigs.k8s.io/kueue/pkg/util/priority"
	"sigs.k8s.io/kueue/pkg/workload"
	workloadevict "sigs.k8s.io/kueue/pkg/workload/evict"
)

// partialVictim is a workload the partial strategy may take Pods from, and how many it has
// taken so far. The number taken lives in target.Reclaim.Count, and the snapshot always has
// exactly that many of the victim's Pods removed.
type partialVictim struct {
	target    *Target
	candidate classical.Candidate
	// headroom is how many Pods may be taken in total: the grant above the floor.
	headroom int32
	// marked is how many of those Pods the victim's job has already chosen to remove.
	marked int32
	// inFlight is set for a victim with a partial preemption already outstanding; its
	// Count is that preemption's and is never changed.
	inFlight bool
	priority int64
	reserved time.Time
	// configurable is set for a victim selected by the ClusterQueue's PreemptionConfig rather
	// than by its classical policy. The rules have already decided it may be preempted, so the
	// classical validity check does not apply to it.
	configurable bool
	// stage orders victims before priority: 0 for the baseline (classical candidates and those of
	// the PreemptionConfig's Always rules), 1 for those of its InsufficientQuota rules, which
	// are only drawn on once every baseline victim is at its floor.
	stage int
}

func (v *partialVictim) taken() int32 { return v.target.Reclaim.Count }

// partialPlanner plans one partial-preemption attempt (docs/design/spark-aware-preemption.md §5.3).
type partialPlanner struct {
	pCtx           *preemptionCtx
	candidates     classicalCandidateSource
	allowBorrowing bool
	victims        []*partialVictim
	byTarget       map[*Target]*partialVictim
}

// setTaken moves the victim from its current number of taken Pods to n, keeping the
// snapshot in step. The usage removed for n Pods is always computed afresh, so that adding
// it back restores the snapshot exactly whatever path led to n.
func (pp *partialPlanner) setTaken(v *partialVictim, n int32) {
	snap := pp.pCtx.snapshot
	wl, ps := v.target.WorkloadInfo, v.target.Reclaim.PodSet
	if k := v.taken(); k > 0 {
		snap.AddPartialWorkloadUsage(wl, ps, k)
	}
	if n > 0 {
		snap.RemovePartialWorkloadUsage(wl, ps, n)
	}
	v.target.Reclaim.Count = n
}

// partialPreemptionStrategyIterator yields the partial-preemption strategy (§5.3): one per
// borrowing option the classical strategy would try, over the same candidates.
func (p *Preemptor) partialPreemptionStrategyIterator(ctx context.Context, pCtx *preemptionCtx) iter.Seq[PreemptionStrategy] {
	if !features.Enabled(features.SparkPartialPreemption) || pCtx.preemptorCQ == nil ||
		!pCtx.preemptorCQ.PartialPreemption.Enabled || p.enableFairSharing {
		// Fair-sharing preemption keeps whole eviction in v1 (§5.3).
		return func(func(PreemptionStrategy) bool) {}
	}
	candidates, attemptPossibleOpts := newClassicalCandidates(ctx, p, pCtx)
	return func(yieldStrategy func(PreemptionStrategy) bool) {
		for _, opts := range attemptPossibleOpts {
			pp := &partialPlanner{pCtx: pCtx, candidates: candidates, allowBorrowing: opts.borrowing}
			strategy := newPreemptionStrategy(pp.iterate(ctx), opts.borrowing, pCtx)
			strategy.partial = true
			strategy.fillBack = pp.fillBack
			if !yieldStrategy(strategy) {
				return
			}
		}
	}
}

// partialCandidate is a workload the partial strategy may draw on, from the classical
// candidates or from the PreemptionConfig.
type partialCandidate struct {
	classical classical.Candidate
	target    *Target // set for a PreemptionConfig candidate
	stage     int
}

// partialCandidates lists the classical candidates, then those the ClusterQueue's
// PreemptionConfig selects that the classical policy did not: first its Always rules', then
// its InsufficientQuota rules'. Without them, a ClusterQueue that preempts only through a
// PreemptionConfig (spec.preemption Never) would never partially preempt, and would fall back
// to evicting the configured candidates whole. QuotaFeasibleAndInsufficientTopology rules are
// left out: they free topology capacity, and partial preemption never reduces a PodSet with a
// topology request.
func (pp *partialPlanner) partialCandidates() []partialCandidate {
	var out []partialCandidate
	seen := sets.New[types.UID]()
	for _, cand := range pp.candidates.Candidates() {
		seen.Insert(cand.WorkloadInfo.Obj.UID)
		out = append(out, partialCandidate{classical: cand})
	}
	if !features.Enabled(features.ConfigurablePreemptions) {
		return out
	}
	for stage, trigger := range []kueuealpha.PreemptionConfigActivationTrigger{kueuealpha.Always, kueuealpha.InsufficientQuota} {
		for _, t := range pp.pCtx.configurableEvaluator.CandidateTargets(pp.pCtx.snapshot, &pp.pCtx.preemptor, pp.pCtx.frsNeedPreemption, trigger) {
			if seen.Has(t.WorkloadInfo.Obj.UID) {
				continue
			}
			seen.Insert(t.WorkloadInfo.Obj.UID)
			out = append(out, partialCandidate{target: t, stage: stage})
		}
	}
	return out
}

// collect builds the victims from the candidates (partialCandidates): those that declare
// partial preemption on a PodSet without a topology request, are not evicted and are not part
// of the preemptor's own slice chain.
func (pp *partialPlanner) collect(ctx context.Context) {
	logger := log.FromContext(ctx)
	now := pp.pCtx.clock.Now()
	pp.victims = nil
	pp.byTarget = make(map[*Target]*partialVictim)
	for _, pc := range pp.partialCandidates() {
		cand := pc.classical
		if pc.target != nil {
			cand = classical.Candidate{WorkloadInfo: pc.target.WorkloadInfo, Reason: pc.target.Reason}
		}
		wl := cand.WorkloadInfo
		if workloadevict.IsEvicted(wl.Obj) || inPreemptorChain(pp.pCtx.preemptor.Obj, wl.Obj) {
			continue
		}
		spec, ok := workload.PartialPreemption(wl.Obj)
		if !ok || !reclaimablePodSet(wl, spec.PodSet) {
			continue
		}
		v := &partialVictim{
			candidate:    cand,
			priority:     priority.EffectivePriority(logger, wl.Obj),
			reserved:     policy.QuotaReservationTime(wl.Obj, now),
			configurable: pc.target != nil,
			stage:        pc.stage,
		}
		if target, pending := workload.PartialPreemptionTarget(wl.Obj); pending {
			granted, admitted := workload.GrantedCount(wl.Obj, spec.PodSet)
			if !admitted || granted <= target {
				continue
			}
			v.inFlight = true
			v.headroom = min(granted, podSetCount(wl, spec.PodSet)) - min(target, podSetCount(wl, spec.PodSet))
			if v.headroom <= 0 {
				continue
			}
		} else {
			v.headroom = min(workload.ReclaimableCount(wl.Obj), podSetCount(wl, spec.PodSet)-spec.MinCount)
			if v.headroom <= 0 {
				continue
			}
			v.marked = min(spec.MarkedPods, v.headroom)
		}
		v.target = &Target{
			WorkloadInfo: wl,
			Reason:       cand.Reason,
			WorkloadCq:   pp.pCtx.snapshot.ClusterQueue(wl.ClusterQueue),
			Reclaim:      &policy.PartialReclaim{PodSet: spec.PodSet},
		}
		if pc.target != nil {
			v.target.ConfigurablePreemptionReasonData = pc.target.ConfigurablePreemptionReasonData
		}
		pp.victims = append(pp.victims, v)
		pp.byTarget[v.target] = v
	}
}

// iterate takes Pods one at a time, in the order of §5.3, yielding the victim's target after
// each. getTargets stops it as soon as the preemptor fits.
func (pp *partialPlanner) iterate(ctx context.Context) iter.Seq[*Target] {
	return func(yield func(*Target) bool) {
		pp.collect(ctx)
		logger := log.FromContext(ctx).V(3)

		// Reclaims already under way free their Pods anyway: count them first, and return
		// them again so that the preemptor waits on them instead of picking new victims.
		for _, v := range pp.victims {
			if !v.inFlight {
				continue
			}
			pp.setTaken(v, v.headroom)
			logger.Info("Partial preemption: reusing outstanding reclaim", "targetWorkload", klog.KObj(v.target.WorkloadInfo.Obj), "count", v.headroom)
			if !yield(v.target) {
				return
			}
		}

		for _, tier := range pp.tiers() {
			// First the Pods the victims' own jobs have already chosen to remove.
			if !pp.roundRobin(tier, func(v *partialVictim) int32 { return v.marked }, yield) {
				return
			}
			// Then one Pod from every victim with headroom per round, never below the floor.
			if !pp.roundRobin(tier, func(v *partialVictim) int32 { return v.headroom }, yield) {
				return
			}
		}
	}
}

// tiers groups the victims that are not in flight by stage, then by priority, lowest first.
func (pp *partialPlanner) tiers() [][]*partialVictim {
	var rest []*partialVictim
	for _, v := range pp.victims {
		if !v.inFlight {
			rest = append(rest, v)
		}
	}
	slices.SortStableFunc(rest, func(a, b *partialVictim) int {
		return cmp.Or(cmp.Compare(a.stage, b.stage), cmp.Compare(a.priority, b.priority))
	})
	var tiers [][]*partialVictim
	for i := 0; i < len(rest); {
		j := i
		for j < len(rest) && rest[j].stage == rest[i].stage && rest[j].priority == rest[i].priority {
			j++
		}
		tiers = append(tiers, rest[i:j])
		i = j
	}
	return tiers
}

// roundRobin takes one Pod from every victim of the tier still below limit per round, until
// none is. Within a round, victims with the most headroom left go first, then the most
// recently admitted. It returns false when yield asked to stop.
func (pp *partialPlanner) roundRobin(tier []*partialVictim, limit func(*partialVictim) int32, yield func(*Target) bool) bool {
	order := slices.Clone(tier)
	for {
		slices.SortStableFunc(order, func(a, b *partialVictim) int {
			return cmp.Or(
				cmp.Compare(b.headroom-b.taken(), a.headroom-a.taken()),
				b.reserved.Compare(a.reserved),
				cmp.Compare(a.target.WorkloadInfo.Obj.UID, b.target.WorkloadInfo.Obj.UID),
			)
		})
		progressed := false
		for _, v := range order {
			if v.taken() >= limit(v) || (!v.configurable && !pp.candidates.IsValid(v.candidate, pp.allowBorrowing)) {
				continue
			}
			pp.setTaken(v, v.taken()+1)
			progressed = true
			if !yield(v.target) {
				return false
			}
		}
		if !progressed {
			return true
		}
	}
}

// fillBack returns surplus Pods while the preemptor still fits, one at a time, from the
// victims that gave up the most first, so that the spread stays even. Outstanding reclaims
// are kept. It returns one target per victim that still gives up at least one Pod.
func (pp *partialPlanner) fillBack(ctx context.Context, targets []*Target) []*Target {
	victims := make([]*partialVictim, 0, len(targets))
	for _, t := range targets {
		if v := pp.byTarget[t]; v != nil {
			victims = append(victims, v)
		}
	}
	for {
		order := slices.Clone(victims)
		slices.SortStableFunc(order, func(a, b *partialVictim) int {
			return cmp.Or(
				cmp.Compare(b.taken(), a.taken()),
				// Of equal losers, return from the one that gave up an unmarked Pod.
				cmp.Compare(b.taken()-min(b.taken(), b.marked), a.taken()-min(a.taken(), a.marked)),
				// Then from the one that would have given it last.
				cmp.Compare(a.headroom-a.taken(), b.headroom-b.taken()),
				a.reserved.Compare(b.reserved),
				cmp.Compare(b.target.WorkloadInfo.Obj.UID, a.target.WorkloadInfo.Obj.UID),
			)
		})
		returned := false
		for _, v := range order {
			if v.inFlight || v.taken() == 0 {
				continue
			}
			pp.setTaken(v, v.taken()-1)
			if workloadFits(ctx, pp.pCtx, pp.allowBorrowing) {
				returned = true
				break
			}
			pp.setTaken(v, v.taken()+1)
		}
		if !returned {
			break
		}
	}
	result := make([]*Target, 0, len(victims))
	for _, v := range victims {
		if v.taken() > 0 {
			result = append(result, v.target)
		}
	}
	return result
}

// reclaimablePodSet reports whether partial preemption may reduce wl's podSet: it exists and
// has no topology request (§3, PNG-3).
func reclaimablePodSet(wl *workload.Info, podSet kueue.PodSetReference) bool {
	found := false
	for i := range wl.TotalRequests {
		if wl.TotalRequests[i].Name == podSet {
			if wl.TotalRequests[i].TopologyRequest != nil {
				return false
			}
			found = true
		}
	}
	if !found {
		return false
	}
	for i := range wl.Obj.Spec.PodSets {
		if wl.Obj.Spec.PodSets[i].Name == podSet && workload.IsExplicitlyRequestingTAS(wl.Obj.Spec.PodSets[i]) {
			return false
		}
	}
	return true
}

// podSetCount is the count of wl's podSet the snapshot charges.
func podSetCount(wl *workload.Info, podSet kueue.PodSetReference) int32 {
	for i := range wl.TotalRequests {
		if wl.TotalRequests[i].Name == podSet {
			return wl.TotalRequests[i].Count
		}
	}
	return 0
}

// inPreemptorChain reports whether candidate is the slice the preemptor replaces, or another
// slice of the preemptor's own chain. Taking Pods from it would shrink the preemptor itself.
func inPreemptorChain(preemptor, candidate *kueue.Workload) bool {
	if preemptor == nil || candidate == nil || preemptor.Namespace != candidate.Namespace {
		return false
	}
	if key, ok := preemptor.Annotations[constants.WorkloadSliceReplacementForAnnotation]; ok && key == string(workload.Key(candidate)) {
		return true
	}
	return sliceName(preemptor) == sliceName(candidate) &&
		preemptor.Labels[controllerconstants.JobUIDLabel] == candidate.Labels[controllerconstants.JobUIDLabel]
}

func sliceName(wl *kueue.Workload) string {
	if name, ok := wl.Annotations[kueue.WorkloadSliceNameAnnotation]; ok {
		return name
	}
	return wl.Name
}
