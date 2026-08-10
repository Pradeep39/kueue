---
title: "Elastic Scaling for SparkApplication via Workload Slices"
subtitle: "Reference Implementation — Detailed Design"
author: "Pradeep Reddy"
date: "2026-08-09"
---

# 1. About this document

This is the implementation companion to the proposal *Elastic Scaling for SparkApplication via
Workload Slices*. The proposal argues **what** should be built and **why**; this document
describes **how** the reference implementation works, in enough detail that a reviewer can judge
the approach and an implementer can reproduce or adapt it.

It documents a working implementation running on a real cluster, not a plan. Code references are
to `github.com/Pradeep39/kueue`, branch `workloadslice-quota-accounting` (PRs #15–#21). Where the
implementation has known gaps or unresolved questions, they are stated as such rather than
smoothed over — §11 and §12.

Reading order for a reviewer short on time: §3 (the central asymmetry), §5.3 (chain-scoped
accounting), §6.3 (the ungating cap). Those three carry the load; the rest is supporting detail.

# 2. System context

## 2.1 The actors

| Actor | Role | Controlled by us? |
|---|---|---|
| Spark driver (`ExecutorAllocationManager`) | Creates and deletes executor Pods directly against the API server according to Dynamic Allocation policy. | No |
| Kubeflow Spark Operator | Reconciles the `SparkApplication` CR; submits the driver. | No |
| Kueue job reconciler (`jobframework`) | Builds `Workload` objects from a job's `PodSets()`, manages slice lifecycle. | Yes |
| Kueue scheduler | Assigns flavours, grants quota, admits `Workload`s. | Yes |
| Kueue scheduler cache | Holds admitted usage; source of `ClusterQueue.status.flavorsUsage`. | Yes |
| `ElasticJobUngater` | Removes scheduling gates from Pods once quota is granted. | Yes |

The defining constraint is the first row. Dynamic Allocation is an autonomous control loop that
Kueue cannot instruct. It cannot be asked to stop creating Pods, and it does not consult or update
the `SparkApplication` CR when it scales. Everything in this design follows from having to
*observe* that loop rather than *drive* it.

## 2.2 Two hard constraints discovered during implementation

**C-1: Kueue must never write to `SparkApplication.Spec`.** The Spark Operator's `event_filter.go`
performs an unconditional `DeepEqual` on `.Spec` and force-kills and resubmits a running
application on any difference. An earlier design that patched `spec.executor.instances` to the
observed count was implemented, then reverted, because it restarted the very jobs it was
accounting for.

**C-2: A scheduling gate can only be applied through the Pod template.** The gate is injected into
`spec.executor.template` at CR-creation time. Spark's driver loads that template file **once at
startup** and merges it — Fabric8 merge semantics, not replace — into every executor Pod for the
application's lifetime. This is what makes a template-level gate reach Pods that DA creates hours
later. It also means gating is all-or-nothing per application: there is no per-Pod decision point
at creation time, only the later decision of whether to *remove* the gate.

# 3. The central asymmetry

Everything in §5 and §6 follows from one fact: on a scale-up, three structures legitimately hold
three different numbers.

An elastic job expresses growth by creating a **replacement slice** — a new `Workload` annotated
`kueue.x-k8s.io/workload-slice-replacement-for: <predecessor>` and
`kueue.x-k8s.io/workload-slice-name: <chain root>`. The predecessor is then `Finish`ed with reason
`WorkloadSliceReplaced`.

| Consumer | Value it sees | Why that is correct for it |
|---|---|---|
| Scheduler snapshot | **Delta** (new − replaced) | `flavorassigner.Assignment.append` subtracts the replaced slice's request. `Scheduler.schedule` then calls `cq.AddUsage(usage)` on a snapshot that *already* holds the predecessor's full usage, so `old_full + delta = new_full`. |
| `status.admission` | **Full** new count | `Assignment.ToAPI()` is not delta-adjusted. It must describe the entire PodSet, because pod gating, ungating and reclaimable-pod accounting all read it. |
| Persistent scheduler cache | Re-derived from `status.admission` | It is the source of `ClusterQueue.status.flavorsUsage`, via `Cache.Usage` → `clusterQueue.AdmittedUsage`. |

The snapshot is ephemeral and rebuilt every scheduling cycle, so a delta is safe there. The
persistent cache is long-lived and had **no concept of slice replacement at all** — it simply
summed each cached `Workload`'s full admitted usage. Two slices describing the same Pods were
therefore charged twice.

## 3.1 The one behaviour that is easy to get wrong

Usage is **not** simply the granted count. `workload.totalRequestsFromAdmission` rescales the
admitted usage down to the spec count whenever the spec count is lower:

```go
currentCounts := podSetsCountsAfterReclaim(wl)      // = spec.podSets[].Count (minus reclaimable)
...
if countAfterReclaim := currentCounts[psa.Name]; countAfterReclaim < setRes.Count {
    setRes.Requests.Divide(int64(setRes.Count))
    setRes.Requests.Mul(int64(countAfterReclaim))
}
```

So the effective charge is **`min(spec, granted)`**.

This matters twice over. It means a stale-high grant never inflates usage — which is why §6.2 is
hygiene rather than a correctness fix. And it means a grant *lower* than the spec is not corrected
by the ledger, which is exactly the hole §6.3 closes. During development this behaviour was
misread in both directions before a test settled it; a reviewer should verify it before reasoning
about any of the accounting.

# 4. Deriving the executor count (PR #16)

## 4.1 Data flow

```
Spark driver creates/deletes executor Pod
        │
        ▼
Pod event ──> executorPodPredicate ──> executorPodHandler (debounce)
                                              │
                                              ▼
                            reconcile.Request{ns, sparkAppName}
                                              │
                                              ▼
              jobframework reconciler ──> SparkApplication.PodSets(ctx, client)
                                              │
                                              ▼
                        liveExecutorCount ──> List Pods by label ──> count
                                              │
                                              ▼
                          EnsureWorkloadSlices ──> patch in place | new slice
```

Nothing in this path writes to the CR (C-1). The count is a read-only observation of Pods.

## 4.2 What counts as live

`isVerifiedLiveExecutor` excludes only terminal phases:

```go
switch pod.Status.Phase {
case corev1.PodSucceeded, corev1.PodFailed:
    return false
default:
    return true
}
```

Three deliberate consequences:

- **`Pending` counts.** Quota must be reserved as soon as a Pod exists, not once it reaches
  `Running`; otherwise there is a window in which DA has consumed real capacity that Kueue's
  accounting does not know about.
- **Terminating Pods count.** A Pod with a `DeletionTimestamp` keeps occupying node resources and
  running containers until it reaches `Succeeded`/`Failed`. Excluding it the instant a delete is
  issued would undercount live consumption and manufacture spurious intermediate counts as DA
  works through a batch of deletions.
- **Gate-blocked Pods count.** This is load-bearing and is also the origin of the open issue in
  §11.2. A gated Pod consumes no node capacity, so counting it overstates consumption — but it is
  the *only* signal that DA wants more capacity. Excluding gated Pods would mean the count never
  grows, no replacement slice is created, no quota is granted, the gate is never removed, and the
  job cannot scale at all. The count is therefore "what DA wants", not "what is consuming".

## 4.3 Reconcile-scoped memoisation

`PodSets()` is called several times per reconcile — equivalence checks against the existing
`Workload`, then construction. Two calls observing different live counts would produce a spurious
"not equivalent" verdict and self-inflicted slice churn.

The count is memoised on the `*SparkApplication` wrapper (`cachedLiveExecutorCount`). Because
`NewJob()` allocates a fresh wrapper per reconcile, the cache is automatically scoped to one
reconcile pass and can never go stale across passes. The same pattern carries
`cachedWorkloadSequenceNumber`.

## 4.4 Cold start

Before any executor Pod exists, `initialExecutorCount()` walks a fallback chain:

1. `spec.executor.instances`, if set;
2. `dynamicAllocation.initialExecutors` — structured field, else the
   `spark.dynamicAllocation.initialExecutors` sparkConf key;
3. the same for `minExecutors`;
4. otherwise zero.

`dynamicAllocationExecutorCount` reads the structured `spec.dynamicAllocation` field first and
falls back to `spec.sparkConf["spark.dynamicAllocation."+field]`, because the Spark Operator accepts
either form.

**Operational consequence.** With `initialExecutors: 1` and `minExecutors: 3`, the first Workload
contains driver 1 + executor 1, and the floor of 3 is reached by two further slice replacements —
each needing fresh quota. Omitting `initialExecutors` makes both Kueue and Spark start at
`minExecutors`, so the floor is admitted atomically as one Workload. This is the cheapest lever on
§11.2.

## 4.5 Event handling and debouncing

**Watch predicate.** Executor Pods are owned by the **driver Pod**, not the CR, so there is no
OwnerReference chain to key an `Owns()` watch off. `isTrackedExecutorPod` filters on the
`sparkoperator.k8s.io/app-name` label instead — the same label `PodLabelSelector()` and
`liveExecutorCount()` already depend on.

**Debounce.** DA bursts produce many Pod events in quick succession. `executorPodHandler` keeps a
per-key timer, reset on each new event, so a request reaches the workqueue only after a quiet
window (`executorPodDebounce = 5s`). A pure quiet-window debounce can be starved indefinitely by a
continuous stream, so a ceiling (`executorPodMaxWait = 30s`) forces a flush:

```go
now := h.clock.Now()
if t, ok := h.timers[key]; ok {
    if now.Sub(h.burstStarts[key]) >= h.maxWait {
        t.Stop(); delete(h.timers, key); delete(h.burstStarts, key)
        q.Add(req)                     // flush: burst has run long enough
        return
    }
    t.Reset(h.debounce)                // still bursting: push the enqueue out
    return
}
h.burstStarts[key] = now
h.timers[key] = h.clock.AfterFunc(h.debounce, func() { q.Add(req); /* cleanup under mu */ })
```

Per-key timers guarded by a mutex are used rather than the workqueue's own `AddAfter` delay heap,
because staggered `AddAfter` calls from a burst each fire independently and defeat the coalescing.
The clock is injected (`clock.WithTickerAndDelayedExecution`) so the behaviour is unit-testable.

## 4.6 Slice naming

`GetWorkloadNameExtraPart` must produce a name that is never reused for the lifetime of the
SparkApplication. Two obvious inputs both fail:

- **`Generation`** — the framework default. DA never changes `.Spec`, so it stays frozen across
  every scale event after the first.
- **The live executor count** — a Finished slice is never deleted absent a retention policy, so its
  deterministic name persists in etcd forever. A DA workload oscillating within a narrow band would
  eventually revisit a previously-used count, recompute the same hash, and collide with a dead
  object — a permanent, self-reinforcing failure once every count in the band has been used.

The implementation folds in `workloadSequenceNumber()`: the count of every `Workload` ever owned by
this job, Finished or not. It only ever grows. It is computed in `PodSets()` (which has a client)
and cached for `GetWorkloadNameExtraPart()` (which does not).

# 5. Quota gating of executor Pods (PRs #18, #20)

## 5.1 Injection

The SparkApplication webhook `Default` injects the gate for elastic jobs only:

```go
if isAnElasticJob(obj) {
    if job.Spec.Executor.Template == nil {
        job.Spec.Executor.Template = emptyExecutorPodTemplateSpec.DeepCopy()
    }
    utilpod.GateTemplate(job.Spec.Executor.Template, kueue.ElasticJobSchedulingGate)
}
```

Validation requires the gate on create and update for elastic jobs, so it cannot be stripped.

**Only executors are gated.** The driver is created by the Spark Operator only after Kueue
unsuspends the CR, which already implies admission, so gating it would be redundant. A consequence
worth knowing when reading metrics: drivers never appear in a gated-Pod count.

## 5.2 Reach

Per C-2, the template is loaded once by the driver and merged into every executor Pod thereafter.
This was established by tracing the Spark Operator's submission path and Apache Spark's
`KubernetesExecutorBuilder`, and it is the reason a template-level gate is sufficient — no
per-Pod webhook, and no SparkApplication-specific code in the ungating path.

## 5.3 Ungating

No new controller. The generic `ElasticJobUngater` already handles Job and RayCluster and picks up
SparkApplication executor Pods once they carry `constants.PodSetLabel` and
`kueue.WorkloadSliceNameAnnotation` — both already written by the existing
`RunWithPodSetsInfo` annotation/label merge.

`Reconcile` resolves the chain's **active slice** (latest non-Finished with a quota reservation) and
`podsToUngate` decides how many Pods may proceed:

```
room = granted[podSet] - alreadyUngated[podSet]
ungate min(room, gatedCandidates)      # lowest-named first, for determinism
```

Already-ungated Pods are subtracted because they consume quota too. The source of `granted` is the
subject of §6.3.

# 6. Quota-accounting correctness (PR #21)

Three defects, found in the order below. Each is presented as mechanism, then fix, then the design
alternative that was rejected.

## 6.1 Overlapping slices double-counted

### Mechanism

`Scheduler.admit` adds the new slice to the persistent cache **synchronously** via
`assumeWorkload` → `Cache.AddOrUpdateWorkload`, with the full admitted count (§3). The predecessor
leaves the cache only when its `Finish` propagates back, which is asynchronous — and the scheduler
does not retry a failed `Finish`:

```go
// replaceOldWorkloadSlice ... If this fails, the job reconciler's
// EnsureWorkloadSlices detects both slices admitted and finishes the old one.
```

The predecessor is also deliberately excluded from preemption targets
(`FindReplacedSliceTarget` — it is "evicted rather than preempted"), so nothing removes it
synchronously either. Between those two events the cache holds `old_full + new_full`.

Measured on the reporting cluster: 13–95 ms per replacement. Unbounded in principle, since any
event that re-adds a not-yet-Finished predecessor re-inflates the ledger.

### Fix: charge the chain, not the slice

Usage is accounted per **slice chain**. `clusterQueue` gains:

```go
sliceGroups         map[string]sets.Set[workload.Reference]   // chain key -> member slices
countedSliceInGroup map[string]workload.Reference             // chain key -> the slice charged
```

`addOrUpdateWorkload` and `deleteWorkload` maintain membership and then call
`reconcileSliceGroup`, which moves the charge to the chain's current tip:

```go
tip := c.sliceGroupTip(groupKey)
prev, hadPrev := c.countedSliceInGroup[groupKey]
if hadPrev && prev == tip { return }                       // nothing to do
if hadPrev { subtract usage of prev (still present in c.Workloads) }
if tip == "" { delete(c.countedSliceInGroup, groupKey); return }
add usage of tip
c.countedSliceInGroup[groupKey] = tip
```

Two properties make this robust:

- **Declarative, not event-ordered.** The charge depends only on current cache membership, so it is
  correct whether or not a `Finish` ever lands, and in any event order.
- **Idempotent.** Re-adding a superseded slice leaves the charge where it is. This matters because
  a predecessor stays admitted in the API until its `Finish` lands, so unrelated events can re-add
  it.

Ordering within a chain is decided by `sliceIsLater`: creation timestamp first (matching
`FindNotFinishedWorkloads`' existing ordering), then the replacement annotation to break
same-second ties, then UID for determinism.

### The chain key

```
namespace + "/" + WorkloadSliceNameAnnotation + "#" + job UID
```

with two defensive fallbacks (the replaced slice's key, then the workload's own key) for a slice
somehow lacking the chain-root annotation, and an empty key — meaning "not a slice, use the
original path" — for anything with no elastic or slice annotation. That last branch keeps the hot
path allocation-free for ordinary workloads.

**The job UID is not decoration.** A cluster capture showed chain root `als-1-7005f` shared by two
different SparkApplication UIDs after a delete-and-recreate: the new instance's slices inherited
the previous instance's chain-root name. Without the UID those two independent jobs would share a
chain and only the newest would be charged — an *undercount*, the more dangerous direction.

### Preemption interaction

A superseded slice's usage is not in the ledger, so it must not be offered as a preemption
candidate: `SimulateWorkloadRemoval` would subtract usage that was never added, understating the
ClusterQueue and over-admitting. Preempting one frees nothing anyway, since its Pods are attributed
to its replacement.

`ClusterQueueSnapshot` therefore carries `supersededSlices` (populated from
`clusterQueue.supersededSliceKeys()`), exposed as `SliceSuperseded`, and both candidate collectors
skip them: `classical.getCandidatesFromCQ` and `preemption.findCandidatesForPolicy`. The latter's
signature changed from taking a workload map to taking the owning `*ClusterQueueSnapshot`, so the
predicate is reachable.

No other snapshot change was needed: `snapshotClusterQueue` clones the already-netted
`resourceNode`, so it inherits corrected totals rather than re-summing workloads.

### Rejected alternative

Keying chain membership off the **replacement-pointer graph** directly. With chain `a → b → c` all
cached, deleting the middle slice `b` left `a` with no cached replacement, so `a` resumed being
charged and the overcommit returned — 7Gi against a 6Gi quota in test. Chain grouping has no
equivalent hole because membership does not depend on surviving intermediate links.

## 6.2 Granted counts left stale after scale-down

### Mechanism

`EnsureWorkloadSlices` handles scale-down by patching the slice in place, which writes
`spec.podSets[].Count` only. `status.admission.podSetAssignments[].Count` keeps the value granted at
admission. Observed divergence in the field: spec 22 against granted 5; spec 0 against granted 4.

### This is hygiene, not accounting

Per §3.1 the ledger charges `min(spec, granted)`, so a stale-high grant does **not** inflate usage.
Fed the two live workloads from the cluster capture (spec 3, granted 5 and 7) the cache reports
4Gi, not 7Gi. A regression test pins this specifically, because the opposite conclusion is an easy
and tempting inference from "usage is derived from `status.admission`".

It is fixed anyway for three reasons: the stale value is what forced the `scaledDownPodSetNames`
and `isPreexistingStaleCount` validation workarounds; the replacement delta in `flavorassigner` is
computed against it, so the admission path reasons from a stale number; and it is what operators
see via `kubectl`.

### Fix

`scaleDownAdmission` lowers each `PodSetAssignment.Count` that exceeds its new spec count and
brings the dependent fields with it:

- **`ResourceUsage`** rescaled proportionally. It is the PodSet **total**, not per-Pod — confirmed
  against cluster data (count 7 ↔ 3584Mi at 512Mi per Pod). It is divided by the old count and
  multiplied by the new, the same arithmetic `totalRequestsFromAdmission` uses for reclaimable Pods.
- **`TopologyAssignment`** truncated via `utiltas.TruncateAssignment`, so TAS domain accounting stays
  consistent with the reduced count.

It is applied from `updatePodSetCountsWithRetry` after the spec update lands, as a separate
`Status().Update` under `retry.RetryOnConflict`, because admission lives on the status subresource.
A failure there leaves the previous behaviour (spec low, grant high) and is retried on the next
reconcile — it degrades rather than corrupting.

### The webhook exception this required

`validateAdmissionUpdate` made `status.admission` **fully immutable** once set, except for TAS
topology assignments. Without relaxing it, the patch above is rejected by Kueue's own validating
webhook — a blocker discovered only when the first implementation attempt failed.

The exception is deliberately narrow: a **decrease only**, for an **elastic workload only**. It is
implemented by copying the lowered `Count` and `ResourceUsage` onto the old value before the
immutability comparison, so anything else that differs is still rejected — increases, flavour
changes, PodSet-count mismatches. Growing a grant therefore still goes through the scheduler, where
quota is checked.

It mirrors the exception `validateImmutablePodSet` already makes for `spec.podSets[].Count` on
elastic jobs, which is the precedent that made this shape acceptable. **Non-elastic workloads —
including plain `batch/v1.Job` — keep a fully immutable admission.**

## 6.3 Pods ungated beyond their grant

### Mechanism

`podsToUngate` computed its cap as:

```go
granted := workload.ExtractPodSetCountsFromWorkload(wl)
```

The variable is named `granted` and the comment above it says "its granted PodSet counts are the
right cap". Neither is true:

```go
func ExtractPodSetCountsFromWorkload(wl *kueue.Workload) PodSetsCounts {
    return ExtractPodSetCounts(wl.Spec.PodSets)      // the REQUEST, not the grant
}
```

For an elastic job the request can exceed the grant — a scale-up creates a replacement slice rather
than growing the grant, and a stale read can raise the request on an already-admitted slice. So the
ungater authorised more Pods than the scheduler had paid for.

**Field evidence.** One incident, three jobs, one live slice each:

| Job | Granted `[driver, executor]` | Running | Delta |
|---|---|---|---|
| als-1 | `[1, 2]` = 3 slots | 1 driver + **3** executors | **+1** |
| als-2 | `[1, 3]` = 4 slots | 1 driver + 3 executors | 0 |
| als-3 | `[1, 4]` = 5 slots | 1 driver + 4 executors | 0 |

Grants summed to 12 slots = 6144Mi, exactly the reported `flavorsUsage` and exactly `nominalQuota`.
The ledger was internally consistent and inside quota; reality had one Pod more. The scheduler log
confirms als-1's slice was admitted at executor count 2 while its spec read 3.

This is the mirror image of §6.1 and more dangerous. An over-count is visible — usage climbs above
`nominalQuota` and someone notices. An **under-count is invisible**: the queue looks healthy and
Kueue will admit further work into capacity that is already consumed.

### Fix

A new extractor, deliberately distinct from the existing one:

```go
func ExtractGrantedPodSetCounts(wl *kueue.Workload) PodSetsCounts {
    counts := make(PodSetsCounts)
    if wl.Status.Admission == nil { return counts }        // nothing granted yet
    requested := ExtractPodSetCountsFromWorkload(wl)
    for _, psa := range wl.Status.Admission.PodSetAssignments {
        counts[psa.Name] = ptr.Deref(psa.Count, requested[psa.Name])
    }
    return counts
}
```

`PodSetAssignment.Count` is optional in the API. The scheduler always sets it via
`Assignment.ToAPI`, but an assignment lacking it falls back to the PodSet's own count — matching
`totalRequestsFromAdmission`, so a legacy or hand-written admission is not read as a grant of zero,
which would deadlock ungating entirely.

`ExtractPodSetCountsFromWorkload` keeps its behaviour and gained a doc note pointing at the new
function. The naming is what made this defect easy to introduce and easy to miss on review, so the
mitigation is partly documentary.

### A note on how this was found

Five pre-existing ungater tests failed against the fix. Investigation showed the **fixtures** were
wrong, not the change: they declared `resourceUsage` for N Pods while leaving
`PodSetAssignment.Count` at the test wrapper's default of 1, and passed only because the cap was
read from the spec. They now set `Count` to match what the scheduler actually produces. The defect
was partly masked by tests that encoded it.

# 7. Concurrency and consistency

| Concern | Handling |
|---|---|
| Cache mutation | `Cache.AddOrUpdateWorkload` and `Cache.DeleteWorkload` take the cache write lock; `clusterQueue.addOrUpdateWorkload`/`deleteWorkload`/`reconcileSliceGroup` run under it. |
| Snapshot reads | `Cache.Snapshot` holds the read lock; `supersededSliceKeys()` allocates a fresh set and mutates nothing. |
| Race verification | `go test -race` across the cache, scheduler, workload and elasticjobs packages. |
| Optimistic-lock conflicts | `updatePodSetCountsWithRetry` re-fetches and reapplies counts, **re-validating eligibility before every attempt**; returns `errWorkloadAdmittedConcurrently` if the workload was admitted concurrently at a count that no longer permits an in-place patch, so the caller creates a new slice rather than desyncing spec from the admission record. |
| Watch-cache lag | Observing more than one not-finished slice is legitimate — `EnsureWorkloadSlices` may have Finished one moments earlier while the watch cache still shows it. `prepareWorkloadSlice` reuses `NormalizeActiveSlices` to converge on the same selection instead of erroring. |
| Debounce state | Per-key timers and burst-start times guarded by a mutex inside `executorPodHandler`. |

# 8. Failure modes and degradation

| Failure | Behaviour | Rationale |
|---|---|---|
| Predecessor's `Finish` never lands | Accounting stays correct; the superseded slice contributes zero regardless. | §6.1 is declarative, not event-driven. |
| `scaleDownAdmission` status patch fails | Falls back to previous behaviour (spec low, grant high); retried next reconcile. | The ledger charges `min(spec, granted)`, so this is not a correctness regression. |
| `PodSetAssignment.Count` absent | Falls back to the PodSet count. | Prevents a grant of zero deadlocking ungating. |
| Executor Pod list fails | `PodSets()` returns the error; reconcile retries. | No partial accounting is written. |
| Job deleted and recreated with the same chain-root name | Treated as separate chains via the job UID. | Prevents undercounting two independent jobs. |
| Feature gate off | `sliceChainKey` returns empty; original accounting path. | No behaviour change for non-elastic clusters. |

# 9. Observability

| Signal | Where | Use |
|---|---|---|
| `Workload slice chain usage moved to a later slice` (V3) | `reconcileSliceGroup` | Confirms §6.1 engaging; includes chain key, from and to. |
| `Workload slice superseded ... releasing its quota usage` (V3) | chain accounting | The moment a predecessor stops being charged. |
| `elastic ungating quota accounting for PodSet` (V4) | `podsToUngate` | `grantedCount`, `alreadyUngatedCount`, `gatedCount`, `ungatingCount` — the §6.3 cap in action. |
| `Failed to finish old workload slice ...` (Error) | `replaceOldWorkloadSlice` | The unretried failure; visible at any verbosity. |
| `kueue_cluster_queue_resource_usage` | metrics | Same source as `flavorsUsage`; graphable against `..._nominal_quota`. |

The two chain-accounting lines are at V(3), so `--v=3` is required to observe the fix working.

# 10. Validation

## 10.1 Unit test strategy

Coverage is organised around the invariants rather than the functions: each test states the
invariant it protects. Notable cases:

- Both slice arrival orders (replacement before and after its predecessor).
- In-place update, re-add after delete, replacement rollback.
- Three-slice chains, including deletion of the middle slice — the case that killed the rejected
  pointer-graph design.
- Two replacements racing for one predecessor.
- Recreated-job chain-root collision.
- Feature gate off — accounting inert.
- Full `scheduler.schedule()` cycle with the predecessor's `Finish` **forced to fail** via a client
  interceptor, reproducing the pathological state directly.

**Negative controls.** Every correctness test was run with its fix disabled and confirmed to fail
with the bug's signature. This was adopted after a first-draft assertion proved tautological, and
it is the reason the suite is trustworthy.

## 10.2 Cluster validation harness

A shell harness samples both invariants every two seconds and captures forensics on violation.
Two design details make its verdicts trustworthy:

- **Snapshot ordering.** Pods are read *before* the ClusterQueue. If a scale-up lands between the
  two reads, the ledger is the newer number, so usage is if anything too high — biasing against
  false "Pods exceed usage" reports.
- **Confirm-before-report.** A suspected violation is re-read once, a second later, and only
  reported if it survives. This is what distinguishes a real violation from sampling skew, and it
  is why the earlier manual two-command sampling was inconclusive.

On violation it captures Pods, ClusterQueue, Workloads, LocalQueues, SparkApplications, namespace
events, controller logs (current and previous), filtered metrics, and a per-slice breakdown of spec
versus granted counts with chain and job UID.

One limitation, since it changes how §10.3 should be read: the harness computes the GRANT side as
`running Pods × POD_MIB`, with `POD_MIB` a constant set from `spec.executor.memory`. That is the
same figure Kueue charges, so for **memory** the check compares Kueue's ledger against a
restatement of its own assumption. It verifies that the ledger tracks Pod *count* correctly; it
cannot detect a systematic per-Pod under-charge, and §11.3 establishes that there is one. Summing
actual `resources.requests` from the Pod snapshot removes the circularity and is a prerequisite for
the next validation round.

## 10.3 Results

| Run | Build | Samples | CEILING | GRANT |
|---|---|---|---|---|
| 2 apps | #15–#20 | ~30 | Held; usage tracked live Pods exactly at 512Mi each | Held |
| 3 apps | #15–#20 | 316 | Held on every sample | **Failed 39×** → found §6.3 |
| 3 apps | #15–#21 | 159 | Held on every sample; peak = `nominalQuota` | Held on every sample |

Before this work the same two-app workload reported 9Gi against a 6Gi `nominalQuota`.

The final run also produced the first in-cluster evidence for §6.1, previously only test-proven: a
sample showing four quota-reserved, non-Finished workloads charging only three jobs' worth of
quota — one slice contributing zero mid-replacement.

Scope of the GRANT column, per §10.2: it establishes that every running Pod is backed by a grant
*at the per-Pod cost Kueue believes in*. It does not establish that that cost matches what the
kubelet reserved. §11.3 is that gap.

# 11. Known gaps

## 11.1 Original overcommit: resolved empirically, mechanism unconfirmed

Post-fix, usage never exceeded `nominalQuota` across 346 samples, where the same workload
previously reported 9Gi. But the pre-fix capture only ever accounted for 4Gi of live demand, so the
5Gi gap was never explained, and no post-fix measurement identifies which mechanism was
responsible. The leading hypothesis is that the cache retained usage for slices already `Finished`
in the API — most of the ~75 captured slices were Finished, many stuck terminating with the
`kueue.x-k8s.io/resource-in-use` finaliser — which §6.1 suppresses regardless. Confirming it would
require the metrics-versus-API comparison on a build *without* §6.1. Treated as fixed in practice,
not root-caused.

## 11.2 Gate-blocked Pods inflate the requested count

Per §4.2 a gated Pod is counted as live. When the queue is saturated the replacement slice cannot
be admitted, so nothing raises the grant, so the Pods stay gated and keep being counted, so the
requested count grows. Observed: a request climbing from 13 to 23 executors against a 12-slot
quota; 68 gate-blocked Pods; a 24 s admission wait; sustained reconcile-conflict churn.

Fixing §6.3 makes this **more** visible, because surplus executors now correctly remain `Pending`
rather than running.

The obvious fix is wrong, as §4.2 explains: excluding gated Pods removes the only scale-up signal.
A correct fix must **bound** the request. Two candidates, both behaviour changes warranting their
own proposal:

- **Latch.** Once a replacement slice exists and is unadmitted, stop revising its count upward
  until it is admitted or dropped. Bounds churn without needing quota awareness.
- **Incremental growth.** Cap the request at `granted + step`. Simpler, at the cost of slower
  scale-up.

Configuration mitigations, pending measurement: size `maxExecutors` against available quota, and
set `initialExecutors` equal to `minExecutors` so the floor is admitted atomically (§4.4).

## 11.3 The PodSet under-charges CPU and memory (accepted; workaround for CPU only)

Found after §6.1–§6.3 had shipped, while extending the harness to CPU. Both halves are in upstream
`sparkapplication_podset.go` (commit `e787fd071`, upstream PR #7268) — outside this work, and not
workload-slice defects. They are recorded because they bound what §10.3 proves.

**CPU has no fallback to `cores`.** `addCPURequests` reads only `Spec.*.CoreRequest` and returns
early when it is nil, so the PodSet carries no `cpu` entry and `flavorsUsage` reports `cpu: 0`
however many Pods run. Observed: 11 Pods each requesting 1 core, against `cpu: 0` of a
`nominalQuota` of 15. Spark's own precedence is
`spark.kubernetes.executor.request.cores ?? spark.executor.cores`
(`BasicExecutorFeatureStep.scala`); there is deliberately no fallback to `limit.cores`, which Spark
reads only for the CPU limit.

**Memory drops `memoryOverhead`.** `addMemoryRequests` uses `Spec.*.Memory` verbatim, and the
integration contains no reference to overhead at all, so `spec.executor.memoryOverhead` is ignored
even when set explicitly. Spark charges
`memory + (overhead ?? max(factor × memory, minMemoryOverhead)) + offHeap + pysparkMemory`, with
defaults 0.1 and 384m. At 512m executors that is 896Mi actual against 512Mi charged — a 75%
under-charge, so a queue reporting itself exactly at 6Gi had reserved roughly 10.5Gi.

**Why admission still succeeded**, which the CPU result makes worth stating plainly: Kueue does not
vend CPU. `PodSets()` builds a synthetic template for ledger arithmetic, while the Pod that runs is
built independently by the Spark driver and never read back. A resource absent from the template is
simply unenforced — the kube-scheduler placed each Pod against its real request, and the `cpu` quota
was decorative. Divergence between the two derivations is silent by construction, which generalises
to any integration reconstructing a Pod template from a CR (§12).

**Decision.** Not fixed, to avoid widening scope into upstream resource derivation. `coreRequest` is
instead treated as a **mandatory attribute** on driver and executor specs, written as `1000m` rather
than `"1"` — the field is `*string` with no coercion, and an unquoted `1` fails inside Kueue's own
`msparkapplication.kb.io` webhook with `cannot unmarshal number into Go struct field ... of type
string`, an error that names neither the manifest nor the quoting. The workaround is viable and not
constraining: it supplies a value Spark already derives from `cores`, so real Pod requests do not
change and only the ledger moves. Verified on the cluster.

Memory has **no equivalent workaround**, since the overhead field is ignored even when set, so it
remains under-charged by the overhead term. Anything that does fix it must not copy the Spark
Operator's `isJavaApp(Java || Scala)` test: Spark selects its 0.4 non-JVM factor from a
**Python-only** check, so the two disagree for `type: R`.

## 11.4 Smaller items

- `Scheduler.replaceOldWorkloadSlice` does not retry a failed `Finish`. Harmless for accounting
  after §6.1, but latent.
- `scaledDownPodSetNames` and `isPreexistingStaleCount` are retirable once §6.2 has shipped long
  enough that no objects carry stale grants.
- No envtest or live-cluster integration tests; the development environment cannot reach the
  kubebuilder-tools host.
- `suite_test.go`'s `managerSetup()` does not wire the additional reconcilers.
- Admission latency instrumentation exists but the configuration comparison has not yet been run.

# 12. Applying this pattern to another integration

The generic machinery is integration-agnostic. Adapting it to another autoscaling framework
requires:

1. **A live-count source.** Implement the equivalent of `liveExecutorCount`: list the framework's
   Pods by a stable label and count non-terminal ones. Memoise per reconcile.
2. **A watch and debounce.** Filter Pod events by that label; coalesce bursts with a quiet window
   *and* a max-wait ceiling.
3. **A collision-free slice name.** Provide `GetWorkloadNameExtraPart` backed by a monotonic
   sequence, unless the framework updates its CR on scale (in which case `Generation` suffices).
4. **A gate in the Pod template** — only if the framework's controller creates Pods outside Kueue's
   admission path, and only if its Pod template is applied to Pods created later.
5. **Nothing else.** Chain-scoped accounting, ungating, preemption exclusion and the scale-down
   patch are all generic and require no per-integration code.

The two questions to answer before starting are: does the framework's controller restart the
workload if its spec is modified (determining whether the count may be written back), and is its
Pod template applied to Pods created after startup (determining whether template-level gating
reaches them).

# 13. Appendix: hypotheses investigated and disproven

Recorded so they are not re-derived. Each was believed during implementation, then falsified by
evidence. Three of the four concern the accounting in §3.1, which is the most misread behaviour in
this area.

| Hypothesis | Verdict |
|---|---|
| An unretried `Finish` in `Scheduler.replaceOldWorkloadSlice` sustained the double-count window. | **Disproven.** Controller logs show every `Finish` succeeding, via the job reconciler's `NormalizeActiveSlices` path, which *is* retried through reconcile requeue. Windows were 13–95 ms, not indefinite. The no-retry gap is real but was not what was firing. |
| The spec-versus-`status.admission` divergence inflated reported usage. | **Disproven by test.** `totalRequestsFromAdmission` rescales admitted usage to the spec count, so the charge is `min(spec, granted)` (§3.1). Fed the two live workloads from the field capture — spec 3, granted 5 and 7 — the cache reports 4Gi, not 7Gi. This is why §6.2 is hygiene rather than a correctness fix. |
| Two slice-name chains for one job indicated a chain-fork defect. | **Disproven.** The chains belonged to different SparkApplication UIDs across delete-and-recreate cycles — separate job instances, not one job forking. Not a defect, but it is what motivated including the job UID in the chain key (§6.1). |
| Large values in the controller logs were reported queue usage. | **Disproven.** Controller logs never print ClusterQueue status; those values are a pending workload's own requested size, i.e. the growth described in §11.2. |

Two designs were built and rejected:

- **Writing the observed count back to `spec.executor.instances`.** Implemented, then reverted: the
  Spark Operator force-kills and resubmits the application on any `.Spec` change (C-1).
- **Keying chain membership off the replacement-pointer graph.** Rejected: with chain `a → b → c`
  all cached, deleting the middle slice left `a` with no cached replacement, so `a` resumed being
  charged and the overcommit returned (§6.1).

# 14. Implementation inventory

| Component | File | Contribution |
|---|---|---|
| Live-count derivation | `pkg/controller/jobs/sparkapplication/sparkapplication_podset.go` | `liveExecutorCount`, `computeLiveExecutorCount`, `isVerifiedLiveExecutor`, `initialExecutorCount`, `dynamicAllocationExecutorCount`, `workloadSequenceNumber` |
| Pod watch and debounce | `..._executor_pod_handler.go` | `executorPodPredicate`, `isTrackedExecutorPod`, `executorPodHandler`, `schedule` |
| Slice naming, PodSets | `..._controller.go` | `PodSets`, `GetWorkloadNameExtraPart`, `RunWithPodSetsInfo`, `RestorePodSetsInfo` |
| Gate injection and validation | `..._webhook.go` | `Default`, `validateElasticJob` |
| Chain-scoped accounting | `pkg/cache/scheduler/clusterqueue.go` | `sliceChainKey`, `sliceReplacementFor`, `sliceIsLater`, `sliceGroupTip`, `reconcileSliceGroup`, `supersededSliceKeys` |
| Snapshot exposure | `pkg/cache/scheduler/clusterqueue_snapshot.go`, `snapshot.go` | `supersededSlices`, `SliceSuperseded` |
| Preemption exclusion | `pkg/scheduler/preemption/preemption.go`, `.../classical/hierarchical_preemption.go` | Superseded slices skipped in both candidate collectors |
| Grant-aware extractor | `pkg/workload/podsetscounts.go` | `ExtractGrantedPodSetCounts` |
| Ungating cap | `pkg/controller/elasticjobs/elastic_job_ungater.go` | `podsToUngate` caps by the grant |
| Scale-down of grants | `pkg/workloadslicing/workloadslicing.go` | `scaleDownAdmission`, `updatePodSetCountsWithRetry` |
| Admission immutability exception | `pkg/webhooks/workload_webhook.go` | `validateAdmissionUpdate` |
| Shared annotation constant | `pkg/constants/constants.go` | `WorkloadSliceReplacementForAnnotation` |

**One layering note.** `WorkloadSliceReplacementForAnnotation` lives in `pkg/constants` rather than
`pkg/workloadslicing`, because `pkg/cache/scheduler` needs it and `pkg/workloadslicing` imports the
cache — a direct import would be a cycle. `workloadslicing` re-exports it as
`WorkloadSliceReplacementFor` so existing call sites are unchanged.
