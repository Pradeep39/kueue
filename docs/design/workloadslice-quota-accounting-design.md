# Design: Workload-slice quota accounting — overlapping slices, stale grants, ungating beyond grant

## 1. Goal

Make `ClusterQueue.status.flavorsUsage` reflect what an elastic (workload-slice) job actually
holds, so it cannot exceed `nominalQuota`, and so quota is not left reserved for pods that no
longer exist.

Three related defects are addressed. All were found while running SparkApplication with Spark
Dynamic Allocation — the workload that exercises slice replacement hardest — and all live in
generic `ElasticJobsViaWorkloadSlices` code rather than in the SparkApplication integration.

Scope note, stated up front:

- **Defect 1** (§3) — overlapping slices double-counted. Quota correctness: usage could exceed
  `nominalQuota`.
- **Defect 2** (§4) — `status.admission` left stale after a scale-down. **Hygiene only.** It
  was initially believed to cause the observed overcommit; it does not (§4.2). Included because
  it removes real staleness and lets two validation workarounds be retired.
- **Defect 3** (§5) — Pods ungated beyond their grant. Quota correctness in the opposite
  direction: the ledger stays inside `nominalQuota` while the cluster is oversubscribed. Found
  only after 1 and 2 were deployed and a validation harness was watching both invariants.

The two invariants this design is written against, and which the validation harness checks
continuously:

```
CEILING   flavorsUsage <= nominalQuota
GRANT     sum(running pods * per-pod request) <= flavorsUsage
```

Defect 1 breaks CEILING. Defect 3 breaks GRANT. Defect 2 breaks neither.

## 2. Background

### 2.1 How a scale-up is accounted

An elastic job's growth is expressed by creating a *replacement slice*: a new Workload
carrying `kueue.x-k8s.io/workload-slice-replacement-for` pointing at its predecessor, and
`kueue.x-k8s.io/workload-slice-name` naming the chain root. The predecessor is then
`Finish`ed with reason `WorkloadSliceReplaced`.

Two structures account for this, and they disagree by design:

- **The scheduler snapshot.** `flavorassigner.Assignment.append` charges only the *delta*
  when `replaceWorkloadSlice != nil`:

  ```go
  if features.Enabled(features.ElasticJobsViaWorkloadSlices) && a.replaceWorkloadSlice != nil {
      oldRequest := a.findOldPodSetRequest(psAssignment.Name, resource)
      requestAmount -= oldRequest
  }
  ```

  Correct for its consumer: `Scheduler.schedule` does `cq.AddUsage(usage)` against a snapshot
  that already holds the predecessor's full usage, so `old_full + delta = new_full`.

- **`status.admission`.** `Assignment.ToAPI()` is *not* delta-adjusted —
  `PodSetAssignment.Count` and `.ResourceUsage` carry the full new count, because the
  admission record must describe the whole PodSet (pod gating, ungating and reclaim all read
  it).

Both values are right for their own consumer. The hazard is the third consumer: the
**persistent scheduler cache**, which re-derives usage from `status.admission` via
`workload.totalRequestsFromAdmission`, and has no notion of slice replacement at all.

### 2.2 Cluster evidence

Observed on a cluster running two `SparkApplication`s (`als-1`, `als-2`) with Dynamic
Allocation, 512Mi driver and executors, `minExecutors: 3` / `maxExecutors: 30`, against a
6Gi `nominalQuota` (12 executor slots): `flavorsUsage` reported **9Gi**.

A `kubectl get workloads -o json` capture taken during the incident contained ~75 slices, of
which exactly two lacked a `Finished` condition — i.e. only two were eligible to be counted:

| Workload | `spec` executor | `status.admission` executor | Charged |
|---|---|---|---|
| `als-1-b99b3` | 3 | 5 (2560Mi) | 2Gi |
| `als-2-2011a` | 3 | 7 (3584Mi) | 2Gi |

Total real demand: **4Gi**, comfortably inside quota. The gap between that and the reported
9Gi is **not explained by defects 1 or 2** and remains open — see §8.

Controller logs from the same window also showed a replacement slice's own requested size
climbing 6656Mi → 11776Mi (13 → 23 executors) while it waited for quota, because
gate-blocked executor Pods are counted as live by the SparkApplication integration. That is
a separate issue, tracked in §8.

## 3. Defect 1: overlapping slices double-counted in the persistent cache

### 3.1 Mechanism

`Scheduler.admit` calls `assumeWorkload` → `Cache.AddOrUpdateWorkload` **synchronously** with
the new slice's full admission record. The predecessor is removed only when its `Finish`
propagates back — and that is asynchronous:

```go
// replaceOldWorkloadSlice finishes the old slice after the new slice has been
// admitted. ... If this fails, the job reconciler's
// EnsureWorkloadSlices detects both slices admitted and finishes the old one.
```

The predecessor is also deliberately excluded from the preemption targets
(`workloadslicing.FindReplacedSliceTarget` — it is "evicted rather than preempted"), so
nothing removes it synchronously. For the duration of the window the cache holds
`old_full + new_full` instead of `new_full`, and `ClusterQueue.status.flavorsUsage` is
rendered straight from that (`ClusterQueueReconciler.updateCqStatusIfChanged` ←
`Cache.Usage` ← `clusterQueue.AdmittedUsage`).

Measured on the reported cluster the window was 13–95ms per replacement. It is unbounded in
principle: a failed `Finish` is logged and never retried by the scheduler, and any event that
re-adds a not-yet-Finished predecessor re-inflates the ledger.

### 3.2 Fix: account per slice chain, not per slice

`clusterQueue` gains two fields:

```go
sliceGroups         map[string]sets.Set[workload.Reference]
countedSliceInGroup map[string]workload.Reference
```

`sliceChainKey(w)` maps every slice of one elastic job instance to one key, and
`reconcileSliceGroup` keeps exactly the chain's **latest** cached slice charged, moving the
charge when membership changes. Earlier slices in the chain contribute zero: they describe
Pods already accounted for by the slice that superseded them.

This is declarative rather than event-ordered, which matters — it is correct regardless of
whether a `Finish` ever lands, and idempotent if a superseded slice is re-added.

**Chain key.** Primary source is `kueue.WorkloadSliceNameAnnotation`, which
`jobframework.prepareWorkloadSlice` sets on every slice including the chain root. The owning
job's UID (`kueue.x-k8s.io/job-uid`) is folded in: the cluster capture showed chain root
`als-1-7005f` shared by two different SparkApplication UIDs after a delete-and-recreate, and
without the UID those two independent job instances would share a chain and only the newest
would be counted — understating usage. Two defensive fallbacks cover a slice missing the
chain-root annotation. Workloads with no elastic or slice annotation return `""` and keep the
original, allocation-free path.

**Ordering within a chain.** `sliceIsLater` uses creation timestamp first (matching
`FindNotFinishedWorkloads`' existing ordering), then the replacement annotation to break
same-second ties, then UID for determinism.

An earlier iteration keyed directly off the replacement-pointer graph. It was rejected: with
chain `a → b → c` all cached, deleting the middle slice `b` left `a` with no cached
replacement, so `a` resumed being counted and the overcommit returned (7Gi against 6Gi in
test). Chain grouping has no such hole.

### 3.3 Preemption interaction

A superseded slice's usage is not in the ledger, so it must not be offered as a preemption
candidate: `SimulateWorkloadRemoval` would subtract usage that was never added, understating
the ClusterQueue and over-admitting. Preempting one would free nothing anyway — its Pods are
attributed to its replacement.

`ClusterQueueSnapshot` therefore carries `supersededSlices`, exposed as `SliceSuperseded`,
and both candidate collectors skip them:
`classical.getCandidatesFromCQ` and `preemption.findCandidatesForPolicy` (whose signature
changes from a workload map to the owning `*ClusterQueueSnapshot`).

The snapshot needs no other change: `snapshotClusterQueue` clones the already-netted
`resourceNode`, so it inherits the corrected totals rather than re-summing workloads.

## 4. Defect 2: `status.admission` left stale after a scale-down

### 4.1 Mechanism

`EnsureWorkloadSlices` handles a scale-down by patching the existing slice in place
(`updatePodSetCountsWithRetry` → `workload.ApplyPodSetCounts` → `Update`). That writes
`spec.podSets[].Count` only; `status.admission.podSetAssignments[].Count` keeps the value
granted at admission.

The result is visible throughout the cluster capture — `als-1-dfa01` has spec count 22
against granted 5; `als-1-fb3ea` has spec 0 against granted 4.

### 4.2 What this does *not* cause — corrected

It does **not** inflate `ClusterQueue` usage. `totalRequestsFromAdmission` rescales the
admitted usage down to `podSetsCountsAfterReclaim` — the spec count — whenever the spec count
is lower:

```go
currentCounts := podSetsCountsAfterReclaim(wl)      // = spec.podSets[].Count (minus reclaimable)
...
if countAfterReclaim := currentCounts[psa.Name]; countAfterReclaim < setRes.Count {
    setRes.Requests.Divide(int64(setRes.Count))
    setRes.Requests.Mul(int64(countAfterReclaim))
}
```

So the cache effectively charges `min(spec, granted)`. Fed the two live workloads from the
capture verbatim (spec 3, granted 5 and 7), it reports **4Gi**, not 7Gi. This is pinned by
`TestClusterQueueUsageFrozenAdmissionCountDoesNotOvercommit`, which exists specifically to
stop this wrong conclusion being drawn again — it is an easy and tempting inference from
"usage is derived from `status.admission`".

### 4.3 Why fix it anyway

The stale grant is load-bearing in the wrong places:

- Validation has accumulated workarounds for it — `scaledDownPodSetNames` and
  `isPreexistingStaleCount` in `pkg/webhooks/workload_webhook.go` both exist to tolerate
  counts that outlive a scale-down. With the grant kept in step, both become no-ops and can
  eventually be retired.
- `flavorassigner` computes a replacement's delta as
  `new_spec_count − old_admission_count`. Once the granted count is frozen above the real
  one, that delta is zero or negative, so a replacement is admitted without the absolute
  total ever being compared against `nominalQuota`. The ledger is still correct today thanks
  to §4.2, but the admission path is reasoning from a stale number, which is fragile.
- `status.admission` is what operators and `kubectl` show. Reporting a grant of 7 for a job
  running 3 pods is misleading.

### 4.4 Fix

`scaleDownAdmission(wl, counts)` lowers each `PodSetAssignment.Count` that exceeds its new
spec count, rescales `ResourceUsage` proportionally (it is the podSet *total* — the cluster
capture confirms: count 7 ↔ 3584Mi), and truncates any `TopologyAssignment` with
`utiltas.TruncateAssignment` so TAS domain accounting stays consistent.

It is applied by `updatePodSetCountsWithRetry` after the spec update lands, as a separate
`Status().Update` with conflict retry, because admission lives on the status subresource. A
failure there leaves the previous (stale-grant) behavior and is retried on the next
reconcile, so it degrades rather than corrupting.

### 4.5 Webhook: the immutability exception

`validateAdmissionUpdate` made `status.admission` **fully immutable** once set, except for
TAS topology assignments. Without a change there the patch above is rejected by Kueue's own
validating webhook.

The exception added is narrow: for a workload that is elastic *and* with
`ElasticJobsViaWorkloadSlices` enabled, a `PodSetAssignment.Count` that **decreases** is
accepted along with its `ResourceUsage`. Everything else — a count that increases, a flavor
change, a podSet-count mismatch — is still rejected, so growing a grant still has to go
through the scheduler where quota is checked.

This mirrors the exception `validateImmutablePodSet` already makes for `spec.podSets[].Count`
on elastic jobs. **Non-elastic workloads keep a fully immutable admission**, so plain
`batch/v1` Job behavior under `ElasticJobsViaWorkloadSlices` is unchanged — that property is
the reason this shape was chosen over relaxing immutability generally.

## 5. Defect 3: Pods ungated beyond their grant

Found by running the validation harness against three concurrent Dynamic-Allocation
SparkApplications after defects 1 and 2 were deployed. The ceiling invariant held — usage
never exceeded `nominalQuota` — but a second invariant broke: **running Pods × 512Mi exceeded
`flavorsUsage`**, i.e. Pods were running that no grant covered.

### 5.1 Evidence

One incident, three jobs, one live slice each:

| Job | granted `[driver, executor]` | running | Δ |
|---|---|---|---|
| als-1 | `[1, 2]` = 3 slots | 1 driver + **3** executors = 4 | **+1** |
| als-2 | `[1, 3]` = 4 slots | 1 driver + 3 executors = 4 | 0 |
| als-3 | `[1, 4]` = 5 slots | 1 driver + 4 executors = 5 | 0 |

Grants summed to 12 slots = 6144Mi, exactly the reported `flavorsUsage`, and exactly
`nominalQuota`. So the ledger was internally consistent and inside quota; reality had one Pod
more. The scheduler log confirms `als-1`'s slice was admitted at executor count 2
(`resourceUsage: 1Gi, count: 2`) while its `spec.podSets[executor].count` read 3.

Sustained across 39 samples and four incidents in one run, always exactly one Pod per affected
job.

### 5.2 Mechanism

`elasticJobUngater.podsToUngate` capped ungating by the **requested** count:

```go
granted := workload.ExtractPodSetCountsFromWorkload(wl)   // returns wl.Spec.PodSets counts
```

despite the variable name and the comment above it both saying *granted*. For an elastic job
the request can exceed the grant (§2.1: a scale-up creates a replacement slice instead of
growing the grant; a stale read can raise the request on an already-admitted slice), so the
ungater authorized more Pods than the scheduler had paid for.

This is the mirror image of defect 1 and arguably worse. An over-count is visible — usage
climbs above `nominalQuota` and someone notices. An under-count is invisible: the queue looks
healthy and correct, and Kueue admits further work into capacity that is already in use.

### 5.3 Fix

`workload.ExtractGrantedPodSetCounts` reads `status.admission.podSetAssignments[].Count`, and
`podsToUngate` uses it. `PodSetAssignment.Count` is optional in the API — the scheduler always
sets it via `Assignment.ToAPI`, but an assignment lacking it falls back to the PodSet's own
count, matching `totalRequestsFromAdmission` so a hand-written or legacy admission is not read
as a grant of zero.

`ExtractPodSetCountsFromWorkload` keeps its behavior and gains a doc note pointing at the new
function, since the naming is what made this defect easy to introduce and easy to miss on
review.

## 6. Mechanisms investigated and ruled out

Recorded so they are not re-litigated:

- **Unretried `Finish` as the sustaining cause.** Hypothesised that
  `Scheduler.replaceOldWorkloadSlice`'s unretried failure held the double-count window open
  indefinitely. Controller logs show no such failure: every `Finish` succeeded, via the job
  reconciler's `NormalizeActiveSlices` path (which *is* retried through reconcile requeue),
  with 13–95ms windows. The no-retry gap is real but was not what was firing.
- **Frozen granted counts as the ceiling violation.** See §4.2 — disproved by test.
- **`als-1`'s two slice chains as a chain fork.** Chain `7a41b` belongs to job UID
  `76b1805a…`; chain `7005f` to `b1f31f4d…` and `c7afad18…`. Those are different
  SparkApplication objects across delete-and-recreate cycles, not one job forking. Not a bug
  — but it is what motivated the job-UID component of the chain key (§3.2).

Upstream search found no existing issue or PR for either defect. Related but distinct:
`#11195` (introduced the admit-new-then-finish-old ordering), `#12538` (double-subtraction of
preempted slice usage), and the `#12958` / `#13044` / `#12670` / `#13117` chain
(reclaimable-pods-after-scale-down, which operates on `Status.ReclaimablePods`).

## 7. Testing

| Test | Covers |
|---|---|
| `cache/scheduler: TestClusterQueueUsageWorkloadSliceScaleUpDoubleCount` | Defect 1 with the reported cluster's shape: 5Gi not 9Gi against 6Gi |
| `cache/scheduler: TestClusterQueueSliceReplacementUsageAccounting` | 13 cases — both arrival orders, in-place update, re-add after delete, replacement rollback, 3-chains, middle-slice deletion, racing replacements, recreated-job chain-root collision, non-slice workloads |
| `cache/scheduler: TestClusterQueueSliceReplacementDisabledFeatureGate` | Grouping inert with the gate off |
| `cache/scheduler: TestClusterQueueUsageFrozenAdmissionCountDoesNotOvercommit` | §4.2 invariant: a frozen grant is charged at the spec count |
| `scheduler: TestScheduleWorkloadSliceScaleUpDoesNotOvercommit` | Defect 1 end-to-end through `scheduler.schedule()`, with the `Finish` patch forced to fail |
| `scheduler/flavorassigner: TestWorkloadSliceAssignmentUsageIsDeltaButAPICountIsFull` | The §2.1 delta-vs-full asymmetry, as an intended invariant |
| `webhooks: TestValidateAdmissionUpdateElasticScaleDown` | §4.5: decrease allowed for elastic; increase, flavor change, and non-elastic decrease all rejected |
| `webhooks: TestValidateAdmissionUpdateElasticGateOff` | Exception inert with the gate off |
| `workloadslicing: TestEnsureWorkloadSlices` (scale-down cases) | Grant shrinks with the spec |
| `elasticjobs: TestReconcile/"cap is the granted count, not the requested count"` | Defect 3: spec 3, grant 2, three gated Pods, exactly two ungated |

Every defect-1 and defect-3 test was negative-controlled: with the fix disabled they fail with
the bug's signature (9Gi end-to-end and 7Gi/9Gi in the defect-1 unit cases; all three Pods
ungated for defect 3), confirming they are not tautologies.

Fixing defect 3 also required correcting five pre-existing `TestReconcile` fixtures whose
admission records were internally inconsistent — they declared `resourceUsage` for N Pods while
leaving `PodSetAssignment.Count` at the wrapper default of 1, and passed only because the cap
was read from the spec. They now set `Count` to match, which is what the scheduler always
produces.

`go build ./...`, `go vet ./pkg/...`, `gofmt`, `go test ./pkg/...` and
`go test -race ./pkg/cache/scheduler ./pkg/scheduler/...` all pass.

Not covered by unit tests: envtest and live-cluster integration tests, which the development
sandbox cannot run.

### 7.1 Cluster validation

Defects 1 and 2 were deployed to the reporting cluster and exercised with two, then three,
concurrent Dynamic-Allocation SparkApplications against the same 6Gi `nominalQuota` that
originally reported 9Gi.

A validation harness sampled both invariants every 2s, reading a pods snapshot *before* the
ClusterQueue snapshot (so a scale-up landing between the two biases against false GRANT
positives) and re-reading once before reporting any violation.

Two-app run, ~30 samples: `flavorsUsage` tracked live Pods exactly at 512Mi each (5→2560Mi,
8→4Gi, 9→4608Mi, 10→5Gi, 11→5632Mi, 12→6Gi) and **never exceeded 6Gi**, including sustained
stretches pinned at the cap. Previously the same workload reported 9Gi.

Three-app run, 316 samples: **CEILING held on every sample** (peak usage 6144Mi = exactly
`nominalQuota`). **GRANT failed on 39 samples** across four incidents — which is how defect 3
was found. Peak gated Pods reached 44 against a 12-slot quota, quantifying open item 2.

## 8. Open items

1. **The original 9Gi overcommit is resolved empirically, mechanism unconfirmed.** Across 346
   post-fix samples spanning two- and three-app runs, `flavorsUsage` never exceeded
   `nominalQuota` (§7.1), where the same workload previously reported 9Gi. But the pre-fix
   capture only ever accounted for 4Gi of live demand, so the 5Gi gap was never explained, and
   no post-fix measurement distinguishes *which* mechanism was responsible. The leading
   hypothesis remains that the cache retained usage for slices already `Finished` in the API —
   most of the ~75 slices in the capture were Finished, many stuck terminating with the
   `kueue.x-k8s.io/resource-in-use` finalizer — which defect 1's fix suppresses regardless of
   whether the `Finish` landed. Confirming it needs
   `kueue_cluster_queue_resource_usage` compared against the live-workload sum at the same
   instant, on a build *without* the defect-1 fix. Treat this as fixed-in-practice,
   not root-caused.
2. **Gated executor Pods inflate the requested count.** `isVerifiedLiveExecutor` counts any
   Pod not `Succeeded`/`Failed`, including Pods still blocked by
   `kueue.ElasticJobSchedulingGate`. A replacement slice that cannot be admitted therefore
   keeps growing (observed: 13 → 23 executors against a 12-slot quota) while DA adds more
   gated Pods, resolving only when `executorIdleTimeout` reaps them — a 23s admission delay
   with heavy reconcile-conflict churn. The three-app run reached **44 gated Pods**. Fixing
   defect 3 makes this *more* visible, because surplus executors now correctly stay `Pending`
   instead of running. Note the trap: gated Pods must keep being counted or scale-up is never
   detected at all, so the fix has to bound or clamp the requested count rather than filter
   them out. Not addressed here.
3. **`Finish` is not retried** by `Scheduler.replaceOldWorkloadSlice`. Defect 1's fix makes
   this harmless for quota accounting, so it is left alone, but it remains a latent gap.
4. Retiring `scaledDownPodSetNames` / `isPreexistingStaleCount` once §4.4 has shipped long
   enough that no pre-existing objects carry stale grants.

## 9. Files changed

- `pkg/cache/scheduler/clusterqueue.go` — chain grouping, `sliceChainKey`, `sliceIsLater`,
  `sliceGroupTip`, `reconcileSliceGroup`, `supersededSliceKeys`.
- `pkg/cache/scheduler/{cache,clusterqueue_snapshot,snapshot}.go` — map init, snapshot
  `supersededSlices` + `SliceSuperseded`.
- `pkg/scheduler/preemption/preemption.go`,
  `pkg/scheduler/preemption/classical/hierarchical_preemption.go` — skip superseded
  candidates.
- `pkg/constants/constants.go` — `WorkloadSliceReplacementForAnnotation`, moved here so
  `pkg/cache/scheduler` can read it without importing `pkg/workloadslicing` (which imports
  the cache); re-exported as `workloadslicing.WorkloadSliceReplacementFor`.
- `pkg/workloadslicing/workloadslicing.go` — `scaleDownAdmission`, applied from
  `updatePodSetCountsWithRetry`.
- `pkg/webhooks/workload_webhook.go` — elastic scale-down exception in
  `validateAdmissionUpdate`.
- `pkg/workload/podsetscounts.go` — `ExtractGrantedPodSetCounts`, plus a doc note on
  `ExtractPodSetCountsFromWorkload` distinguishing requested from granted counts.
- `pkg/controller/elasticjobs/elastic_job_ungater.go` — `podsToUngate` caps by the grant.
- `docs/design/sparkapplication-executor-quota-gate-design.md` — §7 records the ungating-cap
  correction against the design that introduced it.
