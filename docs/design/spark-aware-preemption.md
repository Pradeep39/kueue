---
title: "Spark-Aware Partial Preemption in Kueue"
subtitle: "Reclaim executors, not applications"
author: "Pradeep Reddy"
date: "2026-10-12"
---

# 0. About this document

This document proposes a design for preempting Spark applications. Kueue preempts **only the
executors it needs**. It never touches the driver, and it never takes an application below a
protected floor. Today Kueue evicts the whole application instead.

It builds on the elastic quota management design in
[`spark-elastic-quota-management.md`](./spark-elastic-quota-management.md), referred to as **EQM**
below. Read that design's slice accounting (EQM §10), executor gating (EQM §9) and quota release on
eviction (EQM §12) first. This document assumes them.

Status: **design, not implemented.** Code references are to `main` at `4a79c790d`.

Related work:

- [kubernetes-sigs/kueue#16233](https://github.com/kubernetes-sigs/kueue/issues/16233) states the
  requirements. It is also filed internally as kubernetes-sigs/kueue#44.
- KEP-975 is upstream's generic partial preemption proposal:
  [#14589](https://github.com/kubernetes-sigs/kueue/pull/14589) is the KEP and
  [#14590](https://github.com/kubernetes-sigs/kueue/pull/14590) the proof of concept. It is deferred
  to v0.21.
- [#16806](https://github.com/kubernetes-sigs/kueue/issues/16806) tracks upstream elastic
  SparkApplication support via workload slices.

---

# 1. Summary

When a higher-priority workload needs capacity, Kueue currently evicts whole lower-priority
workloads. A Spark victim is suspended and later **restarts from scratch**.

Under Dynamic Allocation the preemptor grows in small steps, and each step is an independent
preemption decision. So a one-executor scale-up evicts a four-slot application, again and again.

We propose a **partial-reclaim preemption target** for elastic Spark applications:

1. **Plan.** The scheduler takes only executors above each victim's protected floor. It spreads the
   loss across **as many victims as possible**, one executor from each in turn, so that each
   application loses as few executors as possible.
2. **Execute in two phases.** The victim's job reconciler deletes the chosen executor Pods. Each one is
   told to leave through Spark's graceful decommissioning, which migrates its shuffle and cached blocks
   to the executors that stay before it exits. Only after the Pods have stopped is the victim's grant
   lowered, which releases the quota to the preemptor.
3. **The reduction holds without Spark cooperating.** Spark recreates the executors it lost. Those
   Pods are born gated, and the ungater, which is capped at the grant, leaves them gated.
4. **Grow-back comes naturally.** The victim's live count now exceeds its grant, so a scale-up slice
   asks for the difference. That slice is admitted when quota frees up, and it is never allowed to
   preempt.
5. **Whole-application eviction remains the fallback.** It is used only when even reclaiming every
   victim down to its floor cannot fit the preemptor's own minimum.

The design works for Spark applications **with or without Dynamic Allocation**, provided they take the
elastic path (`kueue.x-k8s.io/elastic-job: "true"`). It needs no Spark change. Kueue turns on Spark's
existing decommissioning settings when a reclaimable application is created (§5.4).

# 2. Motivation

## 2.1 What whole-application eviction costs

The baseline comes from five preemption runs: three Dynamic Allocation SparkALS applications on a
12-slot cohort, where one slot is 1 CPU plus 896Mi. Two applications are low priority and one is high
priority. All capture streams were clean, with no watch restarts. The full tables are in
`/tmp/preempt-analysis/summary.md`.

| Measure | Value |
|---|---|
| Evictions | 50, every one with a single victim |
| Evictions where the preemptor needed ≤ 1 slot | 36 |
| Slots freed against slots needed | 248 freed vs 89 needed (median 4 freed per eviction) |
| Low-priority application restarts | 47 |
| Work discarded | 4,856 executor-seconds plus 1,619 driver-seconds |
| A low-priority application unfinished at the end of the run | 4 of 5 runs |

The worst single application was evicted and restarted **8 times** in one run.

A Spark application evicted by preemption loses all of its progress. The operator resubmits it through
`PENDING_RERUN`, which starts it over rather than resuming it.

## 2.2 Why reclaiming exactly the shortfall is not enough

A counterfactual replay of the same runs separates two policies.

**Partial reclaim without a floor or a preemptor limit** halves evictions, from 50 to 25 with a floor
of 1 executor. Most victims are still pushed out a few seconds later, because the high-priority
application keeps bursting to 11–12 of the 12 slots. A victim shrunk and then evicted at the burst
peak can lose *more* work than one evicted immediately.

**A protected floor per victim, with the preemptor admitted only partially above those floors**,
removes all 47 restarts and all 4,856 discarded executor-seconds. The cost is 461 slot-seconds denied
to the high-priority application across the five runs.

Two conclusions follow:

1. **The floor and the preemptor limit are the design, not refinements of it.** Without them, partial
   reclaim mostly postpones the eviction.
2. **The floor must be tunable below `minExecutors`.** Protecting two victims at
   driver + `minExecutors: 3` leaves a 12-slot cohort with no room for the preemptor to grow at all.

## 2.3 Why not wait for KEP-975

KEP-975 has the right core, and this design keeps it:

- only the elastic PodSet shrinks
- Kueue decides *how many*, and the runtime decides *which*
- quota is released only after the Pods are gone

It falls short for Spark in five ways:

| KEP-975 | Gap for Spark |
|---|---|
| Uses `PodSet.MinCount` as the floor | The Spark integration sets `MinCount` to the live executor count for partial scale-up (`sparkapplication_controller.go:263`), so it would protect every executor. Reviewers on #14589 already questioned reusing it. |
| Greedy: drains one victim, larger ones first | The opposite of spreading the loss, which #16233 item 3 asks for. |
| No total reclaim cap and no timeout fallback | Both are needed to bound the preemptor's wait. |
| No grow-back | A reclaimed victim never recovers its executors. |
| Releases quota by lowering the runtime's Workload spec | Not slice-aware: superseded slices and replacement probes are not handled. |

This design fills those gaps in a form that can be offered upstream as extensions to KEP-975 (§9).

# 3. Goals and non-goals

**Goals:**

- **PG-1:** never preempt the driver. Only the executor PodSet is reclaimable.
- **PG-2:** never take a victim below a protected floor of executors, configured per application.
- **PG-3:** reclaim only what the preemptor needs. Spread it across as many victims as possible in
  the lowest eligible priority tier, taking as few executors from each as possible.
- **PG-4:** release quota only after the reclaimed executors have stopped, so SC-1 and SC-2 (EQM §4)
  hold throughout.
- **PG-5:** let a reclaimed victim grow back as quota frees up, without preempting anyone to do so.
- **PG-6:** apply to elastic Spark applications with or without Dynamic Allocation.
- **PG-7:** require no Spark change and no polling of Spark metrics.
- **PG-8:** leave workloads that have not opted in exactly as today.

**Non-goals:**

- **PNG-1: driver-side executor selection.** Kueue picks which executors to delete in v1. A driver
  plugin that lets Spark choose is phase 2 (§10).
- **PNG-2: fair-sharing preemption.** In v1, fair-sharing preemption keeps whole-workload eviction
  (§5.3).
- **PNG-3: Topology-Aware Scheduling.** PodSets with a topology request are not reclaimable
  (`PodSetResources.ScaledTo` does not scale TAS assignments).
- **PNG-4: other job kinds.** The Workload-level contract is generic (§5.2), but only Spark integrations
  implement it in v1.
- **PNG-5: MultiKueue.**

# 4. Requirements and success criteria

| ID | Requirement |
|---|---|
| PR-1 | A preemption that partial reclaim can satisfy MUST NOT evict a whole application. |
| PR-2 | The driver PodSet MUST never be reduced. |
| PR-3 | A victim's granted executors MUST NOT go below its resolved floor through partial reclaim. |
| PR-4 | Quota from a partial reclaim MUST be released only after the reclaimed executor Pods are terminal or gone, or after the reclaim timeout. |
| PR-5 | Executors recreated by Spark after a reclaim MUST stay gated until quota is granted for them. |
| PR-6 | A victim's grow-back MUST NOT preempt. |
| PR-7 | With the feature gate off, or for a workload that has not opted in, behaviour MUST be unchanged. |
| PR-8 | SC-1 and SC-2 MUST hold at every point of a reclaim. |

**Success, measured with the EQM harness on the baseline scenarios plus a static variant:**

- no whole-application eviction where the floors allowed a partial reclaim
- SC-1 and SC-2 hold on every check
- restarts and discarded executor-seconds well below the §2.1 baseline (target: zero restarts in the
  two-low, one-high scenario)
- preemptor wait bounded by the reclaim timeout

# 5. Design

## 5.1 Scope and opt-in

Partial reclaim applies to a victim only when all of the following hold:

- the Kueue feature gate `SparkPartialPreemption` (alpha, default off) is on;
- the victim's Workload declares a reclaimable PodSet and a floor (§5.2);
- that PodSet has no topology request.

Every other candidate is handled by today's whole-workload eviction.

The Spark integrations declare reclaimability for every application on the elastic path. That
includes static applications, because the elastic path is keyed off the annotation, not Dynamic
Allocation: `isAnElasticJob` is `workloadslicing.Enabled`. A static elastic application already gets
the gated executor template and slices. Its count is simply fixed at `spark.executor.instances`.

Applications without the elastic annotation are evicted whole, as today. That keeps EQM's R-9
guarantee: plain Workloads stay fully immutable.

## 5.2 The floor

**The Spark property.** The floor is set with a sparkConf key Kueue reads but never writes:

```yaml
spec:
  sparkConf:
    "spark.kueue.preemption.minExecutors": "2"
```

The name was chosen with care:

- **It survives `spark-submit`.** `spark-submit` drops keys that do not start with `spark.`
  (`SparkSubmitArguments.scala:180`). Spark ignores `spark.*` keys it does not recognise.
- **`spark.kueue.` is unused** in both Spark and Kueue.
- **It avoids `spark.dynamicAllocation.*`.** Serverless Spark strips that namespace, and the floor must
  also apply to static applications.
- **It avoids `spark.kubernetes.*`,** which Spark owns.

Serverless Spark passes all other `spark.*` keys through.

**Resolution**, executors only (the driver is always protected on top of the floor):

| Case | Floor |
|---|---|
| `spark.kueue.preemption.minExecutors` set | that value |
| unset, Dynamic Allocation on | `minExecutors`, resolved structured-field-first as in EQM §8.3, raised to **at least 1** |
| unset, Dynamic Allocation off | `spark.executor.instances`, so a static application is evicted whole until it opts in |

- **Capping.** A floor above the executor count is capped at the count. Such an application is never
  partially reclaimed.
- **No floor of 0.** Spark's own default `minExecutors` is 0. A Dynamic Allocation application left
  with only its driver makes no progress, so the floor is never below 1.
- **Why `minExecutors` is not used for static applications.** It is inert there: Spark reads it only in
  `ExecutorAllocationManager` and in branches taken when Dynamic Allocation is on
  (`SchedulerBackendUtils.scala:34-35`, Spark 4.0 and master). Serverless Spark strips it anyway.
- **The property needs the elastic path.** On an application without
  `kueue.x-k8s.io/elastic-job: "true"` the floor is ignored and the application is preempted whole, as
  before. Because the property alone looks like an opt-in, the webhook returns an admission warning
  (never a rejection) when it is set without the annotation, on create and whenever an update adds or
  changes it.
- **Malformed values.** The Kubeflow webhook rejects a malformed value, as it does a malformed
  `spark.executor.instances`. kueue-workload, whose webhook never rejects, treats it as unset and
  emits an event.

**On the Workload.** The scheduler reads Workloads, not jobs. So the integration stamps the resolved
floor onto every slice it builds, through a new optional jobframework interface that sits next to
`ElasticWorkloadNameProvider` (`interface.go:239`):

```go
// JobWithPartialPreemption is implemented by jobs whose elastic PodSet can be partially preempted.
type JobWithPartialPreemption interface {
	// PartialPreemptionFloor returns the reclaimable PodSet and the count it must never go below.
	PartialPreemptionFloor() (podSet kueue.PodSetReference, minCount int32, ok bool)
	// ReclaimPods deletes n running Pods of the PodSet, never below the floor (§5.4).
	ReclaimPods(ctx context.Context, c client.Client, podSet kueue.PodSetReference, n int32) error
}
```

jobframework writes the floor as Workload annotations:

| Annotation | Value |
|---|---|
| `kueue.x-k8s.io/partial-preemption-podset` | `executor` |
| `kueue.x-k8s.io/partial-preemption-min-count` | the resolved floor |

A Workload without these annotations is not reclaimable. This is why the driver can never be
reclaimed: it is never named.

## 5.3 Planning: who gives up how much

**Where it plugs in.** `getAssignments` (`scheduler.go:1563`) runs `planner.Plan` (`:1595`) and returns
as soon as preemption can fit the full delta (`CanFit()`). Reduced counts are tried only when even
preemption cannot fit (`:1597-1606`), and then each of those also plans with preemption. Victims are
always whole Workloads:

- `Target` holds only a `WorkloadInfo` (`policy/types.go:32`);
- `YieldFromSnapshot` removes the victim's full usage (`policy/types.go:66-68`).

**Partial targets.** `Target` gains a reclaim amount:

```go
type Target struct {
	WorkloadInfo *workload.Info
	// Reclaim, when non-nil, means "reduce this PodSet by Count" instead of "evict".
	Reclaim *PartialReclaim
	...
}
type PartialReclaim struct {
	PodSet kueue.PodSetReference
	Count  int32
}
```

A partial target's yielder removes only `Count × per-Pod usage` from the snapshot, via
`PodSetResources.ScaledTo`, instead of the victim's whole usage. Superseded slices are already
excluded from candidates (`preemption.go:511`), so a reclaim always targets a chain's admitted tip.

**Order of attempts.** For a preemptor, with the gate on, the planner tries each step in turn and
stops at the first that fits:

1. **The full delta into free quota**, as today.
2. **A reduced count into free quota** (preemptor `MinCount` only). This is new: a smaller increment
   that fits without preemption beats any preemption.
3. **The full delta by partial reclaim.**
4. **A reduced count by partial reclaim** (preemptor `MinCount` only). This is the **preemptor
   limit**: the preemptor takes what lies above the floors rather than evicting anyone. **On by
   default.**
5. **The fallback** (below).

**The fallback.** A ClusterQueue annotation chooses the fallback:
`kueue.x-k8s.io/partial-preemption-fallback`, either `Evict` or `Wait`.

- **`Evict` (default)** runs today's whole-workload preemption.
- **`Wait`** leaves the preemptor pending.

**A scale-up slice never whole-evicts a workload that opted into partial preemption.** It belongs
to a workload that is already running. Evicting a whole application so that another can grow by a few
executors is the thrash this design exists to stop. So growth takes what lies above opted-in victims'
floors, or waits, whatever the ClusterQueue's fallback setting. Its fallback (`WholeOnlyUndeclared`)
may still evict workloads that did not opt in, exactly as before, which keeps PR-7. This was found on
the cluster (§11a): with every victim at its floor, a one-executor scale-up evicted a whole
application.

Because step 4 comes first, whole eviction only happens when the preemptor's own minimum cannot fit
even after every eligible victim is reduced to its floor. In the baseline, that is a high-priority
application's *first* admission, its driver plus initial gang (5 of the 50 evictions). It is not a
scale-up step.

**Spreading the loss: as many victims as possible, as few executors from each.** Losing one executor
is a minor disruption to a Spark application. Losing many at once means recomputing more stages and
risks dropping it to its floor. So the reclaim is spread as widely as the priority rules allow, as
#16233 item 3 asks. Within steps 3 and 4:

1. **Tiers.** Eligible candidates are grouped by priority tier, lowest first. A higher tier is touched
   only once every victim in the lower tiers is at its floor, which keeps preemption's priority
   semantics.
2. **Order within a tier.** Victims are ordered by headroom above their floor, largest first. Ties go
   to the most recently admitted, which has the least work at stake.
3. **Round-robin.** Each round takes **one executor from every victim in the tier that still has
   headroom**. Only then does any victim give up a second executor. Rounds repeat until the shortfall is
   met. The last round may be partial, and then follows the order in step 2.
4. **Fill-back.** Fill-back (`fillBackWorkloads`, `preemption.go:425`) then returns any surplus. It
   returns from the victims that lost the most first, so the spread stays even.

With five victims and a shortfall of five executors, each victim gives up one executor and none is
asked for two. The result is a set of partial targets, often one per eligible victim, each usually
reducing its victim by a single executor.

This deliberately disturbs *more* applications, each by less. Grow-back (§5.5) returns each one's
executor as quota frees up.

**First, executors the driver has already given up.** Some executors cost nothing to reclaim: their own
driver has already decided to remove them. Spark 4.2+ marks these with Kubernetes'
`controller.kubernetes.io/pod-deletion-cost` annotation (§5.4).

The integration counts each application's ungated executors whose cost is below the default of 0. It
publishes the count on the Workload as `kueue.x-k8s.io/partial-preemption-marked-pods`, refreshed from
the Pod watch EQM already has (EQM §8.6).

Before the round-robin, each tier is **swept for marked executors first, across all of its victims, in
ascending cost order**, still never below a floor. Only the shortfall left after that sweep is spread
round-robin. So when the driver has already given up enough executors, the preemptor is satisfied
without disturbing any work.

**Fair sharing.** In v1 only the classical strategy produces partial targets. Fair-sharing preemption
keeps whole eviction. Its share simulation (`fairsharing/target.go`) works on whole Workloads, and
§5.5's grow-back rule has not been analysed against DRS ordering.

## 5.4 Execution: two phases, quota last

`IssuePreemptions` (`preemption.go:274`) evicts a whole target with `workloadevict.Evict` (`:313`).
For a partial target it instead:

- writes `kueue.x-k8s.io/partial-preemption-target: "<new executor grant>"` on the victim Workload;
- sets a `PartiallyPreempted` condition naming the preemptor and the count.

It records a preemption expectation, so the preemptor is not planned again while it waits.

The victim's job reconciler sees the target and runs the reclaim:

1. **Delete the Pods, which tells each one to decommission.** It calls
   `ReclaimPods(n = granted − target)`. The Spark integration **sweeps the victim's ungated executor
   Pods in ascending `controller.kubernetes.io/pod-deletion-cost` order** until it has chosen `n`. A Pod
   without the annotation counts as cost 0, the Kubernetes default. Ties are broken by:
   - Pods not yet bound to a node first
   - then Running Pods, newest first, since they have done the least work

   It never deletes the driver, and never deletes below the floor. Gated Pods are left alone: they
   hold no grant, so deleting them frees nothing.

   This is the same rule a ReplicaSet uses to scale down (KEP-2255), and Karpenter reads the same
   annotation. So any producer of costs is honoured, and Kueue never polls Spark metrics.

   Each Pod is deleted with its own `terminationGracePeriodSeconds`. Deletion is how the executor
   learns it is being evicted (below).
2. **Wait for the decommission to finish.** It waits until those Pods are terminal or gone. The ungater counts deleting Pods as still
   ungated, so while the grant is still high, `room = granted − ungated` cannot rise and nothing new is
   ungated.
3. **Lower the grant, and only then.** It lowers the victim's executor grant to the target with the
   in-place scale-down EQM already uses (`scaleDownAdmission`, `workloadslicing.go:603`). That path is
   allowed by the decrease-only `validateAdmissionUpdate` exception (`workload_webhook.go:410`). The
   cache charges `min(spec, granted)` (EQM §10.1), so usage drops by exactly the reclaimed executors.
   The preemptor's next scheduling pass sees the free quota.
4. **Clear.** It removes the target annotation and resolves the condition.

**Why the order is load-bearing.** Lowering the grant before the Pods stop would hand their quota to
the preemptor while they still run. That is the SC-2 breach EQM §12's `IsActive()` fix exists to
prevent.

**Timeout.** If the reclaimed Pods are not gone within the timeout, the grant is lowered anyway, with a
warning event. The timeout is `kueue.x-k8s.io/partial-preemption-timeout` on the ClusterQueue, and
defaults to 60s: the 30s executor grace period plus 30s.

This matches `IsActive()`'s treatment of Pods stuck terminating on a lost node, which do not pin quota.
It bounds the preemptor's wait, as #16233 asks. Escalating to whole eviction would not help, because
the same stuck Pods would block that too.

**Races:**

- **The victim has a pending scale-up slice.** That slice is not admitted while a reclaim on its
  predecessor is outstanding, so a replacement never lands mid-reclaim. Its count is re-derived once
  the reclaim clears.
- **The victim finishes or is evicted mid-reclaim.** The target is dropped, and normal release (EQM §12)
  applies.
- **A second preemptor arrives.** It plans against a snapshot in which the victim is already at its
  target. That snapshot comes from the expectation, as today.

**Where deletion costs come from.** The sweep needs no producer. Without costs, every Pod is 0 and the
tie-breaks decide.

**What Spark 4.2+ writes** (branch-4.2, `KubernetesClusterSchedulerBackend.scala:205-275`):

- **Only with the Deployment allocator.** The annotation is written only when
  `spark.kubernetes.allocation.pods.allocator` is `deployment`. That allocator also stamps every
  executor Pod template with cost `0` (`DeploymentPodsAllocator.scala:157-161`). The default `direct`
  allocator never writes it.
- **It is binary, not a ranking.** `spark.kubernetes.executor.podDeletionCost` is a single value. Spark
  writes it only on executors the driver has chosen to remove:
  - executors Dynamic Allocation found idle (`doKillExecutors`)
  - executors the driver decommissions
- **The configured value must be below 0** for those Pods to sort first. Spark's own documentation
  says to enable it only with the Deployment allocator and Dynamic Allocation.
- **Static applications are never marked.** Spark marks Pods only when it removes executors itself,
  which a static application does not do.

**Phase 2's driver plugin (§10) produces a full ranking.** It writes a cost on every executor (idle
lowest, then least shuffle data), so the same ascending sweep follows the driver's choice. This gives
#16233 item 4 a standard annotation as its contract, rather than a Kueue-specific label.

**With the Deployment allocator,** a deleted executor is recreated by its ReplicaSet rather than by
Spark. The ReplicaSet builds it from the same executor template, so it is born gated like any other
(to verify, §9).

**Informing the executor: Spark's graceful decommissioning.** Kueue does not signal Spark directly. It
relies on Spark's own decommissioning, which turns a Pod deletion into an orderly hand-off (Spark 4.0
and master).

**How deletion triggers it:**

1. With `spark.decommission.enabled`, Spark's `BasicExecutorFeatureStep` gives every executor container
   a `preStop` hook running `spark.kubernetes.decommission.script`, `/opt/decom.sh` by default
   (`BasicExecutorFeatureStep.scala:241-254`).
2. When Kueue deletes the Pod, the kubelet runs that hook. The script sends **SIGPWR** to the executor
   JVM, then blocks until the JVM exits.
3. On SIGPWR the executor stops accepting tasks and tells the driver it is decommissioning. The driver
   handles this as `ExecutorDecommission`, so tasks lost this way do not count as failures.
4. With `spark.storage.decommission.enabled` and its `shuffleBlocks`/`rddBlocks` options, the executor's
   block manager **migrates its shuffle files and cached RDD blocks to peer executors**. Optionally it
   uses `spark.storage.decommission.fallbackStorage.path` when no peer has room.
5. The executor then exits 0. Spark records the loss as a decommission, not a failure, and the shuffle
   output survives on the executors that remain. Later stages read it there rather than recomputing it.

So for a reclaim, deleting a Pod *is* the eviction notice, and no Spark change or driver plugin is
needed. The grace period bounds the hand-off. A block that has not finished migrating when the period
ends is lost and recomputed, as it would be without decommissioning.

**Kueue turns decommissioning on at create time.** For every application that declares a reclaimable
PodSet, the Spark webhook defaults these settings when the application is created:

| Setting | Defaulted to |
|---|---|
| `spark.decommission.enabled` | `"true"` |
| `spark.storage.decommission.replicationReattemptInterval` | `5s` |
| `spark.storage.decommission.maxReplicationFailuresPerBlock` | 90s ÷ interval, capped at 9, at least 3 (`9` at 5s) |
| `spark.storage.decommission.enabled` | `"true"` |
| `spark.storage.decommission.shuffleBlocks.enabled` | `"true"` |
| `spark.storage.decommission.rddBlocks.enabled` | `"true"` |
| Executor grace period: the Pod template's `terminationGracePeriodSeconds` and `spark.kubernetes.executor.terminationGracePeriodSeconds` | `30` seconds, in both |

- **It is safe.** This is the same one-time write before the application exists that injects the
  executor gate, which EQM §9.4 shows is safe under the operator's resubmission rule (C-1).
- **A shorter migration round.** A decommissioning executor exits only once every block is
  migrated and a round has run since its last task finished. Rounds run every
  `spark.storage.decommission.replicationReattemptInterval` (Spark's default 30s), so a reclaimed
  executor lingers up to 30s after its data has moved. On the cluster, migration of ~70 MiB of shuffle
  per executor took about a second (`validation/…/experiments/shuffle-migration`). The webhook defaults
  the interval to **5s**, which cut a reclaimed executor's exit from 30s to 11s on the cluster. It
  sets `spark.storage.decommission.maxReplicationFailuresPerBlock` from the interval: 90s ÷ interval,
  capped at **9** and never below Spark's 3. At 5s that is 9 attempts, about 45s of retries, already
  longer than the 30s grace period. An app that sets its own interval gets attempts derived from it;
  explicit values are kept. Both are Spark-internal
  settings.
- **Both grace-period settings are needed.** From Spark 4.1, `spark.kubernetes.executor.terminationGracePeriodSeconds`
  (default 30s) is written onto every executor Pod and overrides the template
  (`BasicExecutorFeatureStep.scala:309-310`, branch-4.2). Before 4.1 that property does not exist and
  the template applies. So the webhook sets both. If the application sets either, the other takes its
  value, so every executor gets the same grace period on any Spark version.
- **Why 30 seconds.** The grace period caps how long a preemptor waits for a reclaimed executor.
  On the cluster, busy executors finished their running tasks and migrated their blocks in about 30s
  (idle ones in about 1s), so the cap rarely binds. An executor still draining at 30s is killed, and the
  work it had not handed off is recomputed. An application with large shuffle output per executor
  should raise it; a first version defaulted to 60s.
- **It is a default, not an override.** Any value the application sets explicitly, including `"false"`,
  is left alone. An application that opts out of decommissioning still gets partial reclaim. It just
  loses the reclaimed executors' shuffle output, which Spark then recomputes.

**Without decommissioning,** deleting executor Pods is still not counted as an application failure
(`PodDeleted` maps to `ExecutorExited(exitCausedByApp = false)`). Spark re-runs those executors' tasks
and recomputes their shuffle output.

The allocator then recreates Pods to reach its target, `spark.executor.instances` or Dynamic
Allocation's target, because a deletion does not lower that target. **Those Pods are gated**, and stay
Pending. A gated Pod costs the application nothing, never times out
(`spark.kubernetes.allocation.executor.timeout` applies only to Pods never seen), and counts against
`maxPendingPods`.

## 5.5 Grow-back

After a reclaim, the victim's live executor count includes the recreated, gated Pods. That count now
exceeds the grant, so the job reconciler creates a scale-up slice for the difference, exactly as EQM
§14.1 describes. That slice *is* the grow-back. It is admitted when quota frees up, and the ungater
then releases the gated Pods.

**Static applications reach the same state by a different route.**

- **Kueue's request does not come from live Pods.** With Dynamic Allocation off, the executor count
  Kueue requests is the declared `spark.executor.instances` (`sparkapplication_podset.go:224-225`).
  So the request stays at the full count. As soon as the grant is lowered, the reconciler sees request >
  grant and creates the grow-back slice. That would happen even if Spark recreated nothing.
- **The driver does not scale up.** Its target is fixed at startup
  (`KubernetesClusterSchedulerBackend.scala:111-112`). Only Dynamic Allocation calls the path that
  changes it, `doRequestTotalExecutors` (`:187`).
- **It replaces lost executors, up to that same target.** On the allocator's next round, the
  replacements are born gated and stay Pending. Pending Pods count toward the target, so nothing more
  is created. Spark never deletes them: it deletes Pending Pods only when Pods exceed the target, and
  `spark.kubernetes.allocation.executor.timeout` covers only Pods that never appear.
- **The reclaim is not a failure.** A deleted executor is `PodDeleted`. Only `PodFailed` is registered
  with the failure tracker (`ExecutorPodsLifecycleManager.scala:99-107`), whose static-application limit
  is `max(3, 2 × instances)`. §8 covers the one exception, and decommissioning avoids it.
- **The application keeps running on fewer executors** until the grow-back slice is admitted and the
  gated replacements are released.

**Grow-back never preempts.** A reclaim records the victim's grant before it, in
`kueue.x-k8s.io/partial-preemption-restore-count: "<n>"` on the victim Workload. Only the first
reclaim records it; later ones leave it alone.

- **The record is carried forward.** The job framework copies it onto each replacement slice of the
  chain. It removes it once the admitted slice's grant is back at or above it, but never while a
  reclaim is outstanding.
- **While it stands, the slice may only use free quota.** The scheduler runs only the free-quota steps
  (1 and 2 of §5.3) for it, and the planner never preempts on its behalf.
- **v1 is deliberately stricter than splitting the request.** The whole slice is grow-back while the
  record stands, including any growth beyond the restore count. A Dynamic Allocation application that
  wants more than it had before it was reclaimed waits for free quota until it is back at its old grant.
  Splitting the request into a non-preempting part and an ordinary part is a follow-up.

Without this, a victim's grow-back slice could preempt its own preemptor. That can happen under
`LowerOrNewerEqualPriority` or in a fair-sharing cohort, and would ping-pong executors back and forth.

**Ordering.** Grow-back slices are ordered by priority, then by **the chain root's creation time**
rather than the slice's own. So the application admitted first recovers first, as #16233 asks.

## 5.6 Correctness

**SC-1 (quota is a ceiling).** A partial target removes from the snapshot exactly the usage the
eventual grant lowering removes from the cache, `Count × per-Pod usage`. The preemptor is admitted only
after that lowering. Its admission therefore fits in the cache as it did in the plan.

**SC-2 (every running worker has a grant).** The grant falls only after the reclaimed Pods are terminal
or gone. A recreated Pod is born gated, and is ungated only up to the grant. So no Pod runs without a
grant at any point.

The one exception is the timeout of §5.4. It has the same trade-off as `IsActive()`'s handling of Pods
stuck terminating, and is reported by an event.

**Superseded slices.** These are already excluded from candidates (EQM §10.2). Reclaiming from the tip
and lowering the tip's grant keeps the chain accounting unchanged.

## 5.7 Observability

| Signal | Meaning |
|---|---|
| `PartiallyPreempted` condition on the victim | Preemptor, count, start time |
| Events `PartiallyPreempted`, `PartialPreemptionComplete`, `PartialPreemptionTimedOut` | Lifecycle of a reclaim |
| `kueue_partial_preemptions_total{cluster_queue, outcome}` | `completed` / `timed_out` / `fell_back_to_eviction` |
| `kueue_partial_preemption_reclaimed_pods_total{cluster_queue}` | Executors reclaimed |
| V(3) log in the planner | Per-victim reclaim decision, floor and tier |

# 6. Configuration summary

| Setting | Where | Default |
|---|---|---|
| `SparkPartialPreemption` | Kueue feature gate | off |
| `spark.kueue.preemption.minExecutors` | application `sparkConf` | `minExecutors` (at least 1) with Dynamic Allocation, else `spark.executor.instances` |
| `kueue.x-k8s.io/partial-preemption-fallback` | ClusterQueue annotation | `Evict` |
| `kueue.x-k8s.io/partial-preemption-timeout` | ClusterQueue annotation | `60s` (the 30s executor grace period + 30s) |
| Preemptor limit (step 4 of §5.3) | always on with the gate | on |

| `spark.decommission.enabled` and `spark.storage.decommission.{enabled,shuffleBlocks.enabled,rddBlocks.enabled}` | defaulted by the Spark webhook at create time for reclaimable applications (§5.4) | `"true"`, unless set explicitly |
| Executor grace period (template and `spark.kubernetes.executor.terminationGracePeriodSeconds`) | defaulted by the same webhook | `30` seconds; if the application sets one, the other follows it |

**What an application sets.** Only the floor, and only if the default does not suit it:

```yaml
spec:
  sparkConf:
    "spark.kueue.preemption.minExecutors": "1"
```

Decommissioning is defaulted for it (§5.4). An application that writes large shuffle output per
executor should raise `spark.kubernetes.executor.terminationGracePeriodSeconds` so that migration
finishes inside the grace period. The webhook copies it to the Pod template, and the ClusterQueue's
timeout should be raised to match.

# 7. Alternatives considered

- **Keep whole eviction, tune configuration.** The mitigations available today are a small
  `maxExecutors` on high-priority applications, `withinClusterQueue: Never`, or equal priorities. Each
  gives up priority or elasticity, and none stops the thrash where priorities must differ (§2.1).
- **`PodSet.MinCount` as the floor (KEP-975).** Rejected (§2.3). The Spark integration owns `MinCount`
  for partial scale-up, and the API allows only one `MinCount` PodSet.
- **`ReclaimablePods`.** Rejected for three reasons:
  - The job writes it, and SparkApplication does not implement it.
  - The webhook forbids decreasing it while the Workload is admitted.
  - The ungater caps by `ExtractGrantedPodSetCounts`, not the after-reclaim count
    (`podsetscounts.go:82` vs `:99`), so recreated Pods would be ungated and the reduction would not
    hold.
- **Shrink in Spark only (a driver plugin, no Kueue change).** Rejected as the sole mechanism:
  - Spark's Dynamic Allocation bounds are read once at startup.
  - `killExecutors` throws under Dynamic Allocation.
  - Dynamic Allocation overwrites `requestTotalExecutors`.

  So Spark cannot be made to hold a lower count, and Kueue would still need to lower the grant. It is
  kept as phase 2, for *choosing* executors (§10).
- **Lower the grant first and let the ungater sort it out.** Rejected. This releases quota while the
  Pods still run (§5.4).
- **Only add step 2 (reduced count without preemption).** It is cheap and is kept. But it does nothing
  on a saturated cohort, which is the baseline's situation.

# 8. Risks

| Risk | Mitigation |
|---|---|
| Spark classifies a deleted executor as failed, if the Pod reports `Running` after its container exits 143 (likelier with sidecars). That counts toward `spark.executor.maxNumFailures`. | Decommissioning, defaulted on, exits 0. Verify on a cluster (§9). The failure limit defaults to max(3, 2 × maxExecutors). |
| Shuffle loss makes a reclaim costly late in a job. | Decommissioning with block migration, defaulted on (§5.4). Choose the newest executors first. |
| Migration does not finish within the grace period. | Unmigrated blocks are recomputed, as without decommissioning. The grace period is configurable per application. The reclaim timeout follows it (§5.4). |
| The image lacks the decommission script (`/opt/decom.sh` in Spark's own images). The preStop hook then fails, and the Pod is killed without migrating. | Check the platform's Spark images (§9). `spark.kubernetes.decommission.script` points elsewhere if needed. |
| Publishing `partial-preemption-marked-pods` adds Workload writes as Dynamic Allocation marks and unmarks executors. | Written only when the count changes, through the debounced Pod watch (EQM §8.6). |
| A victim with very little headroom gives up its only spare executor in a round. | Expected under round-robin, and bounded by its floor. Grow-back returns it first-come by chain age (§5.5). |
| Recreated gated Pods fill `maxPendingPods`. | Harmless: they are the grow-back request (EQM §14.1). |
| Repeated small reclaims of one victim. | The spread rule (§5.3) and the floor bound the total. A cap on total reclaim per application is a follow-up (§11). |
| Annotations as API. | Alpha only. Graduation moves them to Workload and ClusterQueue fields, aligned with KEP-975 (§9). |

# 9. Validation plan

**Unit tests.** Each correctness test is organised around an invariant and negative-controlled, as in
EQM §13. Coverage:

- floor resolution across every row of §5.2
- partial-target snapshot arithmetic
- the five-step order of attempts
- the spread rule: with N victims and a shortfall of N, each gives up exactly one; no victim gives up a
  second before every victim with headroom has given up one; ties; multiple tiers; fill-back returning
  from the largest losers first
- the cost sweep: marked executors taken across victims before any round-robin, ascending cost,
  floors respected; Pods without the annotation treated as 0; tie-breaks
- webhook defaulting of the decommission settings and grace period, including explicit values (and
  `"false"`) left alone
- the two-phase ordering, with the grant lowered only after Pods are gone (a negative control lowers
  it first and must breach SC-2)
- timeout
- grow-back non-preemption
- each race in §5.4

**On a cluster.** Use the EQM harness, adding:

- **The baseline scenarios.** S4 (partial scale-up with `maxPendingPods: 2`) and S5 (atomic), two low
  plus one high, each with and without the gate.
- **The same scenarios with decommissioning defaulted on, and with it explicitly off.** On a reclaimed
  executor, confirm three things:
  - the preStop hook ran;
  - the driver logged a decommission rather than an executor failure;
  - shuffle blocks migrated, by comparing the number of recomputed stages between the two runs.
- **A spread check.** With four low-priority victims and a one-executor-at-a-time preemptor, the
  reclaim should land on different victims round by round.
- **The cluster's Spark image ships `/opt/decom.sh`.**
- **A cost-sweep check on Spark 4.2+.** Run with `spark.kubernetes.allocation.pods.allocator: deployment`
  and `spark.kubernetes.executor.podDeletionCost: "-100"`, and check two things:
  - Executors Dynamic Allocation marked idle are reclaimed before any unmarked one, across victims.
  - Executors the ReplicaSet recreates are born gated.
- **A static variant.** The victims run `spark.executor.instances: 6` with the elastic annotation. No
  static application has been preempted on a cluster yet.
- **A check of the executor-failure classification risk in §8.**

Two harness fixes:

- **Count evictions per eviction, not per slice.** `edge-analyze.py` undercounts: 41 of 50.
- **Count restarts, not suspends.** Its `suspends=` counts both `SUSPENDING` and `SUSPENDED`, which
  doubles the apparent number.

**Pass criteria** are §4.

# 10. Phasing

1. **Phase 1 (this design).** Kueue chooses the executors, for the Kubeflow integration on the fork.
2. **Phase 2: the driver chooses.** A Spark `DriverPlugin`, modelled on Spark's `ExecutorRollPlugin`
   and living in an `org.apache.spark.*` package, ranks executors from the driver's own status store:
   idle first, then least shuffle data, then least memory. It writes that ranking as each executor's
   `controller.kubernetes.io/pod-deletion-cost`, so §5.4's ascending sweep follows the driver's choice.
   This meets #16233 item 4 with no Kueue polling of metrics. On Spark 4.2+ with the Deployment allocator,
   Spark's own binary marking (§5.4) already covers the idle case without the plugin.
3. **Phase 3: wider coverage.** The Apache integration, the kueue-workload port (aci/kueue-workload#36),
   and fair-sharing preemption.
4. **Upstream.** Propose §5.2's separate floor, §5.3's spread rule and preemptor limit, §5.4's timeout,
   and §5.5's grow-back as extensions to KEP-975. Coordinate with #16806, which is bringing elastic
   SparkApplication support upstream.

# 11. Open questions

1. **A cap on total reclaim per application** across reclaims, as a percentage (#16233 item 2). The
   floor already bounds it. A percentage cap would let a platform say "never more than half"
   regardless of size. Should it be a second property, `spark.kueue.preemption.maxReclaimPercent`, or a
   ClusterQueue default?
2. **A low-priority application at its floor for a long time.** It is protected, but makes slow
   progress. Should there be an age after which it may be evicted whole, so it restarts with a full
   gang?
3. **Ordering grow-back by the chain root's creation time** changes queue ordering for those slices.
   Does that interact with `StrictFIFO` queueing?
4. **How the operators react to many deleted executors.** The Apache operator's
   `RunningWithPartialCapacity` state, and Apple's fork's extra states, have not been checked against
   executors disappearing below `minExecutors`.

# 11a. Implementation notes (v1)

Where the implementation on this branch differs from, or adds to, the sections above:

- **Grow-back** uses the restore count, and the whole slice is grow-back while it stands (§5.5).
- **The fallback is held off while a reclaim is in flight.** While any workload in the preemptor's
  cohort tree has a partial-preemption target outstanding, the scheduler skips the `Evict` fallback.
  Otherwise the preemptor's next pass would find nothing left to reclaim, because the victim's reclaim
  is already pending, and fall through to evicting a whole application.
- **Topology-aware preemptors never use partial reclaim.** The TAS simulation removes victims whole.
  This matches PNG-3.
- **Decided preemptions are tracked as partial-aware targets** (`preemption.PreemptedTargets`), so a
  partial victim frees only its reclaimed Pods when the scheduler checks later workloads in the same
  cycle (SC-1).
- **The workload controller handles two new transitions.**
  - It observes the preemption expectation when a target appears, since the victim stays admitted.
  - It requeues inadmissible workloads when an admitted workload's *grant* drops, since a completed
    reclaim lowers the grant, not the spec.
- **Field ownership.** The `PartiallyPreempted` condition is written under the field owner
  `kueue-partial-preemption` by both the scheduler and the job framework. The annotations are written
  with merge patches.
- **Scale-up slices never whole-evict an opted-in workload** (§5.3), whatever the ClusterQueue's
  fallback. They may still evict workloads that did not opt in, as before (PR-7). A first version
  forbade all whole eviction for scale-ups, which also changed behaviour for victims that never
  opted in and for other elastic integrations' scale-ups.
  The first cluster run showed why: with both low-priority applications at their floor, a
  one-executor scale-up of the high-priority application evicted a whole low-priority one.
- **The ungater caps at the reclaim target while a partial preemption is outstanding.** The grant is
  lowered only after the reclaimed Pods stop. In the gap between the last of them disappearing and
  the grant falling, the old grant made room for Pods the reclaim had just given away. On the cluster
  (P3) two recreated executors were ungated in that gap, leaving four running against a grant of two,
  an SC-2 breach. The target annotation is cleared only after the grant is lowered, so capping by it
  closes the gap.
- **A finished elastic job finishes every slice.** When a job finishes, the job framework finished
  only the Workload it had just picked. If a scale-up slice was still pending, that was the slice it
  picked, and the admitted slice kept its quota after the job ended. Found on the cluster (P5): a
  partially preempted static application had a grow-back slice pending when it completed, and its
  admitted slice held 7 slots for six minutes. The bug is in the shared slicing code, not partial
  preemption: any elastic job that finishes with a scale-up pending hits it. Partial preemption makes
  it routine for static applications, whose request stays above a lowered grant until they finish.
- **A stale completion pass is a no-op.** A reconcile that reads the Workload before the previous
  pass's writes land finds the grant already at the target. It finishes the cleanup without lowering
  the grant again or emitting a second completion event.
- **Metrics (§5.7) are not implemented yet.** Events cover the lifecycle.

# 12. Implementation inventory

| Component | File | Change |
|---|---|---|
| Floor resolution | `pkg/controller/jobs/sparkapplication/sparkapplication_podset.go` | `partialPreemptionFloor` |
| Optional interface | `pkg/controller/jobframework/interface.go` | `JobWithPartialPreemption` |
| Workload annotations, reclaim orchestration | `pkg/controller/jobframework/reconciler.go` | stamp floor; observe target; call `ReclaimPods`; wait; lower grant |
| Pod selection and deletion | `pkg/controller/jobs/sparkapplication/sparkapplication_controller.go` | `ReclaimPods` |
| Cost sweep and marked-pod count | `pkg/controller/jobs/sparkapplication/sparkapplication_podset.go`, `..._controller.go` | ascending `pod-deletion-cost` selection in `ReclaimPods`; publish `partial-preemption-marked-pods` |
| Decommission defaults | `pkg/controller/jobs/sparkapplication/sparkapplication_webhook.go` | default the §5.4 settings in `Default()` for reclaimable applications |
| Grant lowering, restore count on new slices | `pkg/workloadslicing/workloadslicing.go`, `pkg/controller/jobframework/partial_preemption.go` | `LowerGrantedCounts` on `scaleDownAdmission`; carry `partial-preemption-restore-count` |
| Partial targets | `pkg/scheduler/preemption/policy/types.go`, `preemption.go`, `classical_strategy.go` | `Target.Reclaim`; partial yielder; spread rule; `IssuePreemptions` branch |
| Order of attempts, grow-back rule | `pkg/scheduler/scheduler.go` | `getAssignments` steps 1–5 |
| Expectations | `pkg/scheduler/preemption/expectations` | partial expectation |
| Feature gate | `pkg/features/kube_features.go` | `SparkPartialPreemption` |
| Metrics | `pkg/metrics` | §5.7 |
