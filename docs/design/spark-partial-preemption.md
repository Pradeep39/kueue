# Spark partial preemption

## Summary

When a higher-priority workload needs quota, Kueue normally preempts a lower-priority Spark
application whole: its driver is killed and the application restarts from scratch. With
partial preemption, Kueue instead reclaims only some of the application's executors, above a
protected floor:

- the application keeps running on the executors it has left;
- the reclaimed executors decommission gracefully and migrate their shuffle and cached blocks;
- the application grows back when quota becomes available again.

This is a reactive design. Kueue evicts the executor Pods it reclaims, and lowers the
application's grant only after those Pods have stopped. The Spark driver needs no changes: it
sees executors decommission and re-requests them, and the re-requested executors stay gated
until quota is available again.

- **Feature gate:** `SparkPartialPreemption` (alpha, off by default).
- **Applies to:** both SparkApplication integrations (Kubeflow and Apache), for applications on
  the elastic path (`kueue.x-k8s.io/elastic-job: "true"`), with or without Dynamic Allocation. An
  application without the annotation is still preempted whole.

## Declaration

Each reclaimable application's Workload carries, maintained by the job reconciler:

| Annotation | Meaning |
|---|---|
| `kueue.x-k8s.io/partial-preemption-podset` | The PodSet that may be reduced: the executors. The driver PodSet is never named, so it is never reclaimed. |
| `kueue.x-k8s.io/partial-preemption-min-count` | The floor: the count the executor PodSet is never reduced below. |
| `kueue.x-k8s.io/partial-preemption-marked-pods` | Executors with a negative `controller.kubernetes.io/pod-deletion-cost`, i.e. ones the application has already chosen to remove. These cost nothing to reclaim. |
| `kueue.x-k8s.io/partial-preemption-termination-grace-period` | The executor grace period the application sets, from the executor Pod template's `terminationGracePeriodSeconds` or else `spark.kubernetes.executor.terminationGracePeriodSeconds`. Absent when it sets neither. |

**Floor.**

- **Kubeflow:** `spark.kueue.preemption.minExecutors` if set. Otherwise, with Dynamic
  Allocation, `minExecutors` raised to at least 1. Otherwise the static executor count, so a
  static application gives nothing up unless it sets the property.
- **Apache:** the same, then raised to `spec.applicationTolerations.instanceConfig.minExecutors`.
  Below that threshold the operator reports the application unhealthy.
- **Both:** the floor is capped at the executor PodSet's current count, the executors the
  application currently asks for.

**Topology.** An executor PodSet with a topology request (Topology Aware Scheduling) is never
reduced. Such an application can only be preempted whole.

**Webhook defaults.** On create, for an application partial preemption could ever take an
executor from (its floor is below `maxExecutors`, or below its executor count without Dynamic
Allocation), the webhook defaults these to `true`, leaving any value the application sets,
including `false`, alone:

- `spark.decommission.enabled`
- `spark.storage.decommission.enabled`
- `spark.storage.decommission.shuffleBlocks.enabled`
- `spark.storage.decommission.rddBlocks.enabled`

If the application sets only one of the executor Pod template's `terminationGracePeriodSeconds`
and `spark.kubernetes.executor.terminationGracePeriodSeconds`, the webhook copies it to the
other.

## Choosing victims

The scheduler tries, in order:

1. **Free quota:** the full count, then (with partial admission) a reduced count, without
   preempting anything.
2. **Partial reclaim:** the same counts, by reclaiming executors from declared victims.
3. **The ClusterQueue's fallback**, `kueue.x-k8s.io/partial-preemption-fallback`:
   - `Evict` (default): today's whole-workload preemption;
   - `Wait`: leave the preemptor pending.

Three kinds of workload get a shorter ladder:

| Workload | What it may do |
|---|---|
| Grow-back slice (see below) | Free quota only. It never preempts, least of all its own preemptor. |
| Scale-up slice | Free quota, partial reclaim, then whole eviction only of workloads that did not opt into partial preemption. Growth never evicts an opted-in application whole. |
| Any workload while a reclaim is in flight in the ClusterQueue's cohort tree | No `Evict` fallback until the reclaim completes, since the quota it frees isn't visible yet. |

Fair-sharing preemption keeps whole-workload eviction.

**Partial reclaim planner.**

1. **Candidates.** Victims are the workloads classical preemption would consider, together with
   those a ClusterQueue's `PreemptionConfig` selects through its `Always` rules. Candidates from
   its `InsufficientQuota` rules are used only once every earlier victim is at its floor.
   Each victim can give up only its grant above its floor. A slice that is being replaced, and
   the preemptor's own slice chain, are skipped.
2. **Reclaims in flight count first,** so a preemptor waits on them rather than picking new
   victims.
3. **Tiers.** Victims are grouped by priority, lowest first. A higher tier is used only once
   every victim in the lower one is at its floor.
4. **Within a tier:**
   1. first the executors the victims have themselves marked for removal;
   2. then **round-robin**: one executor from every victim with headroom per round, never below
      its floor. Within a round, the victim with the most headroom left goes first, then the most
      recently admitted.
5. **Stop and hand back.** Selection stops as soon as the preemptor fits. Surplus executors are
   then handed back one at a time, starting from the victim that gave up the most, so the loss
   stays even.

## Reclaim

**Issuing.** When the preemptor is admitted, the scheduler issues the reclaim on each victim
Workload:

- `kueue.x-k8s.io/partial-preemption-target`: the executor count to reduce to;
- `kueue.x-k8s.io/partial-preemption-deadline`: when to lower the grant even if the Pods have
  not stopped;
- `kueue.x-k8s.io/partial-preemption-reclaim-grace-period`: the grace period to delete the Pods
  with;
- `status.admission.podSetAssignments[].reclaimTargetCount` set to the target;
- the `PartiallyPreempted` condition, `True` with reason `PartialPreemptionIssued`;
- events: `PartiallyPreempted` on the victim ("Partially preempted N Pod(s) of PodSet executor,
  from A to B, …"), and `PartiallyPreemptedWorkload` on the preemptor, with the victim as its
  related object, so each victim gets its own event.

**Executing.** The job reconciler then runs the reclaim on the victim:

1. **Evict.** `ReclaimPods` evicts executors until only the target count still holds a grant.
   - It works by level: Pods already being deleted count, so re-running it evicts nothing more.
   - Order: lowest `pod-deletion-cost` first, then unbound before bound, then newest, then by
     name. The driver and gated executors are never touched.
   - Pods are removed through the Eviction API with the grace period, so PodDisruptionBudgets
     apply. A 429 is retried on the next reconcile; a 403 falls back to a plain delete.
   - With decommissioning on, each evicted executor migrates its blocks before it exits.
2. **Wait** until the evicted Pods have stopped, or the deadline passes.
3. **Lower the grant.** In one status update, lower the grant to the target and clear
   `reclaimTargetCount`, then clear the target annotations. `spec.podSets` is left alone, so the
   next reconcile creates the grow-back slice. The quota goes to the preemptor only now, after
   the Pods have stopped, so the queue is never overcommitted.
4. **Resolve the condition:** `PartiallyPreempted` becomes `False` with reason
   `PartialPreemptionCompleted`, or `PartialPreemptionTimedOut` if the deadline passed.

A target below the victim's floor is raised to the floor. A reclaim is dropped, with its grant left
untouched, when the victim is evicted, finishes or loses its admission mid-reclaim, or no longer
declares a reclaimable PodSet with a grant. The condition then becomes `False` with reason
`PartialPreemptionAbandoned`, and a `PartialPreemptionAbandoned` event is recorded on the job.

**Grace period and timeout.**

- **Grace period:** the job's own, from the Workload's
  `kueue.x-k8s.io/partial-preemption-termination-grace-period` (see Declaration). Otherwise the ClusterQueue's
  `kueue.x-k8s.io/partial-preemption-termination-grace-period`. Otherwise 30s.
- **Timeout:** the ClusterQueue's `kueue.x-k8s.io/partial-preemption-timeout`, or else the grace
  period plus 30s.

## Readiness

A reclaim must not make the application look unready, or `waitForPodsReady`'s recovery timeout
(which defaults to its `timeout`) would evict it whole. Both integrations therefore measure
`PodsReady` against the executors Kueue currently grants, through the job framework's optional
`JobWithGrantAwarePodsReady` interface:

- **Kubeflow:** the driver runs and at least `min(expected, granted)` executors are running or
  completed, where `expected` is `minExecutors` with Dynamic Allocation and the executor instances
  otherwise.
- **Apache:** the operator reports `RunningHealthy` or `RunningWithPartialCapacity`; or, in its
  below-threshold states, every executor Kueue grants is running.

At the full grant this is as strict as before, so executors stuck above it are still caught.

## ClusterQueue settings

Partial preemption applies to every ClusterQueue while the gate is on. These ClusterQueue
annotations tune it:

| Annotation | Default | Meaning |
|---|---|---|
| `kueue.x-k8s.io/partial-preemption-fallback` | `Evict` | What happens when partial reclaim can't fit the preemptor: `Evict` falls back to whole-workload preemption, `Wait` leaves the preemptor pending. |
| `kueue.x-k8s.io/partial-preemption-termination-grace-period` | 30s | Grace period for reclaimed Pods when their job sets none. The same annotation on a Workload, the job's own, takes precedence. |
| `kueue.x-k8s.io/partial-preemption-timeout` | grace period + 30s | How long a reclaim waits for its Pods to stop before lowering the grant anyway. |

## Grow-back

- **Restore count.** The victim's Workload records the grant it had before its first outstanding
  reclaim, in `kueue.x-k8s.io/partial-preemption-restore-count`.
- **Grow-back slices.** While the grant is below that count, its scale-up slices are grow-back:
  they are admitted into free quota only and never preempt.
- **Gating.** The executors the driver re-requests stay gated until a grow-back slice is
  admitted.
- **Lifetime.** The restore count is carried onto replacement slices, and removed once the grant
  reaches it again.

## API and validation

- **`reclaimTargetCount`.** The field
  `Workload.status.admission.podSetAssignments[].reclaimTargetCount` is Kueue-owned and only
  available with the gate on. The Workload CRD must include it, or the API server prunes it.
- **Admission updates.** `status.admission` stays immutable, except that with the gate on an
  elastic Workload may have a PodSet's count, and its resource usage, lowered.
- **Floor property.** The webhook rejects a negative or non-integer
  `spark.kueue.preemption.minExecutors` when it is set or changed. It warns when the property is
  set on an application that is not elastic.
- **RBAC.** The job reconciler needs `create` on `pods/eviction`.

## Code map

| Area | Location |
|---|---|
| Planner | `pkg/scheduler/preemption/partial_strategy.go` |
| Attempt ladder | `planLadder` in `pkg/scheduler/scheduler.go` |
| Victim side | `pkg/controller/jobframework/partial_preemption.go` |
| Annotations and defaults | `pkg/constants/constants.go` |
| Integrations | `JobWithPartialPreemption` in `pkg/controller/jobs/sparkapplication/` and `pkg/controller/jobs/apachesparkapplication/` |
