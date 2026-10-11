---
title: "Elastic Quota Management for Spark Workloads in Kueue — Condensed"
subtitle: "The proposal and design in brief"
author: "Pradeep Reddy"
date: "2026-10-11"
---

# About this version

This is the short form of
[`spark-elastic-quota-management.md`](./spark-elastic-quota-management.md). It keeps every
decision, mechanism and result and leaves out most of the reasoning behind them. References like
**(→ §10.2)** point to the section of the full document that has the detail and the evidence.

Diagrams: [`diagrams/`](./diagrams/README.md).

---

# 1. Problem

Spark Dynamic Allocation (DA) grows and shrinks an application's executors while it runs. Iceberg
compaction fans out and then collapses. A notebook session sits idle between cells, spikes when one
runs, and loses its state if it is requeued. For both, peak demand is a poor guide to average demand,
and the workload must not be interrupted in order to change size.

Kueue already has the primitive for this: `ElasticJobsViaWorkloadSlices` (KEP-77), supported for
`batch/v1.Job` and `RayCluster`. It does not work for Spark, because DA scales outside the
`SparkApplication` resource. The driver creates and deletes executor Pods directly, and never
updates the CR. (→ §1, §2.1, §7.3)

**Gang-admit the floor, not the peak.** A DA application cannot make progress without its driver
plus `max(minExecutors, initialExecutors)` executors. (`spark.executor.instances`, where it is also
set, joins that maximum.) That floor is admitted atomically. Everything above it is elastic. A peak
reservation holds idle capacity for the application's whole life. It can also be unadmittable
outright: with 512Mi executors and `maxExecutors: 30`, the peak needs 15.5Gi against a 6Gi queue
that runs the same application comfortably. (→ §2.2)

DA already provides elasticity, but it scales against the cluster, not the tenant's quota. **The
framework decides how much it wants; Kueue decides how much it may have, and keeps the accounting
honest as that changes.** (→ §2.3)

# 2. Goals and non-goals

**Goals:**

- Admit an application, then grow and shrink it within quota without requeueing it (G1).
- Track the real footprint in reported usage (G2).
- Never let a workload use capacity that has not been granted (G3).
- Return capacity promptly on scale-down (G4).
- Never modify the workload's own resource to account for it (G5).
- Leave non-elastic behaviour and the existing elastic integrations unchanged (G6).
- Admit the driver and its floor together or not at all (G7).

**Non-goals:**

- NG1: node-level co-scheduling.
- NG2: changing the framework's autoscaling policy.
- NG3: guaranteeing that an application reaches its maximum.
- NG4: MultiKueue.
- NG5: CI integration tests against a live scheduler.

(→ §2.4, §2.5)

# 3. Requirements and success criteria

| ID | Requirement | Status |
|---|---|---|
| R-1 | Accounted size tracks what is actually running | Met |
| R-2 | Resize while admitted, without requeue or restart | Met |
| R-3 | No capacity consumed before quota is granted | Met |
| R-4 | Capacity returned on scale-down, not at finish | Met |
| R-5 | Kueue never modifies the workload's resource | Met |
| R-6 | Correct mid-resize, with several slices alive | Met |
| R-7 | Correct regardless of event order or lost events | Met |
| R-8 | Can start below its maximum; a large maximum is not a rejection | Met |
| R-9 | Non-elastic behaviour, including admission immutability, unchanged | Met |
| R-10 | Existing elastic integrations unaffected | Met |
| R-11 | Workers awaiting quota stay proportionate to quota | Met, opt-in: `spark.kubernetes.allocation.maxPendingPods` |
| R-12 | Time-to-admission bounded under a saturated queue | Met, opt-in: partial scale-up |
| R-13 | Per-worker charge matches what the framework requests | Met for CPU and memory |

Two invariants define correctness, and neither implies the other:

- **SC-1 — quota is a ceiling.** Reported usage never exceeds nominal quota. A breach is loud.
- **SC-2 — every running worker has a grant.** Capacity actually consumed never exceeds reported
  usage. A breach is silent: the cluster is oversubscribed while the queue looks healthy.

(→ §4)

# 4. Constraints that shape the design

- **C-1: never write a count to `SparkApplication.Spec`.** The Kubeflow operator compares `.Spec`
  with `DeepEqual`. On any change outside a short exemption list (`suspend`, `timeToLiveSeconds`) it
  kills the running application and resubmits it. (→ §7.2)
- **C-2: a gate can reach executors only through the Pod template.** The driver loads the executor
  template once at startup, and merges it into every executor it ever creates. (→ §7.2, §9.3)
- **KEP-77's real contract is `PodSets()`, not `.Spec`.** `PodSets()` receives a client. It can
  therefore derive the count from live Pods, and `EnsureWorkloadSlices` needed no change. (→ §7.3)

# 5. Design

## 5.1 Sizing: observe, never write

The flow is: a Pod event, then a debounced reconcile, then `PodSets()`, which lists executor Pods by
label, counts them and clamps the count. `EnsureWorkloadSlices` then patches the slice in place or
creates a replacement slice. (→ §8)

- **What counts as live.** Every executor that is not `Succeeded` or `Failed`. This includes
  Pending, terminating and **gate-blocked** Pods. A gated Pod is the only signal that DA wants more,
  so excluding it would deadlock scale-up. The count means "what DA wants", not "what is consuming".
  (→ §8.1)
- **Memoised per reconcile.** `PodSets()` is called several times per reconcile, and the informer
  can move between calls. The count is cached on the per-reconcile wrapper so that the calls agree.
  (→ §8.2)
- **Before any executor exists**, the count comes from the declaration:
  - Static applications use `spec.executor.instances`, then `spark.executor.instances`, then Spark's
    default of 2 (never 0).
  - DA applications use the *maximum* of `minExecutors`, `initialExecutors` and the instances count,
    as Spark does.
  - A malformed value is rejected rather than treated as absent.
  - `dynamicAllocationEnabled` is an OR of the structured field and `sparkConf`, and must stay one:
    `Enabled` is a non-pointer `bool`, so an explicit `false` cannot be told apart from an omitted
    field.

  (→ §8.3)
- **Clamped to `[minExecutors, maxExecutors]`.** The lower bound matters most. It stops a reconcile
  that lands mid-startup, and sees only a prefix of the initial executors, from dismantling the gang
  that was just admitted. (→ §8.4)
- **Charge what the kubelet is asked for.** Spark overwrites the pod template's CPU and memory, so
  the charge is derived from Spark's configuration:
  - Memory is heap plus overhead (`max(factor × heap, 384MiB)`), computed in truncated whole MiB.
    A 512m executor requests 896Mi.
  - CPU reads `request.cores`, then `cores`, and never `limit.cores`.

  For Kubeflow this charging is upstream code. The `coreRequest: "1000m"` workaround is retired.
  (→ §8.5)
- **Debounce, except during teardown.** Executor Pods are owned by the driver, so the watch keys
  off the `app-name` label instead of `Owns()`. Pod events are debounced: a 5s quiet window with a
  30s maximum wait, on per-key timers.

  Once the driver goes away, events enqueue immediately until a new driver appears (a 2-minute TTL).
  This is so the quota release in §5.5 is not delayed. A fired timer clears its own bookkeeping only
  if it is still the key's current timer. (→ §8.6)
- **Slice names use a sequence number.** The number is the count of every Workload the application
  has ever owned. `Generation` never moves under DA. A count-based name would eventually collide
  with a dead slice. (→ §8.7)

## 5.2 Admission: gate in the template, ungate up to the grant

- **The gate.** The webhook adds `ElasticJobSchedulingGate` to `spec.executor.template` at create
  time, and `validateElasticJob` rejects an elastic application without it. Only executors are
  gated. The driver starts only after Kueue unsuspends the CR. (→ §9.5)
- **This one spec write is safe.** It happens before the object exists. `RunWithPodSetsInfo` only
  merges into the gates, and later scale-ups never re-enter that path. (→ §9.4)
- **The ungater needed one fix: cap by the grant, not the request.** `podsToUngate` read
  `spec.podSets[].Count`, the request, and so ungated Pods that the scheduler had not paid for. That
  was an invisible SC-2 breach. `ExtractGrantedPodSetCounts` reads `status.admission`; a missing
  `Count` falls back to the PodSet count. (→ §9.5)
- **Sizing and gating answer different questions:** "how much is it asking for?" and "may this
  existing Pod run?". RayCluster needs only gating, because Ray's autoscaler writes the CR. A Pod
  webhook and an ungate controller specific to Spark were both rejected. (→ §9.6)

## 5.3 Accounting: charge the chain, not the slice

- **The asymmetry.** A scale-up is a replacement slice. Three consumers see three numbers:
  - the scheduler snapshot charges the **delta**
  - `status.admission` holds the **full** count
  - the persistent cache re-derives from `status.admission`, and had no notion of replacement

  So until the predecessor's `Finish` lands, the cache held `old + new`, a double count. (→ §10.1)
- **Usage is `min(spec, granted)`, not the grant.** `totalRequestsFromAdmission` rescales down to
  the spec count. So a stale-high grant never inflates usage, and a grant below the spec is not
  corrected by the ledger. This is the most misread behaviour in this area. (→ §10.1)
- **Chain-scoped accounting.**
  - `clusterQueue` groups slices by chain and charges only the chain's tip.
  - It is declarative and idempotent: correct even if a `Finish` never lands, and in any event order.
  - The chain key includes the **job UID**, because a recreated job reuses the chain-root name.
  - Superseded slices are excluded from both preemption candidate collectors.
  - Keying off the replacement pointer was rejected, because deleting a middle slice brings the
    overcommit back.

  (→ §10.2)
- **Scale-down lowers the grant.** `scaleDownAdmission` lowers `status.admission` in place,
  rescaling `ResourceUsage` and truncating the topology assignment. This needed a narrow exception
  in `validateAdmissionUpdate`: decrease-only, and elastic-only. Plain Jobs keep a fully immutable
  admission. Per the previous point this is hygiene, not a correctness fix. (→ §10.3)
- **Two generic slicing fixes:**
  - Re-fetch and re-validate before every count-update retry, so a concurrent admission is not
    overwritten.
  - Tolerate a lagging cache that shows two not-finished slices, via `NormalizeActiveSlices`.

  (→ §11)

## 5.4 Partial growth and bounded pending Pods (R-11, R-12)

Gated Pods count as live (§5.1), so on a saturated queue the request climbs above the grant.
Measured on a cluster, the request oscillates rather than growing without limit. DA raises its
target in steps, and Spark deletes Pending executors older than `executorIdleTimeout`. The cost is
write churn and conflicts.

The real defect was **all-or-nothing scale-up**. Capacity that freed in pieces smaller than the
request stayed idle. (→ §14.1)

- **R-11: bound the request at its source.** `spark.kubernetes.allocation.maxPendingPods` caps
  Spark's Pending executors, and gated Pods are Pending. At 2 it:
  - held peak gated Pods to 6
  - halved Workload writes
  - removed the conflicts in the Workload controller

  It is the application's setting, not Kueue's (NG2).
- **R-12: admit the part that fits.** This needs the feature gate
  `ElasticJobsViaWorkloadSlicesWithPartialReplicaScaleUp` and the application annotation
  `kueue.x-k8s.io/elastic-job-scale-up-strategy: "partial"`. With both, `PodSets()` sets the
  executor `MinCount` (KEP-12100).
  - The first Workload stays atomic.
  - Each scale-up admits whatever fits, and probes for the remainder.
  - Idle capacity behind a pending scale-up fell from 7–15s to 0s.
  - `ApplyPodSetCounts` now caps `MinCount` at `Count`. This was a latent defect for Ray too.

Recommended for elastic DA applications:

```yaml
metadata:
  annotations:
    kueue.x-k8s.io/elastic-job: "true"
    kueue.x-k8s.io/elastic-job-scale-up-strategy: "partial"
spec:
  sparkConf:
    "spark.kubernetes.allocation.maxPendingPods": "2"
```

## 5.5 Releasing quota on eviction

The reconciler releases an evicted Workload's quota once `IsActive()` is false. `IsActive()` was
`AppState == RUNNING`, which turns false about a second after suspend while the Pods run for several
more. So the preemptor got quota the victim still held, an SC-2 breach.

`IsActive()` now also holds while any driver or executor Pod is non-terminal, not being deleted and
not scheduling-gated. This is `batch/v1.Job`'s `status.active` semantics:

- **Gated Pods** were never bound, so they are excluded. Counting them pinned quota for minutes.
- **Terminating Pods** are excluded, so a Pod stuck on a lost node cannot pin quota.

The Pod watch includes drivers, which can outlive every executor. (→ §12)

# 6. Validation

- **Unit tests** are organised around the invariants, and every correctness test is
  negative-controlled. `go test -race` passes on the affected packages. (→ §13)
- **First cluster round.** Three DA applications ran in a 6Gi queue. SC-1 and SC-2 held on every
  sample, with peak usage equal to `nominalQuota`. Before this work, the same workload reported 9Gi
  against that 6Gi. That harness took the per-Pod cost from Kueue's own charge, so it validated
  counts, not cost. (→ §13)
- **Second cluster round.** A watch-based harness read live Pods directly, removing that
  circularity. Three SparkALS applications ran with `minExecutors: 3` and `maxExecutors: 30`, on a
  12-slot cohort. (→ §13)

| Configuration | Peak gated | Largest pending request | Workload writes | Conflicts | Idle behind scale-up |
|---|---|---|---|---|---|
| Neither | 16 | 10 | 434 | 50 | 7s |
| Partial scale-up | 22 | 12 | 426 | 70 | **0s** |
| `maxPendingPods: 2` | **6** | **5** | 194 | **0** | 0s |
| Both | **6** | **5** | 216 | 4 | ~0s |

Other results from the second round:

- Applications that did not opt in were unchanged.
- Aggressive timeouts gave the same results.
- Under preemption, SC-2 was breached before the §5.5 fix. After it, SC-1, SC-2 and the live-Pod
  ceiling held across 9,720 SC-2 checks.
- In every run, the per-Pod charge matched live Pods' CPU and memory (R-13).

# 7. Known gaps

- **Preemption under DA evicts whole applications** (→ §14.2). Each scale-up step of a
  high-priority application evicts a whole lower-priority one, and the victim restarts from
  scratch. Measured: 5–6 evictions per seven-minute run.
  - Partial scale-up and `maxPendingPods` do not help.
  - KEP-975 partial preemption is not enough, because the choice needs to be Spark-aware
    (kubernetes-sigs/kueue#16233).
  - Today's mitigations: a small `maxExecutors` on high-priority applications,
    `withinClusterQueue: Never`, or equal priorities.
- **Slice names assume superseded slices are kept** (→ §14.3). A Finished-Workload retention policy
  that deletes slices of a running application can produce an `AlreadyExists` collision. Leave it
  unset for these namespaces.
- **Smaller items** (→ §14.3):
  - `replaceOldWorkloadSlice` does not retry a failed `Finish`. This is harmless after §5.3.
  - A transient "granted 5 of 6" self-heals.
  - The stale-grant webhook workarounds are retirable.
  - There are no envtest integration tests.

# 8. Applying the pattern to another integration

An integration needs the following (→ §15):

1. A live count from a label-keyed Pod list, memoised per reconcile.
2. A label-filtered Pod watch with a quiet window and a maximum wait.
3. A collision-free slice name.
4. A template-level gate, if the framework creates Pods outside Kueue and applies its template to
   later Pods.
5. An `IsActive()` that reports live Pods.
6. Optionally, `MinCount` for partial scale-up.
7. A per-worker cost derived the way the framework derives it, checked once against live Pods.

Everything else is generic: chain accounting, ungating, preemption exclusion and grant scale-down.
**No `kubeflow/spark-operator` change is required.** The integration runs against the released
operator and CRD. (→ §15.1)

The main code (→ §16):

- the `pkg/controller/jobs/sparkapplication/` podset, controller, Pod-handler and webhook files
- `pkg/cache/scheduler/clusterqueue.go` (chain accounting)
- `pkg/workload/podsetscounts.go` (grant extractor, `MinCount` cap)
- `pkg/controller/elasticjobs/elastic_job_ungater.go`
- `pkg/workloadslicing/workloadslicing.go`
- `pkg/webhooks/workload_webhook.go`

---

# 9. Addendum: the Apache Spark Kubernetes Operator

The Apache operator's `spark.apache.org/v1` `SparkApplication` is integrated in-tree as
`pkg/controller/jobs/apachesparkapplication`. It sits behind the alpha gate
`ApacheSparkApplicationIntegration`, which defaults to `false`, and also needs its framework name
enabled. It is a separate package because the two CRDs share only a name: a different group,
schema, count surface and state machine. (→ §A1)

**Prerequisite, and the collision.**

- SPARK-59475 added `spec.suspend` upstream.
- SPARK-59490 and SPARK-59503 added the operator's own `KueueWorkloadFactory`. It builds the
  Workload whenever the queue-name label is present, and throws when DA is enabled.
- With both sides creating Workloads, jobframework claims the operator's Workload, finds it
  non-equivalent and deletes it. The operator recreates it. The expected result is a delete loop,
  with the driver held indefinitely.
- The fix is an operator toggle, `spark.kubernetes.operator.kueue.enabled`. The Helm chart defaults
  it to `false`, while the Java default is `true`.

(→ §A2, §A2.1)

**Removing the operator's DA exception would not help.** The exception is a correct refusal:

- The operator freezes the count at build time.
- It stops reconciling an admitted Workload.
- It reacts to a podSet change by deleting and recreating the Workload.
- It never gates Pods.

Owning DA Workloads would mean reimplementing Part II in Java. It would also still need Kueue's
decrease-only admission exception. (→ §A2.2)

**Two architectures, not combinable.**

- **B — Kueue owns the Workload. This is chosen.** The operator supplies only `spec.suspend`.
- **A — the operator owns the Workload.** Traced in
  [`diagrams/apache-da-architecture-a.png`](./diagrams/apache-da-architecture-a.png).

The Workload-level machinery is owner-agnostic. The job-level half (watch, derive, ensure slices)
must live in a Kueue reconciler.

Prebuilt workloads do not bridge the two: setting the prebuilt label *disables* slicing. A cheaper
split was considered: the operator skips DA applications and Kueue takes only those. It needs a
per-object opt-out that `IntegrationCallbacks` lacks. (→ §A3, §A3.1)

**How it differs from the Kubeflow integration:**

- **The Go types are a hand-written partial projection** of the Java CRD. Never `client.Update`
  one, because that drops the omitted fields. jobframework patches. (→ §A4)
- **Charging** uses the same Spark arithmetic as §5.1. Here it is part of this contribution, not
  upstream. The limit equals the request. (→ §A5)
- **The `sparkConf` count wins over `instanceConfig`.** `instanceConfig` never reaches Spark: the
  operator uses it only for readiness thresholds. The rule is to prefer whichever surface Spark acts
  on. (→ §A6)
- **The elastic path mirrors Part II**: live counts, the debounced Pod watch, the template gate,
  the DA clamp and sequence-number slice names. (→ §A7)
- **Lifecycle:**
  - `StoppedByScheduler` is not terminal, because the operator reopens the application as
    `Suspended`.
  - Preemption never consumes restart budget.
  - The terminal outcome walks the state-transition history.
  - `PodsReady` keys off `RunningHealthy`.

  (→ §A8)

**Gaps:**

- The webhook does not reject a malformed `spark.executor.instances`.
- Partial scale-up is not wired here, though `maxPendingPods` works.
- The §5.5 quota-release fix is not applied here.
- There are only unit tests.
- It has not been run end-to-end against an operator built from upstream `main`.

(→ §A9)

**Deployment notes:**

- Helm never upgrades CRDs, so apply the chart's CRD server-side.
- `watchedNamespaces` needs an operator restart, and defaults to `default`.
- `helm upgrade` regenerates the workload ClusterRole.
- The image tag never changes, so restart the rollout.
- The chart's fixed ServiceAccount name prevents two releases in one namespace.

(→ §A10)
