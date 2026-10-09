---
title: "Elastic Quota Management for Spark Workloads in Kueue"
subtitle: "Proposal, design, and the Apache Spark Kubernetes Operator addendum"
author: "Pradeep Reddy"
date: "2026-10-09"
---

# 0. About this document

- **Part I — Proposal.** What should be built and why.
- **Part II — Design.** How it works, for the Kubeflow Spark Operator
  (`sparkoperator.k8s.io/v1beta2`).
- **Part III — Addendum.** The second integration, for the Apache Spark Kubernetes Operator
  (`spark.apache.org/v1`), which shares all the elastic machinery and differs in its
  prerequisites, charging surfaces and lifecycle mapping.

Reading order for a reviewer short on time: §2.2 (why gang scheduling is the wrong tool), §11.1
(the central asymmetry), §11.2 (chain-scoped accounting).

Code references are to `github.com/Pradeep39/kueue`. This describes a working implementation
validated on a live cluster. Known gaps are stated as such — §6, §15, §A9.

Diagrams: [`diagrams/`](./diagrams/README.md).

---

# Part I — Proposal

# 1. Summary

We propose that Kueue support **elastic quota management for Spark workloads driven by Spark
Dynamic Allocation (DA)**, so a Spark application can be admitted, grow and shrink within a quota
boundary while it runs, without being requeued and without reserving its peak footprint up front.

Kueue already has the primitive: `ElasticJobsViaWorkloadSlices` (KEP-77), supported today for
`batch/v1.Job` and `RayCluster`. It does not work for Spark, because Spark's autoscaler operates
outside the `SparkApplication` custom resource. This proposal closes that gap and offers a working
implementation as evidence that it is tractable.

# 2. Motivation

## 2.1 The workloads that need this

**Iceberg table compaction and similar maintenance jobs.** These fan out to rewrite data files,
then collapse. Parallelism is a function of how much data needs compacting, which varies by table,
by partition, and by how much changed since the last run. A single job's demand can move by an
order of magnitude during its own execution.

**Notebook-bound interactive sessions.** Between cells the session is idle — minutes, sometimes
hours. When a cell runs, demand spikes. Peak concurrency is unknowable in advance because it
depends on what the user does next, and the session must **survive resizing**: requeueing it
destroys the user's session state.

Both share three properties:

1. Demand varies widely **during** the workload's life, not just between workloads.
2. Peak demand is a poor predictor of average demand.
3. The workload must not be interrupted in order to change size.

## 2.2 Why gang scheduling is the wrong tool

Gang scheduling — equivalently, all-or-nothing admission of a statically sized workload — is
appropriate when a workload cannot progress without its full complement of workers and that
complement is known and stable. Neither holds here, and the reservation is sized for a peak that
is rarely reached.

Applying it anyway forces one of two bad outcomes. **Size for peak** and the reservation is held
for the workload's whole lifetime whether used or not — for an idle interactive session, hours of
capacity paid for and not used. **Size for the floor** and the workload never uses genuinely
available headroom, so jobs take longer and the infrastructure is under-used.

There is also a hard failure mode. With 512Mi executors, `minExecutors: 3` and `maxExecutors: 30`
against a 6Gi `ClusterQueue` (twelve slots), gang-admitting at the maximum needs 1 driver + 30
executors = **15.5Gi**, exceeding the entire quota: the workload would **never be admitted at
all**, despite running comfortably within quota in practice. Gang-admitting at the floor gives 2Gi
per application, so three applications consume the whole quota with no headroom.

Under elastic admission the same three applications ran between 2Gi and 6Gi of measured usage,
tracking real demand, never exceeding quota.

The point generalises: **for a workload whose demand varies during its life, a static reservation
is either wasteful or restrictive, and the gap between peak and average is the cost of the
choice.** Elasticity removes the choice.

## 2.3 Why quota-aware elasticity, not just autoscaling

Spark DA already provides elasticity at the framework level. What it does not provide is awareness
of a quota boundary. Left alone it scales against the cluster, not the tenant's allocation:
capacity is consumed with no corresponding grant so a queue's accounting understates real usage;
capacity released on scale-down is not returned to the queue so another tenant cannot reuse it;
and reported usage cannot be trusted for capacity decisions.

The value is the combination: **the framework decides how much it wants; Kueue decides how much it
may have, and keeps its accounting honest as that changes.**

## 2.4 Goals and non-goals

- **G1.** Admit a Spark application, then let it grow and shrink within quota while it runs,
  without requeueing it.
- **G2.** Keep the queue's reported usage tracking the workload's actual footprint as its
  autoscaler changes it.
- **G3.** Prevent a workload consuming capacity for which quota has not been granted.
- **G4.** Return capacity to the queue promptly on scale-down.
- **G5.** Never require the workload's own resource to be modified in order to account for it.
- **G6.** Leave existing elastic integrations and all non-elastic admission behaviour unchanged.

Non-goals: node-level gang or co-scheduling (placement remains the kube-scheduler's concern, and
for these workloads gang placement is explicitly not wanted); changing or overriding the
framework's autoscaling policy; guaranteeing a workload can always reach its configured maximum;
and MultiKueue support in the first iteration.

# 3. Proposal

Extend `ElasticJobsViaWorkloadSlices` to cover Spark applications autoscaled by DA, by treating
the workload's **observed footprint** as the source of truth for its size and withholding capacity
until quota has been granted for it. Three capabilities follow:

**Observed sizing.** The workload's size is determined by observing what it is actually running,
not by reading a declared value from its resource. Necessary because DA changes the footprint
without updating the resource, and because modifying that resource is not safe (§8.2, C-1).

**Granted-capacity enforcement.** Workers created by the framework do not begin consuming capacity
until Kueue has granted quota for them. The framework may ask for more at any time; it receives
more only when quota allows.

**Honest, symmetric accounting.** Reported usage reflects the real footprint in both directions, so
released capacity is promptly available to other tenants.

User stories, briefly: compaction jobs sharing a fixed namespace quota rather than one job
reserving it (US-1); an interactive session that grows and shrinks without restarting (US-2);
reported usage a capacity planner can trust (US-3); capacity released by one tenant promptly
usable by another (US-4); no behaviour change for existing elastic `batch/v1.Job` users (US-5).

# 4. Requirements and success criteria

| ID | Requirement | Status |
|---|---|---|
| **R-1** | The size used for quota accounting MUST track what the workload is actually running, as its autoscaler changes it. | Met |
| **R-2** | A workload MUST be able to change size while admitted, without being requeued or restarted. | Met |
| **R-3** | Workers MUST NOT consume cluster capacity before quota has been granted for them. | Met |
| **R-4** | Capacity MUST be returned to the queue on scale-down, without waiting for the workload to finish. | Met |
| **R-5** | Kueue MUST NOT modify the workload's own resource in order to account for it. | Met |
| **R-6** | Accounting MUST remain correct mid-resize, when several representations of the workload exist at once. | Met |
| **R-7** | Accounting MUST remain correct irrespective of the order or delivery of cluster events, including events that never arrive. | Met |
| **R-8** | A workload MUST be able to start below its configured maximum, and MUST NOT be rejected merely because its maximum exceeds available quota. | Met |
| **R-9** | Behaviour for non-elastic workloads MUST be unchanged, including the immutability guarantees they rely on. | Met |
| **R-10** | Existing elastic integrations MUST be unaffected. | Met |
| **R-11** | The number of workers awaiting quota SHOULD remain proportionate to the quota available. | **Not met** — §15.2 |
| **R-12** | Time-to-admission under a saturated queue SHOULD remain bounded. | **Not met** — §15.2 |
| **R-13** | The per-worker cost used for accounting MUST match what the framework actually requests of Kubernetes, for every resource the queue governs. | Met for CPU and memory — upstream for Kubeflow, this contribution for Apache (§9.5, §A5); the general risk remains — §6.3 |

Two invariants define correctness. Both are checked continuously by an automated harness, and both
are necessary — neither implies the other.

**SC-1 — Quota is a ceiling.** Reported usage never exceeds nominal quota. A breach means capacity
was granted twice for the same resources.

**SC-2 — Every running worker has a grant.** Capacity actually consumed by running workers never
exceeds reported usage. A breach means the cluster is oversubscribed **while the queue reports that
it is healthy**.

SC-1 fails loudly. SC-2 fails silently, and is the more dangerous of the two. A proposal for
elastic quota management should be judged against both.

**SC-2 is only as strong as the per-worker cost it is measured against.** If that cost is taken
from the same value the queue charges, the check is circular and passes regardless — see §14.

# 5. Risks

| Risk | Mitigation |
|---|---|
| Accounting for a workload requires modifying it, restarting it. | R-5: size is observed, never written back. |
| Quota released on scale-down is not actually reusable. | R-4 is stated as an outcome and validated, not assumed. |
| Elasticity weakens guarantees non-elastic users depend on. | R-9; every relaxation is confined to elastic workloads. |
| A workload is admitted, then starved of the growth it needs. | R-8; §15.2 records where this is currently imperfect. |
| Silent oversubscription. | SC-2 is a first-class success criterion, not an implementation detail. |
| The predicted worker Pod diverges from the one the framework actually creates. | R-13 holds for CPU and memory; the structural risk remains — §6.3. |

# 6. Open questions

## 6.1 Sizing guidance versus enforcement

An application configured to want far more than its quota can ever supply behaves correctly but
inefficiently: it repeatedly asks and is repeatedly refused. It is unsettled whether the answer is
documented guidance (size the maximum against the quota; start at the floor), a bound enforced by
Kueue, or both. Affects R-11 and R-12.

## 6.2 Scope deferred

MultiKueue support, and integration testing against a live scheduler in CI.

## 6.3 Predicted cost versus actual request — the structural risk

The item a reviewer should weigh most carefully, because it is a property of the *approach* rather
than of this implementation.

Kueue admits a workload by reconstructing a predicted worker Pod from the workload's resource and
charging quota for it. The Pod that actually runs is built independently, by the framework, from
the framework's own configuration. Nothing reconciles the two and Kueue never reads the running Pod
back. When the two derivations disagree, **the cluster follows reality and the ledger follows the
prediction**, silently.

Two such divergences existed in the Kubeflow integration and were fixed upstream (§9.5), which is
why R-13 is marked met. **The general risk is not closed by fixing two instances.** Any integration that predicts a
worker Pod from a custom resource inherits this failure mode, and elasticity makes it worse: a
static workload's divergence is a fixed error found once, whereas an autoscaling workload
multiplies it by a worker count that changes continuously and without bound. A complete design
would say how the predicted cost is kept faithful — validating it against observed Pods, deriving
it from the framework's own logic rather than re-implementing it, or reporting a discrepancy rather
than absorbing it. This proposal guarantees only the two resources Kueue governs here.

# 7. Alternatives considered

**Gang scheduling at a static size.** Rejected for these workloads — §2.2.

**Size at the floor and accept the throughput cap.** What these workloads do today. Leaves idle
capacity unused and makes compaction jobs take substantially longer than the infrastructure allows.

**Size at the peak.** Reserves capacity idle most of the time; for interactive sessions the peak is
not knowable in advance, so this is not merely wasteful but impossible to do correctly.

**Manual resizing by an operator.** Requires a human in a loop operating on the timescale of
notebook cells.

**Let the framework autoscale without quota awareness.** The status quo outside Kueue — §2.3.

**Per-integration elastic accounting.** Rejected: the hard problems are in the generic slicing
machinery, and solving them once benefits every integration. Three of the four bodies of work here
fix defects in **generic** `ElasticJobsViaWorkloadSlices` code, not in the Spark integration, and
two were only reachable under sustained autoscaling churn. Spark workloads exercise slice
replacement far harder than a `batch/v1.Job` resize does; any future elastic integration would have
hit the same issues.

**Writing the observed count back to the job's spec**, so the generic spec-diff pipeline works
unmodified. Implemented and reverted — it is incompatible with the operator (§8.2, C-1).

**Adding a declared count field to the Spark Operator API.** Dropped; the integration needs no
operator change at all (§16.1).

---

# Part II — Design

# 8. Context

## 8.1 The actors

| Actor | Role | Controlled by us? |
|---|---|---|
| Spark driver (`ExecutorAllocationManager`) | Creates and deletes executor Pods directly against the API server per DA policy. | No |
| Kubeflow Spark Operator | Reconciles the `SparkApplication` CR; submits the driver. | No |
| Kueue job reconciler (`jobframework`) | Builds `Workload`s from a job's `PodSets()`; manages slice lifecycle. | Yes |
| Kueue scheduler | Assigns flavours, grants quota, admits `Workload`s. | Yes |
| Kueue scheduler cache | Holds admitted usage; source of `ClusterQueue.status.flavorsUsage`. | Yes |
| `ElasticJobUngater` | Removes scheduling gates from Pods once quota is granted. | Yes |

The defining constraint is the first row. DA is an autonomous control loop Kueue cannot instruct.
It cannot be asked to stop creating Pods, and it does not consult or update the CR when it scales.
Everything here follows from having to *observe* that loop rather than *drive* it.

## 8.2 Two hard constraints

**C-1: Kueue must never write a count to `SparkApplication.Spec`.** The operator's
`EventFilter.Update` compares `.Spec` with `equality.Semantic.DeepEqual` and force-kills and
resubmits a running application on any difference outside a narrow exemption list:

| Exempt from resubmission | Condition |
|---|---|
| `spec.suspend` | always — this is what makes Kueue's suspend-based admission and `stopJob` viable |
| `spec.timeToLiveSeconds` | always |
| `spec.executor.{priorityClassName,nodeSelector,tolerations,affinity,schedulerName}` | only behind the operator's `PartialRestart` feature gate |

Executor counts are in none of them. A non-exempt difference sets
`status.applicationState.state` to `INVALIDATING`, which calls `deleteSparkResources`, resets the
status and moves the application to `PENDING_RERUN` — tearing down the running driver and re-running
the whole application. The operator documents this and names incremental executor scaling as
unimplemented future work
([Updating a SparkApplication](https://spark.kubeflow.org/en/latest/user-guide/working-with-sparkapplication.html)):

> If the application is currently running, the operator kills the running application before
> submitting a new run with the updated specification. [...] if the change was to increase the
> number of executor instances, instead of killing the currently running application and starting a
> new run, it is a much better user experience to incrementally launch the additional executor pods.

**C-2: A scheduling gate can only be applied through the Pod template.** The gate is injected into
`spec.executor.template` at CR-creation time. Spark's driver loads that template file **once at
startup** and merges it — Fabric8 merge semantics, not replace — into every executor Pod for the
application's lifetime. This is what makes a template-level gate reach Pods DA creates hours later.
It also means gating is all-or-nothing per application: there is no per-Pod decision point at
creation time, only the later decision of whether to *remove* the gate.

## 8.3 Why the existing feature does not fit

`ElasticJobsViaWorkloadSlices` assumes a job's desired PodSet counts are visible on the job object
(e.g. `batch/v1.Job.Spec.Parallelism`), and that changing that field is how a user or autoscaler
expresses "I want N pods now". The generic reconciler diffs the job's `PodSets()` against the
active slice's admitted counts and creates or updates a slice to match.

DA breaks the assumption: the driver's executor manager creates and deletes Pods directly against
the API, bypassing the CR, and `spec.executor.instances` is read once at submission and never
updated.

**The controlling insight** is that the feature's actual contract is `PodSets()` returning the
desired counts — it does not require them to originate from `.Spec`. `PodSets()` already receives a
`client.Client`, so it can compute the count by listing live Pods and never touch `.Spec` while the
application runs. Nothing in `EnsureWorkloadSlices` needed to change: it already takes desired
counts as an opaque input and does not care where they came from.

# 9. Sizing the workload

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
              liveExecutorCount ──> List Pods by label ──> count ──> clamp
                                              │
                                              ▼
                          EnsureWorkloadSlices ──> patch in place | new slice
```

Nothing in this path writes to the CR (C-1).

## 9.1 What counts as live

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
  `Running`; otherwise there is a window where DA has consumed real capacity Kueue does not know
  about.
- **Terminating Pods count.** A Pod with a `DeletionTimestamp` keeps occupying node resources until
  it reaches a terminal phase. Excluding it the instant a delete is issued would undercount live
  consumption and manufacture spurious intermediate counts as DA works through a batch of
  deletions.
- **Gate-blocked Pods count.** Load-bearing, and the origin of §15.2. A gated Pod consumes no node
  capacity, so counting it overstates consumption — but it is the *only* signal that DA wants more.
  Excluding gated Pods would mean the count never grows, no replacement slice is created, no quota
  is granted, the gate is never removed, and the job cannot scale at all. **The count is "what DA
  wants", not "what is consuming".**

## 9.2 Reconcile-scoped memoisation

`PodSets()` is called several times per reconcile — `ensureOneWorkload`, `EquivalentToWorkload`,
`ConstructWorkload` — all backed by the same informer cache, which can observe a new Pod event
*between* calls while DA is scaling. Two calls seeing different counts would produce a spurious
"not equivalent" verdict and self-inflicted slice churn.

The count is memoised on the `*SparkApplication` wrapper. Because `NewJob()` allocates a fresh
wrapper per reconcile, the cache is scoped to one reconcile pass by construction and can never go
stale across passes. The same pattern carries the workload sequence number.

## 9.3 Resolving a declared count

Both the structured field and `sparkConf` declare executor counts, and the operator accepts both,
so Kueue must read both. Reading only the structured field is a **quota-evasion path**: an
application declaring `spark.executor.instances: "15"` with no `spec.executor.instances` has its
executor PodSet sized at zero, so fifteen executors run against a queue charged for one driver —
and nothing can hold them back, because a non-elastic job gets no scheduling gate. This is upstream
behaviour, not introduced here.

```go
if j.Spec.Executor.Instances != nil {
    return *j.Spec.Executor.Instances, nil
}
n, ok, err := j.sparkConfExecutorInstances()
if err != nil { return 0, err }
if ok { return n, nil }
return defaultExecutorInstances, nil      // Spark's 2, not 0
```

- **Precedence, not a maximum.** The structured field is the declared intent; the conf key is the
  fallback for applications that configure Spark directly.
- **Default to Spark's 2, not 0.** An application declaring no count anywhere still gets two
  executors from Spark; reserving zero is the same hole in miniature.
- **A malformed value is an error**, rejected by `validateCreate` with a message naming the field.
  Treating it as absent — which the DA bound lookups deliberately do — would resurrect a zero-sized
  PodSet from a typo.

**On the DA path the three-way combination is a maximum, not a precedence.**
`declaredInitialExecutors()` resolves as Spark's `Utils.getDynamicAllocationInitialExecutors` does
— the largest of `minExecutors`, `initialExecutors` and the resolved instances count. Each
individual property still resolves its structured field before its `sparkConf` equivalent, so the
rules compose: **precedence decides where one property's value comes from; the maximum decides
which property governs the count.** Spark starts the largest of the three regardless of which the
author thought authoritative, so a precedence ladder across them would under-reserve.

**`dynamicAllocationEnabled` is an OR, and must stay one.** It reads the structured field *or* the
`sparkConf` key, so a structured `enabled: false` alongside `sparkConf` `"true"` yields **true**.
That looks like an inconsistency but is not fixable: the CRD declares

```go
Enabled bool `json:"enabled,omitempty"`
```

a **non-pointer `bool` with `omitempty`**, making an explicit `false` indistinguishable from an
omitted field. Honouring "explicit false" would need an upstream API change to `*bool`, which this
integration deliberately avoids depending on (§16.1). Letting the structured surface win regardless
would misread an ordinary manifest that sets bounds structurally and enablement through
`sparkConf`: `dynamicAllocation` is non-nil but `Enabled` reads false, so the application would be
classified **static** and sized from `spec.executor.instances` while DA actually scaled the Pods.
Under-reserving because a bool cannot be told apart from its zero value is the worse failure. The
OR is also safer on its own terms — treating an application as elastic when either surface says so
routes accounting through the live-Pod derivation, which tracks reality, and the cost of a false
positive is bounded. `TestDynamicAllocationEnabled` pins this as a tripwire so a future
"consistency" cleanup fails a named test with the reasoning attached.

## 9.4 Clamping to Dynamic Allocation's own bounds

The declared estimate applies only while **zero** executor Pods exist; the instant one appears the
live count takes over. But the driver creates its initial executors one API call at a time, so
there is a window where the live count is a strictly smaller prefix of the intended initial count.
A reconcile landing in that window reads a scale-down and patches the slice — and the grant — down,
dismantling a gang that was just admitted. The Pod-event debounce (§9.6) does not help, because the
reconciler also watches the `SparkApplication` itself and the operator updates its status
repeatedly during startup; those reconciles are not debounced, and a transient prefix observed
through a foreign trigger is indistinguishable from a real scale-down.

```go
func (j *SparkApplication) clampToDynamicAllocationBounds(count int32) int32 {
	if n, ok := j.dynamicAllocationExecutorCount("minExecutors"); ok && count < n { count = n }
	if n, ok := j.dynamicAllocationExecutorCount("maxExecutors"); ok && count > n { count = n }
	return count
}
```

The **lower bound is the load-bearing half**. `minExecutors` is a floor DA never sustains fewer
executors than, so reporting below it is never *more* accurate — it only describes a transient DA is
actively correcting. Holding the floor removes the whole class of startup churn, and the churn from
an executor dying and being replaced. `maxExecutors` is applied last, so a configuration with
`minExecutors > maxExecutors` can never inflate the count above the declared maximum.

The same clamp applies to the initial estimate, so `instances: 1` with `minExecutors: 5` reserves
the floor rather than admitting the driver without its initial executors.

The upper clamp **narrows but does not close** the gated-Pod loop (§15.2): it converts an unbounded
climb into a bounded over-request, but when `maxExecutors` exceeds what the queue can grant the
over-request still cannot be admitted. Closing it properly needs a bound derived from the queue's
capacity, which the `PodSets(ctx, client)` signature does not expose.

Unchanged: with DA disabled, the static count path short-circuits before any of this; with no
bounds configured the clamp is the identity function.

**Operational consequence.** With `initialExecutors: 1` and `minExecutors: 3` the first Workload
is driver 1 + executor 1 and the floor of 3 is reached by two further slice replacements, each
needing fresh quota. Omitting `initialExecutors` makes both Kueue and Spark start at
`minExecutors`, so the floor is admitted atomically. This is the cheapest lever on §15.2.

## 9.5 What Kueue charges — upstream behaviour this design depends on

**Attribution.** For the Kubeflow integration the charging rules below are implemented in
`pkg/controller/jobs/sparkapplication/sparkapplication_resources.go`, which is **upstream code
this contribution does not modify**. They are documented here because elastic accounting is only
as good as the per-worker cost it multiplies (§6.3), and because the Apache integration in Part III
implements the equivalent arithmetic in its own package, where it *is* part of this contribution.

The governing rule: **a PodSet must charge what the kubelet will be asked for.** Where two surfaces
disagree, the one Spark reads wins — not the one that looks most structured.

**A pod-template resource request is not authoritative.** Neither operator hands the template to
the kubelet; both serialise it to a file and pass
`spark.kubernetes.{driver,executor}.podTemplateFile`. Spark loads that file as the *initial* pod
and runs its feature steps over it — `BasicExecutorFeatureStep` and `BasicDriverFeatureStep` both
do:

```scala
new ContainerBuilder(pod.container)
  .editOrNewResources()
    .addToRequests("memory", memoryQuantity)   // base + overhead
    .addToLimits("memory", memoryQuantity)
    .addToRequests("cpu", cpuQuantity)
```

`addToRequests` replaces the key, so a template request of `512Mi` alongside Spark's default 1g
heap produces a pod requesting 1408Mi. Charging the template can only ever under-charge or
introduce divergence: when a submitter writes the correct total, the arithmetic already computes
that same number. Both integrations therefore always derive from Spark's configuration. The Apache
operator's own Kueue integration overwrites the template the same way, commented *"Like Spark,
overwrite the requests of the pod template."*

**Memory is base plus overhead.** `spec.{driver,executor}.memory` is the JVM **heap** size, not the
pod's request:

```
base      = spec.{role}.memory  ->  spark.{role}.memory  ->  Spark's 1g default
overhead  = spec.{role}.memoryOverhead  ->  spark.{role}.memoryOverhead
            ->  max(trunc(factor x base), spark.{role}.minMemoryOverhead or 384MiB)
factor    = spec.memoryOverheadFactor  ->  spark.kubernetes.memoryOverheadFactor
            ->  0.4 for Python/R, else 0.1
total     = base + overhead
            + spark.executor.pyspark.memory   (executors only)
            + spark.memory.offHeap.size        (only when offHeap.enabled)
```

A 512m executor therefore requests **896Mi**, not 512Mi — 512 of heap plus Spark's 384MiB floor,
because 0.1 × 512 is below it. Four fidelity details, each of which silently produces
plausible-looking but wrong numbers if missed:

- **The arithmetic is in whole MiB and truncates.** Spark computes
  `(overheadFactor * memoryMiB).toInt`, so 0.4 × 8192 is 3276, not 3277. Doing it in bytes with
  rounding disagrees with the pod by a fraction of a MiB.
- **A value with no unit suffix is MiB** — Spark's convention and what the CRD documents for
  `memoryOverhead`. Parsed as a `resource.Quantity`, `"384"` would otherwise be 384 *bytes*.
- **The factor default depends on application type**, but only when no factor is set explicitly. An
  explicit `memoryOverheadFactor` applies to Python applications too.
- **`spark.{driver,executor}.minMemoryOverhead` is honoured.** Spark 4.0 made the 384MiB floor
  configurable, *"ignored if `spark.{role}.memoryOverhead` is set directly"* — so it is read on the
  factored path only. Neither CRD has a structured equivalent.

Constants come from the operator's own `pkg/common` rather than being duplicated, so they cannot
drift from the operator Kueue is integrating with. When no memory is configured through any Spark
surface the request is left unset, rather than inventing Spark's 1g default for an application that
never asked for memory. An explicitly configured `memoryLimit` is raised to the computed request
when it sits below it, since it is written against the heap size and Spark sets the limit equal to
the request.

**CPU resolves through a fallback chain**: `spark.kubernetes.{role}.request.cores` / `coreRequest`,
then `spark.{role}.cores` / `spec.Cores`, then Spark's default. There is deliberately **no**
fallback to `limit.cores`, matching Spark's own precedence, which reads that only for the CPU limit.
Reading only `coreRequest` previously left the PodSet with no `cpu` entry at all, so the CPU quota
was decorative and admission was governed by memory alone. That is fixed upstream, which **retires
the `coreRequest: "1000m"` workaround** this work formerly required; it should no longer be
recommended.

**Behaviour change, and it is not silent.** Usage for an unchanged manifest rises by the overhead —
at minimum 384MiB per pod, more for large executors or Python applications. Queues sized against
the old under-charge will admit fewer workloads. That is the point: the new number is what the pods
actually request, so the previous behaviour was over-admitting against real node capacity. Anyone
upgrading should expect to re-size per-queue `nominalQuota`.

## 9.6 Event handling and debouncing

Executor Pods are owned by the **driver Pod**, not the CR, so there is no OwnerReference chain to
key an `Owns()` watch off. `isTrackedExecutorPod` filters on the `sparkoperator.k8s.io/app-name`
label instead — the same label the Pod listing already depends on.

DA bursts produce many Pod events in quick succession. `executorPodHandler` keeps a per-key timer,
reset on each new event, so a request reaches the workqueue only after a quiet window
(`executorPodDebounce = 5s`). A pure quiet-window debounce can be starved indefinitely by a
continuous stream, so a ceiling (`executorPodMaxWait = 30s`) forces a flush.

Per-key timers guarded by a mutex are used rather than the workqueue's own `AddAfter` delay heap,
because staggered `AddAfter` calls from a burst each fire independently and defeat the coalescing.
The clock is injected so the behaviour is unit-testable.

## 9.7 Slice naming

`GetWorkloadNameExtraPart` must produce a name never reused for the lifetime of the application.
Two obvious inputs both fail:

- **`Generation`** — the framework default. DA never changes `.Spec`, so it stays frozen across
  every scale event after the first.
- **The live executor count** — a Finished slice is never deleted absent a retention policy, so its
  deterministic name persists in etcd forever. A workload oscillating within a narrow band would
  eventually revisit a previously-used count, recompute the same hash, and collide with a dead
  object: a permanent, self-reinforcing failure once every count in the band has been used.

The implementation folds in a **workload sequence number** — the count of every `Workload` ever
owned by this job, Finished or not, via the existing owner-reference index. It only ever grows. It
is computed in `PodSets()` (which has a client) and cached for `GetWorkloadNameExtraPart()` (which
does not).

## 9.8 Registration

The GVK is added to `supportedElasticJobGVKs` in `pkg/controller/jobframework/validation.go` and to
the supported-integration list in the site docs, so the `kueue.x-k8s.io/elastic-job: "true"`
annotation is accepted for it.

Note also what was **removed**: `RestorePodSetsInfo` previously wrote the admitted count back to
`spec.executor.instances` on stop or evict. That is the spec mutation C-1 forbids, and it is
unsound on its own terms — a scaled-to-zero PodSet count is legal for a Workload (`Minimum=0`) but
not for `spec.executor.instances` (`Minimum=1`), so it could produce a validation-rejected patch.

# 10. Admission control

## 10.1 Why accurate sizing is not enough

Deriving the count accurately is purely descriptive. Nothing in it stops a DA-created executor Pod
from running. DA creates Pods directly against the API, outside any Kueue-mediated admission step,
so by the time the reconciler learns a new executor exists the Pod is already `Pending` or
`Running` and already consuming real node resources. If the cohort has no spare quota the new slice
simply sits un-admitted while the running Pod keeps consuming ungoverned.

## 10.2 The pattern that already exists

`ElasticJobsViaWorkloadSlices` solves this for `Job` and `RayCluster` with a scheduling gate:

- Each integration's mutating webhook injects `kueue.ElasticJobSchedulingGate` into its own Pod
  template at CR-creation time. A gated Pod can exist and sit `Pending`, but cannot be scheduled.
- One generic controller, `ElasticJobUngater`, watches gated Pods and their owning Workloads. For
  each admitted slice it computes `room := granted - alreadyUngated` per PodSet and removes the
  gate from up to `room` Pods, lowest-name-first for determinism.
- Critically, it **never queries the scheduler cache**. It trusts the Workload's own
  `QuotaReserved`/`Admitted` conditions, which the main scheduler set after checking real cohort
  capacity. It is a thin "translate an already-vetted grant into ungated Pods" step, not a second
  admission-decision engine.

Extending this was the only design considered: it reuses machinery already hardened for two
integrations rather than inventing a parallel capacity-check subsystem.

## 10.3 Why the gate reaches Dynamic-Allocation Pods

Non-obvious, and traced across three codebases before implementing. `Job` and `RayCluster` gate
Pods that the *same controller* which read the gated template instantiates on every scale event.
SparkApplication is different: DA-created Pods are built by the **Spark driver process itself**,
not by the operator reconciling the CR, and DA never re-reads the CR after startup.

1. **Operator submission.** At `spark-submit` time `spec.executor.template` is serialised to a
   pod-template **file**, mounted into the driver Pod, and passed as
   `spark.kubernetes.executor.podTemplateFile=<path>`.
2. **Spark's driver.** `KubernetesExecutorBuilder`/`ExecutorPodsAllocator` load that file **once,
   at driver startup**, and reuse the *same* loaded `Pod` object as the base for **every** executor
   it ever builds — the initial batch and every later DA-created executor go through the identical
   `buildFromFeatures` call.
3. **Nothing drops the gate.** Both `spec.schedulingGates` and `metadata.labels`/`.annotations`
   survive: the feature steps use Fabric8 fluent methods that **merge**, not replace, and no feature
   step in the executor build path reconstructs `PodSpec` from an allow-list that would drop an
   unrecognised field.

So a gate baked into the template at CR-creation time reaches every DA-created executor Pod for the
application's whole lifetime, with no time-window or generation cutoff. No new Kueue-owned Pod
webhook is needed.

## 10.4 Why the one spec write is safe

Gating mutates `spec.executor.template` — a `.Spec` field, not exempt under C-1 — so the write had
to be verified safe. It is, because it happens exactly once, before the application runs:

- The webhook's `Default()` runs only for the `create` verb, i.e. before the object exists; there is
  no `Update` event for the operator's filter to compare against yet.
- `RunWithPodSetsInfo` also touches `Template.Spec.SchedulingGates`, but only merges into it
  (`podset.PodSetInfo.Merge` is append/dedup, never replace), so the webhook's gate is preserved.
- `RunWithPodSetsInfo` fires only at the `Suspend: true → false` transition. A later scale-up
  creates a new slice for an already-unsuspended application and never re-enters that branch —
  `EnsureWorkloadSlices` operates purely on `Workload` objects.

## 10.5 Injection, validation and ungating

```go
if isAnElasticJob(obj) {
    if job.Spec.Executor.Template == nil {
        job.Spec.Executor.Template = emptyExecutorPodTemplateSpec.DeepCopy()
    }
    utilpod.GateTemplate(job.Spec.Executor.Template, kueue.ElasticJobSchedulingGate)
}
```

A direct mirror of RayCluster's pattern, scoped to the **executor** template only. `GateTemplate`
is idempotent. `validateElasticJob` rejects an elastic application whose executor template lacks
the gate, on create and update, so a future code path that replaced the template wholesale would
fail validation rather than fail silently at runtime.

**Only executors are gated.** The driver is created by the operator only after Kueue unsuspends the
CR, which already implies admission, and there is no scale-up scenario for the driver PodSet.
Consequence worth knowing when reading metrics: drivers never appear in a gated-Pod count.

**Ungating needed no changes.** `ElasticJobUngater` already matches Pods via
`constants.PodSetLabel` and `kueue.WorkloadSliceNameAnnotation`, both of which executor Pods carry
— `RunWithPodSetsInfo`'s pre-existing merge writes them from the same `PodSetInfo` the generic
reconciler populates. These reach DA-created Pods too: the operator translates
`spec.executor.Labels`/`.Annotations` into `spark.kubernetes.executor.label.*` confs at submission,
which the driver applies by the same merge-not-replace mechanism as §10.3.

**The cap must be the grant, not the request.** `podsToUngate` originally read
`ExtractPodSetCountsFromWorkload`, which returns `spec.podSets[].Count` — the **request**. For an
elastic job the two diverge, because a scale-up creates a replacement slice rather than growing the
grant, so the ungater authorised more Pods than the scheduler had paid for. The ledger stayed
inside `nominalQuota` while the cluster was oversubscribed — an SC-2 violation, and invisible.

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

`PodSetAssignment.Count` is optional in the API. The scheduler always sets it, but an assignment
lacking it falls back to the PodSet's own count — matching `totalRequestsFromAdmission`, so a
legacy or hand-written admission is not read as a grant of zero, which would deadlock ungating
entirely. `ExtractPodSetCountsFromWorkload` gained a doc note pointing at the new function: the
naming is what made this easy to get wrong, so part of the mitigation is documentary.

With the cap correct, surplus executors stay `Pending` rather than running, which makes §15.2 more
visible. The two want addressing together.

## 10.6 Why sizing and gating are both needed

They look alike — both watch executor Pods — but answer different questions:

- **The live count → `PodSets().Count`** answers *"how much is this job currently asking for?"* It
  sizes the Workload so the scheduler has an accurate number to admit against in the first place.
- **The gate plus ungater** answer *"is there capacity to let a Pod that already exists actually
  run?"* It is the admission-side check, acting on Pods already counted.

RayCluster needs no live-count equivalent, for a reason specific to its operator model rather than
because gating alone suffices: Ray's in-tree autoscaler scales by patching
`spec.workerGroupSpecs[i].replicas` on the CR itself, and KubeRay reconciles that spec into Pods —
`RayCluster.PodSets()` reads `wgs.Replicas` directly, with no Pod listing anywhere in that path.
Ray's autoscaler is a client of the CR's spec field; Spark's DA is not. That is a difference in
where each system's scaling state lives, not in whether gating is needed. Both use the identical
gate/ungate mechanism for admission; only SparkApplication additionally needs live-Pod counting for
*sizing*, because only its spec field goes stale.

**Alternatives considered.** A SparkApplication-specific Pod-admission webhook intercepting raw
executor Pod `CREATE`s: rejected once §10.3 confirmed the template approach already reaches every
DA-created Pod — a new webhook would duplicate the ungater-matching machinery for no added
coverage and add a second failure-mode surface (`failurePolicy`, ordering against the operator's
own pod webhook). A SparkApplication-specific ungate controller doing a live scheduler-cache query:
rejected as duplicate admission logic that could disagree with the scheduler and would be
significantly harder to review upstream. Gating the driver template: rejected per above.

# 11. Accounting correctness

## 11.1 The central asymmetry

On a scale-up, three structures legitimately hold three different numbers. An elastic job expresses
growth by creating a **replacement slice** — a new `Workload` annotated
`kueue.x-k8s.io/workload-slice-replacement-for: <predecessor>` and
`kueue.x-k8s.io/workload-slice-name: <chain root>`. The predecessor is then `Finish`ed with reason
`WorkloadSliceReplaced`.

| Consumer | Value it sees | Why that is correct for it |
|---|---|---|
| Scheduler snapshot | **Delta** (new − replaced) | `flavorassigner.Assignment.append` subtracts the replaced slice's request. `Scheduler.schedule` then calls `cq.AddUsage(usage)` on a snapshot that *already* holds the predecessor's full usage, so `old_full + delta = new_full`. |
| `status.admission` | **Full** new count | `Assignment.ToAPI()` is not delta-adjusted. It must describe the entire PodSet, because gating, ungating and reclaimable-pod accounting all read it. |
| Persistent scheduler cache | Re-derived from `status.admission` | It is the source of `ClusterQueue.status.flavorsUsage`, via `Cache.Usage` → `clusterQueue.AdmittedUsage`. |

The snapshot is ephemeral and rebuilt every cycle, so a delta is safe there. The persistent cache
is long-lived and had **no concept of slice replacement at all** — it simply summed each cached
`Workload`'s full admitted usage, so two slices describing the same Pods were charged twice.

**The one behaviour that is easy to get wrong.** Usage is **not** simply the granted count.
`workload.totalRequestsFromAdmission` rescales admitted usage down to the spec count whenever the
spec count is lower:

```go
currentCounts := podSetsCountsAfterReclaim(wl)      // = spec.podSets[].Count (minus reclaimable)
...
if countAfterReclaim := currentCounts[psa.Name]; countAfterReclaim < setRes.Count {
    setRes.Requests.Divide(int64(setRes.Count))
    setRes.Requests.Mul(int64(countAfterReclaim))
}
```

The effective charge is **`min(spec, granted)`**. This matters twice over: a stale-high grant never
inflates usage, which is why §11.3 is hygiene rather than a correctness fix; and a grant *lower*
than the spec is not corrected by the ledger, which is the hole §10.5's grant-aware cap closes. **A
reviewer should verify this before reasoning about any of the accounting below** — it is the most
misread behaviour in this area.

## 11.2 Charge the chain, not the slice

The predecessor leaves the cache only when its `Finish` propagates back, which is asynchronous. It
is also deliberately excluded from preemption targets (it is "evicted rather than preempted"), so
nothing removes it synchronously either. Between admission of the replacement and the
predecessor's removal, the cache holds `old_full + new_full`.

Usage is therefore accounted per **slice chain**. `clusterQueue` gains:

```go
sliceGroups         map[string]sets.Set[workload.Reference]   // chain key -> member slices
countedSliceInGroup map[string]workload.Reference             // chain key -> the slice charged
```

`addOrUpdateWorkload` and `deleteWorkload` maintain membership, then call `reconcileSliceGroup`,
which moves the charge to the chain's current tip:

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
- **Idempotent.** Re-adding a superseded slice leaves the charge where it is — which matters,
  because a predecessor stays admitted in the API until its `Finish` lands, so unrelated events can
  re-add it.

Ordering within a chain is decided by creation timestamp (matching `FindNotFinishedWorkloads`),
then the replacement annotation to break same-second ties, then UID for determinism.

**The chain key** is `namespace + "/" + WorkloadSliceNameAnnotation + "#" + job UID`, with
defensive fallbacks for a slice lacking the chain-root annotation, and an empty key — meaning "not
a slice, use the original path" — for anything with no elastic or slice annotation. That last branch
keeps the hot path allocation-free for ordinary workloads.

**The job UID is not decoration.** A job deleted and recreated produces slices that inherit the
previous instance's chain-root name. Without the UID those two independent jobs would share a chain
and only the newest would be charged — an *undercount*, the more dangerous direction.

**Preemption interaction.** A superseded slice's usage is not in the ledger, so it must not be
offered as a preemption candidate: `SimulateWorkloadRemoval` would subtract usage that was never
added, understating the ClusterQueue and over-admitting. Preempting one frees nothing anyway, since
its Pods are attributed to its replacement. `ClusterQueueSnapshot` therefore carries
`supersededSlices`, exposed as `SliceSuperseded`, and both candidate collectors skip them:
`classical.getCandidatesFromCQ` and `preemption.findCandidatesForPolicy`. The latter's signature
changed from taking a workload map to taking the owning `*ClusterQueueSnapshot` so the predicate is
reachable. No other snapshot change was needed: `snapshotClusterQueue` clones the already-netted
`resourceNode`, inheriting corrected totals rather than re-summing.

**Rejected alternative: keying membership off the replacement-pointer graph.** With chain
`a → b → c` all cached, deleting the middle slice leaves `a` with no cached replacement, so `a`
resumes being charged and the overcommit returns. Chain grouping has no equivalent hole, because
membership does not depend on surviving intermediate links.

## 11.3 Lowering the grant on scale-down

`EnsureWorkloadSlices` handles scale-down by patching the slice in place, which writes
`spec.podSets[].Count` only; `status.admission.podSetAssignments[].Count` keeps the value granted at
admission.

Per §11.1 the ledger charges `min(spec, granted)`, so **a stale-high grant does not inflate
usage** — this is hygiene, not a correctness fix, and a regression test pins that specifically
because the opposite is a tempting inference from "usage is derived from `status.admission`". It is
fixed anyway because the stale value forced two validation workarounds, the replacement delta in
`flavorassigner` is computed against it, and it is what operators see via `kubectl`.

`scaleDownAdmission` lowers each `PodSetAssignment.Count` that exceeds its new spec count and brings
the dependent fields with it:

- **`ResourceUsage`** rescaled proportionally. It is the PodSet **total**, not per-Pod — divided by
  the old count and multiplied by the new, the same arithmetic `totalRequestsFromAdmission` uses for
  reclaimable Pods.
- **`TopologyAssignment`** truncated via `utiltas.TruncateAssignment`, so TAS domain accounting
  stays consistent with the reduced count.

It is applied after the spec update lands, as a separate `Status().Update` under
`retry.RetryOnConflict`, because admission lives on the status subresource. A failure leaves the
previous behaviour and is retried next reconcile — it degrades rather than corrupting.

**This required a webhook exception.** `validateAdmissionUpdate` made `status.admission` fully
immutable once set, except for TAS assignments, so the patch above was rejected by Kueue's own
validating webhook. The exception is deliberately narrow: a **decrease only**, for an **elastic
workload only**, implemented by copying the lowered `Count` and `ResourceUsage` onto the old value
before the immutability comparison — so increases, flavour changes and PodSet-count mismatches are
still rejected, and growing a grant still goes through the scheduler where quota is checked. It
mirrors the exception `validateImmutablePodSet` already makes for `spec.podSets[].Count` on elastic
jobs, which is the precedent that made this shape acceptable. **Non-elastic workloads, including
plain `batch/v1.Job`, keep a fully immutable admission.**

# 12. Generic hardening of workload slicing

Two fixes general to any elastic-job integration, not specific to Spark:

1. **Admission-race retry correctness.** `updatePodSetCountsWithRetry` could retry an in-place
   update of PodSet counts after the scheduler had concurrently admitted the same workload,
   reapplying a stale target count and permanently desyncing `spec.PodSets` from
   `status.admission.podSetAssignments`. Every attempt, including the first, now re-fetches the
   workload and re-validates eligibility before proceeding, aborting with
   `errWorkloadAdmittedConcurrently` so the caller creates a new slice instead of corrupting the
   existing one.
2. **Cache-lag tolerance.** `EnsureWorkloadSlices` and `prepareWorkloadSlice` run moments apart in
   the same reconcile and can legitimately both observe "one admitted slice + one pending
   replacement". The latter previously treated any not-finished count above 1 as a fatal error; it
   now reuses the exported `NormalizeActiveSlices` — the same deterministic algorithm
   `EnsureWorkloadSlices` already uses — to resolve the ambiguity into a single answer.

# 13. Concurrency, failure modes and observability

| Concern | Handling |
|---|---|
| Cache mutation | `Cache.AddOrUpdateWorkload`/`DeleteWorkload` take the write lock; `addOrUpdateWorkload`/`deleteWorkload`/`reconcileSliceGroup` run under it. |
| Snapshot reads | `Cache.Snapshot` holds the read lock; `supersededSliceKeys()` allocates a fresh set and mutates nothing. |
| Race verification | `go test -race` across the cache, scheduler, workload and elasticjobs packages. |
| Optimistic-lock conflicts | Re-fetch and re-validate before every retry attempt (§12). |
| Watch-cache lag | More than one not-finished slice is legitimate; converge via `NormalizeActiveSlices` (§12). |
| Debounce state | Per-key timers and burst-start times guarded by a mutex. |

| Failure | Behaviour | Rationale |
|---|---|---|
| Predecessor's `Finish` never lands | Accounting stays correct; the superseded slice contributes zero. | §11.2 is declarative, not event-driven. |
| `scaleDownAdmission` patch fails | Falls back to spec-low/grant-high; retried next reconcile. | The ledger charges `min(spec, granted)`. |
| `PodSetAssignment.Count` absent | Falls back to the PodSet count. | Prevents a grant of zero deadlocking ungating. |
| Executor Pod list fails | `PodSets()` returns the error; reconcile retries. | No partial accounting is written. |
| Job deleted and recreated with the same chain-root name | Separate chains, via the job UID. | Prevents undercounting two independent jobs. |
| Feature gate off | `sliceChainKey` returns empty; original accounting path. | No behaviour change for non-elastic clusters. |

| Signal | Where | Use |
|---|---|---|
| `Workload slice chain usage moved to a later slice` (V3) | `reconcileSliceGroup` | Chain accounting engaging; includes chain key, from and to. |
| `Workload slice superseded ... releasing its quota usage` (V3) | chain accounting | The moment a predecessor stops being charged. |
| `elastic ungating quota accounting for PodSet` (V4) | `podsToUngate` | `grantedCount`, `alreadyUngatedCount`, `gatedCount`, `ungatingCount`. |
| `kueue_cluster_queue_resource_usage` | metrics | Same source as `flavorsUsage`; graphable against `..._nominal_quota`. |

The two chain-accounting lines are at V(3), so `--v=3` is required to observe them.

# 14. Validation

**Unit tests are organised around the invariants** rather than the functions: each states the
invariant it protects. Coverage includes both slice arrival orders, in-place update, re-add after
delete, replacement rollback, three-slice chains including deletion of the middle slice, two
replacements racing for one predecessor, the recreated-job chain-root collision, the feature gate
off, and a full `scheduler.schedule()` cycle with the predecessor's `Finish` forced to fail via a
client interceptor. Count resolution and the clamp are covered across both declaration surfaces and
all bound combinations; charging is covered across the overhead floor, the factor, explicit
overhead, bare MiB values, the non-JVM factor, PySpark and off-heap memory, and the
`minMemoryOverhead` floor.

**Every correctness test is negative-controlled** — run with its fix disabled and confirmed to fail
with the defect's signature. This caught one first-draft assertion that proved tautological, and is
the reason the suite is trustworthy.

`gofmt -l` clean, `go vet`, `go test`, `go test -race -count=1` on the affected packages, and
`go build ./...` across the tree.

**Cluster validation** samples both invariants every two seconds and captures forensics on
violation. Two details make its verdicts trustworthy: Pods are read *before* the ClusterQueue, so a
scale-up landing between reads biases the ledger high and against false "Pods exceed usage"
reports; and a suspected violation is re-read a second later and only reported if it survives,
which is what distinguishes a real violation from sampling skew.

Final results: SC-1 held on every sample with peak usage equal to `nominalQuota`, and SC-2 held on
every sample, against three concurrent DA applications in a 6Gi queue. Before this work the same
workload reported 9Gi against that 6Gi quota. Usage tracked the workload's footprint as counted in
workers exactly, and quota released on scale-down was promptly reusable.

**One limitation bounds what that proves.** The harness derived each worker's cost from the same
configured value Kueue charges, so for memory it compared the ledger against a restatement of its
own assumption. It verifies that the ledger tracks Pod *count* correctly; it cannot detect a
systematic per-Pod under-charge. Summing actual `resources.requests` from the Pod snapshot removes
the circularity and is a prerequisite for the next validation round.

# 15. Known gaps

## 15.1 Gate-blocked Pods inflate the requested count

Per §9.1 a gated Pod counts as live. When the queue is saturated the replacement slice cannot be
admitted, so nothing raises the grant, so the Pods stay gated and keep being counted, so the
requested count grows. Observed: 68 gate-blocked Pods against a twelve-slot quota, a 24s admission
wait, and sustained reconcile-conflict churn. Correcting the ungating cap (§10.5) makes this more
visible, because surplus executors now correctly remain `Pending`.

The obvious fix is wrong, as §9.1 explains: excluding gated Pods removes the only scale-up signal. A
correct fix must **bound** the request. Two candidates, both behaviour changes warranting their own
proposal:

- **Latch.** Once a replacement slice exists and is unadmitted, stop revising its count upward
  until it is admitted or dropped. Bounds churn without needing quota awareness.
- **Incremental growth.** Cap the request at `granted + step`. Simpler, at the cost of slower
  scale-up.

The `maxExecutors` clamp (§9.4) already converts an unbounded climb into a bounded over-request.
Configuration mitigations, pending measurement: size `maxExecutors` against available quota, and set
`initialExecutors` equal to `minExecutors` so the floor is admitted atomically.

## 15.2 Smaller items

- `Scheduler.replaceOldWorkloadSlice` does not retry a failed `Finish`. Harmless for accounting
  after §11.2, but latent.
- `scaledDownPodSetNames` and `isPreexistingStaleCount` are retirable once §11.3 has shipped long
  enough that no objects carry stale grants.
- No envtest or live-cluster integration tests; the development environment cannot reach the
  kubebuilder-tools host.
- Admission-latency instrumentation exists but the configuration comparison has not been run.

# 16. Applying this pattern to another integration

The generic machinery is integration-agnostic. Adapting it requires:

1. **A live-count source.** List the framework's Pods by a stable label and count non-terminal
   ones. Memoise per reconcile.
2. **A watch and debounce.** Filter Pod events by that label; coalesce bursts with a quiet window
   *and* a max-wait ceiling.
3. **A collision-free slice name.** A monotonic sequence, unless the framework updates its CR on
   scale (in which case `Generation` suffices).
4. **A gate in the Pod template** — only if the framework's controller creates Pods outside Kueue's
   admission path, and only if its Pod template is applied to Pods created later.
5. **Nothing else.** Chain-scoped accounting, ungating, preemption exclusion and the scale-down
   patch are all generic.

The two questions to answer before starting: does the framework's controller restart the workload
if its spec is modified (determining whether the count may be written back), and is its Pod template
applied to Pods created after startup (determining whether template-level gating reaches them).

Part III is a worked example.

## 16.1 No Spark Operator dependency

**The integration requires no change to `kubeflow/spark-operator`.** It runs against the released,
open-source operator and its unmodified `v1beta2` CRD, so a reviewer does not need to coordinate an
API addition in a second project, wait for an operator release, or reason about version skew. Users
install the open-source operator, enable Dynamic Allocation, and set no Kueue-specific field on the
CR beyond the queue name and the elastic-job annotation.

A declared executor-count field (`spec.parallelism`, mirroring `batch/v1.Job`) was considered as a
stable, operator-agnostic place for an external system to record a desired count. The live-Pod
derivation supersedes the need for it: Kueue never needs to *write* a desired count, only to
*observe* DA's actual state, and Pods carry that with more fidelity and no propagation delay than
any CR field. Writing such a field would also have been actively harmful, since a new count field
would not be in the operator's exemption list (C-1).

How to confirm the dependency is zero — each independently sufficient: `go.mod` pins the released
module with no `replace` directive, so any code path reading such a field would fail to build; no
`jobframework` interface requires a desired-count field (`GenericJob` has no count setter, and the
only elastic-specific optional interface exists purely for naming); `RunWithPodSetsInfo` merges only
node selectors, tolerations, labels, annotations and scheduling gates; and the one place the
reconciler writes a count back onto pod sets is guarded by
`canBePartiallyAdmitted && ps.MinCount != nil`, the partial-admission path, which `ValidateWorkload`
explicitly forbids combining with elastic jobs.

What plays the role `spec.parallelism` plays for a plain Job:

| Concern | `batch/v1.Job` | `SparkApplication` |
|---|---|---|
| Current desired count | `spec.parallelism` | live executor Pod count |
| Count before any Pods exist | `spec.parallelism` | `declaredInitialExecutors()`, clamped to DA's bounds |
| Detecting a scale event | `spec.parallelism` bumps `metadata.generation` | label-keyed Pod watch, debounced; sequence number for naming, since DA never bumps generation |
| Preventing use of ungranted capacity | job stays suspended | scheduling gate on the executor pod template, released by the ungater |

# 17. Implementation inventory

| Component | File | Contribution |
|---|---|---|
| Live-count derivation | `pkg/controller/jobs/sparkapplication/sparkapplication_podset.go` | `liveExecutorCount`, `computeLiveExecutorCount`, `isVerifiedLiveExecutor`, `initialExecutorCount`, `declaredInitialExecutors`, `numInitialExecutors`, `sparkConfExecutorInstances`, `dynamicAllocationExecutorCount`, `clampToDynamicAllocationBounds`, `workloadSequenceNumber` |
| Pod watch and debounce | `..._executor_pod_handler.go` | `executorPodPredicate`, `isTrackedExecutorPod`, `executorPodHandler` |
| Slice naming, PodSets | `..._controller.go` | `PodSets`, `GetWorkloadNameExtraPart`, `RunWithPodSetsInfo`, `RestorePodSetsInfo` |
| Gate injection and validation | `..._webhook.go` | `Default`, `validateElasticJob`, malformed-count rejection |
| Chain-scoped accounting | `pkg/cache/scheduler/clusterqueue.go` | `sliceChainKey`, `sliceReplacementFor`, `sliceIsLater`, `sliceGroupTip`, `reconcileSliceGroup`, `supersededSliceKeys` |
| Snapshot exposure | `pkg/cache/scheduler/clusterqueue_snapshot.go`, `snapshot.go` | `supersededSlices`, `SliceSuperseded` |
| Preemption exclusion | `pkg/scheduler/preemption/preemption.go`, `.../classical/hierarchical_preemption.go` | Superseded slices skipped in both candidate collectors |
| Grant-aware extractor | `pkg/workload/podsetscounts.go` | `ExtractGrantedPodSetCounts` |
| Ungating cap | `pkg/controller/elasticjobs/elastic_job_ungater.go` | `podsToUngate` caps by the grant |
| Scale-down of grants | `pkg/workloadslicing/workloadslicing.go` | `scaleDownAdmission`, `updatePodSetCountsWithRetry`, `NormalizeActiveSlices` |
| Admission immutability exception | `pkg/webhooks/workload_webhook.go` | `validateAdmissionUpdate` |
| Shared annotation constant | `pkg/constants/constants.go` | `WorkloadSliceReplacementForAnnotation` |

**One layering note.** `WorkloadSliceReplacementForAnnotation` lives in `pkg/constants` rather than
`pkg/workloadslicing`, because `pkg/cache/scheduler` needs it and `pkg/workloadslicing` imports the
cache — a direct import would be a cycle. `workloadslicing` re-exports it so existing call sites are
unchanged.

---

# Part III — Addendum: the Apache Spark Kubernetes Operator

Applies to the Apache operator's `spark.apache.org/v1` `SparkApplication`, integrated as
`pkg/controller/jobs/apachesparkapplication`. The Kubeflow integration of Part II is a separate
package and is unaffected.

Registration: framework name `spark.apache.org/sparkapplication`, behind the alpha feature gate
`ApacheSparkApplicationIntegration`, **default `false`**. Both the gate *and* the framework name in
`.integrations.frameworks` must be on; with either missing the integration is inert.

# A1. Why a second Spark integration exists

Two unrelated operators both call their CRD `SparkApplication`. They share no API group, no schema
and no state machine:

| | Kubeflow | Apache |
|---|---|---|
| CRD | `sparkoperator.k8s.io/v1beta2` | `spark.apache.org/v1` |
| operator language | Go | Java |
| executor count surface | `spec.executor.instances` (structured) | `spark.executor.instances` in `sparkConf` |
| suspend field | `spec.suspend` (native) | added upstream 2026-09-14 — §A2 |
| lifecycle | phase string | explicit state machine with transition history |

`jobframework.GenericJob` is per-GVK, so nothing about the Kubeflow integration is reusable beyond
the general elastic-scaling machinery. Hence a new package rather than a variant.

# A2. The prerequisite, and how upstream overtook it

`GenericJob` requires a suspend-like spec field, which upstream Apache originally lacked. That
changed on 2026-09-14:

- **SPARK-59475** put `suspend` on `BaseSpec`, parent of both `ApplicationSpec` and `ClusterSpec`,
  as `protected boolean suspend = false` with `@Default("false")` — so SparkCluster is suspendable
  too, and the CRD carries `default: false`, meaning the API server always materialises the field
  and an explicit `suspend: false` is indistinguishable from an omitted one. Harmless for Kueue,
  which always writes an explicit value. Upstream implemented the lifecycle too (`SuspendUtils`,
  plus handling in `AppInitStep`, `ClusterInitStep`, `EventUtils`, `ApplicationStatus`). There is
  **no** `AppSuspendStep` and no `StoppedByScheduler`, so whether `SuspendUtils` covers preempting
  an already-running driver is **unverified**.
- **SPARK-59490 / SPARK-59503** added `kueue/KueueWorkloadFactory.java`: the *operator* builds the
  Kueue `v1beta2` Workload itself, driver + executor PodSets, `active = !suspend`. It **throws
  `UnsupportedOperationException` when `spark.dynamicAllocation.enabled=true`.**

So upstream covers the static case and refuses exactly the case this integration exists to handle.
**Dynamic Allocation is the differentiator**: the static gating surface overlaps upstream; the
elastic path (§A7) does not.

## A2.1 The collision, and the toggle that resolves it

Upstream keys its Workload creation off the same label this integration needs:
`Constants.LABEL_QUEUE_NAME` is `kueue.x-k8s.io/queue-name`, and `AppInitStep`'s
`holdForKueueAdmission` runs whenever `hasQueueName(app)` is true. So labelling a SparkApplication
for Kueue *is* what triggers the operator's half.

And jobframework will **claim** the operator's Workload rather than coexist with it. Upstream builds
it with an ownerReference to the SparkApplication and `controller = true`; `FindMatchingWorkloads`
lists by the owner index this package registers and keeps any Workload whose controller matches the
job's Kind, APIVersion and name — which upstream's does. `EquivalentToWorkload` then compares
podSets, and they will not match: different counts (live-derived versus `spark.executor.instances`)
and a template carrying the executor scheduling gate. A non-equivalent Workload goes to `toDelete`
and jobframework deletes it, while the operator's `requestAdmission` recreates it on its next
reconcile. The expected failure mode is therefore **two controllers deleting each other's Workload
in a loop**, and because `holdForKueueAdmission` holds the driver until *its* Workload is admitted,
an application in that state hangs rather than merely mis-accounting.

**This is resolved by a toggle.** When first analysed there was no way to switch the operator's half
off — the only Kueue option was `spark.kubernetes.operator.kueue.workloadInformer.enabled`, which
controls whether the operator *watches* Workloads, not whether it creates them. A
`spark.kubernetes.operator.kueue.enabled` option has since been added on the operator fork, rendered
by the Helm chart from `operatorRbac.kueue.enabled`, which defaults to **`false`** — so a default
chart install leaves Kueue owning the Workload, which is what Architecture B needs, with no extra
flags. The Java-side default is `true`, so a deployment that bypasses the chart and supplies its own
properties must set it explicitly. Contributing the toggle upstream is the cleanest first upstream
change: small, reasonable on its own terms, and independent of the Dynamic Allocation question.

## A2.2 Removing upstream's DA exception would not make upstream's path work

Worth stating, because the `UnsupportedOperationException` reads like the only obstacle. It is a
*correct refusal*, and removing it would turn a clean error into silent under-reservation:

- **The count is frozen at build time.** `buildExecutorPodSet` reads `spark.executor.instances` once
  (default 2). DA changes the live Pod count, not `sparkConf`.
- **After admission the operator stops reconciling the Workload.** `requestAdmission` early-returns
  on `isAdmitted(workload)` before comparing podSets; the only in-place update is the priority
  class.
- **Its one reaction to a changed podSet is destructive.** On a pod-sets-hash mismatch it deletes
  the Workload and returns `STALE`. Delete-and-recreate is the opposite of the slice-replacement
  protocol, which needs the predecessor to remain in the snapshot so the replacement is charged only
  the delta.
- **No gating and no slice awareness anywhere upstream.** DA-created Pods are never gated, and with
  no slice-name annotation the per-chain netting never engages.

For the operator to own DA Workloads it would need, in Java: live-Pod-derived counts with a debounced
Pod watch, slice annotations and the replacement protocol, gate injection coordinated with Kueue's
ungater, removal of the admitted early-return, and in-place scale-down instead of
delete-and-recreate — i.e. Part II reimplemented. And that still would not suffice, because in-place
scale-down depends on the decrease-only `validateAdmissionUpdate` relaxation (§11.3), which lives in
Kueue rather than the operator.

# A3. The two architectures

Not combinable, for the reasons in §A2.1.

**Architecture B — Kueue owns the Workload.** What §§A4–A8 describe, and **the chosen direction**.
The operator supplies `spec.suspend` and nothing else; Kueue derives counts from live Pods, drives
slice replacement and gates executor Pods. Requires the operator's Kueue path switched off, which
the chart default already does. Removing upstream's DA exception is irrelevant here.

**Architecture A — the operator owns the Workload.** Upstream's model extended to DA. Traced lane by
lane in [`diagrams/apache-da-architecture-a.png`](./diagrams/apache-da-architecture-a.png). Requires
everything at the end of §A2.2 and still cannot be delivered operator-side alone. This integration
would be disabled and §§A4–A8 would not apply.

## A3.1 What of the elastic logic is reusable under an operator-owned Workload

**Already owner-agnostic** — keys off Workload and Pod annotations, indifferent to who created them:
`ReplacedWorkloadSlice`/`FindReplacedSliceTarget`, `Assignment.append`'s delta charging, the
per-chain cache netting, `elasticJobUngater`, and the decrease-only `validateAdmissionUpdate`
exception.

**Integration-bound** — requires a registered `GenericJob`: the executor Pod watch, whose
`reconcile.Request`s only the integration's reconciler consumes; `PodSets()`,
`computeLiveExecutorCount` and `clampToDynamicAllocationBounds`, which are `GenericJob` methods; and
`EnsureWorkloadSlices`, which has exactly one production caller (`ensureOneWorkload`), reachable
only via `ReconcileGenericJob` and gated on an annotation on the *job*, not the Workload.

So the Workload-level half is already reusable; the job-level half — watch, derive, ensure slices —
must live in a Kueue reconciler.

**Prebuilt workloads do not bridge this.** `kueue.x-k8s.io/prebuilt-workload-name` looks like the
supported way for an external actor to create the Workload while an integration manages the job, and
`ensureOneWorkload`'s prebuilt branch even mentions workload slicing. It does not work: the prebuilt
branch is checked *before* the slice branch and returns, so setting the label **disables** slicing
rather than enabling it. Every in-tree producer of that label is a MultiKueue adapter — where the
manager cluster owns the real Workload and the worker's early return stops it clobbering the
manager's decisions — or a pod-owning reconciler. It was never an external-Workload-creation hook.
Making it one is a coherent upstream-Kueue feature request that would serve any operator-side
integration, but it is a code change, not configuration.

**A cheaper split, if upstream will take it.** The operator already computes the DA condition in
order to throw on it. Having `holdForKueueAdmission` *skip* when
`spark.dynamicAllocation.enabled=true` would route static applications through upstream's factory and
DA ones to this integration, with no application served by both. The catch: both sides gate on the
same `queue-name` label, so this integration would also have to decline static applications, and
`IntegrationCallbacks` has no per-object opt-out. Small changes on both sides rather than one, and it
reframes upstream's existing check as a delegation rather than asking them to support DA.

# A4. The Go API types are a hand-written partial projection

The operator is Java and publishes no Go module, so `api/v1/types.go` projects only the fields this
integration reads. That is safe **only** because jobframework *patches* the job rather than calling
`client.Update`, which would serialise the projection and silently drop every omitted field.

**Never introduce a `client.Update` on a `spark.apache.org` SparkApplication.**

# A5. What Kueue charges

Same rule as §9.5. `buildPodTemplateSpec` starts from the submitter's
`spec.{driverSpec,executorSpec}.podTemplateSpec` when present, then **overwrites the Spark
container's cpu and memory** with values derived from `sparkConf`, because the operator passes the
template to Spark as a `podTemplateFile` and Spark's feature steps replace those values before the
pod is created. Upstream's own `KueueWorkloadFactory.decorateContainerResources` does the same.

`totalMemoryBytes` reproduces Spark's formula identically to §9.5, including the truncation, the
bare-value-is-MiB rule, the JVM/non-JVM factor and `minMemoryOverhead`. **The limit equals the
request** — Spark intends heap+overhead to be the whole allocation. CPU reads
`spark.kubernetes.{role}.request.cores` then `spark.{role}.cores`, with deliberately no fallback to
`limit.cores`.

Measured: with `spark.executor.memory: 512m`, real executor pods show `req=896Mi lim=896Mi`.

A middle path was considered and not implemented: pass the template through *and* have the webhook
reject a declared request below `memory + max(0.1 × memory, 384Mi)`.

# A6. Count precedence: `sparkConf` first on this CRD

The inverse of the Kubeflow package, and load-bearing rather than an inconsistency:

```
instances  = spark.executor.instances  ->  instanceConfig.initExecutors  ->  Spark's default 2
bound      = spark.dynamicAllocation.{initial,min,max}Executors
             ->  instanceConfig.{init,min,max}Executors  ->  unset
```

**Why the structured field loses here:** on this CRD it never reaches Spark. The operator does not
create executors — the driver does, from `spark.executor.instances` — and `instanceConfig` is
consumed only by the operator's own readiness thresholds (`AppRunningStep`), never translated into a
`--conf` at submission. Preferring it would reserve quota for an application declaring
`initExecutors: 2` while fifteen pods from `spark.executor.instances: 15` actually run. Upstream's
factory reads the conf key alone, for the same reason.

`instanceConfig` is retained as the fallback rather than dropped: when the conf key is absent it is
the only declaration of intent available, and for `minExecutors` it supplies the startup floor of
§9.4.

The Kubeflow CRD is the opposite case, because there the operator maps the structured field onto a
`--conf` at submission. **The rule is therefore not "structured first" or "conf first" but prefer
whichever surface Spark actually acts on.**

`instanceConfig` fields are plain `int32`, so zero is treated as unset — harmless here because a
zero bound would be meaningless. `dynamicAllocationEnabled` reads `sparkConf` only; no OR, because
this CRD has no structured enablement field at all. `declaredInitialExecutors` takes a maximum, not
a precedence, exactly as in §9.3.

# A7. Elastic scaling

Mirrors Part II, and is the part with no upstream equivalent: counts **derived from live Pods**,
never written back; a **label-keyed executor Pod watch** with a 5s debounce and 30s max wait; the
**executor scheduling gate** baked into the template at CR-create time, required by
`validateElasticJob`; **`clampToDynamicAllocationBounds`** (§9.4), whose upper bound is DA's ceiling
and **not** grantable capacity, so it narrows but does not close the gated-Pod loop; and slice names
from a **per-job sequence number**, since DA scaling never bumps `Generation`.

The quota-evasion hole of §9.3 does not exist here: counts always read `spark.executor.instances`,
error on a malformed value, and default to Spark's 2.

# A8. Lifecycle mapping

The operator exposes a real state machine plus a `StateTransitionHistory`, which makes two things
possible that the Kubeflow phase string does not.

**`StoppedByScheduler` is deliberately not terminal.** The operator releases the driver on a
scheduler-requested stop but reopens the application in `Suspended`; treating it as terminal would
end the Workload for what is actually a preemption. Relatedly, preemption is not counted as a
failure and never consumes restart budget — being preempted is a scheduling decision, not an
application fault.

**`terminalOutcome` walks the transition history.** `ResourceReleased` and
`TerminatedWithoutReleaseResources` are reachable from both success and failure, so they carry no
outcome of their own; when the application sits in one, the outcome is the highest-numbered history
entry that is not itself a release state. An application whose history is unavailable is reported
unsuccessful rather than guessed at.

**`PodsReady`** keys off `RunningHealthy`/`RunningWithPartialCapacity`, since the operator only
advances to those once at least `minExecutors` are ready — so no Pod listing is needed.

# A9. Known gaps

- **The webhook does not reject a malformed `spark.executor.instances` at admission**, the way the
  Kubeflow one does. `staticExecutorCount()` still returns a proper error, so the failure surfaces
  during reconcile rather than at create time.
- **The gated-Pod feedback loop is narrowed, not closed** (§15.1).
- **No integration tests.** envtest binaries are unreachable in this environment, so coverage is
  unit-level only: pod-template construction, the memory and CPU arithmetic, static and DA counts,
  the elastic path, webhook validation, and setup/registration.
- **Never exercised end-to-end against an operator built from upstream `main`.** The collision
  analysis in §A2.1 was established by reading both sides; the toggle that resolves it is built but
  not yet observed working on a cluster.

# A10. Deployment notes

- **Helm never upgrades CRDs in `crds/`.** `helm upgrade` silently leaves the old CRD, so a new
  field is pruned by the API server and appears to do nothing — the symptom is
  `unknown field "spec.suspend"` in Kueue's log. Fix:
  `kubectl apply --server-side --force-conflicts -f` the chart's CRD.
- **`spark.kubernetes.operator.watchedNamespaces` needs an operator restart** despite
  `enableDynamicOverride(true)`, and defaults to `default` only — so an application in any other
  namespace is never reconciled and no driver is created. The chart's comment names a non-existent
  key.
- **`helm upgrade` regenerates the workload ClusterRole**, which previously dropped a hand-patched
  `deletecollection` verb and brought back a driver-shutdown 403 on services. The verb is now part
  of the chart's `workloadRbacRules`, but the regeneration behaviour is worth knowing for any other
  hand-patched rule.
- **The image tag never changes** (`1.1.0-SNAPSHOT`, `pullPolicy: IfNotPresent`), so a rebuilt image
  is not picked up by `helm upgrade` alone — the pod spec is identical and no new pod is created. A
  `kubectl rollout restart` is required.
- **The chart hardcodes `operatorRbac.serviceAccount.name`**, so two releases can never coexist in
  one namespace; they collide on that ServiceAccount regardless of release name.
