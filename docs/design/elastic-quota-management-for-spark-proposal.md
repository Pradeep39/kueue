---
title: "Elastic Quota Management for Spark Workloads in Kueue"
subtitle: "Proposal — supporting highly elastic Spark workloads without gang scheduling"
author: "Pradeep Reddy"
date: "2026-08-09"
---

# 1. Summary

We propose that Kueue support **elastic quota management for Spark workloads driven by Spark
Dynamic Allocation (DA)**, so that a Spark application can be admitted, grow and shrink within a
quota boundary while it runs, without being requeued and without reserving its peak footprint up
front.

Kueue already has the necessary primitive: `ElasticJobsViaWorkloadSlices` (KEP-77), supported today
for `batch/v1.Job` and `RayCluster`. It does not work for Spark, because Spark's autoscaler operates
outside the `SparkApplication` custom resource. This proposal argues for closing that gap, and
offers a working reference implementation as evidence that it is tractable.

The alternative available today — sizing Spark applications statically and admitting them
all-or-nothing — forces a choice between paying for peak capacity that is idle most of the time,
and capping throughput below what the infrastructure can deliver. For the workloads described in
§2 that trade-off is expensive enough to be the deciding factor.

# 2. Motivation

## 2.1 The workloads that need this

Two classes of Spark workload dominate our platform, and both are elastic by nature rather than by
configuration.

**Iceberg table compaction and similar maintenance jobs.** These jobs fan out to rewrite data
files, then collapse. Parallelism is a function of how much data needs compacting, which varies by
table, by partition and by how much has changed since the last run. A single job's demand can move
by an order of magnitude during its own execution, and the same job's demand differs between runs.
Choosing one executor count in advance means choosing it wrong most of the time.

**Notebook-bound interactive sessions.** A data scientist attaches a Spark session to a notebook
and works through it. Between cells the session is idle — often for minutes, sometimes for hours
across a working day. When a cell runs, demand spikes. Peak concurrency is unknowable in advance
because it depends on what the user does next. Crucially, the session must **survive resizing**:
requeueing it to change its size would destroy the user's session state and their work.

Both classes share three properties that matter here:

1. Demand varies widely **during** the workload's life, not just between workloads.
2. Peak demand is a poor predictor of average demand.
3. The workload must not be interrupted in order to change size.

## 2.2 Why gang scheduling is the wrong tool

Gang scheduling — and equivalently, all-or-nothing admission of a statically sized workload — is
appropriate when a workload cannot make progress without its full complement of workers, and when
that complement is known and stable. Neither holds for the workloads above.

For highly elastic workloads it is also **cost-prohibitive and a poor use of the infrastructure**:
the reservation is sized for a peak that is rarely reached, and the difference between that peak and
actual demand is capacity that is paid for and not used.

Applying it anyway forces one of two bad outcomes:

**Size for peak, and pay for idle.** The reservation is held for the workload's entire lifetime,
whether or not the capacity is being used. For an interactive session idle between cells, this is
capacity paid for and not used, for hours. For a compaction job, it is peak capacity held through
the long tail after the fan-out collapses.

**Size for the floor, and cap throughput.** The workload runs, but never uses the headroom that is
genuinely available, so jobs take longer and the infrastructure is under-used even when idle.

There is also a hard failure mode. Our test configuration uses 512Mi executors with
`minExecutors: 3` and `maxExecutors: 30`, against a `ClusterQueue` of 6Gi — twelve 512Mi slots.
Gang-admitting at the maximum requires 1 driver + 30 executors = **15.5Gi**, which exceeds the
entire quota: the workload would **never be admitted at all**, despite running comfortably within
quota in practice. Gang-admitting at the floor gives 2Gi per application, so three applications
consume the whole quota with zero headroom and no ability to use idle capacity.

Under elastic admission the same three applications ran between 2Gi and 6Gi of measured usage,
tracking real demand, and never exceeded the quota.

The point generalises: **for a workload whose demand varies during its life, a static reservation
is either wasteful or restrictive, and the gap between peak and average is the cost of the
choice.** Elasticity removes the choice.

## 2.3 Why quota-aware elasticity, not just autoscaling

Spark DA already provides elasticity at the framework level. What it does not provide is any
awareness of a quota boundary. Left alone, DA scales against the cluster, not against the tenant's
allocation, and:

- capacity is consumed with no corresponding quota grant, so a queue's accounting understates real
  usage and multi-tenant fairness breaks down;
- capacity released when a workload scales down is not returned to the queue, so it cannot be
  reused by other tenants;
- the queue's reported usage cannot be trusted for capacity decisions.

The value of this proposal is the combination: **the framework decides how much it wants; Kueue
decides how much it may have, and keeps its accounting honest as that changes.**

## 2.4 Goals

- **G1.** Admit a Spark application, then let it grow and shrink within quota while it runs,
  without requeueing it.
- **G2.** Keep the queue's reported usage tracking the workload's actual footprint as its
  autoscaler changes it.
- **G3.** Prevent a workload from consuming capacity for which quota has not been granted.
- **G4.** Return capacity to the queue promptly when a workload scales down, so other tenants can
  use it.
- **G5.** Never require the workload's own resource to be modified in order to account for it.
- **G6.** Leave existing elastic integrations (`batch/v1.Job`, `RayCluster`) and all non-elastic
  admission behaviour unchanged.

## 2.5 Non-goals

- **NG1.** Node-level gang or co-scheduling. This proposal is about quota admission; placement
  remains the kube-scheduler's concern. For the workloads in §2.1, gang placement is explicitly not
  wanted.
- **NG2.** Changing or overriding the framework's autoscaling policy. Kueue should bound what a
  workload may consume, not decide how much it wants.
- **NG3.** Guaranteeing a workload can always reach its configured maximum. Quota is a limit; a
  workload may legitimately be held at less than its maximum.
- **NG4.** Support for Spark operators other than the Kubeflow Spark Operator in the first
  iteration.
- **NG5.** Multi-cluster (MultiKueue) support in the first iteration.

# 3. Proposal

Extend `ElasticJobsViaWorkloadSlices` to cover Spark applications autoscaled by Dynamic Allocation,
by treating the workload's **observed footprint** as the source of truth for its size, and by
withholding capacity until quota has been granted for it.

Three capabilities follow, stated here as outcomes; the mechanisms are in the companion design
document.

**Observed sizing.** The workload's current size is determined by observing what it is actually
running, not by reading a declared value from its resource. This is necessary because Spark's
autoscaler changes the workload's footprint without updating its resource, and because modifying
that resource is not safe (§4, R-5).

**Granted-capacity enforcement.** Workers created by the framework do not begin consuming capacity
until Kueue has granted quota for them. The framework may ask for more at any time; it receives
more only when quota allows.

**Honest, symmetric accounting.** The queue's usage reflects the workload's real footprint in both
directions: it rises as the workload grows and falls as it shrinks, so released capacity is
promptly available to other tenants.

## 3.1 User stories

**US-1 — Compaction under a shared quota.** As a platform operator, I run Iceberg compaction jobs
in a namespace with a fixed quota. I need each job to use idle capacity when it is available and
give it back when its fan-out collapses, so that several jobs share the allocation rather than one
job reserving it.

**US-2 — Interactive session that survives resizing.** As a data scientist, my notebook's Spark
session must grow when I run a heavy cell and shrink when I am reading results — without being
restarted, because a restart loses my session state.

**US-3 — Quota as a real limit.** As a capacity planner, I use the queue's reported usage to decide
whether more work fits. It must never overstate or understate what is actually consumed.

**US-4 — Fair sharing between tenants.** As an operator of a multi-tenant queue, capacity released
by one workload scaling down must become available to another tenant promptly, not remain reserved.

**US-5 — No regression.** As an existing Kueue user with elastic `batch/v1.Job` workloads, I need
this change not to alter my jobs' behaviour.

# 4. Requirements

Stated as capabilities and outcomes. Each is independently testable; how each is satisfied is the
subject of the companion design document.

| ID | Requirement | Status |
|---|---|---|
| **R-1** | The workload's size, as used for quota accounting, MUST track what the workload is actually running, as its autoscaler changes it. | Met |
| **R-2** | A workload MUST be able to change size while admitted, without being requeued or restarted. | Met |
| **R-3** | Workers MUST NOT consume cluster capacity before quota has been granted for them. | Met |
| **R-4** | Capacity MUST be returned to the queue when a workload scales down, without waiting for the workload to finish. | Met |
| **R-5** | Kueue MUST NOT modify the workload's own resource in order to account for it, because doing so restarts the workload. | Met |
| **R-6** | Accounting MUST remain correct while the workload is mid-resize, when several representations of it exist at once. | Met |
| **R-7** | Accounting MUST remain correct irrespective of the order or delivery of cluster events, including events that never arrive. | Met |
| **R-8** | A workload MUST be able to start below its configured maximum, and MUST NOT be rejected merely because its maximum exceeds available quota. | Met |
| **R-9** | Behaviour for non-elastic workloads MUST be unchanged, including the immutability guarantees they rely on. | Met |
| **R-10** | Existing elastic integrations MUST be unaffected. | Met |
| **R-11** | The number of workers awaiting quota SHOULD remain proportionate to the quota available. | **Not met** — §7.2 |
| **R-12** | Time-to-admission under a saturated queue SHOULD remain bounded. | **Not met** — §7.2 |
| **R-13** | The per-worker resource cost used for accounting MUST match what the framework actually requests of Kubernetes, for every resource the queue governs. | **Not met** — §7.5 |

## 4.1 Success criteria

Two invariants define correctness. Both are checked continuously by an automated harness, and both
are necessary — neither implies the other.

**SC-1 — Quota is a ceiling.** The queue's reported usage never exceeds its nominal quota. A
breach means capacity was granted twice for the same resources.

**SC-2 — Every running worker has a grant.** The capacity actually consumed by running workers
never exceeds the queue's reported usage. A breach means the cluster is oversubscribed **while the
queue reports that it is healthy** — an invisible failure, and the more dangerous of the two.

SC-1 fails loudly; someone notices usage above quota. SC-2 fails silently. A proposal for elastic
quota management should be judged against both.

Note that SC-2 is only as strong as the per-worker cost it is measured against. If that cost is
taken from the same value the queue charges, the check is circular and passes regardless. §7.5
records a case where exactly that happened, and what it hid.

# 5. Risks and mitigations

| Risk | Mitigation |
|---|---|
| Accounting for a workload requires modifying it, restarting it. | R-5: size is observed, never written back. |
| Quota released on scale-down is not actually reusable. | R-4 is stated as an outcome and validated, not assumed. |
| Elasticity weakens guarantees non-elastic users depend on. | R-9; the reference implementation confines every relaxation to elastic workloads. |
| A workload is admitted, then starved of the growth it needs. | R-8; and §7.2 documents the case where this is currently imperfect. |
| Silent oversubscription. | SC-2 is a first-class success criterion, not an implementation detail. |
| The predicted worker Pod diverges from the one the framework actually creates. | R-13; **currently unmet** — §7.5 records two live instances and why validation missed them. |
| Elastic accounting is subtle and easy to get wrong. | Acknowledged: three distinct accounting defects were found and fixed during implementation, two of them only under sustained load. §6 explains why that argues *for* upstreaming rather than against. |

# 6. Reference implementation

A complete implementation exists and has been validated on a live cluster. It is described in the
companion document *Elastic Scaling for SparkApplication via Workload Slices — Reference
Implementation: Detailed Design*. In outline:

- Generic hardening of workload slicing against admission races and stale reads.
- A Spark integration that derives the workload's size from observation rather than declaration.
- Capacity gating, so framework-created workers wait for a grant.
- Three quota-accounting corrections in the generic slicing machinery.

## 6.1 What implementation experience contributes to this proposal

Three of the four bodies of work fix defects in **generic** `ElasticJobsViaWorkloadSlices` code, not
in the Spark integration. Two of them were only reachable under sustained autoscaling churn: one
caused reported usage to exceed quota (violating SC-1), the other allowed workers to run with no
grant behind them (violating SC-2, invisibly).

This is the strongest argument for treating elastic quota management as a first-class, upstream
concern rather than a per-integration add-on. The Spark workloads in §2.1 exercise slice
replacement far harder than a `batch/v1.Job` resize does, and that is what surfaced the defects. Any
future elastic integration would have hit the same ones.

## 6.2 Validation summary

An automated harness sampled both success criteria every two seconds against three concurrent
Spark applications with Dynamic Allocation, in a 6Gi queue.

| Stage | Samples | SC-1 (ceiling) | SC-2 (every worker granted) |
|---|---|---|---|
| Before this work | — | **Violated**: 9Gi reported against a 6Gi quota | Not measured |
| Mid-implementation | 316 | Held on every sample | **Violated on 39 samples** — which is how the second defect was found |
| Final | 159 | Held on every sample; peak equal to quota | Held on every sample |

Reported usage tracked the workload's footprint **as counted in workers** exactly, and quota
released on scale-down was promptly reusable by the other applications.

The qualification matters: the harness derived each worker's cost from the same configured value
Kueue charges, so the SC-2 column above confirms that every running worker had a grant *at the cost
Kueue believed in*. It does not confirm that cost matched what Kubernetes actually reserved. §7.5 is
that gap.

# 7. Open questions and known gaps

## 7.1 Sizing guidance versus enforcement

An application configured to want far more than its quota can ever supply behaves correctly but
inefficiently: it repeatedly asks and is repeatedly refused. It is not yet settled whether the
right answer is documented guidance (size the maximum against the quota; start at the floor), a
bound enforced by Kueue, or both. This affects R-11 and R-12.

## 7.2 Workers awaiting quota accumulate (R-11, R-12)

When the queue is saturated, workers created by the framework wait for a grant that cannot come
until other work finishes. In our tests this reached 68 waiting workers against a twelve-slot
quota, with a 24-second wait to admission and sustained controller churn.

This is a bounded-growth problem, not an accounting one: quota was never violated. But it is real,
and it is made **more visible** by correctly enforcing SC-2, since workers that previously ran
without a grant now correctly wait instead.

The naive fix — ignoring workers that are waiting — is wrong: their existence is the only signal
that the framework wants more capacity, so ignoring them means the workload never grows and never
gets admitted. A correct fix must bound how fast the request may grow. Two candidate designs are
described in the companion document. Both are behaviour changes to elastic scaling and warrant
their own proposal.

## 7.3 Confidence in the original defect

The overcommit that prompted this work no longer reproduces, but the precise mechanism was never
conclusively isolated. The evidence is consistent with the fix addressing it; it is not proof. This
is documented rather than claimed as closed.

## 7.4 Scope deferred

Multi-cluster support, Spark operators other than Kubeflow's, and integration testing against a
live scheduler in CI are all outstanding.

## 7.5 Per-worker cost can diverge from what the framework requests (R-13)

Distinct from every gap above, and the one a reviewer should weigh most carefully, because it is a
property of the *approach* rather than of this implementation.

Kueue admits a workload by reconstructing a predicted worker Pod from the workload's resource and
charging quota for it. The Pod that actually runs is built independently — by the framework, from
the framework's own configuration. Nothing reconciles the two, and Kueue never reads the running
Pod back. When the two derivations disagree, **the cluster follows reality and the ledger follows
the prediction**, silently.

This is not hypothetical. Two such divergences were found in the existing Spark integration after
the accounting work was complete:

- **A resource omitted entirely.** The predicted Pod carried no CPU request, because the derivation
  read only an optional field and did not fall back to the one users actually set. The queue reported
  `cpu: 0` while eleven workers each held a core against a nominal quota of 15. The CPU quota was
  decorative, and admission was governed by memory alone.
- **A resource under-stated proportionally.** The predicted Pod omitted Spark's memory overhead term,
  charging 512Mi where the kubelet reserved 896Mi — a 75% under-charge. A queue reporting itself
  exactly at its 6Gi limit had reserved roughly 10.5Gi.

Both are violations of SC-2, and both were invisible to the harness for the reason given in §6.2.
The CPU case surfaced only because a zero is too stark to mistake for agreement; the memory case
surfaced only once actual Pod requests were compared against the ledger.

**Why this belongs in a proposal rather than a bug tracker.** Any integration that predicts a worker
Pod from a custom resource inherits this failure mode, and elasticity makes it worse: a static
workload's divergence is a fixed error found once, whereas an autoscaling workload multiplies it by
a worker count that changes continuously and without bound. A design for elastic quota management
should therefore say how the predicted cost is kept faithful — by validating it against observed
Pods, by deriving it from the framework's own logic rather than re-implementing it, or by reporting
a discrepancy rather than absorbing it. This proposal does not yet answer that, and R-13 is
consequently marked not met.

In the reference implementation the CPU case is worked around by configuration, treating the
relevant field as mandatory; the memory case has no workaround, because the overhead field is
ignored even when set explicitly.

# 8. Alternatives considered

**Gang scheduling / all-or-nothing admission at a static size.** Rejected for these workloads; see
§2.2. Appropriate where a workload cannot progress without its full complement and that complement
is known and stable — neither of which holds for compaction fan-out or interactive sessions.

**Size at the floor and accept the throughput cap.** Workable, and it is what these workloads do
today. It leaves idle capacity unused and makes compaction jobs take substantially longer than the
infrastructure allows.

**Size at the peak.** Reserves capacity that is idle most of the time. For interactive sessions the
peak is not knowable in advance, so this is not merely wasteful but impossible to do correctly.

**Manual resizing by an operator.** Requires a human in a loop that operates on the timescale of
notebook cells. Not viable.

**Let the framework autoscale without quota awareness.** This is the status quo outside Kueue: the
framework scales against the cluster rather than the tenant's allocation. Capacity is consumed
without a grant, released capacity is not returned to the queue, and reported usage cannot be
trusted. See §2.3.

**Per-integration elastic accounting.** Each framework integration solves elastic accounting for
itself. Rejected on the evidence in §6.1: the hard problems are in the generic slicing machinery,
and solving them once benefits every integration.
