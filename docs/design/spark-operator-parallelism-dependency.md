# No Spark Operator API dependency: `spec.parallelism` considered and dropped

**Decision (2026-09-06): the SparkApplication elastic-scaling integration requires no change
to `kubeflow/spark-operator`.** It runs against the released, open-source operator and its
unmodified `v1beta2` CRD. Nothing needs to be advocated upstream there, and no fork of the
operator needs to be deployed alongside Kueue.

This document previously recorded a fork
([`Pradeep39/spark-operator#1`](https://github.com/Pradeep39/spark-operator/pull/1), commit
`2f914dd`) that added an optional `Spec.Parallelism *int32` to `SparkApplicationSpec`. That
addition is being reverted. The rationale is kept here because a reviewer will reasonably
ask whether a declared executor-count field is needed, and the answer is load-bearing for
this design.

## 1. Why a declared count was considered

`batch/v1.Job.Spec.Parallelism` is the canonical, operator-agnostic signal of desired pod
count, and it is what Kueue's `ElasticJobsViaWorkloadSlices` support was originally built
around. `SparkApplication` has no equivalent single field. The closest analogues —
`spec.executor.instances`, `spec.dynamicAllocation.*` — are Spark-specific, and an external
system reading them has to understand Dynamic Allocation's on/off state and which of several
configuration surfaces is authoritative (the structured field, or the equivalent
`spark.dynamicAllocation.*` key in `spec.sparkConf`) just to recover a current desired count.
Adding one unambiguous field looked like the cheaper path.

## 2. Why it is not needed

The design that shipped never records a desired count anywhere on the CR. It **reads** the
live executor Pod count and derives `PodSets()` from that — see
[`sparkapplication-elastic-scaling-design.md`](./sparkapplication-elastic-scaling-design.md)
§2.3. This removes the original motivation entirely: Kueue never needs to *write* a desired
count, only to *observe* Dynamic Allocation's actual current state, and Pods carry that with
more fidelity and no propagation delay than any field on the CR can.

Writing such a field would also have been actively harmful. The Spark Operator's
`event_filter.go` does an unconditional `DeepEqual` on the entire `.Spec` on every `Update`
and treats any difference — including a change to an otherwise-inert field — as a request to
kill and resubmit the running application (§2.2 of the design doc). A field that exists only
to be written by an external controller would have walked straight into that.

## 3. The dependency is zero — how to confirm it

Each of these is independently sufficient:

- **`go.mod` pins `github.com/kubeflow/spark-operator/v2 v2.5.1` with no `replace`
  directive.** The integration compiles against the released upstream API types, which have
  no `parallelism` field. Any code path that read it would fail to build.
- No reference to `Parallelism` exists anywhere under
  `pkg/controller/jobs/sparkapplication/`, `pkg/workloadslicing/`, or
  `pkg/cache/scheduler/`.
- **No `jobframework` interface requires a desired-count field.** The only elastic-specific
  optional interface is `ElasticWorkloadNameProvider`
  (`pkg/controller/jobframework/interface.go`), whose single method
  `GetWorkloadNameExtraPart() string` exists purely for workload naming. `GenericJob` has no
  count setter.
- `SparkApplication.RunWithPodSetsInfo` merges only node selectors, tolerations, labels,
  annotations, and scheduling gates. It never writes a count into `.Spec`.
- The one place the job reconciler writes a count back onto pod sets
  (`expectedRunningPodSets`) is guarded by `canBePartiallyAdmitted && ps.MinCount != nil` —
  the partial-admission path, which `ValidateWorkload` explicitly forbids combining with
  elastic jobs ("partial admission and elastic job cannot be used together").

## 4. What plays the role `spec.parallelism` plays for a plain Job

| Concern | For `batch/v1.Job` | For `SparkApplication` |
|---|---|---|
| Current desired count | `spec.parallelism` | live executor Pod count, via `computeLiveExecutorCount` |
| Count before any Pods exist | `spec.parallelism` | `spec.executor.instances`, else DA `initialExecutors`, else DA `minExecutors` (`initialExecutorCount`) — all released upstream fields |
| Detecting a scale event | `spec.parallelism` change bumps `metadata.generation` | label-keyed executor Pod watch, debounced; sequence number via `GetWorkloadNameExtraPart` since DA scaling never bumps generation |
| Preventing use of ungranted capacity | job stays suspended | `kueue.ElasticJobSchedulingGate` on the executor pod template, released by the ungater once the slice is granted |

`validateElasticJob` requires exactly one thing of an elastic `SparkApplication`: the
scheduling gate on the executor pod template. There is no required count field.

## 5. Consequence: no declared executor bound on the CR

Dropping the field means the CR carries no Kueue-specific declared upper bound on executors.
The one place a bound is wanted is clamping the *requested* executor count so gated Pods
cannot inflate it past what the ClusterQueue can ever grant. `spec.dynamicAllocation.maxExecutors`
is the right source for that: it exists in the released operator, is already optional, and
already means "upper bound on the number of executors". Note that
`dynamicAllocationExecutorCount` currently handles only `initialExecutors` and
`minExecutors`; extending it to `maxExecutors` (with the matching
`spark.dynamicAllocation.maxExecutors` `sparkConf` fallback) is the small piece of work that
a clamp would need. No API change is required for it.

## 6. Contribution implications

This is the strongest argument that the integration is upstreamable as-is: a reviewer of the
Kueue changes does not need to coordinate an API addition in a second project, wait for a
Spark Operator release, or reason about version skew between the two. Users install the
open-source `kubeflow/spark-operator`, enable Dynamic Allocation, and set no
Kueue-specific field on the CR at all beyond the queue name and the elastic-job annotation.
