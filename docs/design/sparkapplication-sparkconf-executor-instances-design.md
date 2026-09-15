# Design: accounting for `spark.executor.instances` declared in `sparkConf`

Status: proposed
Date: 2026-09-15

Closes a quota-evasion path in the SparkApplication integration: an application that
declares its executor count through `spec.sparkConf` rather than the structured
`spec.executor.instances` field had its executors sized at **zero**, so Kueue reserved
quota for the driver alone while the driver went on to create the declared executors
entirely outside quota management.

Companion to
[`sparkapplication-executor-count-bounds-design.md`](./sparkapplication-executor-count-bounds-design.md),
which fixed the Dynamic Allocation bounds. This is the static (non-DA) counterpart.

## 1. Observed

A Kueue-managed, non-elastic `SparkApplication` with Dynamic Allocation off:

```yaml
metadata:
  labels:
    kueue.x-k8s.io/queue-name: batch-queue-uno
spec:
  sparkConf:
    "spark.executor.instances": "15"
  executor:
    cores: 1
    coreRequest: "1000m"
    memory: 512m          # no `instances:` field
```

Fifteen executor Pods ran in the namespace against a ClusterQueue that had charged for one
driver. The Workload's executor PodSet was admitted at `count: 0`.

## 2. Mechanism

With Dynamic Allocation off, `computeLiveExecutorCount` short-circuits to
`numInitialExecutors()`, which read exactly one source:

```go
func (j *SparkApplication) numInitialExecutors() int32 {
	return ptr.Deref(j.Spec.Executor.Instances, 0)
}
```

`spec.executor.instances` is unset here, so this returns 0. `spark.executor.instances` was
never read anywhere in the package — a grep for that key returned nothing.

Two properties turn an under-count into total evasion:

- **The executor PodSet is sized at 0**, so nothing is reserved for executors and the
  ClusterQueue's usage reflects the driver only.
- **A non-elastic job gets no scheduling gate.** The webhook injects
  `kueue.ElasticJobSchedulingGate` into `spec.executor.template` only for elastic jobs, so
  there is no mechanism that could hold those Pods back even in principle. The driver creates
  them directly against the API server and they run immediately.

Spark Operator is not at fault. Spark's own convention is that `spark.executor.instances` is
authoritative, and the operator maps the structured field onto that same key when submitting;
the 15 executors were created exactly as configured. The gap is that Kueue reads only one of
the two surfaces the operator accepts — the same asymmetry `dynamicAllocationEnabled()` and
`dynamicAllocationExecutorCount()` already avoid for the Dynamic Allocation settings.

**This is upstream behavior, not introduced by the elastic-scaling work.**
`numInitialExecutors` is byte-identical to its pre-PR-#16 version
(`git show cc69cceaa^`), so `kubernetes-sigs/kueue` has the same hole. Kueue's own
`pkg/controller/jobs/apachesparkapplication` integration already gets this right:
`staticExecutorCount()` reads the conf key first, then `instanceConfig.InitExecutors`, then
Spark's default of 2.

## 3. Fix

`numInitialExecutors()` now consults both surfaces and takes the larger:

```go
count := ptr.Deref(j.Spec.Executor.Instances, 0)
n, ok, err := j.sparkConfExecutorInstances()
if err != nil {
	return 0, err
}
if ok {
	count = max(count, n)
}
if !ok && j.Spec.Executor.Instances == nil {
	return defaultExecutorInstances, nil
}
return count, nil
```

Three deliberate choices:

- **Max, not precedence.** Which surface Spark ends up honouring when both are set depends on
  the order in which the operator assembles `--conf` arguments. Taking the larger makes a
  disagreement between them over-reserve rather than leave executors uncounted — the safe
  direction for a quota system.
- **Default to Spark's 2, not 0.** An application declaring no count anywhere still gets two
  executors from Spark. Reserving zero for them is the same hole in miniature.
- **A malformed value is an error.** `dynamicAllocationExecutorCount` deliberately ignores
  parse failures, treating the field as absent; doing that here would resurrect a zero-sized
  PodSet from a typo. `sparkConfExecutorInstances` returns an error, and
  `validateCreate` rejects it at admission with a message naming the field, so the failure is
  immediate and legible rather than a silently unaccounted run.

`initialExecutorCount()` — the Dynamic Allocation pre-startup estimate — had the same blind
spot, since `ptr.Deref(instances, 0)` contributed nothing to its max when the count lived only
in `sparkConf`. It now folds `sparkConfExecutorInstances()` in alongside
`initialExecutors`/`minExecutors`.

## 4. What is unchanged

- **The structured field alone** behaves exactly as before.
- **Dynamic Allocation bounds.** The `minExecutors`/`maxExecutors` clamp from the companion
  design still applies to the DA path, unchanged.
- **The live-Pod derivation.** Once executor Pods exist under Dynamic Allocation, the observed
  count still wins; `numInitialExecutors` is only consulted with DA off, and
  `initialExecutorCount` only before any Pod exists.

## 5. Considered and rejected

**Rejecting a resolved count of 0 in the webhook.** Attractive at first — a Kueue-managed
static Spark job with zero executors is always either a mistake or a quota hole. But after
defaulting to Spark's 2, a resolved 0 is only reachable by writing
`"spark.executor.instances": "0"` explicitly, and an application that genuinely runs zero
executors consumes nothing and evades nothing. The check would be near-dead code, so the
validation added here targets the malformed-value case instead, which is reachable and does
cause silent mis-accounting.

## 6. Testing

`TestLiveExecutorCount` gains: sparkConf-only count, both surfaces set with each in turn being
the larger, no count declared anywhere, a malformed conf value, and the Dynamic Allocation
path folding the conf value into its max.

Negative-controlled both halves:

- Restoring `numInitialExecutors` to the structured field alone fails four cases, including
  `dynamic_allocation_disabled_reads_spark.executor.instances_from_sparkConf` returning 0 —
  the cluster-observed signature.
- Removing the conf value from `initialExecutorCount`'s max fails exactly
  `dynamic_allocation_folds_sparkConf_instances_into_the_max`.

`gofmt -l` clean, `go vet`, `go test`, `go test -race -count=1` on the package, and
`go build ./...` across the tree.
