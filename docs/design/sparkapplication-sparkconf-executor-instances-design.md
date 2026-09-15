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

`numInitialExecutors()` now consults both surfaces, in precedence order:

```go
if j.Spec.Executor.Instances != nil {
	return *j.Spec.Executor.Instances, nil
}
n, ok, err := j.sparkConfExecutorInstances()
if err != nil {
	return 0, err
}
if ok {
	return n, nil
}
return defaultExecutorInstances, nil
```

Three deliberate choices:

- **Precedence, not a maximum.** The structured `spec.executor.instances` field is the
  application's declared intent; the raw `spark.executor.instances` key in `sparkConf` is the
  fallback for applications that configure Spark directly. This mirrors how
  `dynamicAllocationEnabled()` and `dynamicAllocationExecutorCount()` already resolve their
  own properties, so the package is internally consistent about which surface wins.
- **Default to Spark's 2, not 0.** An application declaring no count anywhere still gets two
  executors from Spark. Reserving zero for them is the same hole in miniature.
- **A malformed value is an error.** `dynamicAllocationExecutorCount` deliberately ignores
  parse failures, treating the field as absent; doing that here would resurrect a zero-sized
  PodSet from a typo. `sparkConfExecutorInstances` returns an error, and `validateCreate`
  rejects it at admission with a message naming the field, so the failure is immediate and
  legible rather than a silently unaccounted run.

On the Dynamic Allocation path, `declaredInitialExecutors()` applies the same principle as a
precedence ladder, structured field before `sparkConf` for each property:

1. `spec.executor.instances` — with Dynamic Allocation on this is treated as the **initial**
   executor count, since that is the role it plays for Spark
2. `spark.executor.instances` from `sparkConf`, the fallback for the above
3. `initialExecutors`, structured then `sparkConf`
4. `minExecutors`, structured then `sparkConf`
5. zero, which the caller's clamp raises to `minExecutors` when one is configured

### Divergence from Spark, and what it costs

Spark's own `Utils.getDynamicAllocationInitialExecutors` takes the **largest** of
`minExecutors`, `initialExecutors` and `spark.executor.instances`. Precedence therefore
under-reserves for one configuration: `spec.executor.instances: 2` alongside
`initialExecutors: 7` reserves 2 while Spark starts 7.

`minExecutors` still acts as a floor through `clampToDynamicAllocationBounds`, so the common
shape — a small `instances` with a larger `minExecutors` — is covered. A deliberate
`instances < initialExecutors` configuration is not. This was accepted knowingly: predictable
precedence between a structured field and its `sparkConf` fallback was preferred over matching
Spark's arithmetic exactly, and the two values disagreeing is a configuration to correct rather
than a shape to support.

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

`TestLiveExecutorCount` covers: sparkConf-only count, the structured field taking precedence
in both directions (smaller and larger than the `sparkConf` value), no count declared anywhere,
a malformed conf value, the Dynamic Allocation path falling back to the `sparkConf` count, and
`instances` outranking an explicit `initialExecutors`.

Negative-controlled: restoring max-based resolution on both paths fails exactly
`structured_field_takes_precedence_over_sparkConf_when_smaller` and
`instances_is_treated_as_initialExecutors_and_outranks_initialExecutors`. Restoring the
original structured-field-only read fails the sparkConf-fallback cases, including the
cluster-observed signature of a sparkConf-declared count resolving to 0.

`gofmt -l` clean, `go vet`, `go test`, `go test -race -count=1` on the package, and
`go build ./...` across the tree.
