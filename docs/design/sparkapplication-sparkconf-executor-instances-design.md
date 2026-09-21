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

On the Dynamic Allocation path, `declaredInitialExecutors()` resolves the count exactly as
Spark's `Utils.getDynamicAllocationInitialExecutors` does — **the largest of** `minExecutors`,
`initialExecutors` and the resolved executor-instances count:

```go
count, err := j.numInitialExecutors()   // instances: field -> sparkConf -> Spark's 2
if n, ok := j.dynamicAllocationExecutorCount("initialExecutors"); ok {
	count = max(count, n)
}
if n, ok := j.dynamicAllocationExecutorCount("minExecutors"); ok {
	count = max(count, n)
}
```

Only the three-way combination is a maximum. Each individual property still resolves its own
structured field before its `sparkConf` equivalent, so the two rules compose without
contradiction: precedence decides *where a property's value comes from*, the maximum decides
*which property governs the initial count*.

Spark starts the largest of the three regardless of which one the author considered
authoritative, so a precedence ladder across them would under-reserve — `spec.executor.instances: 2`
alongside `initialExecutors: 7` would reserve two executors while Spark started seven.

## 3a. Memory: a pod-template request is authoritative

> **Superseded (2026-09-16)** by
> [`spark-podset-align-with-spark-design.md`](./spark-podset-align-with-spark-design.md) §1.
> The premise below is wrong: both operators pass the template to Spark as
> `spark.kubernetes.{role}.podTemplateFile`, and `Basic{Driver,Executor}FeatureStep` then
> overwrites the Spark container's memory with base+overhead. A template request is never what
> the pod asks for, so charging it verbatim under-charges. Both integrations now always apply
> Spark's arithmetic. The rest of this section is kept as a record of the original reasoning.

`spec.{driver,executor}.memory` is the **JVM heap size**, not the pod's request. Spark derives
the request by adding overhead — explicit `memoryOverhead`, or a factor-derived value with a
384MiB floor — so reading the field verbatim under-charges, which is the defect recorded in
PR #23.

A memory request or limit written directly on `spec.{driver,executor}.template` is a different
kind of statement: it is the total the submitter intends the pod to ask for, overhead already
included. `addMemoryRequests`/`addMemoryLimit` therefore use a template value verbatim and fall
back to `spec.{driver,executor}.memory` only when the template declares none. Applying overhead
arithmetic on top of a template value would either double-count it or contradict what the pod
will actually request.

Note what this does **not** do: the fallback path still reads `spec.{driver,executor}.memory`
without adding overhead, so an application that declares memory only through that field is
still under-charged. Closing that requires porting Spark's overhead formula into this
integration, which changes charging for every existing application and is deliberately out of
scope here. The Apache integration already implements the formula, and this change gates it
behind the same template check.

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

**Giving the structured field precedence for `dynamicAllocation.enabled`.** Every other
Kubeflow property resolved here prefers its structured spec field over the equivalent
`sparkConf` key, so `dynamicAllocationEnabled` reads as an inconsistency:

```go
if da := j.Spec.DynamicAllocation; da != nil && da.Enabled {
	return true
}
enabled, _ := strconv.ParseBool(j.Spec.SparkConf["spark.dynamicAllocation.enabled"])
return enabled
```

A structured `enabled: false` alongside `sparkConf` `"true"` yields **true**, which looks like
a bug. It is not fixable, because the CRD leaves no room:

```go
// Enabled controls whether dynamic allocation is enabled or not.
Enabled bool `json:"enabled,omitempty"`
```

`DynamicAllocation.Enabled` is a **non-pointer `bool` with `omitempty`**, so an explicit
`enabled: false` is indistinguishable from omitting the field. Honouring "explicit false"
would require `kubeflow/spark-operator` to make it `*bool` — an upstream API change, and one
this integration deliberately avoids taking a dependency on (see
[`spark-operator-parallelism-dependency.md`](./spark-operator-parallelism-dependency.md), which
records the last such dependency being removed rather than added).

Letting the structured surface win regardless would misread an ordinary manifest:

```yaml
spec:
  dynamicAllocation:
    minExecutors: 3          # enabled omitted, set through sparkConf instead
  sparkConf:
    spark.dynamicAllocation.enabled: "true"
```

Because `dynamicAllocation` is non-nil but `Enabled` reads false, this would be classified as a
**static** application — sizing the executor PodSet from `spec.executor.instances` while
Dynamic Allocation actually scales the pods. Under-reserving because a bool could not be told
apart from its zero value is a worse failure than over-reading enablement from either surface.

The OR is also the safer direction on its own terms: treating an application as elastic when
either surface says so routes accounting through the live-Pod derivation, which tracks reality,
rather than through a static count that Dynamic Allocation would leave stale. The cost of a
false positive is bounded — an application that is genuinely static gets the elastic path, whose
initial count still resolves through the same `numInitialExecutors` ladder.

Scope note: this applies to the Kubeflow CRD. The Apache integration resolves
`staticExecutorCount` conf-key-first, so "structured field first" is not a package-wide rule to
be consistent with in the first place — see
[`apache-sparkapplication-integration-design.md`](./apache-sparkapplication-integration-design.md)
§5 for why that CRD inverts it.

## 6. Testing

`TestLiveExecutorCount` covers: sparkConf-only count, the structured field taking precedence
in both directions (smaller and larger than the `sparkConf` value), no count declared anywhere,
a malformed conf value, the Dynamic Allocation path falling back to the `sparkConf` count, and
`instances` outranking an explicit `initialExecutors`.

`TestAddMemoryPrefersThePodTemplate` covers a template request and limit used verbatim, and
the fallback to `spec.executor.memory` when the template declares neither. (Superseded: that
test is now `TestAddMemoryIgnoresThePodTemplate`, asserting the opposite — see
[`spark-podset-align-with-spark-design.md`](./spark-podset-align-with-spark-design.md) §1.)

`TestDynamicAllocationEnabled` pins the OR semantics from §5: neither surface, each surface
alone, an unparseable `sparkConf` value, and the load-bearing case of bounds declared
structurally with enablement through `sparkConf`. It is a tripwire, not coverage of new
behaviour — a future "consistency" cleanup that gives the structured field precedence fails a
named test with the reasoning attached, instead of silently reclassifying elastic applications
as static.

Negative-controlled: replacing the three-way maximum with a precedence ladder fails
`initial_count_is_the_largest_of_instances,_initialExecutors_and_minExecutors`; ignoring the
pod template fails `template_request_and_limit_are_used_verbatim`; and restoring the original
structured-field-only read fails the sparkConf-fallback cases, including the cluster-observed
signature of a sparkConf-declared count resolving to 0.

`gofmt -l` clean, `go vet`, `go test`, `go test -race -count=1` on the package, and
`go build ./...` across the tree.
