# Design: charging Spark's real memory request in the Kubeflow integration

Status: proposed
Date: 2026-09-15

Closes the memory under-charge recorded in PR #23: `spec.{driver,executor}.memory` is the JVM
**heap** size, not the pod's request, and Kueue was charging the heap size verbatim.

Stacked on [`sparkapplication-sparkconf-executor-instances-design.md`](./sparkapplication-sparkconf-executor-instances-design.md)
§3a, which established that a memory value written on the pod template is taken verbatim. This
document covers the other branch of that decision: what to charge when the value comes from
Spark's own memory configuration.

> **Amended (2026-09-16)** by
> [`spark-podset-align-with-spark-design.md`](./spark-podset-align-with-spark-design.md):
> the pod-template branch is gone, so this arithmetic is no longer gated behind it — it now
> applies to every application. §3's first bullet and the floor in §2 are updated below.

## 1. The under-charge

The operator maps `spec.{driver,executor}.memory` onto `spark.{driver,executor}.memory`, and
Spark's `BasicDriverFeatureStep`/`BasicExecutorFeatureStep` then add overhead before setting
the container's request. So a 512m executor requests **896Mi**, not 512Mi — 512 of heap plus
Spark's 384MiB `MEMORY_OVERHEAD_MIN_MIB` floor, because 0.1 × 512 is smaller than the floor.

Measured on the cluster: executor pods requesting 896Mi against a ClusterQueue charged
512Mi each, so a 7680Mi quota admitted 15 pods that really needed 13440Mi. Setting
`memoryOverhead: 384m` explicitly did not help — the field was never read.

## 2. What is charged now

`totalMemoryBytes(role)` reproduces Spark's arithmetic:

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

Every property resolves its structured field before its `sparkConf` equivalent, matching the
precedence rule the rest of the package follows.

Three fidelity details worth stating, because getting them wrong produces plausible-looking
but wrong numbers:

- **The arithmetic is in whole MiB and truncates.** Spark computes
  `(overheadFactor * memoryMiB).toInt`, so 0.4 × 8192 is 3276, not 3277. Doing it in bytes
  with rounding disagrees with the pod by a fraction of a MiB.
- **A value with no unit suffix is MiB**, which is both Spark's convention and what the CRD
  documents for `memoryOverhead`. `sparkutil.ConvertJavaMemoryStringToK8sMemoryString` returns
  bare digits unchanged, so `"384"` parsed as a `resource.Quantity` would otherwise be 384
  *bytes*.
- **The factor default depends on application type**, but only when no factor is set
  explicitly. An explicit `memoryOverheadFactor` applies to Python applications too.

Constants come from the operator's own `pkg/common` — `DefaultJVMMemoryOverheadFactor`,
`DefaultNonJVMMemoryOverheadFactor`, `MinMemoryOverhead` — rather than being duplicated, so
they cannot drift from the operator Kueue is integrating with.

## 3. When the math does *not* apply

- ~~**A pod-template memory request** is used verbatim.~~ Removed 2026-09-16: Spark overwrites
  the template's container resources when it builds the pod, so the arithmetic applies there
  too. See [`spark-podset-align-with-spark-design.md`](./spark-podset-align-with-spark-design.md) §1.
- **No memory configured through any Spark surface.** The request is left unset rather than
  inventing Spark's 1g default for an application that never asked for memory. `defaultMemoryMiB`
  is only reached once *some* Spark memory property is present.

## 4. Limits

An explicitly configured `spec.{role}.memoryLimit` is raised to the computed request when it
sits below it. It is written against the heap size, so `memory: 512m` with
`memoryLimit: 512m` would otherwise produce a PodSet with a request of 896Mi above a limit of
512Mi — invalid, and not what Spark does either; Spark sets the limit equal to the request.
When no `memoryLimit` is configured the limit is left unset, as before.

## 5. Behavior change

**This changes charging for every existing Kubeflow SparkApplication.** Quota usage for an
unchanged manifest rises by the overhead — at minimum 384MiB per pod, more for large
executors or Python applications. Queues sized against the old under-charge will admit fewer
workloads.

That is the point: the new number is what the pods actually request, so the previous behavior
was over-admitting against real node capacity. But it is not a silent improvement, and anyone
upgrading should expect to re-size per-queue `nominalQuota`.

`TestPodSets`'s expectations moved from 512Mi to 896Mi for exactly this reason, and the fixture
comment records why.

## 6. Testing

`TestTotalMemoryBytes` covers thirteen cases: the floor and the factor each winning, an
explicit `memoryOverhead`, a bare (MiB) overhead value, an overridden factor, the Python
non-JVM factor, `sparkConf` supplying the heap, the structured field beating `sparkConf`,
PySpark memory on executors and its absence on drivers, off-heap counted only when enabled,
and the all-defaults case.

Negative-controlled: removing the overhead term fails 15 tests, with the signature
`totalMemoryBytes() = 536870912 (512Mi), want 939524096 (896Mi)` — the cluster-observed
under-charge.

`gofmt -l` clean, `go vet`, `go test`, `go test -race -count=1` on the package, and
`go build ./...` across the tree.
