# Design: align both Spark integrations with what Spark actually puts on the pod

Status: proposed
Date: 2026-09-16

Four corrections to the two SparkApplication integrations — `pkg/controller/jobs/sparkapplication`
(Kubeflow, `sparkoperator.k8s.io/v1beta2`) and `pkg/controller/jobs/apachesparkapplication`
(`spark.apache.org/v1`). Each one replaces a rule that looked reasonable from the CRD with the
rule the pod is actually created under.

The governing principle, stated once: **a PodSet must charge what the kubelet will be asked
for.** Where two surfaces disagree, the one Spark reads wins — not the one that looks most
structured.

## 1. A pod-template memory request is not authoritative

[`sparkapplication-sparkconf-executor-instances-design.md`](./sparkapplication-sparkconf-executor-instances-design.md)
§3a introduced the rule that a memory request written on `spec.{driver,executor}.template` is
"the total the submitter intends" and must be charged verbatim, and
[`sparkapplication-memory-overhead-design.md`](./sparkapplication-memory-overhead-design.md)
§3 kept Spark's arithmetic gated behind it. Both integrations implemented it.

The premise is false. Neither operator hands the pod template to the kubelet:

- Kubeflow's `executorPodTemplateOption`/`driverPodTemplateOption` (`internal/controller/sparkapplication/submission.go`,
  v2.5.1) serialise the template to a file and pass
  `spark.kubernetes.{driver,executor}.podTemplateFile`.
- The Apache operator's `SparkAppResourceSpecFactory` does the same thing, then builds the
  driver pod through Spark's own `KubernetesDriverBuilder.buildFromFeatures`
  (`SparkAppSubmissionWorker.java`).

Spark loads that file as the *initial* pod and then runs its feature steps over it.
`BasicExecutorFeatureStep` (Spark v4.0.1, lines 200-208) and `BasicDriverFeatureStep`
(lines 102-136) both do:

```scala
new ContainerBuilder(pod.container)
  .editOrNewResources()
    .addToRequests("memory", memoryQuantity)   // base + overhead
    .addToLimits("memory", memoryQuantity)
    .addToRequests("cpu", cpuQuantity)
```

`addToRequests` replaces the key. A template request of `512Mi` alongside Spark's default 1g
heap produces a pod that requests 1408Mi — so charging the template value under-charges by
almost 900MiB per pod, reopening the hole PR #23 and the memory-overhead work closed.

The rule cannot help, either: when a submitter writes the correct total on the template, the
arithmetic already computes that same number. It can only ever introduce divergence.

Both integrations now always derive memory from Spark's configuration.
`templateMemory`/`templateContainer` are deleted from the Kubeflow package, and the
`memoryDeclared` branch is gone from the Apache `buildPodTemplateSpec`.

Independent corroboration: the Apache operator's own Kueue integration, added upstream on
2026-09-14 ([SPARK-59490], `KueueWorkloadFactory.java`), overwrites the template the same way,
with the comment *"Like Spark, overwrite the requests of the pod template."*

This also retires the "CPU does not honour the pod template" follow-up. The Apache
`buildPodTemplateSpec` overwriting a template CPU request unconditionally is correct for the
same reason; the asymmetry it was reported as no longer exists.

## 2. `spark.executor.instances` outranks `instanceConfig` on the Apache CRD

`staticExecutorCount()` preferred `spec.applicationTolerations.instanceConfig.initExecutors`
over the `spark.executor.instances` conf key, for consistency with how every other property in
that package resolves. The existing code comment already recorded the cost: an application
declaring `initExecutors: 2` alongside `spark.executor.instances: 15` reserved quota for two
pods while fifteen ran.

`instanceConfig` never reaches Spark. The only consumer in the operator is `AppRunningStep`,
which uses it for readiness thresholds; `SparkAppSubmissionWorker` never translates it into a
`--conf`. The driver creates executors from `spark.executor.instances` alone. Upstream's
`KueueWorkloadFactory.buildExecutorPodSet` reads that key and nothing else, for the same reason.

The precedence is therefore inverted for the count and for the Dynamic Allocation bounds:

```
instances  = spark.executor.instances  ->  instanceConfig.initExecutors  ->  Spark's default 2
bound      = spark.dynamicAllocation.{initial,min,max}Executors
             ->  instanceConfig.{init,min,max}Executors  ->  unset
```

`instanceConfig` is retained as the fallback rather than dropped. When the conf key is absent
it is the only declaration of intent available, and for `minExecutors` it keeps the startup
floor that stops a partially-created executor set from being read as a scale-down (see
`clampToDynamicAllocationBounds`).

This does not contradict the structured-field-first rule on the **Kubeflow** CRD, where the
operator does map `spec.executor.instances` onto `--conf spark.executor.instances` at
submission. The rule is "prefer the surface the driver acts on"; on that CRD both surfaces
qualify and the structured one is the declared intent.

## 3. The overhead factor truncates

The Apache `totalMemoryBytes` used `math.Round(factor * base)`. Spark computes
`(factor * memoryMiB).toInt`, and the Kubeflow integration already truncated. Only reachable
above a 3840MiB base, where the floor stops dominating: a 4g executor was charged 4506MiB
instead of 4505MiB. Upstream's `calculateMemoryOverheadMiB` also uses `(int)`.

## 4. `spark.{driver,executor}.minMemoryOverhead` is honoured

Spark 4.0 made the 384MiB floor configurable (`DRIVER_MIN_MEMORY_OVERHEAD`,
`EXECUTOR_MIN_MEMORY_OVERHEAD`, default `384m`, *"ignored if `spark.{role}.memoryOverhead` is
set directly"*). Both integrations hardcoded 384MiB, so an application raising the floor was
under-charged by the difference.

Both now read the key on the factored path only, matching Spark's documented interaction with
an explicit overhead. Neither CRD has a structured equivalent, so `sparkConf` is the only
surface.

## 5. Behavior change

- **Applications that declared memory on the pod template are now charged Spark's arithmetic
  instead of the template value.** For a template that already carried the correct total this
  is a no-op; where it disagreed, usage rises to what the pods request. Re-check
  `nominalQuota` on queues running such manifests.
- **Apache applications where `spark.executor.instances` exceeds `initExecutors` now reserve
  the larger count**, which is the number of pods that actually appear.
- Large-executor charges move by 1MiB in the truncation case.

## 6. Testing

- `TestAddMemoryIgnoresThePodTemplate` (Kubeflow) and
  `TestBuildPodTemplateSpecOverwritesTemplateMemory` (Apache) pin that a template request of
  2Gi is replaced by base+overhead. Both fail with the verbatim branch restored.
- `TestStaticExecutorCount/spark.executor.instances_takes_precedence_over_instanceConfig` and
  `TestLiveExecutorCount/sparkConf_instances_outranks_instanceConfig_initExecutors` pin the
  inverted precedence; `TestLiveExecutorCount/sparkConf_bounds_take_precedence_over_the_instanceConfig_equivalents`
  pins it for the clamp. The `instanceConfig`-only cases still pass, pinning the fallback.
- `TestTotalMemoryBytes` gains the `minMemoryOverhead` floor, its interaction with an explicit
  overhead, and a malformed-value error, in both packages; the 4g case moves from 410 to 409
  MiB of overhead.

`gofmt -l` clean, `go vet`, `go test`, `go test -race -count=1` on both packages, and
`go build ./...` across the tree.

[SPARK-59490]: https://github.com/apache/spark-kubernetes-operator/commits/main/spark-operator/src/main/java/org/apache/spark/k8s/operator/kueue/KueueWorkloadFactory.java
