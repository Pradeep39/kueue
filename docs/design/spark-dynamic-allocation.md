# Spark Dynamic Allocation with elastic SparkApplications

## Summary

Kueue admits a Spark application that uses Dynamic Allocation as an elastic job. Its executor
count follows what the Spark driver actually requests, one Workload slice at a time:

- Executors the driver adds stay gated until a Workload slice that includes them is admitted.
- When the driver removes executors, Kueue releases their quota once they have stopped.

Both Spark operators are supported:

| | Kubeflow Spark Operator | Apache Spark Kubernetes Operator |
|---|---|---|
| Resource | `sparkoperator.k8s.io/v1beta2` SparkApplication | `spark.apache.org/v1` SparkApplication |
| Feature gate | `ElasticJobsViaWorkloadSlicesForSparkApplication` | `ElasticJobsViaWorkloadSlicesForApacheSparkApplication` |
| Requires | `ElasticJobsViaWorkloadSlices`, `SparkApplicationIntegration` | `ElasticJobsViaWorkloadSlices`, `ApacheSparkApplicationIntegration` |

An application opts in with the annotation `kueue.x-k8s.io/elastic-job: "true"`.

## Model

1. **Workload.** An elastic application's Workload has two PodSets: the driver (count 1) and the
   executors. The executor count is the number of executors to charge for.
2. **Gating.** Kueue adds the `kueue.x-k8s.io/elastic-job` scheduling gate to the executor Pod
   template. Every executor the driver creates starts gated. Kueue removes the gate from at most
   as many executors as the admitted slice grants.
3. **Scale-up.** When the executors the driver wants exceed the grant, Kueue creates a new
   Workload slice for the larger count. The slice replaces the admitted one and is admitted only
   when it fits as a whole (scale-up is atomic). Once it is admitted, the new executors are
   ungated.
4. **Scale-down.** When the driver removes executors, Kueue lowers the count of the admitted
   Workload in place. The quota is released once those executors' Pods have stopped, so a
   terminating executor keeps its quota until then.

## Executor count

The operator declares the executors of the current attempt in the SparkApplication status.

- **Count.** Kueue uses `status.activeExecutors`: the operator's count of executor Pods that are
  pending (including gated) or running (including terminating). It is counted from the Pods, so
  it stays exact beyond any cap on the per-executor map, and with that map turned off.
- **Fallback.** With an operator that does not record `activeExecutors`, Kueue counts the
  `PENDING` and `RUNNING` entries of `status.executorState`.
- **Bounds.** The count is clamped to Dynamic Allocation's `minExecutors` and `maxExecutors`.
- **Before the driver runs,** or while the operator has reported no executor, the count is the
  initial one: the largest of the executor instances, `minExecutors` and `initialExecutors`.
  That is the count Spark starts with.
- **Never written.** Kueue derives the count and never writes it back to the SparkApplication.

## Slice names

A new slice needs a name that differs from every earlier slice of the same application, but the
driver adds executors without bumping `metadata.generation`. The slice name therefore folds in:

- the submission attempt: Kubeflow `status.submissionAttempts`, Apache
  `status.currentAttemptSummary.attemptInfo.id`;
- `status.maxExecutorId`, the highest executor ID of the attempt. Spark never reuses an executor
  ID within an attempt, so it grows with every executor added and never on a scale-down.

With an operator that does not record `maxExecutorId`, the number of entries in
`status.executorState` is used instead. It also only grows within an attempt.

## Reading the Dynamic Allocation configuration

Kueue reads Dynamic Allocation the way the Spark operator hands it to spark-submit.

**Kubeflow.** The operator passes `spec.sparkConf` first. It then adds `spec.executor.instances`
when set, and the `spec.dynamicAllocation` fields that are set, only when
`spec.dynamicAllocation.enabled` is true. The last value of a key wins. So:

- `spark.dynamicAllocation.enabled`, `.minExecutors`, `.maxExecutors` and `.initialExecutors` in
  `spec.sparkConf` apply unless the corresponding typed field is set and emitted;
- `spec.executor.instances` takes precedence over `spark.executor.instances`;
- `spec.dynamicAllocation.enabled: false` does not turn off Dynamic Allocation that `sparkConf`
  turns on.

**Apache.** The configuration lives only in `spec.sparkConf`. Keys Spark acts on take precedence
over `spec.applicationTolerations.instanceConfig`, which serves as the fallback.

Property names match Spark exactly (they are case-sensitive). A boolean is `true` in any case. A
malformed or negative count is treated as unset.

## Operator requirements

| Status field | Used for | Kubeflow | Apache |
|---|---|---|---|
| `executorState` | fallback count and slice names | all supported versions | IPR#456 / IPR#458, optional (`executorStateEnabled`, off by default) |
| `activeExecutors` | executor count | `Pradeep39/spark-operator` `active-executors` | IPR#456 / IPR#458 |
| `maxExecutorId` | slice names | `Pradeep39/spark-operator` `active-executors` | IPR#456 / IPR#458 |

- **Kubeflow API.** Kueue vendors the Kubeflow API as `Pradeep39/spark-operator`
  `v2.5.2-active-executors`: v2.5.2 plus the two status fields.
- **CRD schemas.** The SparkApplication CRDs must include these fields, or the API server prunes
  them.

## Readiness

`PodsReady` is measured against the executors Kueue grants: the application is ready once the
driver runs and every granted executor, up to `minExecutors` (or the executor instances without
Dynamic Allocation), is running. For the Apache operator, whose threshold is
`instanceConfig.minExecutors`, an application Dynamic Allocation shrinks below that threshold is
still ready when every granted executor runs.

## Validation

- An elastic application must keep the `kueue.x-k8s.io/elastic-job` scheduling gate in its
  executor Pod template.
- The Kubeflow webhook rejects `spec.dynamicAllocation.enabled: true` on an application that is
  not elastic.

## Limitations

- **Scale-up is atomic.** New executors wait until all of them fit. Partial scale-up
  (`ElasticJobsViaWorkloadSlicesWithPartialReplicaScaleUp`) does not apply to SparkApplications.
- **Gated executors count.** The driver counts gated executors as pending. Set
  `spark.kubernetes.allocation.maxPendingPods` to bound how many it creates ahead of quota.
- **Gated executors may be recreated.** The driver deletes and recreates executors that stay
  gated longer than `spark.kubernetes.allocation.executor.timeout`. The recreated executors are
  gated too.
- **Replacement double charge.** While a slice replaces the previous one, Kueue's cache charges
  both until the previous slice's Finished status is processed, about 0.1–0.2s per scale-up. The
  ClusterQueue status can read above quota for longer than that, because it is updated in
  batches. No extra Pods run in that time.
- **Executor-state cap (Kubeflow).** Without `activeExecutors`, the count relies on
  `status.executorState`, which the operator stops extending after
  `--max-tracked-executor-per-app` (1000 by default).
