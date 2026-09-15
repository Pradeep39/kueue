# Design: bounding the derived executor count to Dynamic Allocation's own limits

Status: proposed
Date: 2026-09-14

Fixes two defects in the SparkApplication elastic-scaling integration, both in how
`computeLiveExecutorCount` / `initialExecutorCount` turn observed state into an executor
`PodSet.Count`. Companion to
[`sparkapplication-elastic-scaling-design.md`](./sparkapplication-elastic-scaling-design.md)
§2.3 and the sequence diagrams in [`diagrams/`](./diagrams/README.md).

## 1. Defect A — a freshly admitted gang is dismantled seconds later

### Observed

An elastic `SparkApplication` with Dynamic Allocation configured through `sparkConf`
(`minExecutors: 3`, `maxExecutors: 30`, no `spec.executor.instances`, no structured
`spec.dynamicAllocation`) was admitted correctly and then immediately shrunk:

| | generation | `spec.podSets[executor].count` | granted count | granted `resourceUsage` |
|---|---|---|---|---|
| created + admitted, 15:17:29 | 1 | 3 | 3 | `cpu: 3`, `memory: 1536Mi` |
| after in-place patch | 2 | **1** | **1** | `cpu: 1`, `memory: 512Mi` |
| Finished, 15:17:36 | 2 | — | — | `WorkloadSliceReplaced` |

Seven seconds. The driver was admitted together with quota for its three initial executors,
that grant was then reduced to one, and the other two had to be re-acquired through a
replacement slice — with no guarantee the ClusterQueue still had room. The user's symptom was
"the driver and the min executors are not gang scheduled".

### Mechanism

`computeLiveExecutorCount` uses `initialExecutorCount()` **only while zero executor Pods
exist**. The instant one Pod appears it returns the live Pod count instead. The driver creates
its initial executors one API call at a time, so there is a window in which the live count is a
strictly-smaller prefix of the intended initial count.

Any reconcile landing in that window derives a lower count, `workloadslicing.ScaledDown()`
reports a scale-down, and `updatePodSetCountsWithRetry` + `scaleDownAdmission` patch
`spec.podSets[].Count` and `status.admission` down in place.

The 5s trailing-edge debounce in `sparkapplication_executor_pod_handler.go` was built to
coalesce exactly this kind of burst, and it does — for reconciles triggered by *Pod* events.
It does not help here, because the job reconciler also watches the `SparkApplication` itself,
and the Spark Operator updates that object's `status` repeatedly during startup
(SUBMITTED → RUNNING, driver state transitions). Those reconciles are not debounced. A
transient prefix observed through a foreign trigger is indistinguishable from a real
scale-down.

### Fix

Clamp the derived count to the bounds Dynamic Allocation itself promises to respect:

```go
func (j *SparkApplication) clampToDynamicAllocationBounds(count int32) int32 {
	if n, ok := j.dynamicAllocationExecutorCount("minExecutors"); ok && count < n {
		count = n
	}
	if n, ok := j.dynamicAllocationExecutorCount("maxExecutors"); ok && count > n {
		count = n
	}
	return count
}
```

The lower bound is the load-bearing half. `minExecutors` is a floor Dynamic Allocation never
sustains fewer executors than, so reporting below it is never *more* accurate — it only
describes a transient that DA is actively correcting. Holding the floor removes the entire
class of startup churn, and also the churn from an executor dying and being replaced.

`maxExecutors` is applied last, so a configuration with `minExecutors > maxExecutors` can never
inflate the count above the declared maximum.

## 2. Defect B — `initialExecutorCount` ignored the Dynamic Allocation bounds

`initialExecutorCount()` returned `spec.executor.instances` outright when set, reaching
`initialExecutors`/`minExecutors` only if it was nil. So `instances: 1` with `minExecutors: 5`
reserved one executor while Spark immediately asked for five — the driver admitted without its
initial executors, remainder via a scale-up slice. Same visible symptom as defect A, different
cause, and it fires even when defect A does not.

The count is now resolved by `declaredInitialExecutors()` and then passed through
`clampToDynamicAllocationBounds`, so `minExecutors` acts as a floor on the initial estimate
regardless of which surface supplied it. The resolution order itself is documented in
[`sparkapplication-sparkconf-executor-instances-design.md`](./sparkapplication-sparkconf-executor-instances-design.md)
§3, which supersedes the max-based scheme this document originally described.

## 3. Relationship to the gated-Pod feedback loop

The upper clamp **narrows but does not close** the gated-executor problem recorded in
[`diagrams/README.md`](./diagrams/README.md) ("Known coupling"). `isVerifiedLiveExecutor`
counts Pods still blocked by `kueue.ElasticJobSchedulingGate`, so a saturated ClusterQueue
leaves Pods gated, gated Pods raise the derived count, and a higher count is harder to admit.
Bounding by `maxExecutors` converts an unbounded climb into a bounded over-request, which is
strictly better — the observed 6656Mi → 11776Mi climb was DA continuing toward a max of 30
against a 12-slot quota. But when `maxExecutors` exceeds what the queue can grant, the
over-request still cannot be admitted.

Closing it properly needs a bound derived from the ClusterQueue's capacity rather than from the
application's own declaration, which the `PodSets(ctx, client)` signature does not currently
expose. Deliberately out of scope here.

## 4. What is unchanged

- **Dynamic Allocation disabled.** `computeLiveExecutorCount` still short-circuits to
  `numInitialExecutors()` — the raw `spec.executor.instances` — before any of this runs. No
  behavior change for non-elastic or non-DA applications.
- **No bounds configured.** With neither `minExecutors` nor `maxExecutors` set (in the
  structured block or `sparkConf`), the clamp is the identity function and the observed count is
  reported as before.
- **Still derive, never write.** Nothing here writes to the `SparkApplication` CR, so the
  Spark Operator's `event_filter.go` `DeepEqual` resubmission trap (§2.2 of the elastic-scaling
  design) remains avoided.

## 5. A related Spark Operator bug, not fixed here

The reported configuration set DA purely through `sparkConf`. The Spark Operator's own DA
detection reads the wrong key (`api/v1beta2/defaults.go:112`):

```go
dynamicAllocationConfVal, _ := strconv.ParseBool(sparkConf["spark.dynamicallocation.enabled"])
```

all-lowercase, where the Spark property is `spark.dynamicAllocation.enabled`. Map lookups are
case-sensitive, so the operator concludes DA is off and `setExecutorSpecDefaults` would default
`spec.executor.instances = 1`.

That default turned out **not** to reach Kueue: it is registered through
`scheme.AddTypeDefaultingFunc`, applied in-process when the operator decodes the object and
never persisted, and the CRD carries no structural default for the field (only `minimum: 1`).
The stored CR keeps `instances: null`, which is what Kueue reads — confirmed on the cluster.
So the typo does not contribute to either defect above; it is recorded here only because it was
investigated in depth and would otherwise be re-derived. Kueue's own
`dynamicAllocationEnabled()` reads the correct camelCase key.

## 6. Testing

`TestLiveExecutorCount` gains cases for: live count below the floor, live count above the
ceiling, count within bounds reported as observed, inverted `min > max`, `instances` below and
above `minExecutors`, and no bounds configured.

Negative-controlled both halves:

- Replacing `clampToDynamicAllocationBounds` with a passthrough fails exactly the three clamp
  cases, including `live_count_below_minExecutors_is_raised_to_the_floor` returning 1 — the
  cluster-observed regression signature.
- Restoring first-match precedence in `initialExecutorCount` fails exactly
  `instances_below_minExecutors_reserves_the_Dynamic_Allocation_floor`.

`gofmt -l` clean, `go vet`, `go test`, `go test -race -count=1` on the package, and
`go build ./...` across the tree.
