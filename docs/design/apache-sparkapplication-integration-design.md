# Design: Kueue quota management for the Apache Spark Kubernetes Operator

Status: **the Kueue-side Go integration has been removed.** `pkg/controller/jobs/apachesparkapplication`
(PR #28, ~3,500 lines) is deleted, along with its `ApacheSparkApplicationIntegration` feature
gate, its `spark.apache.org/sparkapplication` framework name, its webhooks and its RBAC.
Quota management for `spark.apache.org/v1` now follows upstream's **operator-owned Workload**
model, traced in
[`diagrams/apache-da-architecture-a.png`](./diagrams/apache-da-architecture-a.png).

The Kubeflow integration (`pkg/controller/jobs/sparkapplication`,
`sparkoperator.k8s.io/v1beta2`) is untouched and unaffected. Everything below concerns the
Apache CRD only.

**The headline result, verified against the code rather than assumed: no new Kueue-side Go is
required.** Every piece of the elastic machinery is annotation-driven and reachable for a
Workload that Kueue did not create. §4 is the contract; §5 is the proof.

---

## 1. Why the Go integration was removed

Not because it was wrong — it worked, with unit tests — but because it was the **second**
implementation of a responsibility upstream had already claimed, and the two cannot coexist.

The two operators diverge by an **inversion of control**:

| | Kubeflow | Apache |
|---|---|---|
| Who creates the Workload | **Kueue** | **the operator** |
| Where the logic lives | `pkg/controller/jobs/sparkapplication`, Go, `jobframework.GenericJob` | `KueueWorkloadFactory.java`, in the operator |
| Who knows about whom | Kueue knows the operator's CRD | The operator knows Kueue's API |
| Admission gating | Kueue suspends the CR via `spec.suspend` | The operator holds its own driver until its Workload is admitted |

The removed package applied the *Kubeflow* model to the *Apache* CRD. Upstream had chosen the
opposite model for the same CRD, and both key off the same `kueue.x-k8s.io/queue-name` label.

**They collide, and the operator's half cannot be switched off.** `AppInitStep`'s
`holdForKueueAdmission` runs whenever `KueueWorkloadFactory.hasQueueName(app)` is true — that
label *is* the trigger — and the only Kueue option in `SparkOperatorConf` is
`spark.kubernetes.operator.kueue.workloadInformer.enabled`, which controls whether the operator
*watches* Workloads, not whether it creates them.

Worse than double-charging: upstream's Workload carries an ownerReference to the SparkApplication
with `controller = true`, so `FindMatchingWorkloads` claims it — same Kind, APIVersion and name
as the job. `EquivalentToWorkload` would not match it (live-derived count, and a template
carrying the executor scheduling gate), so jobframework deletes it while the operator recreates
it through `getOrCreateSecondaryResource`. Two controllers deleting each other's Workload in a
loop, with `holdForKueueAdmission` holding the driver throughout — the application hangs rather
than merely mis-accounts. Read from both sides; **not observed on a cluster.**

Given that, keeping a second implementation had no path to being deployable. Removing it also
removes the temptation to enable both.

## 2. What upstream provides, and the gap that remains

Upstream `apache/spark-kubernetes-operator` `main` as of 2026-09-21 (none of it in release
`1.0.0`):

- **SPARK-59475** — `suspend` on `BaseSpec`, as `protected boolean suspend = false` with
  `@Default("false")`. On the parent, so `ClusterSpec` gets it too and SparkCluster is
  suspendable. Note the CRD carries `default: false`, so the API server always materialises the
  field and an explicit `suspend: false` is indistinguishable from an omitted one — the same
  shape as the `dynamicAllocationEnabled` problem documented in
  [`sparkapplication-sparkconf-executor-instances-design.md`](./sparkapplication-sparkconf-executor-instances-design.md)
  §5. Harmless here, since Kueue always writes an explicit value, but the distinction is gone.
- **SPARK-59490 / SPARK-59503** — `kueue/KueueWorkloadFactory.java` and `KueueWorkloadUtils.java`:
  the operator builds the `kueue.x-k8s.io/v1beta2` Workload itself, driver + executor PodSets,
  `active = !suspend`.
- The lifecycle, not just the field: `SuspendUtils.java`, plus `suspend` handling in
  `AppInitStep`, `ClusterInitStep`, `EventUtils`, `ApplicationStatus`.

**The gap:** `buildWorkload(SparkApplication)` throws `UnsupportedOperationException` when
`spark.dynamicAllocation.enabled=true`
([KueueWorkloadFactory.java#L93-L97](https://github.com/apache/spark-kubernetes-operator/blob/89f97fd0ead4422ac831a46869ef285fbae08140/spark-operator/src/main/java/org/apache/spark/k8s/operator/kueue/KueueWorkloadFactory.java#L93-L97)).
Dynamic Allocation is precisely what this effort exists to support, so that refusal is the whole
remaining problem.

**It is a correct refusal, not an arbitrary barrier.** Removing the `throw` alone would turn a
clean error into silent under-reservation:

- `buildExecutorPodSet` reads `spark.executor.instances` **once** (default 2). DA changes the
  live Pod count, not `sparkConf`, so the Workload would be admitted at the static number and
  never track scaling.
- `requestAdmission` **early-returns on `isAdmitted(workload)`** before comparing podSets. The
  only in-place update in `KueueWorkloadUtils` is the priority class.
- Its one reaction to a changed podSet is **delete-and-recreate**, and only while pending
  (`spark.operator/kueue-pod-sets-hash` mismatch → `deleteWorkload` → `STALE`). That is the
  opposite of the slice-replacement protocol, which needs the predecessor to remain in the
  snapshot so the replacement is charged only the delta.
- **No `SchedulingGate` usage and no `workload-slice` references anywhere upstream.** Without
  gating there is no admission control over DA-created executor Pods at all: the driver creates
  them and kube-scheduler places them regardless of quota.

## 3. The architecture

See the diagram. Lane tint gives the deployment side — amber for the
`spark-kubernetes-operator` pod (Java), blue for `kueue-controller-manager` (Go), neutral for the
Spark driver pod and the control plane. Four operator lanes against three Kueue lanes, and the
ratio is the point: **everything Workload-level is Kueue's, already built; everything job-level
is new Java.**

Scale-up, in one line each: DA creates gated executor Pods → a new operator-side Pod watch
debounces and enqueues → the operator re-derives the live executor count and clamps it to DA's
bounds → it builds a *replacement* Workload slice carrying the slice annotations → Kueue's
scheduler finds the predecessor, charges only the delta, nets the chain in its cache, and ungates
exactly `granted − alreadyUngated` Pods.

Scale-down is two in-place patches — `spec.podSets[].count` and `status.admission` — permitted by
the decrease-only, elastic-only exception in Kueue's workload webhook.

## 4. The contract the operator must satisfy

This is the load-bearing section, and the reason this document exists after the Go code is gone.
Kueue keys entirely off annotations and labels. Get these right and the machinery in §5 works
untouched; get them wrong and it silently does nothing.

### On the Workload

| Key | Value | Why |
|---|---|---|
| annotation `kueue.x-k8s.io/elastic-job` | `"true"` | Turns on elastic semantics *and* the decrease-only admission exception. `workloadslicing.Enabled` reads it **off the Workload**, so no job-side annotation is needed. |
| annotation `kueue.x-k8s.io/workload-slice-name` | the chain-root Workload's name | The chain grouping key. Must be set on **every** slice including the root. |
| annotation `kueue.x-k8s.io/workload-slice-replacement-for` | `<namespace>/<name>` of the predecessor | What makes the scheduler charge only the delta and preempt the predecessor. |
| label `kueue.x-k8s.io/job-uid` | the SparkApplication's UID | Scopes the chain to one job instance. Without it, a deleted-and-recreated application whose slices inherit the old chain-root name shares a chain, and only the newest slice is counted — an **under**-count. |
| `ownerReferences` | the SparkApplication, `controller: true` | Already done by upstream. |
| `spec.queueName`, `spec.podSets`, `spec.active` | as today | Already done. |

### On executor Pods, via the pod template

| Key | Value | Why |
|---|---|---|
| scheduling gate `kueue.x-k8s.io/elastic-job` | present | Without it there is no admission control: the driver's executors run regardless of quota. |
| label `kueue.x-k8s.io/podset` | `executor` | How the ungater buckets Pods per PodSet to apply the granted cap. |
| annotation `kueue.x-k8s.io/workload-slice-name` | the chain-root name | How the ungater *finds* the Pods, via a field index. |

In the Kueue-owned model a Kueue webhook injected the gate and `RunWithPodSetsInfo` merged the
label and annotation. Neither runs here, so **the operator must stamp all three itself** when it
builds the executor pod template.

### Cluster prerequisites

`ElasticJobsViaWorkloadSlices` must be enabled on Kueue. It gates the Pod and Workload
`workloadSliceName` field indexes (`pkg/controller/core/indexer/indexer.go:371-380`), the
scheduler's replacement detection, and the cache netting. Nothing needs the removed feature gate
or framework name.

## 5. What Kueue already provides, unchanged

Each of these was checked to confirm it does **not** require a registered `GenericJob`. This is
why no new Kueue Go is needed.

| Kueue component | Gated on | File |
|---|---|---|
| `ReplacedWorkloadSlice` / `FindReplacedSliceTarget` — predecessor detection | the feature gate + the Workload's own `replacement-for` annotation | `pkg/workloadslicing/workloadslicing.go:559,606`; called from `pkg/scheduler/scheduler.go:518,921` inside `getInitialAssignments`, which runs for **any** Workload |
| `Assignment.append` — charges only the delta | nothing job-side | `pkg/scheduler/flavorassigner/flavorassigner.go:1026` |
| `sliceChainKey` / `reconcileSliceGroup` — per-chain netting, so overlapping slices are not double-counted | Workload annotations + the `job-uid` label; degrades gracefully if the label is absent | `pkg/cache/scheduler/clusterqueue.go:531,603` |
| `elasticJobUngater` — releases exactly `granted − alreadyUngated` Pods | a **standalone controller**, set up unconditionally at `cmd/kueue/main.go:589`, watching Workloads and Pods | `pkg/controller/elasticjobs/elastic_job_ungater.go:118,265` |
| decrease-only, elastic-only `validateAdmissionUpdate` — permits lowering a grant | `workloadslicing.Enabled(newObj)`, i.e. the **Workload**, at `pkg/webhooks/workload_webhook.go:379` | `pkg/webhooks/workload_webhook.go:404` |
| `totalRequestsFromAdmission` — charges `min(spec, granted)` | nothing job-side | `pkg/workload/workload.go:790` |

Two consequences worth stating plainly:

- **An earlier draft of this document was wrong** to say Architecture A "cannot be delivered
  operator-side alone" because it depends on #21's admission relaxation. The relaxation is
  required, but it is already **merged** and it is Workload-level. The Kueue side is complete.
- **`EnsureWorkloadSlices` is not needed.** It has exactly one caller
  (`pkg/controller/jobframework/reconciler.go:1096`, reachable only via `ReconcileGenericJob`), so
  it is unavailable here — but it is only jobframework's *helper* for creating slices. In this
  model the operator creates them directly, which is the step the helper would have performed.

**Prebuilt workloads do not help and must not be used.** `kueue.x-k8s.io/prebuilt-workload-name`
looks like the supported hook for an externally-created Workload, and `ensureOneWorkload`'s
prebuilt branch even mentions slicing. It is checked *before* the slice branch and returns, so
setting the label **disables** slicing. Every in-tree producer is a MultiKueue adapter — where the
manager cluster owns the real Workload and does the slicing — or a pod-owning reconciler
(statefulset, leaderworkerset). Not an external-creation hook.

## 6. What the operator must implement

The Java task list, with the Go original each piece mirrors. The Go is deleted from `main` but
recoverable from history (§8) and the Kubeflow equivalents are still present and live.

The removed implementation's two scaling flows are drawn in
[`diagrams/apache-kueue-da-upscale.png`](./diagrams/apache-kueue-da-upscale.png) and
[`diagrams/apache-kueue-da-downscale.png`](./diagrams/apache-kueue-da-downscale.png), with the Go
line numbers pinned to the last commit where the package existed. Read them against
[`diagrams/apache-da-architecture-a.png`](./diagrams/apache-da-architecture-a.png) to see which
lanes move into the operator.

1. **An executor Pod watch.** Label-keyed on the app-name and role labels — executor Pods are
   owned by the *driver* Pod, so there is no OwnerReference chain to watch. Trailing-edge
   debounce with a max-wait ceiling; 5s/30s was the tuned pair. *Mirrors
   `sparkapplication_executor_pod_handler.go`: `isTrackedExecutorPod:70`, `schedule:135`.*
2. **A live executor count.** Count non-terminal Pods, **including still-gated ones** — excluding
   them deadlocks scale-up detection, since a gated Pod is exactly the evidence that DA wants to
   grow. Pods with a `DeletionTimestamp` still count until `Succeeded`/`Failed`, because they
   still hold node resources. *Mirrors `sparkapplication_podset.go`: `isVerifiedLiveExecutor:176`,
   `computeLiveExecutorCount:223`.*
3. **A clamp to DA's own bounds.** The **lower** bound is load-bearing, not cosmetic: a reconcile
   landing mid-startup sees a transient prefix of the initial executors, which is
   indistinguishable from a real scale-down, and patching the grant down dismantles the gang that
   was just admitted. Observed on a real cluster as a PodSet admitted at 3 and patched to 1 within
   seven seconds. The upper bound narrows — but does not close — the gated-Pod feedback loop,
   because it bounds by DA's ceiling rather than by grantable capacity. *Mirrors
   `clampToDynamicAllocationBounds:356`.*
4. **Slice creation.** Stamp the §4 annotations and create a *new* Workload for a scale-up rather
   than growing the existing one. *Mirrors `workloadslicing.ScaledUp:235` / `EnsureWorkloadSlices:245`.*
5. **Scale-down as two in-place patches**, not delete-and-recreate: lower `spec.podSets[].count`,
   then lower the grant and rescale `PodSetAssignment.ResourceUsage` proportionally. Note
   `ResourceUsage` is the **podSet total, not per-pod** — confirmed against cluster data (count 7
   ↔ 3584Mi at 512Mi/pod). *Mirrors `updatePodSetCountsWithRetry:422`, `scaleDownAdmission:379`.*
6. **Remove `requestAdmission`'s early return on an admitted Workload**, and its
   delete-and-recreate on a podSets-hash mismatch.
7. **Stamp the gate, PodSet label and slice annotation** into the executor pod template (§4).

### Resource arithmetic to carry over

The deleted Go encoded Spark's behaviour precisely. Upstream's `KueueWorkloadFactory` already does
most of this; these are the details that were got wrong at least once each and are worth checking
against:

- **Charge base + overhead, never `spark.{role}.memory` alone.** A 1g executor at the default
  factor is 1408MiB. Reading the field verbatim is the 384MiB-per-pod under-charge recorded in
  PR #23.
- **Never honour a pod-template resource request verbatim.** Both operators pass the template to
  Spark as `spark.kubernetes.{role}.podTemplateFile`, and `Basic{Driver,Executor}FeatureStep`
  overwrites the container's cpu and memory before the pod is created. A template request is never
  what the pod asks for. Upstream agrees — `decorateContainerResources`, *"Like Spark, overwrite
  the requests of the pod template."*
- **Spark truncates, it does not round**: `(factor * memoryMiB).toInt`.
- **The memory limit equals the request.**
- **Honour `spark.{role}.minMemoryOverhead`** (Spark 4); the Kubeflow package hardcodes 384MiB.
- Overhead factor precedence: `spark.{role}.memoryOverheadFactor`, else
  `spark.kubernetes.memoryOverheadFactor`, else 0.1 JVM / 0.4 non-JVM. Add
  `spark.executor.pyspark.memory` on executors, and `spark.memory.offHeap.size` only when
  `spark.memory.offHeap.enabled`.
- CPU: `spark.kubernetes.{role}.request.cores`, else `spark.{role}.cores`. **No** fallback to
  `limit.cores` — Spark has none either.
- **`spark.executor.instances` outranks `instanceConfig` on this CRD.** `instanceConfig` never
  reaches Spark: the driver creates executors from `spark.executor.instances`, and `instanceConfig`
  feeds only the operator's readiness thresholds (`AppRunningStep`). Upstream reads
  `spark.executor.instances` alone, for the same reason.
- **The initial count is a maximum, not a precedence**: the largest of `minExecutors`,
  `initialExecutors` and the resolved instances count, matching
  `Utils.getDynamicAllocationInitialExecutors`. Spark starts the largest of the three regardless
  of which the author considered authoritative, so precedence across them under-reserves.

## 7. A cheaper path worth trying first

The operator already computes the Dynamic Allocation condition in order to throw on it. Having
`holdForKueueAdmission` **skip** when `spark.dynamicAllocation.enabled=true` would route static
applications through upstream's factory and leave DA ones to another mechanism, with no
application served by both. That reframes the upstream ask as *delegate this case* rather than
*support Dynamic Allocation*, which is a far easier argument.

It is recorded here as an option, not a plan: with the Go integration removed there is currently
nothing on the Kueue side to delegate *to*, so it only becomes useful alongside §6.

Separately, and independent of Dynamic Allocation: **upstream is missing a toggle for its own
Kueue path**. Something like `spark.kubernetes.operator.kueue.enabled`, defaulting true, is small,
defensible on its own terms, and would have made the collision in §1 avoidable.

## 8. What was removed, and how to get it back

Deleted in this change:

- `pkg/controller/jobs/apachesparkapplication/` — the whole package, including
  `api/v1/types.go` (a hand-written partial Go projection of the Java CRD; it existed because the
  operator publishes no Go module, and was safe only because jobframework patches rather than
  Updates — a `client.Update` would have dropped every omitted field)
- the `ApacheSparkApplicationIntegration` feature gate, and its entries in both
  `versioned_feature_list.yaml` copies
- `apachesparkapplication.RegisterIntegration` from `pkg/controller/jobs/jobs.go`
- the `apachesparkv1` GVK from `jobframework/validation.go`'s elastic-job allow-list
- `mapachesparkapplication.kb.io` / `vapachesparkapplication.kb.io` from both webhook manifests
- the `spark.apache.org` RBAC grant from both `role.yaml` copies
- `"spark.apache.org/sparkapplication"` from the four framework-list comments

The code is in history. It was merged as **PR #28** on `Pradeep39/kueue` and the package's final
state is at the parent of the commit that removed it; `git log --diff-filter=D --
pkg/controller/jobs/apachesparkapplication` finds it. If the decision is reversed, revert this
commit rather than rewriting from this document — the unit tests went with it.

## 9. Known gaps and unverified assumptions

- **The whole of §6 is unimplemented.** Nothing supports Dynamic Allocation on this CRD today:
  upstream refuses it, and the Kueue-side implementation has been removed.
- **Unverified:** whether Kueue's workload controller cleanly admits an operator-created Workload
  owned by no Kueue integration. This is the load-bearing assumption in upstream's design, and
  upstream has e2e coverage (`tests/e2e/kueue/spark-example.yaml`) for the static case, so it
  evidently holds there. Not checked for the elastic case.
- **The gated-Pod feedback loop is narrowed, not closed** (§6 item 3). A real fix bounds the
  requested count by grantable capacity rather than by DA's ceiling.
- **The §1 collision is read from code on both sides, not observed on a cluster.**
- **No integration or e2e tests** were possible here: envtest binaries are unreachable in this
  environment.
