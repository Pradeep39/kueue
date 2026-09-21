# Design: Kueue integration for the Apache Spark Kubernetes Operator

Status: **merged** as PR #28 (package `pkg/controller/jobs/apachesparkapplication`), with the
resource-charging rules subsequently **reversed by PR #33**. This document describes the code
as it stands on `main`; §8 records what #28 originally decided and why it changed, because the
PR body still describes the superseded rule.

Applies to the Apache operator's `spark.apache.org/v1` `SparkApplication`. The pre-existing
Kubeflow integration (`pkg/controller/jobs/sparkapplication`,
`sparkoperator.k8s.io/v1beta2`) is a separate package and is not affected by anything here.

---

## 1. Why a second Spark integration exists

Two unrelated operators both call their CRD `SparkApplication`. They share no API group, no
schema, and no state machine:

| | Kubeflow | Apache |
|---|---|---|
| CRD | `sparkoperator.k8s.io/v1beta2` | `spark.apache.org/v1` |
| operator language | Go | Java |
| executor count surface | `spec.executor.instances` (structured) | `spark.executor.instances` in `sparkConf` |
| suspend field | `spec.suspend` (native) | **none upstream** — see §2 |
| lifecycle | phase string | explicit state machine with transition history |

`jobframework.GenericJob` is per-GVK, so nothing about the Kubeflow integration is reusable
beyond the general elastic-scaling machinery in `pkg/workloadslicing`. Hence a new package
rather than a variant of the old one.

Registration surface: framework name `spark.apache.org/sparkapplication`, behind the alpha
feature gate `ApacheSparkApplicationIntegration`, **default `false`** since 0.17
(`pkg/features/kube_features.go`). Both the gate *and* the framework name in
`.integrations.frameworks` must be on; with either missing the integration is inert.

## 2. The hard prerequisite, and how upstream overtook it

`jobframework.GenericJob` requires a suspend-like spec field. When #28 was written, upstream
Apache had none, so the integration could not function against a stock operator build. It
depends on the paired fork change — `Pradeep39/spark-kubernetes-operator`, branch
`spark-application-suspend` — which adds `.spec.suspend`, a `Suspended` state, and
`StoppedByScheduler` for preemption. Neither half does anything observable alone.

**This changed on 2026-09-14, after #28 merged.** Upstream added the field and more:

- **SPARK-59475** put `suspend` on `BaseSpec`, the parent of both `ApplicationSpec` and
  `ClusterSpec`, as `protected boolean suspend = false` with `@Default("false")`. Two
  differences from the fork's version, which is `protected Boolean suspend` on
  `ApplicationSpec` only: upstream's also makes SparkCluster suspendable, and upstream's CRD
  carries `default: false`, so the API server always materialises the field and an explicit
  `suspend: false` is indistinguishable from an omitted one. Harmless for Kueue, which always
  writes an explicit value, but the distinction is gone and cannot be recovered without
  another API change.
- **SPARK-59490 / SPARK-59503** added `kueue/KueueWorkloadFactory.java`: the *operator* builds
  the Kueue `v1beta2` Workload itself, driver + executor PodSets, `active = !suspend`. It
  **throws `UnsupportedOperationException` when `spark.dynamicAllocation.enabled=true`.**

So upstream now covers the static case, and refuses exactly the case this fork exists to
handle. None of it is in release `1.0.0`. Two consequences recorded for whoever picks this up:

1. **Dynamic Allocation is the differentiator.** The static gating surface in this package
   overlaps upstream; the elastic path (§6) does not.
2. **Never enable both.** An operator built from upstream `main` *plus* this integration
   produces two Workloads for one application.

Upstream did implement the lifecycle, not just the field (`SuspendUtils.java`, plus handling
in `AppInitStep`, `ClusterInitStep`, `EventUtils`, `ApplicationStatus`, and a
`SparkOperatorConf` knob). It has **no** `AppSuspendStep` and no `StoppedByScheduler`, so the
fork's preemption-specific state has no upstream counterpart by name. Whether `SuspendUtils`
covers preempting an already-running driver is **unverified**.

## 3. The Go API types are a hand-written partial projection

The operator is Java and publishes no Go module, so `api/v1/types.go` is a hand-written
projection of only the fields this integration reads. That is safe **only** because
jobframework *patches* the job rather than calling `client.Update`. A `client.Update` would
serialise the projection and silently drop every field it omits.

**Never introduce a `client.Update` on a `spark.apache.org` SparkApplication.**

## 4. What Kueue charges: Spark's arithmetic, not the pod template

This is the rule #33 reversed; §8 has the history.

`buildPodTemplateSpec` starts from the submitter's
`spec.{driverSpec,executorSpec}.podTemplateSpec` when present, then **overwrites the Spark
container's cpu and memory** with values derived from `sparkConf`.

The reason is what happens to the real pod. The operator writes the template to a file and
passes it as `spark.kubernetes.{driver,executor}.podTemplateFile`; Spark's
`Basic{Driver,Executor}FeatureStep` then replaces that container's cpu and memory with its own
computed values before the pod is created. **A template resource request is therefore never
what the pod asks for**, so charging it verbatim would charge a number the kubelet never sees.
Upstream's own `KueueWorkloadFactory.decorateContainerResources` overwrites it too, with the
comment *"Like Spark, overwrite the requests of the pod template."*

`totalMemoryBytes` reproduces Spark's formula:

```
base                         spark.{role}.memory, default 1g
overhead                     spark.{role}.memoryOverhead if set, else
                             max(trunc(factor × base), minMemoryOverhead)
factor                       spark.{role}.memoryOverheadFactor, else
                             spark.kubernetes.memoryOverheadFactor, else
                             0.1 JVM / 0.4 non-JVM
minMemoryOverhead            spark.{role}.minMemoryOverhead, else 384MiB
+ spark.executor.pyspark.memory        executors only
+ spark.memory.offHeap.size            only when spark.memory.offHeap.enabled
```

Three details that are easy to get wrong and are deliberate here:

- **Spark truncates, it does not round**: `(factor * memoryMiB).toInt`.
- **The limit equals the request.** Spark intends heap+overhead to be the whole allocation.
- **`spark.{role}.minMemoryOverhead`** is honoured. The Kubeflow package hardcodes 384MiB.

CPU: `cpuRequest` reads `spark.kubernetes.{role}.request.cores` and falls back to
`spark.{role}.cores`. There is deliberately **no** fallback to `limit.cores` — Spark has none
either.

## 5. Count precedence: `sparkConf` first on this CRD

The inverse of the Kubeflow package, and the inversion is load-bearing rather than an
inconsistency.

`staticExecutorCount` prefers `spark.executor.instances` over
`spec.applicationTolerations.instanceConfig.initExecutors`, falling back to Spark's default of
2. `dynamicAllocationCount` likewise prefers `spark.dynamicAllocation.{initialExecutors,
minExecutors, maxExecutors}` over the matching `instanceConfig` field.

**Why the structured field loses here:** on this CRD it never reaches Spark. The operator does
not create executors — the driver does, from `spark.executor.instances` — and `instanceConfig`
is consumed only by the operator's own readiness thresholds (`AppRunningStep`), never
translated into a `--conf` at submission. Preferring `instanceConfig` would reserve quota for
an application declaring `initExecutors: 2` while 15 pods from `spark.executor.instances: 15`
actually run. Upstream's `KueueWorkloadFactory` reads `spark.executor.instances` alone, for the
same reason.

The Kubeflow CRD is the opposite case — there the operator maps the structured field onto a
`--conf` at submission, so it *is* the surface Spark acts on. The rule settled with the
maintainer is therefore not "structured first" or "conf first" but **prefer whichever surface
Spark actually acts on**.

`instanceConfig` fields are plain `int32`, so zero is treated as unset. This is the same class
of problem as `dynamicAllocationEnabled` on the Kubeflow CRD (see
[`sparkapplication-sparkconf-executor-instances-design.md`](./sparkapplication-sparkconf-executor-instances-design.md)
§5), and here it is harmless because a zero bound would be meaningless.

**`dynamicAllocationEnabled` reads `sparkConf` only** — no OR, because this CRD has no
structured enablement field at all.

**`declaredInitialExecutors` takes a maximum, not a precedence.** It is the largest of
`minExecutors`, `initialExecutors` and the resolved instances count, matching
`Utils.getDynamicAllocationInitialExecutors`. Each individual term still resolves by
precedence; only the three-way combination is a max. Precedence decides where one property's
value comes from; the maximum decides which property governs the count. Spark starts the
largest of the three regardless of which the author thought authoritative, so precedence
across them would under-reserve.

## 6. Elastic scaling

Mirrors the Kubeflow integration, and is the part with no upstream equivalent:

- Executor counts **derived from live Pods**, never written back to the CR
  (`liveExecutorCount` / `computeLiveExecutorCount`).
- A **label-keyed executor Pod watch** with a 5s debounce and 30s max wait.
- The **executor scheduling gate** (`kueue.ElasticJobSchedulingGate`) baked into the executor
  template at CR-create time by the webhook's `Default`, with `validateElasticJob` requiring
  it on create.
- **`clampToDynamicAllocationBounds`** (the #26 fix, applied here too): the derived count is
  clamped to DA's own `minExecutors`/`maxExecutors`. The lower bound is what stops a reconcile
  that lands mid-startup — observing a partial executor set — from patching a freshly granted
  PodSet down and dismantling the gang. As in the Kubeflow package, the upper bound is DA's
  ceiling, **not** grantable capacity, so it narrows but does not close the gated-Pod feedback
  loop.
- Workload slice names use a **per-job sequence number**, not `Generation`, because DA scaling
  never bumps `Generation`.

The quota-evasion hole fixed for Kubeflow in #27 does not exist here: counts always read
`spark.executor.instances`, error on a malformed value, and default to Spark's 2.

## 7. Lifecycle mapping

The Apache operator exposes a real state machine plus a `StateTransitionHistory`, which makes
two things possible that the Kubeflow phase string does not.

**`StoppedByScheduler` is deliberately not terminal.** The operator releases the driver on a
scheduler-requested stop but reopens the application in `Suspended`. Treating it as terminal
would end the Workload for what is actually a preemption. Relatedly, preemption is not counted
as a failure and never consumes restart budget — being preempted is a scheduling decision, not
an application fault. (This is one of two deliberate divergences from Apple's internal
implementation of the same field; the other is the absence of a queue timeout, since Kueue owns
queue-time policy. Do not port that internal code into either public repo.)

**`terminalOutcome` walks the transition history.** `ResourceReleased` and
`TerminatedWithoutReleaseResources` are reachable from both success and failure, so they carry
no outcome of their own. When the application sits in one, the outcome is the highest-numbered
history entry that is not itself a release state. An application whose history is unavailable
is reported unsuccessful rather than guessed at.

`PodsReady` keys off `RunningHealthy` / `RunningWithPartialCapacity`, since the operator only
advances to those once at least `minExecutors` are ready — the state machine is a sufficient
signal, so no Pod listing is needed.

## 8. Superseded: what #28 originally decided

#28's PR body describes **"pod template first, `sparkConf` as the fallback"** — a template
value used verbatim for memory, CPU and counts, with Spark's arithmetic applied only when the
template declared nothing. The stated rationale was that splitting by source keeps each rule in
the regime where it is sound: verbatim where the submitter has been explicit, inferred only
where Kueue would otherwise have nothing.

**That premise is wrong, and #33 reversed it.** Both operators pass the template to Spark as
`spark.kubernetes.{role}.podTemplateFile`, and `Basic{Driver,Executor}FeatureStep` overwrites
the container's cpu and memory regardless. A template request is never what the pod asks for,
so honouring it verbatim could only ever under-charge. #33 also inverted the Apache count
precedence to conf-key-first (§5) and, on the Kubeflow side, removed the equivalent
pod-template-first memory rule.

Measured on `sandbox-picluster`: with `spark.executor.memory: 512m`, real executor pods show
`req=896Mi lim=896Mi`. The 512-vs-896 question is closed — 896Mi is what the pod reserves.
See [`spark-podset-align-with-spark-design.md`](./spark-podset-align-with-spark-design.md) §1.

A middle path was considered and **not** implemented: pass the template through *and* have the
webhook reject a declared request below `memory + max(0.1 × memory, 384Mi)`.

## 9. Known gaps

- **The webhook does not reject a malformed `spark.executor.instances` at admission**, the way
  the Kubeflow one does after #27. `staticExecutorCount()` still returns a proper error, so the
  failure surfaces during reconcile rather than at create time. Less immediate, not a
  correctness gap.
- **The gated-Pod feedback loop is narrowed, not closed** (§6).
- **No integration tests.** envtest binaries are unreachable in this environment
  (`storage.googleapis.com` blocked), so coverage is unit-level only: pod-template
  construction, the memory and CPU arithmetic, static and DA executor counts, the elastic
  path, webhook validation, and setup/registration.
- **Unverified upstream assumption** (§2): whether Kueue's workload controller cleanly admits
  an operator-created Workload owned by no Kueue integration.

## 10. Deployment traps

All hit on `sandbox-picluster`, all still true:

- **Helm never upgrades CRDs in `crds/`.** `helm upgrade` silently leaves the old CRD, so
  `spec.suspend` gets pruned by the API server and suspend appears to do nothing — the symptom
  is `unknown field "spec.suspend"` in Kueue's log. Fix:
  `kubectl apply --server-side --force-conflicts -f` the chart's CRD.
- **`spark.kubernetes.operator.watchedNamespaces` needs an operator restart**, despite
  `enableDynamicOverride(true)`. Default is `default` only, so an application in any other
  namespace is never reconciled and no driver is created. The chart's comment names a
  non-existent key `spark.operator.watched.namespaces`.
- **`helm upgrade` regenerates the workload ClusterRole** and drops a hand-patched
  `deletecollection` verb, bringing back the driver-shutdown 403 on services. Durable fix is
  `spark-operator.workloadRbacRules` in `templates/workload-rbac.yaml` on the fork.
- `examples/pi-suspended.yaml` on the operator fork has **no** `queue-name` label on purpose —
  it is an operator-only test that stays suspended until the field is patched by hand. It is
  not a Kueue example.
