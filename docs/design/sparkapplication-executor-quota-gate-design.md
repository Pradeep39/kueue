# Design: Quota-gated admission for SparkApplication executor Pods

Status: implemented, cluster-tested (companion to PR
[#18](https://github.com/Pradeep39/kueue/pull/18) against
`Pradeep39/kueue`, branch `sparkapplication-executor-quota-gate`, based on PR
[#16](https://github.com/Pradeep39/kueue/pull/16)'s branch
`pr-sparkapplication-elastic-scaling`)
Author: Pradeep
Date: 2026-08-06

## 1. Goal

Stop Dynamic-Allocation-created executor Pods from running before Kueue has confirmed
there is cohort/`ClusterQueue` quota for them, using the same generic gate/ungate
machinery Kueue already applies to `Job` and `RayCluster` — with no new capacity-check
code path and no changes to the scheduler, cache, or `ElasticJobUngater`.

## 2. Background

### 2.1 What PR #16 does not do

PR #16 ([`sparkapplication-elastic-scaling-design.md`](./sparkapplication-elastic-scaling-design.md))
makes Kueue's SparkApplication integration *observe* Dynamic Allocation (DA) accurately:
`liveExecutorCount()` lists live executor Pods and reports that as the executor
`PodSet.Count`, so the Workload's requested resources track DA's real, current state
instead of the stale `spec.executor.instances` field.

That is purely descriptive. Nothing in #16 stops a DA-created executor Pod from actually
running. DA creates executor Pods directly against the Kubernetes API, entirely outside
any Kueue-mediated admission step — so by the time Kueue's reconciler even learns a new
executor exists (via the debounced Pod watch in
`sparkapplication_executor_pod_handler.go`), the Pod is already `Pending` or `Running`
and already consuming real node resources. If the cohort has no spare quota for a
scale-up, the new workload slice simply sits un-admitted (`workload.HasQuotaReservation`
false) while the already-running Pod keeps consuming resources ungoverned. There is no
mechanism in #16 that would evict it or otherwise reconcile the discrepancy.

### 2.2 How Job and RayCluster avoid this problem

Kueue's `ElasticJobsViaWorkloadSlices` feature already solves this for `Job` and
`RayCluster`, using a scheduling-gate pattern that predates this work entirely:

- Each integration's mutating webhook injects `kueue.ElasticJobSchedulingGate` into
  its own Pod template(s) at CR-creation time
  (`pkg/controller/jobs/job/job_webhook.go:65`,
  `pkg/controller/jobs/raycluster/raycluster_webhook.go:101-109`). A gated Pod cannot be
  scheduled by the Kubernetes scheduler until the gate is removed — it can exist and sit
  `Pending`, but it cannot run.
- A single generic controller, `pkg/controller/elasticjobs/elastic_job_ungater.go`
  (`ElasticJobUngater`), watches gated Pods and their owning Workloads. For each active
  (admitted) workload slice, it computes `room := grantedCount - alreadyUngatedCount` per
  `PodSet` and removes the gate from up to `room` gated Pods, oldest-name-first for
  determinism.
- Critically, `ElasticJobUngater` never queries `pkg/cache/scheduler` (the
  `ClusterQueue`/`Cohort` cache) directly. It trusts the Workload's own
  `QuotaReserved`/`Admitted` status conditions and its `Spec.PodSets[].Count` — both of
  which the *main scheduler* already set after checking real cohort/ClusterQueue capacity
  during normal Workload admission. The ungater is a thin "translate an already-vetted
  grant into ungated Pods" step, not a second admission-decision engine.

This is why extending the same mechanism to SparkApplication was the only design
considered: it reuses machinery that has already been hardened and reviewed for two
integrations, rather than inventing a parallel capacity-check subsystem specific to
Spark.

### 2.3 The open question: does the gate reach Dynamic-Allocation Pods?

`Job` and `RayCluster` gate pods that Kueue's own webhook constructs indirectly — a
`Job`'s Pods, or a `RayCluster`'s worker Pods spawned by KubeRay reconciling
`spec.workerGroupSpecs[i].replicas` — but in both cases, the *same* controller that reads
the gated template is the one instantiating Pods on every scale event. SparkApplication
is different in a way that made this non-obvious: DA-created executor Pods are built by
the **Spark driver process itself**, not by the Spark Operator's controller reconciling
the CR, and DA never re-reads the CR after the application starts. It was not obvious
that gating `spec.executor.template` at CR-creation time would have any effect on Pods
created much later by an entirely different process (the running driver) that never
looks at the CR again.

This was traced end-to-end before implementing, across three codebases:

1. **Spark Operator submission** (`internal/controller/sparkapplication/submission.go`):
   at `spark-submit` time, `spec.executor.template` is serialized to a pod-template
   **file**, mounted into the driver Pod, and passed as
   `spark.kubernetes.executor.podTemplateFile=<path>`.
2. **Apache Spark's driver** (`KubernetesExecutorBuilder`/`ExecutorPodsAllocator`, in
   `resource-managers/kubernetes/core`) loads that file **once, at driver startup**, via
   Fabric8's `kubernetesClient.pods().load(templateFile).item()`, and reuses the *same*
   loaded `Pod` object as the base for **every** executor it ever builds for the
   application's lifetime — the initial batch and every later DA-created executor go
   through the identical `buildFromFeatures` call.
3. Both `spec.schedulingGates` (PodSpec-level) and `metadata.labels`/`.annotations`
   (ObjectMeta-level) survive this process untouched: Spark's feature-step builders
   (`BasicExecutorFeatureStep.scala`) use `.editOrNewMetadata().addToLabels(...)`
   /`.addToAnnotations(...)` — Fabric8 fluent methods that **merge**, not replace — and no
   feature step anywhere in the executor build path ever reconstructs `PodSpec` from an
   allow-list that would drop an unrecognized field like `schedulingGates`.

Conclusion: a gate baked into `spec.executor.template` at CR-creation time reaches every
DA-created executor Pod for the whole lifetime of the application, with no time-window or
generation cutoff. No new Kueue-owned Pod-admission webhook is needed — the existing
template-mutation pattern transfers directly.

### 2.4 A second risk, also ruled out: does this resurrect the reverted spec-write bug?

PR #16's design doc (§2.2 there) documents why an earlier design that patched
`spec.executor.instances` on every DA scale event was fully reverted: the Spark
Operator's `event_filter.go` runs an unconditional `reflect.DeepEqual` on `.Spec` on
every `Update` and kills + resubmits the running application on *any* spec change. Since
this design also mutates `spec.executor.template` — a `.Spec` field — it had to be
verified that this write is safe.

It is, because it happens exactly once, and only before the application starts running:

- The webhook's `Default()` only runs at admission time for a `create` verb
  (`+kubebuilder:webhook:...verbs=create...` on `SparkApplicationWebhook`), i.e. before
  the `SparkApplication` object exists at all — there is no `Update` event for
  `event_filter.go` to compare against yet.
- `RunWithPodSetsInfo` (`sparkapplication_controller.go:223-289`) also touches
  `sparkPodSpec.Template.Spec.SchedulingGates`, but only merges into it
  (`podset.PodSetInfo.Merge`, `pkg/podset/podset.go:141-146`, is append/dedup, never a
  replace) — so the webhook's gate is preserved, not overwritten, when this runs.
- `RunWithPodSetsInfo` itself only fires once, at the job's `Suspend: true → false`
  transition (`pkg/controller/jobframework/reconciler.go:614-630`, gated on
  `job.IsSuspended()`). A later elastic scale-up, which creates a new workload slice for
  an already-running (already-unsuspended) SparkApplication, never re-enters that branch
  — `EnsureWorkloadSlices` operates purely on `Workload` objects and never touches the
  job's `.Spec`. So there is exactly one `.Spec` write to the executor template, before
  the application is running, and none afterward.

## 3. Implementation

### 3.1 Gate the executor template at CR-creation time

`pkg/controller/jobs/sparkapplication/sparkapplication_webhook.go`, `Default()`:

```go
if isAnElasticJob(obj) {
    if job.Spec.Executor.Template == nil {
        job.Spec.Executor.Template = emptyExecutorPodTemplateSpec.DeepCopy()
    }
    utilpod.GateTemplate(job.Spec.Executor.Template, kueue.ElasticJobSchedulingGate)
}
```

This is a direct mirror of `raycluster_webhook.go:101-109`'s pattern, scoped only to the
**executor** template. The **driver** template is deliberately left ungated: DA only
ever creates/destroys executors, and the driver's own single-admission lifecycle already
goes through the normal, unchanged `RunWithPodSetsInfo` path.

`utilpod.GateTemplate`/`gateSpec` (`pkg/util/pod/pod.go:71-84`) is idempotent — it checks
for the gate's presence before appending, so re-running `Default()` (e.g. on retry) never
double-adds it.

### 3.2 Validation

`validateCreate`/`ValidateUpdate` now reject an elastic SparkApplication whose executor
template lacks the gate, mirroring RayCluster's `validateElasticJob`:

```go
func validateElasticJob(job *sparkv1beta2.SparkApplication) field.ErrorList {
    ...
    if !slices.Contains(executorSchedulingGates, workloadSliceSchedulingGate) {
        allErrors = append(allErrors, field.Invalid(
            executorTemplateSpecPath.Child("schedulingGates"),
            executorSchedulingGates,
            "an elastic job must have the ElasticJobSchedulingGate on its executor pod template",
        ))
    }
    return allErrors
}
```

This closes off the CRD silently regressing out of the gated state — e.g. a future code
path that replaces `spec.executor.template` wholesale instead of merging into it would
now fail validation rather than fail silently at runtime.

### 3.3 No changes to the ungater

`pkg/controller/elasticjobs/elastic_job_ungater.go` required zero modification. It
already:

- Matches Pods to a job/slice-chain via `constants.PodSetLabel` and
  `kueue.WorkloadSliceNameAnnotation` — both of which SparkApplication's executor Pods
  already carry, because `RunWithPodSetsInfo`'s pre-existing merge logic writes
  `sparkPodSpec.Annotations`/`.Labels` from the same `PodSetInfo` the generic reconciler
  populates with those two keys (`reconciler.go:1720-1723`). Confirmed these labels reach
  DA-created executor Pods too: the Spark Operator's `executorConfOption`
  (`submission.go`) translates `spec.executor.Labels`/`.Annotations` into
  `spark.kubernetes.executor.label.*`/`annotation.*` Spark confs at submission time,
  which Spark's driver applies via the same merge-not-replace mechanism described in
  §2.3, to every executor Pod it creates.
- Computes ungate-eligibility purely from the active slice's `Spec.PodSets[].Count`
  (the grant already computed by the main scheduler against cohort/ClusterQueue
  capacity) versus the count of already-ungated Pods for that PodSet — no direct
  scheduler-cache query.

So SparkApplication executor Pods became eligible for existing, previously-validated
machinery simply by carrying the right gate and the right identifying labels — nothing
about the ungater's logic needed to know SparkApplication exists.

### 3.4 Interaction with live executor counting (PR #16)

`isVerifiedLiveExecutor` (`sparkapplication_podset.go`) counts a Pod as live starting
from `Pending`/`ContainerCreating`, deliberately, to reserve quota the instant DA creates
the Pod object rather than once it starts running. A gated Pod sits in `Pending` (a
scheduling gate blocks scheduling, not object creation), so it is already counted by
`liveExecutorCount()` before it is ever ungated. This is intentional and unchanged by
this design — see the companion note in §4 below on why this is a different piece of
machinery from gating and remains necessary.

## 4. Why live-Pod counting (#16) and quota-gating (#18) are not redundant

It might look like gating supersedes the need for `liveExecutorCount()`/
`computeLiveExecutorCount()`, since both "watch executor Pods." They answer two different
questions and neither can replace the other:

- **`liveExecutorCount()` → `PodSets().Count`** answers *"how much is this job currently
  asking for?"* — it sizes the Workload's requested resources so the scheduler has an
  accurate number to admit against in the first place. Without it, Kueue would size the
  Workload from the stale `spec.executor.instances` field, exactly the bug PR #16's
  design (§2.2/§2.3 there) exists to fix.
- **The gate + `ElasticJobUngater`** answer *"is there capacity to let a Pod that already
  exists actually run?"* — it is the admission-side check, acting on Pods that already
  exist and are already counted.

RayCluster does not need `liveExecutorCount()`-equivalent logic at all, for a reason
specific to its own operator model, not because gating alone would have been sufficient:
Ray's in-tree autoscaler scales by patching `spec.workerGroupSpecs[i].replicas` on the
RayCluster CR itself, and KubeRay's own operator then reconciles that spec into Pods
(`RayCluster.PodSets()` reads `wgs.Replicas` directly —
`pkg/controller/jobs/raycluster/common.go:64-73,110-116` — no `client.List` of Pods
anywhere in that path). Ray's autoscaler is a client of the CR's spec field; Spark's DA
is not — it bypasses the CR entirely. That is a difference in where each system's scaling
state lives, not a difference in whether gating is needed. Both integrations use the
identical gate/ungate mechanism for admission; only SparkApplication additionally needs
live-Pod counting for *sizing*, because only SparkApplication's spec field goes stale.

## 5. Alternatives considered

- **A new SparkApplication-specific Pod-admission webhook**, intercepting raw executor
  Pod `CREATE` requests directly (the shape of Kueue's standalone `pod` integration,
  `pkg/controller/jobs/pod/pod_webhook.go`) instead of gating via the CR's template:
  rejected once §2.3 confirmed the CR-template approach already reaches every
  DA-created Pod. A new webhook would duplicate the ungater-matching machinery
  (labels/annotations) for no additional coverage, and would add a second admission
  webhook + failure-mode surface (`failurePolicy`, ordering relative to the Spark
  Operator's own pod-mutating webhook) that the CR-template approach avoids entirely.
- **A new SparkApplication-specific ungate controller performing a live
  `pkg/cache/scheduler` cohort/ClusterQueue query at ungate time**, instead of trusting
  the Workload's already-computed admission status: rejected as unnecessary duplicate
  admission logic. No existing Kueue integration (including RayCluster) does this — every
  elastic-job integration relies on the main scheduler as the single source of truth for
  capacity decisions, and `ElasticJobUngater` deliberately only translates an
  already-vetted grant into ungated Pods. Building a second, parallel capacity-check path
  would risk disagreeing with the scheduler's own decision and would be significantly
  harder to review for an eventual upstream contribution.
- **Gating the driver template as well as the executor template**: rejected — the driver
  Pod's lifecycle is already a single, one-time admission (there is exactly one driver
  Pod per application, created once at submission), fully covered by the existing
  `RunWithPodSetsInfo` path. There is no scale-up scenario for the driver PodSet that
  gating could help with.

## 6. Testing

- `pkg/controller/jobs/sparkapplication/sparkapplication_webhook_test.go`: new
  `TestDefault` cases confirming an elastic SparkApplication's executor template is
  gated and a non-elastic one is not; new `TestValidateCreate` cases for gate-present
  (accepted) and gate-missing (rejected) on an elastic job.
- `go build ./...`, `go vet ./pkg/controller/jobs/sparkapplication/...
  ./pkg/controller/elasticjobs/...`, `go test` on those same packages plus
  `pkg/controller/jobframework/...`, and the full `go test ./pkg/controller/jobs/...`
  regression sweep — all green.
- `gofmt -l` clean.
- Manually tested against a real cluster: an elastic, Dynamic-Allocation-enabled
  SparkApplication submitted into a `ClusterQueue` with constrained quota. Confirmed
  DA-created executor Pods came up gated (`Pending`, `spec.schedulingGates` containing
  `kueue.x-k8s.io/elastic-job` — i.e., `kueue.ElasticJobSchedulingGate`) and were
  ungated by the existing `ElasticJobUngater` once quota freed up, without any
  SparkApplication-specific code in the ungate path. This closes the gap identified in
  §2.1 and confirmed in the earlier investigation this design is based on.

No real envtest integration test exists yet for the gate/ungate path against a live
`ClusterQueue`/`Cohort` and scheduler admission cycle — sandbox networking blocked
envtest during development (same constraint noted in PR #16). The manual cluster test in
this section is the only end-to-end verification to date; an envtest-based integration
test remains open follow-up work.
