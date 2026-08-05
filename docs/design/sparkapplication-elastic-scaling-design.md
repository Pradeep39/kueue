# Design: Elastic scaling support for SparkApplication via ElasticJobsViaWorkloadSlices

Status: proposed (companion to PRs [#15](https://github.com/Pradeep39/kueue/pull/15) and [#16](https://github.com/Pradeep39/kueue/pull/16) against `Pradeep39/kueue`)
Author: Pradeep
Date: 2026-08-05

## 1. Goal

Let a running `SparkApplication` job scale its Kueue-reserved quota up and down in step
with Spark's own **Dynamic Allocation (DA)** feature, using Kueue's existing
`ElasticJobsViaWorkloadSlices` mechanism — without disrupting the running Spark job and
without requiring changes to the Spark Operator's controller behavior.

## 2. Background

### 2.1 Why SparkApplication doesn't fit the existing elastic-job model out of the box

`ElasticJobsViaWorkloadSlices` assumes a job's desired PodSet counts are visible on the
job object itself (e.g. `batch/v1.Job.Spec.Parallelism`), and that changing that field is
how a user (or autoscaler) expresses "I want N pods now." Kueue's generic
`jobframework.Reconciler` diffs the job's current `PodSets()` against the active workload
slice's admitted counts, and creates/updates a workload slice to match.

Spark's Dynamic Allocation breaks that assumption: once a `SparkApplication` is running,
DA's own executor manager (inside the Spark driver) creates and deletes **executor Pods
directly against the Kubernetes API**, entirely bypassing the `SparkApplication` CR.
`spec.executor.instances` is only ever read once at submission; it is never updated by
DA, and is not the live source of truth for how many executors actually exist at any
point in time.

### 2.2 The rejected design: reflecting live count back onto `.Spec`

The first attempt (6 PRs, since fully reverted — tree restored to the pre-work commit
`4ade8353f`) added a standalone `ExecutorInstancesReconciler` that watched executor Pods
and patched `spec.executor.instances` on the `SparkApplication` to track the live count,
so that the generic `PodSets()` → spec diff → workload slice pipeline would "just work"
unmodified.

This is fundamentally incompatible with the upstream Spark Operator. Its
`event_filter.go` runs an unconditional `reflect.DeepEqual` between the old and new
`.Spec` on every `Update` event, and treats **any** difference as a request to
resubmit the application: it kills the running driver Pod and restarts the whole
application under a new `SubmissionID`. There is no field-level allowlist and no
opt-out. Every patch to `spec.executor.instances` — no matter how well debounced —
was therefore observed as a spec change and killed the very job it was trying to keep
running. This was confirmed by direct testing and is why that line of work was reverted.

### 2.3 The fix: derive, don't write

The controlling insight: `ElasticJobsViaWorkloadSlices`'s actual contract with a job
integration is `PodSets()` returning the *desired* counts — it does not require those
counts to originate from `.Spec`. `PodSets()` already receives a `client.Client`
(originally added for other integrations that need cluster reads), so it can compute the
executor count by listing live executor Pods directly and never touch `.Spec` while the
application is running.

```
        Dynamic Allocation                    Kueue job integration
        ------------------                    ----------------------
Spark      creates/deletes    ┌──────────┐
driver ─── executor Pods ────>│   Pods    │<─── liveExecutorCount() lists
                               └──────────┘         (sparkapplication_podset.go)
                                                          │
                                                          ▼
                                                    PodSets() returns
                                                    live count as
                                                    executor PodSet.Count
                                                          │
                                                          ▼
                                          workloadslicing.EnsureWorkloadSlices
                                          (existing, unmodified mechanism):
                                          creates a new slice on scale-up of an
                                          admitted workload, or patches the
                                          existing slice's PodSet count in place
                                          on scale-down / pre-admission.

SparkApplication.Spec is never written to while the app is Running.
```

Nothing about `EnsureWorkloadSlices` needed to change to make this work — it already
takes "desired `PodSet` counts" as an opaque input and doesn't care where they came from.

## 3. Implementation

### 3.1 Live executor count derivation (`sparkapplication_podset.go`)

- `dynamicAllocationEnabled()` / `dynamicAllocationExecutorCount(field string)` detect DA
  from either the structured `spec.dynamicAllocation` block or the legacy
  `spark.dynamicAllocation.*` `sparkConf` keys.
- `isVerifiedLiveExecutor(pod *corev1.Pod) bool` counts a Pod as live unless it has
  reached a terminal `Succeeded`/`Failed` phase. A Pod with a `DeletionTimestamp` set
  still counts — it continues to hold node resources and run workload until it actually
  reaches a terminal phase, so excluding it early would undercount capacity still in use.
- `computeLiveExecutorCount(ctx, c)` lists executor Pods by the same label selector
  `PodSets()` already uses (`sparkoperator.k8s.io/app-name` + `sparkoperator.k8s.io/role
  =executor`), counts the live ones, and — if no executor Pods exist yet (the window
  between submission and DA's first scale-out) — falls back through
  `initialExecutors` → `minExecutors` → `0`. If DA is not enabled at all, it returns the
  static `spec.executor.instances` unchanged, preserving today's non-elastic behavior
  exactly.
- `liveExecutorCount(ctx, c)` wraps the above with a cache (§3.2).

### 3.2 Reconcile-scoped caching

`PodSets()` is called multiple times per `Reconcile()` (from `ensureOneWorkload`,
`EquivalentToWorkload`, and `ConstructWorkload` in the generic
`jobframework.Reconciler`), all backed by the same shared informer cache — which can
observe a new Pod event *between* those calls if DA is actively scaling. Without
caching, two calls could disagree within the same reconcile pass, causing spurious
"not equivalent" churn. The live count is therefore computed once and cached on the
reconcile-scoped `*SparkApplication` wrapper (`NewJob()` allocates a fresh instance per
reconcile), so every caller within one reconcile sees the same value by construction.

### 3.3 Debounced Pod watch

DA scale-out/scale-in produces a burst of individual Pod create/delete events, not one
event per logical scaling decision. Without coalescing, each Pod event would trigger its
own reconcile and its own workload-slice update. `sparkapplication_executor_pod_handler.go`
adds a `predicate.Predicate` that filters to executor Pods belonging to a tracked
`SparkApplication`, and a handler that debounces: it holds a per-application timer,
resetting it on each new event (`executorPodDebounce = 5s`), and only enqueues a
reconcile once events go quiet. A `executorPodMaxWait = 30s` ceiling forces a flush
regardless, so an application under continuous, never-quite-quiet churn (e.g. frequent
readiness flips) is still re-derived periodically instead of starving indefinitely.

### 3.4 Workload slice naming under oscillating counts

`GetWorkloadNameExtraPart` previously used the job's `Generation`, which — being a
`.Spec`-change counter — never increments under DA scaling (§2.3: `.Spec` is
intentionally never written). Left unchanged, a workload slice name could collide with a
previously-`Finished` slice's name once DA's live count happened to repeat a value seen
earlier in the application's lifetime, since `Finished` workload names remain in etcd
indefinitely. The fix folds in a monotonically increasing **workload sequence number** —
a count of every Workload object ever owned by this job (via the existing
`OwnerReferenceIndexFieldMatcher`, filtered by GVK+name, counting Finished and
not-Finished alike) — guaranteeing a fresh, collision-free name on every new slice for
the lifetime of the application.

### 3.5 Removed: spec write on stop/evict

`RestorePodSetsInfo` previously wrote the workload's admitted PodSet count back to
`spec.executor.instances` when a job was stopped or evicted. This is exactly the class
of spec mutation this design avoids (§2.2), and is additionally unsound on its own
terms: a DA-scaled-to-zero PodSet count (`0`, which `PodSets.Count` explicitly permits,
`Minimum=0`) is not a legal value for `spec.executor.instances`, whose CRD schema
enforces `Minimum=1` — so the old code could have produced a validation-rejected patch.
It is removed outright rather than special-cased.

### 3.6 Registration

`SparkApplication`'s GVK is added to `supportedElasticJobGVKs` in
`pkg/controller/jobframework/validation.go`, and to the list of supported integrations
in `site/content/en/docs/concepts/elastic_workload.md`, so the
`kueue.x-k8s.io/elastic-job: "true"` annotation is accepted for it.

## 4. Generic hardening to `ElasticJobsViaWorkloadSlices` ([#15](https://github.com/Pradeep39/kueue/pull/15))

Two fixes surfaced while soak-testing the above against a live cluster running real DA
scaling. Both are general to any elastic-job integration, not specific to
SparkApplication, and are proposed as a separate PR for that reason.

1. **Admission-race retry correctness.** `updatePodSetCountsWithRetry` previously could
   retry an in-place `Update()` of a workload's PodSet counts after Kueue's own
   scheduler had concurrently admitted that same workload — reapplying a now-stale
   target count and permanently desyncing `spec.PodSets` from
   `status.admission.podSetAssignments` (from which `ClusterQueue` usage accounting is
   computed, not from live spec). Every retry attempt, including the first, now
   re-fetches the workload and re-validates eligibility for an in-place patch before
   proceeding, aborting with a new `errWorkloadAdmittedConcurrently` sentinel so the
   caller falls through to creating a new slice instead of corrupting the existing one.
2. **Cache-lag tolerance in `prepareWorkloadSlice`.** `EnsureWorkloadSlices` and
   `prepareWorkloadSlice` run moments apart in the same reconcile, and can legitimately
   both observe "one admitted slice + one pending replacement" — e.g. when a third
   scale-up event arrives before a second one's replacement has fully settled, or when
   `EnsureWorkloadSlices`'s own `Patch`-based `Finish()` of a superseded slice hasn't yet
   propagated through `prepareWorkloadSlice`'s cache-backed `List`. `prepareWorkloadSlice`
   previously treated any not-finished count above 1 as a fatal reconcile error. It now
   reuses the newly-exported `workloadslicing.NormalizeActiveSlices` (renamed from the
   unexported `normalizeActiveSlices`) — the same deterministic algorithm
   `EnsureWorkloadSlices` already uses — to resolve the ambiguity into a single answer
   instead of erroring.

## 5. External dependency: Spark Operator `spec.Parallelism`

See [`spark-operator-parallelism-dependency.md`](./spark-operator-parallelism-dependency.md)
for the full write-up. Summary: `Pradeep39/spark-operator#1` adds a
`Spec.Parallelism *int32` field to `SparkApplicationSpec`, intended as a stable,
operator-agnostic place for an external system to record a desired executor count. **The
design in this document does not use it** — §2.3's live-Pod-count derivation supersedes
the need for it, since Kueue never needs to *write* a desired count anywhere; it only
*reads* the live state. The field is retained in the fork as a forward-looking, currently
inert addition and called out explicitly so it isn't mistaken for a required dependency
of PR #16.

## 6. Alternatives considered

- **Second controller patching `.Spec`** (§2.2): rejected — incompatible with the Spark
  Operator's `event_filter.go` DeepEqual-triggered resubmission.
- **`workqueue.AddAfter` for debouncing**: rejected — `AddAfter` items fire independently
  per call, so a burst of N Pod events produces N staggered reconciles, not one coalesced
  reconcile. A small explicit per-key timer map (§3.3) was needed instead.
- **Writing the live count into `spec.Parallelism`** (once that field existed in the
  Spark Operator fork) and reading it back in `PodSets()`: considered and rejected as an
  unnecessary indirection — it would still require *someone* to write it (recreating the
  exact problem in §2.2, just against a different field, unless the write is scoped to
  only pre-running phases), and live-Pod listing is strictly more accurate since it
  reflects DA's actual current state rather than a value someone last wrote.

## 7. Testing

Both PRs carry their own standalone test plans (see PR descriptions). Summary of the
validation performed across both branches, standalone and combined:
`go build ./...`, `go vet`, `go test`, `go test -race -count=1` on the affected
packages, `go test ./pkg/controller/jobs/...` as a full job-integration regression
sweep, and `gofmt -l` for formatting. The two branches were also merged back together
locally and diffed against the original combined working tree to confirm the PR split
is lossless.
