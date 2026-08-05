# External dependency: Spark Operator `spec.Parallelism`

Fork: [`Pradeep39/spark-operator`](https://github.com/Pradeep39/spark-operator), PR
[#1 "feat(api): add spec.parallelism to SparkApplicationSpec"](https://github.com/Pradeep39/spark-operator/pull/1),
commit `2f914dd`.

## What changed

Adds one new optional field to `SparkApplicationSpec`:

```go
// Parallelism is the desired executor count, mirroring the role batch/v1.Job's
// spec.parallelism plays for Kubernetes Jobs. It is not read or enforced by this
// controller; it exists so that external systems (e.g. Kueue) have a stable,
// operator-agnostic field to record the desired executor count on, instead of
// having to know about spec.executor.instances or DynamicAllocation.
// +optional
Parallelism *int32 `json:"parallelism,omitempty"`
```

Plus the generated companions any new API field requires: `zz_generated.deepcopy.go`,
`zz_generated.openapi.go`, both copies of the CRD YAML (`charts/spark-operator-chart/crds/`
and `config/crd/bases/`), and `docs/api-docs.md`. No controller, webhook, or
`event_filter.go` logic was touched — this is a pure, inert schema addition.

## Why it was added

At the time this field was proposed, the working design for Kueue's SparkApplication
elastic-scaling integration expected to need *some* stable field on the CR where an
external controller could record "this is how many executors should exist right now" —
analogous to how `batch/v1.Job.Spec.Parallelism` is the canonical, operator-agnostic
signal of desired pod count for a plain Job. `SparkApplication` has no equivalent single
field: the closest analogues (`spec.executor.instances`, `spec.dynamicAllocation.*`) are
Spark-specific, and an external system would need to understand DA's on/off state and
which of several config surfaces (structured field vs. `sparkConf` string keys) is
authoritative just to read the "current desired count" correctly, let alone write it.
`spec.Parallelism` was meant to give Kueue (or any other external system) one
unambiguous field to depend on instead.

## Why it ended up unused

The elastic-scaling design that was actually implemented and shipped (see
[`sparkapplication-elastic-scaling-design.md`](./sparkapplication-elastic-scaling-design.md),
§2–3) does not write a desired count anywhere on the CR at all — it **reads** the live
executor Pod count directly and derives `PodSets()` from that. This sidesteps the
original motivation for `spec.Parallelism` entirely: there was never a need for Kueue to
record a desired count on the object, only to observe DA's actual current state, and
Pods already carry that information with more fidelity (and no propagation delay) than
any field on the CR ever could.

Separately, even if a design *did* want to write a desired count back onto the CR, doing
so post-submission runs into the same problem `spec.Parallelism` was never tested
against: the Spark Operator's `event_filter.go` does an unconditional `DeepEqual` on the
entire `.Spec` on every `Update`, and treats any difference — including a change to this
new, otherwise-inert field — as a request to kill and resubmit the running application
(§2.2 of the design doc). Adding `spec.Parallelism` did not change that behavior, since
the field addition was API-only and never touched `event_filter.go`.

## Current status

- **Not read** by any Spark Operator controller, webhook, or admission logic.
- **Not read or written** by Kueue's `pkg/controller/jobs/sparkapplication` integration
  (PR [#16](https://github.com/Pradeep39/kueue/pull/16)) — grepped and confirmed absent.
- Retained in the fork as a **forward-looking, currently inert** API addition. It
  remains a reasonable field for a future design to adopt (e.g. if a use case emerges
  where recording an explicit desired count is preferable to deriving it from live
  Pods — for instance, a system that wants to *pre-scale* an application before any
  Pods exist), but nothing in the current Kueue integration depends on it existing.

## Contribution implications

Because this field is an addition to a separate project (`kubeflow/spark-operator`, via
the `Pradeep39/spark-operator` fork) and is not exercised by any code being proposed to
`kubernetes-sigs/kueue`, it does not need to land upstream in lockstep with PRs #15/#16
here, nor does it block them. It's documented here purely so a reviewer of the Kueue
changes has the full picture of what exists in the adjacent fork and doesn't need to
guess at its relevance.
