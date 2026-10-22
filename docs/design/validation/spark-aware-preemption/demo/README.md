# Demos

Two independent demos of Spark-aware partial preemption. They share the runners in `scripts/` and the
queue setup in `setup/`, but nothing else.

| Where | Demo | Entry point | Guide |
|---|---|---|---|
| this folder | The six-part short demo: whole eviction, shuffle resiliency, spread, round-robin, configurable preemption, default preemption vs. a floor | [`demo-short.sh`](./demo-short.sh) `[--operator kubeflow\|apache]` | [`DEMO-SPREAD-SHUFFLE.md`](./DEMO-SPREAD-SHUFFLE.md), [`SPEAKER-NOTES.md`](./SPEAKER-NOTES.md) |
| [`full/`](./full/) | The Lens + KueueViz walkthrough: scenarios A–D (whole eviction, partial preemption, the protected floor, spread) and a live terminal view for a second screen | [`full/demo.sh`](./full/demo.sh) | [`full/DEMO.md`](./full/DEMO.md) |

`demo-short.sh` runs one of two operator kits:

| Kit | Spark operator | Namespace | Contents |
|---|---|---|---|
| [`kubeflow/`](./kubeflow/) (the default) | Kubeflow Spark Operator | `batch-ns-uno` | `demo-short.sh` and `scenarios/`; it uses the shared runners in `scripts/` |
| [`apache/`](./apache/) | Apache Spark Operator | `batch-ns-dos` | `demo-short.sh`, `scenarios/`, and its own runners, views and app generator: the `spark.apache.org/v1` manifests, labels and states differ |

The operator comes from `--operator`, then the `OPERATOR` environment variable, then
[`demo-short.conf`](./demo-short.conf), and is `kubeflow` if none is set.

`./demo-short.sh preflight` checks the cluster and sets up whatever is missing; see
[`DEMO-SPREAD-SHUFFLE.md`](./DEMO-SPREAD-SHUFFLE.md). `full/demo.sh preflight` only checks.
