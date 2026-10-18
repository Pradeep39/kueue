# Demos

Two independent demos of Spark-aware partial preemption. They share the runners in `scripts/` and the
queue setup in `setup/`, but nothing else.

| Folder | Demo | Entry point | Guide |
|---|---|---|---|
| [`full/`](./full/) | The Lens + KueueViz walkthrough: scenarios A–D (whole eviction, partial preemption, the protected floor, spread) and a live terminal view for a second screen | `full/demo.sh` | [`full/DEMO.md`](./full/DEMO.md) |
| [`short/`](./short/) | The five-part short demo (whole eviction, shuffle resiliency, spread, round-robin, configurable preemption) | `short/kubeflow/demo-short.sh` (Kubeflow Spark Operator, `batch-ns-uno`) or `short/apache/demo-short.sh` (Apache Spark Operator, `batch-ns-dos`) | [`short/DEMO-SPREAD-SHUFFLE.md`](./short/DEMO-SPREAD-SHUFFLE.md), [`short/SPEAKER-NOTES.md`](./short/SPEAKER-NOTES.md) |

`short/kubeflow/scenarios/` and `short/apache/scenarios/` hold the manifests each short-demo part
submits. The Apache kit carries its own runners, views and app generator in `short/apache/`, because
the `spark.apache.org/v1` manifests, labels and states differ.

`./demo-short.sh preflight` checks the cluster and sets up whatever is missing; see the short demo's
guide. `full/demo.sh preflight` only checks.
