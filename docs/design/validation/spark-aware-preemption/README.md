# Validation: Spark-aware partial preemption

Cluster validation of [`../../spark-aware-preemption.md`](../../spark-aware-preemption.md).

**Status: in progress.** The consolidated report (every scenario, its results, and the bugs it
found) replaces this file when the current run finishes.

| Folder | Contents |
|---|---|
| [`setup/`](./setup/) | Cohort, ClusterQueues, LocalQueue and WorkloadPriorityClasses (12-slot baseline) |
| [`scenarios/`](./scenarios/) | One SparkApplication manifest per scenario, both waves, with its setup and checks in the header |
| [`scripts/`](./scripts/) | Capture, analyzers, scenario generator and the suite runners |
| [`results/`](./results/) | Per-scenario verdicts and timelines (added with the report) |
