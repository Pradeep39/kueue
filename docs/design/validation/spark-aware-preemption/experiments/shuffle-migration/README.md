# Experiment: does a reclaimed executor move its shuffle data?

The other scenarios ran SparkALS, which writes no shuffle data (only ResultStages, no ShuffleMapStages).
They could show that reclaimed executors decommissioned gracefully, but not that shuffle data moved.
This experiment uses a shuffle-heavy official example to check that.

**Answer: yes.** Partial preemption sent each reclaimed executor through Spark's graceful
decommissioning:

- Each executor migrated all of its shuffle files and its cached RDD blocks to surviving executors,
  within about a second.
- The driver relocated the map outputs.
- Nothing was recomputed or failed, and the job finished normally.

## Setup

| | |
|---|---|
| Victim | `pr-victim` (low priority): **SparkPageRank** (official example, an iterative `join` + `reduceByKey`, so shuffle files build up on every executor) over a generated 2M-edge graph. The graph is written by an init container with a fixed seed, so it is identical on every Pod. Static elastic app: 10 executors, floor 2, 50 iterations, executor grace period 30s. [`victim.yaml`](./victim.yaml) |
| Preemptor | `urgent` (high priority): SparkALS with Dynamic Allocation. [`urgent.yaml`](./urgent.yaml) |
| Cohort | 12 slots ([`../../setup/queues.yaml`](../../setup/queues.yaml)) |
| Kueue | `18abe488b`, `SparkPartialPreemption` on |
| Spark | 4.0.1, with `spark.decommission.enabled` and `spark.storage.decommission.{shuffleBlocks,rddBlocks}.enabled` defaulted by Kueue's webhook |
| Trigger | [`run-shuffle-migration.sh`](../../scripts/run-shuffle-migration.sh) runs `du` over each executor's `shuffle_*` files every 5s. It submits `urgent` only when **every** victim executor holds ≥ 15 MiB. Each executor's log is streamed from start to exit, so its decommission is recorded before the Pod goes away. |
| Analysis | [`shuffle-migration-report.py`](../../scripts/shuffle-migration-report.py) and `req-check.py` |

## Results

| | Run 1 | Run 2 | Run 3: 5s migration round |
|---|---|---|---|
| `replicationReattemptInterval` / `maxReplicationFailuresPerBlock` | Spark defaults (30s / 3) | Spark defaults | **5s** / 18² |
| Shuffle files per executor at the trigger | 23–42 MiB | 44–72 MiB | 23–42 MiB |
| Executors reclaimed by partial preemption | 3 (5, 6, 10) | 3 (8, 9, 10) | 3 (4, 9, 10) |
| Shuffle blocks migrated, from the executors' own logs | — (logs not captured¹) | 123 + 134 + 165 blocks = **252 MiB**, 0 failures | 73 + 82 + 98 blocks, 0 failures |
| Cached RDD blocks migrated | — | 4 per executor, all "migrated successfully" | 4 per executor |
| Map outputs relocated to surviving executors (driver) | 247 | 422 | 253 |
| **Decommission signal → executor exit** | **30.0s** each (the grace cap) | — (the job ended first, at 19s) | **11.0s** each |
| **Reclaim issued → quota released to the preemptor** | **33s** | — | **12s** |
| Resubmitted stages / fetch failures / lost tasks / executor failures | 0 / 0 / 0 / 0 | 0 / 0 / 0 / 0 | 0 / 0 / 0 / 0 |
| Victim job | finished normally (46s) | finished normally (40s) | finished normally (41s) |
| SC-1 / SC-1b / SC-2 | held | held | held |

¹ Run 1's log streamer used a bash 4 associative array, which macOS's bash 3.2 lacks, so it exited.
Fixed for run 2. Run 1's evidence is the driver log only.

² Run 3 was made before the attempt count was capped at 9. Nothing failed to migrate, so the count
made no difference.

**Evidence:**

- [`executor-10-decommission-excerpt.log`](./executor-10-decommission-excerpt.log) runs from the
  decommission signal, through migration, to exit.
- [`driver-relocation-excerpt.log`](./driver-relocation-excerpt.log) shows the driver relocating outputs.
- [`report-run1.txt`](./report-run1.txt) and [`report-run2.txt`](./report-run2.txt) are the full reports.

Pod IPs and local paths are redacted.

## Finding: the exit waits for Spark's next migration round

The migration itself took about a second. On every decommission the executor logged:

```
… Finished current round refreshing migratable shuffle blocks, waiting for 30000ms before the next round refreshing.
```

A decommissioning executor exits only once `allBlocksMigrated` is true **and** the last migration round
started after its last task finished (`CoarseGrainedExecutorBackend`, Spark 4.0). Rounds run every
`spark.storage.decommission.replicationReattemptInterval`, which defaults to 30s. So a reclaimed
executor typically stays up to about 30s after its data has already moved, and Kueue holds the quota
until it exits.

This is the ~31s per reclaim seen elsewhere. In run 1 it coincided with the 30s grace period: the
executors were stopped at the cap just as they would have exited. That was harmless, because everything
had already been migrated.

**Fix.** Kueue's webhook now defaults `spark.storage.decommission.replicationReattemptInterval` to
**5s** for reclaimable apps, and `spark.storage.decommission.maxReplicationFailuresPerBlock` to
90s ÷ interval, capped at 9 and never below 3 (`9` at 5s). Explicit values are kept.

Run 3 sets the same values explicitly on the current image:

- Each reclaimed executor exited **11s** after the signal, instead of 30s.
- The preemptor got its quota **12s** after the reclaim was issued, instead of 33s.
- Migration was still complete.

Both settings are `internal()` in Spark.

## Checking shuffle data per executor

[`shuffle-per-executor.sh`](../../scripts/shuffle-per-executor.sh) prints each running executor's node,
shuffle file count and MiB on disk, and whether it is decommissioning. Add `--watch` to refresh every 5s.

```bash
./shuffle-per-executor.sh pr-victim [namespace] [--watch]
```

The driver's Spark UI (Executors tab, *Shuffle Write*) shows lifetime totals instead. Reach it with
`kubectl port-forward pod/<app>-driver 4040`.

## Rerunning

```bash
# from a working copy of ../../scripts with runs/ beside it
./run-shuffle-migration.sh 15          # 15 = MiB of shuffle files every executor must hold before the trigger
EXTRA_CONF="spark.storage.decommission.replicationReattemptInterval=5s" ./run-shuffle-migration.sh 15   # run 3
```
