# Demo: whole eviction today, then resilient partial preemption and spreading the reclaim

Short live demos on stock Spark 3.4.3, about 11 minutes of cluster time in total. Speaker notes: [`SPEAKER-NOTES.md`](./SPEAKER-NOTES.md).

| Part | Command | What the audience sees | Live time |
|---|---|---|---|
| 0. The problem | `./run-thrash-demo.sh` | Two PageRank apps that did not opt in are evicted whole, re-admitted, and evicted again by a growing urgent app | ~4:40 |
| 1. Shuffle resiliency | `STOP_AFTER_VICTIM=1 ./run-shuffle-migration.sh 40` | Reclaimed executors hand their shuffle data to the survivors; the job finishes with nothing recomputed | ~2:15 |
| 2a. Spread | `CAPTURE=0 ./run-p11-demo.sh p10s` | Six apps each give exactly one executor; afterwards they grow back | ~1:40 |
| 2b. Round-robin | `CAPTURE=0 ./run-p11-demo.sh p11s` | Five apps each give one before any gives a second; they grow back | ~1:35 |

Measured on the cluster with `AUTO=1`. Spark 4.0.1 works the same but took ~2 minutes longer in
part 2b: on that version the last victims are slower to get their executors back.

## Before the audience arrives

- Kueue is up with `SparkPartialPreemption=true`, and [`queues-setup.yaml`](../setup/queues-setup.yaml)
  is applied (`cluster-queue-tres`, `batch-queue-tres`). `cluster-queue-uno`/`-dos` have 6 CPU each
  (part 1 runs there).
- `batch-ns-uno` is empty: `kubectl -n batch-ns-uno get sparkapplications.sparkoperator.k8s.io`.
- The contents of [`scripts/`](../scripts/) and `setup/queues-setup.yaml` sit in one working
  directory with `runs/` beside them.
- `export SPARK_VERSION=3.4.3` in every terminal.
- Screens: terminal 1 for the scripts; terminal 2 for `shuffle-per-executor.sh` (part 1); Lens on
  `batch-ns-uno` (Pods view); KueueViz on `cluster-queue-uno` (part 1) and `cluster-queue-tres`
  (part 2).
- Dry-run each part once with `AUTO=1`, so the Spark image is cached on the nodes.

## Part 0: whole eviction and thrashing (today's behaviour)

```bash
./run-thrash-demo.sh        # sizes cluster-queue-tres to 12 slots, puts it back to 36 on exit
```

Two low-priority SparkPageRank apps (driver + 4 executors each) without the partial-preemption
opt-in share the queue with a high-priority SparkALS app using Dynamic Allocation. The script pauses
before the urgent app (press Enter), then shows a live table and a log of whole evictions, and stops
when the urgent app finishes or after 240s (`DURATION`). In the test run: 3 whole evictions in about
3 minutes; one app was admitted 3 times and evicted twice. The exact count varies from run to run.

## Part 1: the reclaimed executors move their shuffle data

```bash
STOP_AFTER_VICTIM=1 ./run-shuffle-migration.sh 40     # terminal 1
./shuffle-per-executor.sh pr-victim --watch           # terminal 2, right after terminal 1
```

The victim is SparkPageRank, an official Spark example that writes shuffle data on every executor.
The script submits the urgent app on its own once **every** executor holds at least 40 MiB of shuffle
files (about a minute in), and prints the report as soon as the victim finishes.

1. **Shuffle builds up.** Terminal 2 shows MiB of shuffle files per executor, climbing.
2. **Trigger.** Terminal 1 logs `TRIGGER: urgent applied`. Three to five `pr-victim-exec-*` Pods turn
   `Terminating` in Lens; terminal 2 marks them `decommissioning`.
3. **Migration.** Within about a second their shuffle data is copied to the survivors. They exit within
   the 30s grace period; the urgent app starts about 30s after the trigger.
4. **Report** (printed automatically). Point at: shuffle blocks migrated per reclaimed executor, map
   outputs relocated by the driver, `resubmitted stages / fetch failures / lost or killed tasks: 0`, and
   the timeline (urgent admitted while the victim was still running).

## Part 2: spread the loss (p10s, then p11s)

```bash
CAPTURE=0 ./run-p11-demo.sh p10s    # 6 apps x (driver + 2); the urgent app needs 6 slots
CAPTURE=0 ./run-p11-demo.sh p11s    # 5 apps x (driver + 3); the urgent app needs 8 slots
```

Each run sizes `cluster-queue-tres` to exactly what the victims fill (18 or 20 slots) and puts it back
to 36 on exit. It pauses before the urgent app arrives (press Enter), then follows the reclaim and
the grow-back, and deletes its apps when it ends. `./run-p11-demo.sh watch` shows the same table on a
second screen.

1. **The queue fills (~20s).** Every app shows its driver and executors running; `slots granted: 18 of 18`.
2. **Press Enter.** Within seconds the `reclaim` column shows the target on every victim, with one
   event line per victim (`gave 1 executor(s), 2 -> 1`).
3. **~30s later the urgent app runs.** `deleting` drops to 0: quota moved only after the executors stopped.
4. **Grow-back.** The urgent app finishes after about a minute; `gated` executors are admitted and the
   victims return to full size. The script ends on that table.
5. **p11s.** Same flow; the urgent app needs 8 slots from 5 apps: every app gives one, then three give a
   second (`3 -> 1` on three apps, `3 -> 2` on two).

## Reset

The scripts delete their own apps. If a run was interrupted:

```bash
kubectl -n batch-ns-uno delete sparkapplications.sparkoperator.k8s.io --all
```

## If something goes wrong

| Symptom | Cause and fix |
|---|---|
| `refusing to start: ... other SparkApplications` | Leftover apps: rerun with `FORCE=1`, or reset first |
| `refusing to start: another ... is in progress` | A run was interrupted: remove the `runs/.p11-demo.lock`, `runs/.shuffle-migration.lock` or `runs/.thrash-demo.lock` directory |
| Part 0 shows only one eviction | The urgent app did not scale back down in time: rerun, or raise `DURATION` |
| Part 1 triggers before terminal 2 is up | Raise the threshold: `./run-shuffle-migration.sh 60` |
| Part 1 victim finishes before the trigger | Lower the threshold, or `ITERS=150` |
| Victim executors never all reach `running` | Not enough node capacity: check node readiness in Lens |
| Grow-back takes longer than expected | It stops waiting after 7 minutes; `GROWBACK=0` skips it entirely |
| `cluster-queue-tres` left at 18/20 slots after a crash | `kubectl apply -f queues-setup.yaml` restores 36 |
