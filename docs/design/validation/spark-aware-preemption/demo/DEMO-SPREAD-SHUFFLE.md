# Demo: spreading the reclaim, and moving shuffle data

Two short live demos of Spark-aware partial preemption, each run by one script:

| Part | Script | What the audience sees | Live time |
|---|---|---|---|
| 1. Spread | `run-p11-demo.sh p10`, then `run-p11-demo.sh` (P11) | A high-priority app gets room by trimming executors from many small apps, not by evicting one | ~1.5 min each |
| 2. Shuffle | `run-shuffle-migration.sh 40` | The executors Kueue reclaims hand their shuffle data to the survivors before exiting | ~3 min |

## Before the audience arrives

- Kueue is up with `SparkPartialPreemption=true`, and [`queues-setup.yaml`](../setup/queues-setup.yaml)
  is applied (`cluster-queue-tres`, `batch-queue-tres`). Part 1 applies it itself if it is missing.
- `batch-ns-uno` is empty: `kubectl -n batch-ns-uno get sparkapplications.sparkoperator.k8s.io`.
- The contents of [`scripts/`](../scripts/) and [`setup/queues-setup.yaml`](../setup/queues-setup.yaml)
  sit in one working directory with `runs/` beside them.
- Screens: a terminal for the script; Lens on `batch-ns-uno` (Pods view); KueueViz on
  `cluster-queue-tres` (Part 1) or `cluster-queue-uno` (Part 2).
- Do a dry run with `AUTO=1` for each part. The first run on a node also pulls the Spark image.

## Part 1: spread the loss (P10, then P11)

```bash
CAPTURE=0 ./run-p11-demo.sh p10     # 12 apps x (driver + 2); the urgent app needs 12 slots
CAPTURE=0 ./run-p11-demo.sh         # P11: 9 apps x (driver + 3); the urgent app needs 15 slots
```

`CAPTURE=0` stops the script once the urgent app runs. Without it, the script keeps watching for 15
minutes and then prints the full report (SC-1/SC-2, PR-1..PR-6), which is worth having from the dry
run. `./run-p11-demo.sh watch` shows the same live table on a second screen.

**1. The queue fills (~40s).** The table shows every app with its driver and executors running, and
`slots granted: 36 of 36`.

> **Say:** "Twelve small batch jobs fill this queue exactly. Each has opted in to partial preemption
> with a floor of one executor: Kueue may take executors down to one, never the driver."

**2. Press Enter: the urgent app arrives.** Within seconds the `reclaim` column shows `-> 1` on every
victim, and the event list below the table shows one line per victim:

```
  14:55:53  als-lo1    gave 1 executor(s), 2 -> 1
  14:55:53  als-lo2    gave 1 executor(s), 2 -> 1
  ...
```

> **Say:** "Without partial preemption, Kueue would evict whole jobs: drivers killed, work lost, the
> jobs restarted from scratch. Here it took one executor from each of the twelve, the smallest possible
> loss per job."

**3. ~30s later the urgent app runs.** `deleting` counts drop to 0 and the urgent app's executors go
`running`. The gap is the reclaimed executors' 30s grace period: Kueue releases their quota only once
they have stopped.

> **Say:** "The quota moves only after those executors are gone, so the cluster is never
> over-committed. And every batch job still has its driver: none restarted."

**4. Point at `gated`.** Each victim now shows a gated executor: Spark asked for its lost executor
back, and Kueue holds it until quota is free. When the urgent app finishes, the victims grow back on
their own.

**5. Run P11.** Same story with fewer, bigger apps: the urgent app needs 15 slots from 9 apps, so
every app gives one first, and only then do six give a second (`-> 2` on three apps, `-> 1` on six).

> **Say:** "No app gives up a second executor until every app has given one."

## Part 2: the reclaimed executors move their shuffle data

```bash
./run-shuffle-migration.sh 40                    # terminal 1
./shuffle-per-executor.sh pr-victim --watch      # terminal 2, start it right after terminal 1
```

The victim is SparkPageRank, an official Spark example that writes shuffle data on every executor.
The script submits the urgent app on its own as soon as **every** executor holds at least 40 MiB of
shuffle files (about a minute in). Have terminal 2 up before then.

**1. Shuffle data builds up.** Terminal 2 shows MiB of shuffle files per executor, climbing.

> **Say:** "This is the data that makes executors expensive to lose: if an executor dies, Spark has
> to recompute everything it held."

**2. The trigger.** Terminal 1 logs `TRIGGER: urgent applied`. In Lens, three `pr-victim-exec-*`
Pods turn `Terminating`; terminal 2 marks them `decommissioning`.

> **Say:** "Kueue picked three executors to reclaim. It turned on Spark's graceful decommissioning for
> this job when it was submitted, so deleting an executor tells Spark to move its data first."

**3. Migration.** In about a second the decommissioning executors' shuffle data is copied to the
survivors. Each then exits within the 30s grace period and the urgent app starts.

**4. The proof.** When the victim finishes, terminal 1 prints the report command:

```bash
python3 -I shuffle-migration-report.py runs/SHUF-<stamp>
```

Point at: shuffle blocks migrated per reclaimed executor (from each executor's own log), map outputs
relocated by the driver, and `resubmitted stages / fetch failures: 0`. The job finished normally.

> **Say:** "Every block moved, nothing was recomputed, and the job finished as if nothing happened."

## Reset

```bash
kubectl -n batch-ns-uno delete sparkapplications.sparkoperator.k8s.io --all
```

## If something goes wrong

| Symptom | Cause and fix |
|---|---|
| `refusing to start: ... other SparkApplications` | Leftover apps: rerun with `FORCE=1`, or reset first |
| `refusing to start: another ... is in progress` | A run was interrupted: remove the `runs/.p11-demo.lock` or `runs/.shuffle-migration.lock` directory |
| Victim executors never all reach `running` | Not enough node capacity for 36 one-CPU Pods: check node readiness in Lens |
| Part 2 triggers before terminal 2 is up | Raise the threshold, e.g. `./run-shuffle-migration.sh 60` |
| Part 2 victim finishes before the trigger | The threshold is too high for the job's length: lower it, or set `ITERS=150` |
