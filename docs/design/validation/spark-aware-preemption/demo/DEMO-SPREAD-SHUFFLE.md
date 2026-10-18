# Demo: whole eviction today, then resilient partial preemption and spreading the reclaim

Five short live parts on stock Spark 3.4.3, about 12 minutes of cluster time in total.
[`demo-short.sh`](./demo-short.sh) runs them on either Spark operator, through that operator's kit:

| `--operator` | Spark operator | Kit | Namespace |
|---|---|---|---|
| `kubeflow` (the default) | Kubeflow Spark Operator (`sparkoperator.k8s.io`) | [`kubeflow/demo-short.sh`](./kubeflow/demo-short.sh) | `batch-ns-uno` |
| `apache` | Apache Spark Operator (`spark.apache.org/v1`) | [`apache/demo-short.sh`](./apache/demo-short.sh) | `batch-ns-dos` |

Pick the operator in one of three ways; the first one set wins:
1. the flag: `./demo-short.sh --operator apache all`
2. the environment: `OPERATOR=apache`
3. the config file: `OPERATOR=apache` in [`demo-short.conf`](./demo-short.conf)

Run `./demo-short.sh preflight`, then `./demo-short.sh all` (or `1`–`5` one at a time). After part 2,
`./demo-short.sh logs` prints each reclaimed executor's timeline. The manifests each part submits are in
`kubeflow/scenarios/` and `apache/scenarios/`. Speaker notes: [`SPEAKER-NOTES.md`](./SPEAKER-NOTES.md).

| Part | Command | What the audience sees | Live time |
|---|---|---|---|
| 1. The problem | `./demo-short.sh 1` | Two PageRank apps that did not opt in are evicted whole, re-admitted, and evicted again by a growing urgent app | ~3:05 |
| 2. Shuffle resiliency | `./demo-short.sh 2` | Reclaimed executors hand their shuffle data to the survivors; the job finishes with nothing recomputed | ~2:15 |
| 3. Spread | `./demo-short.sh 3` | Six apps each give exactly one executor; afterwards they grow back | ~1:40 |
| 4. Round-robin | `./demo-short.sh 4` | Five apps each give one before any gives a second; they grow back | ~1:35 |
| 5. Configurable preemption | `./demo-short.sh 5` | With classic preemption off, a `PreemptionConfig` rule, not priority, picks the victims | ~3:00 |

Measured on the cluster with `AUTO=1`. Spark 4.0.1 works the same but took ~2 minutes longer in
part 4: on that version the last victims are slower to get their executors back.

## Before the audience arrives

- `./demo-short.sh preflight` checks the cluster and sets up what is missing:
  - the Kueue feature gates (`SparkPartialPreemption`, `ConfigurablePreemptions`, the Spark integration)
  - `integrations.labelKeysToCopy`
  - the `PreemptionConfig` CRD
  - the queues in [`setup/`](../setup/): [`queues-setup.yaml`](../setup/queues-setup.yaml) for
    parts 1–4, [`queues-quatro.yaml`](../setup/queues-quatro.yaml) for part 5, and, for the Apache
    kit, [`queues-apache.yaml`](../setup/queues-apache.yaml)
  - the Apache operator's watched namespace

  It asks before restarting Kueue or the operator. `preflight --check` reports without changing
  anything. Namespaces, service accounts and the Kubeflow operator's job namespaces are only checked.
- The namespace is empty: preflight says so, and every part refuses to start next to apps it did not
  create.
- `export SPARK_VERSION=3.4.3` in every terminal (the default).
- Screens:
  - one terminal for the script
  - Lens on the demo namespace (Pods view)
  - KueueViz on these queues:

    | Part | ClusterQueue |
    |---|---|
    | 1, 3 and 4 | `cluster-queue-tres` |
    | 2 | `cluster-queue-uno` (Kubeflow) or `cluster-queue-dos` (Apache) |
    | 5 | `cluster-queue-quatro` |
- Dry-run each part once with `AUTO=1`, so the Spark image is cached on the nodes.
- `./demo-short.sh clear-events` removes the preemption events from earlier runs.

## Part 1: whole-workload preemption (today's behaviour)

`./demo-short.sh 1` sizes `cluster-queue-tres` to 12 slots and puts it back to 36 on exit.

Two low-priority SparkPageRank apps (driver + 4 executors each) without the partial-preemption
opt-in share the queue with a high-priority SparkALS app using Dynamic Allocation. The script pauses
before the urgent app (press Enter), then shows a live table and a log of whole evictions. It stops at
whichever comes first:

- the third whole eviction (`EVICTIONS=3`)
- the urgent app finishing
- 240s (`DURATION`)

Press Enter (or Ctrl-C) to end it sooner; either way it prints the summary and deletes its apps.

| Setting | Time | What it covers |
|---|---|---|
| `EVICTIONS=2` | ~1:10 | the first two evictions only |
| `EVICTIONS=3` | ~3:05 | adds a victim re-admitted and evicted again |
| unbounded | ~4:40 | |

In the test runs: 3 whole evictions in about 3 minutes; one app was admitted twice and evicted twice.
The exact count varies from run to run.

## Part 2: the reclaimed executors move their shuffle data

`./demo-short.sh 2` (`THRESH=40` by default)

The victim is SparkPageRank, an official Spark example that writes shuffle data on every executor.

1. **Shuffle builds up.** A live table shows each executor's node, shuffle MiB and state. When every
   executor holds 40 MiB (`THRESH`) a ✅ hint appears; press Enter to submit the urgent app.
2. **Trigger.** A few victim executor Pods turn `Terminating` in Lens; their rows turn `decommissioning`.
3. **Migration.** Within about a second their shuffle data is copied to the survivors; the "handed
   over" column fills in. They exit within the 30s grace period, and the urgent app starts about 30s
   after the trigger.
4. **Summary** (printed automatically). Point at:
   - the executors Kueue reclaimed
   - `fetch failures: 0, stages resubmitted: 0`
   - the urgent app running while the victim was still running

   `./demo-short.sh logs` then prints each reclaimed executor's own decommission timeline (signal,
   blocks migrated, exit). The full logs stay in `runs/SHUF-<stamp>/exec-logs/`.

## Parts 3 and 4: spread the loss, then round-robin

| Part | Victims | The urgent app needs | Queue size |
|---|---|---|---|
| `./demo-short.sh 3` | 6 apps × (driver + 2 executors) | 6 slots | 18 |
| `./demo-short.sh 4` | 5 apps × (driver + 3 executors) | 8 slots | 20 |

Each run sizes `cluster-queue-tres` to exactly what the victims fill and puts it back to 36 on exit. It
pauses before the urgent app arrives (press Enter), follows the reclaim and the grow-back, and deletes
its apps when it ends. `run-partial-preemption-spread.sh watch` shows the same table on a second screen.

1. **The queue fills (~20s).** Every app shows its driver and executors running; `18 of 18 slots granted`.
2. **Press Enter.** Within seconds every victim shows `gave 1` and `giving 1 back`, with `✕` dots.
3. **~30s later the urgent app runs.** Quota moved only after the executors stopped.
4. **Grow-back.** The urgent app finishes after about a minute. The `◌` executors are admitted and the
   victims return to full size; the script ends on that table.
5. **Part 4.** Same flow. The urgent app needs 8 slots from 5 apps: every app gives one, then three give
   a second (`gave 2` on three apps, `gave 1` on two).

## Part 5: configurable preemption picks the victims

`./demo-short.sh 5` runs on `cluster-queue-quatro` (24 slots, from
[`setup/queues-quatro.yaml`](../setup/queues-quatro.yaml)). Classic preemption is off there:
`withinClusterQueue`, `reclaimWithinCohort` and `borrowWithinCohort` are all `Never`, the recommended
setup with configurable preemption. Its `PreemptionConfig` `spark-batch-pool` has one rule: apps
labeled `preemption-role=urgent` may take capacity from apps labeled `preemption-pool=batch`, whatever
their priority. The `candidate` column shows each app's pool membership and priority.

1. **The queue fills.**

   | Apps | Priority | Pool label | Each app |
   |---|---|---|---|
   | `als-lo1`–`als-lo4` | default | `preemption-pool=batch` | driver + 3 executors |
   | `als-lo5`, `als-lo6` | low | none | driver + 3 executors |
2. **A high-priority app without the urgent label** (`plain-hi`) arrives. It waits 40s and preempts
   nothing: no rule matches it, and priority alone no longer preempts. The script then withdraws it.
   `CONTROL=0` skips this step.
3. **The labeled urgent app** (`als-hi`, driver + 3) arrives. The four pool apps give one executor each.
   `als-lo5` and `als-lo6` keep all theirs, although they are lower priority, because they are outside
   the pool. No app is evicted whole.
4. **Grow-back.** The pool apps return to full size when the urgent app finishes. The summary checks
   that the victims followed the rule.

## Reset

The scripts delete their own apps. If a run was interrupted, `./demo-short.sh reset` deletes this
demo's apps, puts `cluster-queue-tres` back to 36 slots and removes stale run locks. Preflight also
restores `cluster-queue-tres` and `cluster-queue-quatro` if a run left them changed.

## If something goes wrong

| Symptom | Cause and fix |
|---|---|
| `refusing to start: ... other SparkApplications` | Apps the demo did not create are in the namespace. Remove them if they are yours to remove. |
| `refusing to start: another ... is in progress` | A run was interrupted: `./demo-short.sh reset` removes the stale lock |
| Part 1 shows only one eviction | The urgent app finished too soon: rerun, or raise `URGENT_ITERS` or `DURATION` |
| Part 2 triggers too soon to point at the shuffle data | Raise the threshold: `THRESH=60 ./demo-short.sh 2` |
| Part 2 victim finishes before the trigger | Lower the threshold, or `ITERS=150` |
| Part 5 refuses: a gate is off or `labelKeysToCopy` is missing | `./demo-short.sh preflight` sets them and restarts Kueue |
| Part 5's `plain-hi` gets admitted | Classic preemption is not off on `cluster-queue-quatro`: preflight restores it |
| Apache apps are admitted but never start | The Apache operator does not watch `batch-ns-dos`: preflight adds it |
| Victim executors never all reach `running` | Not enough node capacity: check node readiness in Lens |
| Grow-back takes longer than expected | It stops waiting after 7 minutes; `GROWBACK=0` skips it entirely |
