# Demo: Spark-aware partial preemption, with Lens and KueueViz

**The story, in one line.** Today, when an urgent Spark job needs room, Kueue evicts whole lower-priority
jobs and they start over. With partial preemption, Kueue takes only the executors it needs. The
victims' drivers keep running, the victims never drop below their protected floor, and the executors
come back when the urgent job is done.

About 25 minutes, or 15 without scenario D.

---

## Before the audience arrives (10 min)

1. **Preflight.** `./demo.sh preflight` should show:
   - the `SparkPartialPreemption` gate on
   - every node Ready
   - no leftover SparkApplications
   - if anything is left over, run `./demo.sh reset` first
2. **Screens:**

   | Where | What |
   |---|---|
   | Screen 1, Lens | **Workloads → Pods**, namespace filter `batch-ns-uno`, sorted by Name |
   | Screen 2, KueueViz | your KueueViz frontend (export `KUEUEVIZ=<url>` so preflight checks it) |
   | Terminal | `./demo.sh watch`, the live table described below |

   - also keep Lens tabs open on **Custom Resources → kueue.x-k8s.io → Workloads** and
     **sparkoperator.k8s.io → SparkApplications**
3. **Warm up.** Run scenario B once beforehand, then reset. This makes sure the Spark image is cached and
   the timing feels right on the day.
4. **Talk through the terminal view once.** Each row is a Spark application:

   | Column | Meaning |
   |---|---|
   | DRIVER | The driver Pod's phase. **This must stay `Running` for partial preemption** |
   | EXEC_RUNNING | Executors running |
   | GATED | Executors Spark recreated that Kueue holds until there is quota |
   | TERMINATING | Executors handing off their data before exiting |
   | GRANT | Executors Kueue has granted quota for |
   | FLOOR | The protected minimum |
   | RECLAIM_TO | The target while a partial preemption is in progress |
   | GROW_BACK_TO | The size the application will grow back to |

**The cast:**

| Application | Priority | Role |
|---|---|---|
| `batch-a`, `batch-b` | `low` | Long batch jobs |
| `batch-c`, `batch-d` | `low` | Scenario D only |
| `urgent` | `high` | The job that needs room |

The cohort has 12 slots, where one slot is 1 executor or driver.

---

## Scenario A: today's behaviour (5 min)

`./demo.sh A`. The batch apps start; when they're running, press Enter to launch `urgent`.

> **Say:** "These two batch jobs are ordinary Spark applications that haven't opted into partial
> preemption. This is what Kueue does today when something more important arrives."

**Show:**

1. **Lens → Pods.** `batch-a` and `batch-b` each have a driver and 4 executors, all Running. In the
   terminal view they show 4 under EXEC_RUNNING.
2. **KueueViz → Cluster Queues / Cohorts.** About 10 of 12 slots are used.
3. **Press Enter.**
4. **Lens → Pods.** Within seconds, a batch app's **driver** and all its executors go Terminating and
   disappear. The terminal shows `SUSPENDED` with DRIVER `-`. In our runs, both batch apps were evicted.
5. **Lens → SparkApplications → batch-a.** It goes `SUSPENDED` → `PENDING_RERUN` → `RUNNING`.
   > **Say:** "It has restarted from scratch. Every bit of work it had done is gone."
6. **Lens → Events.** `Preempted … to accommodate a workload`. The terminal's events panel shows the same.
7. **KueueViz → Workloads.** The batch Workloads are evicted and re-queued.

> **Say:** "One urgent job cost us two batch jobs' worth of progress. With Dynamic Allocation it gets
> worse: every time the urgent job grows by one executor, Kueue can evict another whole job. In our
> baseline runs that was 11–14 evictions in seven minutes."

---

## Scenario B: partial preemption (7 min), the centrepiece

`./demo.sh B`

> **Say:** "Same jobs, same urgent arrival. The only difference is that the batch jobs opted in: they are
> elastic, and they declared a floor of 1 executor with `spark.kueue.preemption.minExecutors`."

**Before pressing Enter, show what Kueue added:**

1. **Lens → SparkApplications → batch-a → YAML.** Under `sparkConf` you'll see these settings, which
   Kueue's webhook added when the app was created:

   | Setting | Value |
   |---|---|
   | `spark.decommission.enabled` | `"true"` |
   | `spark.storage.decommission.*` | `"true"` |
   | `spark.kubernetes.executor.terminationGracePeriodSeconds` | `60s` |

   > **Say:** "When Kueue takes an executor, Spark gets the signal first and moves its shuffle data to the
   > executors that stay. That's what the 60 seconds is for."
2. **Lens → Pods → any batch-a executor:**
   - Containers → Lifecycle: `preStop: /opt/decom.sh`
   - Termination grace period: 60
3. **Lens → Workloads → batch-a's Workload → Annotations:**
   - `kueue.x-k8s.io/partial-preemption-podset: executor`, so only executors are reclaimable and the
     driver never is
   - `…-min-count: "1"`, the floor

**Press Enter, then show:**

4. **Terminal and Lens → Events.**
   `PartiallyPreempted: Partially preempted 1 Pod(s) of PodSet executor, from 3 to 2`.
   > **Say:** "One executor, not one application."
5. **Lens → Pods:**
   - The reclaimed executor goes Terminating and stays there 30–60s while it decommissions.
   - **Both batch drivers stay Running the entire time.** Point at them.
6. **Lens → batch-a's Workload:**
   - annotation `partial-preemption-target: "2"`
   - condition `PartiallyPreempted=True`
   - once the executor has gone, the grant is lowered and the condition becomes `PartialPreemptionCompleted`

   > **Say:** "Kueue releases the quota only after the executor has actually stopped. The urgent job never
   > shares capacity with a running executor."
7. **Lens → Pods.** Spark immediately tries to replace the lost executor, and the new Pod shows
   **`SchedulingGated`**. In the terminal it's counted under GATED.
   > **Say:** "Spark asks for it back, and Kueue holds it until there's quota. No thrash."
8. **Lens → batch-a driver → Logs.** Search for `decommission`. You'll see
   `Decommission executors: …` and `Executor N … is decommissioned after 31.0 s`.
9. **KueueViz → Cluster Queues.** Usage shifts to `urgent` with nothing restarting.

**When `urgent` completes** (about 3 minutes in):

10. **Terminal.** The GATED executors are released and EXEC_RUNNING climbs back up. The batch apps grow
    back on their own.
    > **Say:** "No restarts. The batch jobs lost a few executors for a few minutes and got them back."

---

## Scenario C: the protected floor (5 min)

`./demo.sh C`. Here the floor is the apps' own `minExecutors`, which is 3.

**Show:**

1. **Press Enter.** The batch apps are trimmed to **exactly 3** executors each, and no further. FLOOR
   shows 3.
2. **Lens → Workloads → urgent's pending scale-up Workload → Conditions.** `QuotaReserved=False`, with the
   message *"scaling up a running workload never preempts whole workloads that opted into partial
   preemption, and nothing else can make room"*.
   > **Say:** "Once the batch jobs are at their floor, the urgent job waits rather than evicting anybody.
   > The floor is a guarantee."
3. **KueueViz → Workloads.** `urgent` has a slice pending. The batch apps are still admitted.

---

## Scenario D: spreading the loss (6 min, optional)

`./demo.sh D`. Four batch apps in a 24-slot cohort.

**Show:**

1. **Terminal → events panel.** Each `PartiallyPreempted` names a **different** batch app, one executor at
   a time: batch-a, batch-c, batch-b, batch-d, and so on.
   > **Say:** "Kueue spreads the cost. Four jobs each lose a little, rather than one job losing a lot."
2. **KueueViz → Workloads.** Four victims, each slightly smaller, all still admitted.

Then run `./demo.sh reset`, which restores the 12-slot baseline.

---

## Likely questions

| Question | Answer |
|---|---|
| What stops Spark from asking for its executors back straight away? | It does ask. The new Pods are born gated, and Kueue releases them only when quota frees up (scenario B, step 7). |
| Does the job fail? | No. A deleted executor isn't an application failure, and with decommissioning its shuffle data moves first. Spark reports them as "gracefully decommissioned". |
| What about jobs without Dynamic Allocation? | They work too, with `kueue.x-k8s.io/elastic-job: "true"` and a floor. Their executor count is fixed, so they grow back to it. |
| What about jobs that don't opt in? | Exactly today's behaviour (scenario A). |
| What if executors never stop? | After a timeout (90s by default, set per ClusterQueue) Kueue lowers the grant anyway, so the urgent job's wait is bounded. |
| Can I keep whole eviction off entirely? | Yes. Set the ClusterQueue annotation `kueue.x-k8s.io/partial-preemption-fallback: Wait` and a first admission waits instead of evicting. |
| Is it on by default? | No. It sits behind the alpha feature gate `SparkPartialPreemption`. |
| Where's the evidence? | The [validation report](../README.md): every scenario, results and requirement verdicts. |

---

## If something goes wrong

| Problem | What to do |
|---|---|
| `./demo.sh reset` says the namespace isn't clean | A node probably went away; check `kubectl get nodes`. |
| KueueViz shows nothing | Open it from a browser on the LAN. If the browser console shows a websocket origin error, the KueueViz backend's `KUEUEVIZ_ALLOWED_ORIGINS` needs the frontend's address. |
| The batch apps never reach 4 executors | Spark is slow to start on a cold node. The script continues after about 6 minutes, or press Ctrl-C and rerun. |
| Old events in Lens | Lens shows the namespace's events for the last hour, and the scenarios reuse names. The terminal view shows only the current scenario's events. |
