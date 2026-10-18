# Speaker notes: Spark-aware partial preemption (~23 min)

Runbook: [`DEMO-SPREAD-SHUFFLE.md`](./DEMO-SPREAD-SHUFFLE.md). Everything runs with `./demo-short.sh`
(Spark 3.4.3 by default), from `kubeflow/` for the Kubeflow Spark Operator or `apache/` for the
Apache Spark Operator. Before you start:
`./demo-short.sh reset`, `./demo-short.sh clear-events`, `./demo-short.sh preflight`.
Or run all five parts in order, with a pause between them: `./demo-short.sh all`.

## 0. Opening (2 min)

- **Problem.** Kueue preempts Spark apps whole. One urgent job kills drivers, throws away work, and
  low-priority jobs restart from scratch. On our cluster: 11–14 whole evictions and 13 restarts per run.
- **Idea.** Take only what's needed: a few executors from many apps. Never the driver, never below a
  floor the app declares.
- **Constraint.** No change to Apache Spark or the Spark Operator. It works on the Spark versions
  people run today. This demo runs on stock **Spark 3.4.3**.

## 1. The problem, live: whole-workload preemption (~3 min)

`./demo-short.sh 1` (stops after the third eviction; press Enter to end sooner)

| On screen | Say |
|---|---|
| Two PageRank apps running | "Two batch jobs building shuffle data. They have not opted in, so this is Kueue as it works today." |
| Press Enter; `EVICTED WHOLE` | "An urgent job with Dynamic Allocation arrives. Kueue evicts a whole job: driver killed, shuffle data gone." |
| Second eviction | "The urgent job grows, and Kueue evicts the other job too." |
| Victim re-admitted, then `EVICTED WHOLE` again | "When the urgent job shrinks, the batch job comes back and starts over from scratch. The urgent job grows again, and it is evicted again, repeatedly." |
| Summary (after the 3rd eviction, or press Enter) | "Several evictions in a few minutes, and every one threw away all the work done so far. Now the same kind of job with partial preemption." |

## 2. Shuffle resiliency (~3 min)

`./demo-short.sh 2`, then `./demo-short.sh logs`

| On screen | Say |
|---|---|
| Table: shuffle MiB per executor climbing, then ✅ "every executor holds at least 40 MiB" | "PageRank writes shuffle data on every executor. This is what makes an executor expensive to lose." |
| Press Enter; a few rows turn `decommissioning` | "An urgent job arrived. Kueue took a few executors, not the job." |
| — | "Kueue turned on Spark's own graceful decommissioning for this app at submission. Deleting an executor tells Spark to move its data first." |
| Urgent app starts ~30s later | "Quota moves only after those executors stop, so the cluster is never over-committed." |
| "handed over" column fills in: `N shuffle blocks · M MiB → survivors` | "Each executor Kueue took handed over everything it held before it left." |
| Summary: job finished normally, 0 fetch failures, 0 stages resubmitted | "Every block moved. Nothing was recomputed. The job finished as if nothing happened." |

## 3. Spread the loss (~2 min)

`./demo-short.sh 3`

| On screen | Say |
|---|---|
| 6 apps, `queue: 18 of 18 slots granted`, ⏸ prompt | "Six batch jobs fill this queue. Each opted in with a floor of one executor." |
| Press Enter; `gave 1` on every app, `✕` dots | "The urgent job needs six slots. Kueue took one executor from each of the six: the smallest loss per job." |
| `deleting` → 0, urgent running | "Every job kept its driver. No restarts." |
| `◌` dots turn `●`; status "back to full size" | "When the urgent job finishes, the executors Spark asked back for are admitted. The jobs grow back on their own." |

## 4. Round-robin (~2 min)

`./demo-short.sh 4`

| On screen | Say |
|---|---|
| 5 apps × 3 executors | "Now the urgent job needs more than one executor per app." |
| `gave` 1 on two apps, 2 on three; summary line "round-robin: every app gave 1 before any app gave 2" | "Every app gives one before any app gives a second. The loss stays as even as possible." |
| Grow-back | "And again, they come back without a restart." |

## 5. Configurable preemption (~3 min)

`./demo-short.sh 5`

| On screen | Say |
|---|---|
| Six apps; `candidate` column: four `pool · default`, two `no · low` | "This queue has classic preemption turned off. A PreemptionConfig rule decides instead: urgent apps may take from the batch pool, whatever the priorities." |
| `plain-hi` arrives and waits; nothing changes | "A high-priority app without the urgent label. No rule matches it, so it waits. Priority alone preempts nothing here." |
| `als-hi` arrives; `gave 1` on the four pool apps, `0` on `als-lo5`/`als-lo6` | "The urgent app matches the rule. Kueue takes one executor from each pool app. The two apps outside the pool keep everything, even though they are lower priority." |
| Summary: "the PreemptionConfig chose the victims, not priority"; 0 evicted whole | "Partial preemption follows whatever selects the victims, the classic policy or a PreemptionConfig. No job was evicted whole." |

## 6. Close (3 min)

- **Results.** Whole evictions per run 11–14 → **0**. Low-priority restarts 13 → **0**. Quota invariants
  held in every run (usage ≤ quota; every running executor had a grant).
- **Versions.** Validated on Spark **3.4.3 and 4.0.1**, unchanged. Graceful hand-off needs Spark 3.1+.
  Older versions still get partial reclaim, without block migration.
- **Why on the Kueue side.** The driver needs no Kueue API access. The quota manager enforces the
  reclaim even if a driver is slow. And no Spark upgrade is needed, so 3.x users benefit now.
- **Also fixed.** Partial preemption now works when victims are picked by a `PreemptionConfig`
  (`candidateSelectors`), not only by the classic priority policy.
- **Ask.** Fold the floor, the spread rule and the timeout into KEP-975, and keep the design
  independent of Spark version.

## If asked

- **Which executors go?** Round-robin across victims. If executors carry `pod-deletion-cost` (Spark's
  Deployment allocator, 4.2+), the lowest cost goes first. That's optional.
- **What if Pods won't stop?** A per-ClusterQueue timeout (default: grace period + 30s) lowers the
  grant anyway.
- **Static (non-Dynamic-Allocation) apps?** Covered when they opt in with the elastic annotation and
  a floor.
- **Not opted in?** Preempted whole, exactly as today.
- **Fair sharing, TAS?** Not in v1: those preemptors keep whole eviction.
