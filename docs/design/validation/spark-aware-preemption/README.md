# Validation: Spark-aware partial preemption

Cluster validation of [`../../spark-aware-preemption.md`](../../spark-aware-preemption.md). It was run
on 2026-10-12 against the `spark-aware-preemption` branch.

**Result.** With the `SparkPartialPreemption` gate on, elastic Spark applications were never evicted
whole to make room for another application's growth. Low-priority applications never restarted. Every
victim kept its driver and its floor, and quota was released only after the reclaimed executors had
stopped.

For comparison, the same workload with the gate off evicted whole applications 11–14 times per run, and
low-priority applications restarted 13 times.

The runs found six bugs. All are fixed on the branch, and each fix has a negative-controlled unit test:

- four in partial preemption, one of them (configurable preemption's candidates) in round r4
- one in its events: the preemptor's event named only the first of several victims (r4)
- one already present in Kueue's elastic slicing, which these runs exposed

**Round r4 (2026-10-14)** reran everything on the final build (`adc8c5cbd`) and added three scenarios:
many small victims (P10, P11) and preemption chosen only by a `PreemptionConfig` (P12). P12 found a fifth
partial-preemption bug, now fixed, and P10 a reporting one. See §3.3.

**Defaults at the time.** These runs used a 60s executor grace period and a 90s reclaim timeout. The
defaults have since been lowered to 30s and 60s. The runs showed busy executors draining in about
30s, so the lower cap rarely binds.

| | |
|---|---|
| [`setup/`](./setup/) | Cohort, ClusterQueues, LocalQueue, WorkloadPriorityClasses (12-slot baseline); the 36-slot `cluster-queue-tres` and P12's `PreemptionConfig` (r4) |
| [`scenarios/`](./scenarios/) | One manifest per scenario, both waves; the header records its setup and checks |
| [`scripts/`](./scripts/) | Capture, analyzers, scenario generator, suite runners, results archiver |
| [`results/`](./results/) | Per-run verdicts (`*.txt`), timelines (`*.timeline.csv`), suite logs |
| [`experiments/shuffle-migration/`](./experiments/shuffle-migration/) | Does a reclaimed executor move its shuffle data? SparkPageRank under partial preemption: 252 MiB of shuffle blocks and the cached RDD blocks migrated, 0 recomputation |
| [`demo/`](./demo/) | A live demo for Lens and KueueViz: `demo.sh` (scenarios A–D and a live terminal view) and the presenter guide [`DEMO.md`](./demo/DEMO.md) |

---

## 1. Environment and method

**Cluster:**

- Kubernetes with the Kubeflow Spark Operator.
- Spark 4.0.1 (`docker.io/library/spark:4.0.1`).
- One cohort of two ClusterQueues, both `withinClusterQueue: LowerPriority` and
  `reclaimWithinCohort: Any`.
- One slot is 1 CPU + 896Mi: a 512m executor's charged memory. The baseline cohort has 12 slots.
  Scenarios that need more room raise `nominalQuota`/`borrowingLimit`, as recorded in each scenario
  header.

**Workload.** Every application runs SparkALS (`1500 1500 12 15 200`) with 1 core / 512m for both
driver and executor. With Dynamic Allocation the settings are:

| Setting | Value |
|---|---|
| `minExecutors` | 3 |
| `maxExecutors` | 30 |
| `schedulerBacklogTimeout` | 2s |
| `executorIdleTimeout` | 5s |
| shuffle tracking | on |

Three priority classes are used: `low` (500), `default` (1000) and `high` (1500). A typical scenario
starts two `low` applications, then one `high` application 40s later.

**Capture.** [`scripts/capture.sh`](./scripts/capture.sh) watches every Pod, Workload, SparkApplication
and Event change. It also collects the Kueue and operator logs, the driver logs, and the
ClusterQueue usage every 2–3s.

**Checks:**

- [`scripts/req-check.py`](./scripts/req-check.py) gives the EQM verdicts (R-1..R-13, SC-1/SC-1b/SC-2).
  It reads live Pods directly, not Kueue's charge.
- [`scripts/pp-analyze.py`](./scripts/pp-analyze.py) gives the partial-preemption verdicts (PR-1..PR-7).
  It also reports the spread of each reclaim decision, Spark's own executor remove-reason tally,
  executor failures, and post-completion quota leaks.

**SC-1, SC-1b and SC-2:**

| Check | Meaning |
|---|---|
| SC-1 | The ClusterQueue never charges more than its quota. |
| SC-1b | Ungated live Pods never exceed the cohort's slots. |
| SC-2 | Every running executor has a grant, after a 3s grace for watch skew. |

**Images.** Each round ran on a build of the branch at the commit shown:

| Round | Image | Purpose |
|---|---|---|
| baseline | `096f6ff72`, gate off | Whole-application preemption, for comparison |
| r0 | `ec011894f` | First cluster run of the implementation |
| r1 | `505348177` | P1–P5 after the r0 fix |
| r2 | `a901d16b3` | P1–P5 after the r1 fixes |
| r3 | `18abe488b` | Tailored requirement scenarios, the EQM suite, and reruns |

---

## 2. Scenarios

| Scenario | What it exercises | Setup |
|---|---|---|
| baseline-S4 / S5 | Gate off: today's whole-application preemption | 12 slots |
| P1 | Partial scale-up with `maxPendingPods: 2`; floor = `minExecutors` (3) | 12 slots |
| P2 | As P1, floor 1 (`spark.kueue.preemption.minExecutors: "1"`) | 12 slots |
| P3 / P3R | Atomic scale-up, no pending-Pod cap, floor 1 | 12 slots |
| P4 | As P2 with decommissioning explicitly off | 12 slots |
| P5 | Static elastic victims, 8 executors each, floor 2 | 24 slots |
| P5B | As P5, under contention: the victims exactly fill the cohort | 18 slots |
| P6 | Victims not opted in (static, non-elastic) | 12 slots |
| P7a | First admission with every victim at its floor, fallback `Wait` | 12 slots, CQ annotation |
| P7b | As P7a, fallback `Evict` (the default) | 12 slots |
| P8 | Four victims, to check the spread | 24 slots |
| P9 | A 1s reclaim timeout, forcing the deadline path | 12 slots, CQ annotation |
| REQ S1–S6 | The EQM requirements suite (R-1..R-13) with the gate on | 12 slots |
| P10 | 12 small static elastic victims (driver + 2, floor 1) fill a standalone queue; the preemptor needs 12 slots, so each victim should give exactly 1 | 36 slots ([`queues-tres.yaml`](./setup/queues-tres.yaml)) |
| P11 | 9 victims (driver + 3, floor 1); the preemptor needs 15: each gives 1, then 6 give a second (round-robin) | 36 slots |
| P12 | Classical preemption off (`withinClusterQueue`, `reclaimWithinCohort`: `Never`); a [`PreemptionConfig`](./setup/preemption-config-p12.yaml) selects victims by label. 6 labeled default-priority victims (driver + 3, floor 1) and 4 unlabeled low-priority ones | 36 slots, `ConfigurablePreemptions` gate, `labelKeysToCopy` |

---

## 3. Results

The latest valid run of each scenario is listed. Earlier rounds are in §4. "Restarts" counts
low-priority restarts beyond each application's first start, from the driver Pods each application
created.

| Scenario | Round | Whole evictions | Partial preemptions | Reclaim timeouts | Restarts | SC-1 | SC-1b | SC-2 |
|---|---|---|---|---|---|---|---|---|
| baseline-S4 | gate off | 14 | 0 | – | 13 | ✅ | ❌¹ | ✅ |
| baseline-S5 | gate off | 11 | 0 | – | 11 | ✅ | ❌¹ | ✅ |
| P1 | r2 | **0** | 3 | 0 | **0** | ✅ | ✅ | ✅ |
| P2 | r2 | **0** | 7 | 0 | **0** | ✅ | ✅ | ✅ |
| P3R | r3 | **0** | 4 | 0 | **0** | ✅ | ✅ | ✅ |
| P4 | r2 | **0** | 7 | 0 | **0** | ✅ | ✅ | ✅ |
| P5B | r3 | **0** | 4 | 0 | **0** | ✅ | ✅ | ✅ |
| P6 | r3 | 5 (expected) | 0 | – | 5 | ✅ | ❌¹ | ✅ |
| P7a | r3 | **0** | 0 | – | 0 | ✅ | ✅ | ✅ |
| P7b | r3 | 1 (expected) | 0 | – | 1 | ✅ | ✅ | ✅ |
| P8 | r3 | **0** | 9 | 0 | **0** | ✅ | ✅ | ✅ |
| P9 | r3 | **0** | 6 | 5 (forced) | **0** | ✅ | ❌² | ❌² |
| REQ S1–S3, S6 | r3 | – | – | – | – | ✅ | ✅ | ✅ |
| REQ S4 | r3 rerun | **0** | 2 | 0 | **0** | ✅ | ✅ | ✅ |
| REQ S5 | r3 rerun | **0** | 3 | 0 | **0** | ✅ | ✅ | ✅ |

¹ **Whole-application eviction.** For 1–3s, evicted Pods are still stopping while the preemptor's Pods
start. This happens identically with the gate off, so it is pre-existing behaviour of whole eviction.
P6 shows that partial preemption leaves it unchanged for workloads that did not opt in.

² **The forced timeout.** This is the trade-off design §5.4 accepts. At the deadline the grant is lowered
even though the reclaimed Pods are still stopping. The violations begin at exactly the timeout events,
and last at most the 60s executor grace period. With the default 90s timeout this path was never taken
(P1–P8: 0 timeouts).

### 3.1 Requirements

| Requirement | Verdict | Evidence |
|---|---|---|
| **PR-1** No whole eviction where partial reclaim suffices | ✅ | 0 whole evictions in P1–P5B and P8. P7b's single eviction is a first admission, as designed |
| **PR-2** The driver is never reduced | ✅ | Every application kept one driver per admission, in every run with the gate on |
| **PR-3** Never below the floor | ✅ | Lowest victim grant ≥ floor in every run: floor 3 (P1), 1 (P2/P3R/P4/P8/P9), 2 (P5B), 5 (P7a) |
| **PR-4** Quota released only after reclaimed Pods stop, or at the timeout | ✅ | 0 timeouts with the default 90s, and SC-2 held. P9 exercises the timeout path |
| **PR-5** Recreated executors stay gated | ✅ | SC-2 held in every run without a forced timeout |
| **PR-6** Grow-back never preempts | ✅ | No preemption was issued by a low-priority application's slice in any run |
| **PR-7** Workloads not opted in are unchanged | ✅ | P6: victims not opted in were evicted whole (5 times) with no partial reclaim, and SC-1b behaved as in the gate-off baseline |
| **PR-8** SC-1 and SC-2 hold | ✅ | Every run, except P9's forced timeouts |
| Fallback `Wait` / `Evict` | ✅ | P7a: the first admission waited, with no eviction. P7b: one eviction |
| Spread across victims | ✅ | P8: six decisions of one executor each, rotating across all four victims (2/2/2/3 reclaims each) |
| Quota released when an application finishes | ✅ | No leak in any r2/r3 run. Bug 4 below was the r1 failure |
| R-1..R-13 (EQM) with the gate on | ✅ | S1–S5: every R- check passes. S6: R-9 and R-10 pass |
| Spark decommissioning | ✅ | Reclaimed executors reported "gracefully decommissioned" by Spark. With it off (P4) they report "unexpectedly exited", still with 0 failures. **Shuffle and cached-RDD migration** is shown by [`experiments/shuffle-migration/`](./experiments/shuffle-migration/): every block moved, nothing recomputed |

**Not exercised:**

- **Shuffle migration in the main scenarios.** SparkALS writes no shuffle data, so the main scenarios show
  graceful decommissioning only. The shuffle-migration experiment covers migration.
- **The `pod-deletion-cost` sweep.** Spark 4.0.1 never writes the annotation, so the marked-executor
  sweep ran with every executor at cost 0. It is covered by unit tests only.
- **Metrics (§5.7).** These are not implemented yet.

### 3.2 R-1 and R-4 in preemption runs

`req-check`'s R-1 ("accounting tracks what runs") and R-4 ("capacity returned on scale-down") report
FAIL in many preemption runs, including the gate-off baseline. They pass in every scenario without
preemption (REQ S1–S3), and in the REQ S4/S5 reruns (longest overage 12–15s).

In partial-preemption runs, the longest overage is Kueue holding quota while reclaimed executors
decommission, which takes up to the 60s grace period. PR-4 requires that hold. The checker flags the
gap between the charge and the live Pods without distinguishing a deliberate hold, so these runs are
reported as is rather than relabelled.

### 3.3 Round r4: many victims, configurable preemption, final build

Every scenario was rerun on `adc8c5cbd`, which adds the reclaim grace-period change (`01a46e466`: the
grace period is resolved per reclaim and the timeout derived from it, default 30s + 30s) and fixes 6 and
7 of §4. Results in `results/r4-adc8c5cbd-*`.

| Scenario | Whole evictions | Partial preemptions | Reclaim timeouts | Restarts | SC-1 | SC-1b | SC-2 |
|---|---|---|---|---|---|---|---|
| P1 | **0** | 3 | 0 | **0** | ✅ | ✅ | ✅ |
| P2 | **0** | 4 | 0 | **0** | ✅ | ✅ | ✅ |
| P3 | **0** | 5 | 0 | **0** | ✅ | ✅ | ✅ |
| P3R | **0** | 5 | 0 | **0** | ✅ | ✅ | ✅ |
| P4 | **0** | 11 | 0 | **0** | ✅ | ✅ | ✅ |
| P5 | **0** | 2 | 0 | **0** | ✅ | ✅ | ✅ |
| P5B | **0** | 8 | 0 | **0** | ✅ | ✅ | ✅ |
| P6 | 3 (expected) | 0 | – | 3 | ✅ | ❌¹ | ✅ |
| P7a | **0** | 0 | – | 0 | ✅ | ✅ | ✅ |
| P7b | 1 (expected) | 0 | – | 1 | ✅ | ✅ | ✅ |
| P8 | **0** | 13 | 0 | **0** | ✅ | ✅ | ✅ |
| P9 | **0** | 17 | 15 (forced) | **0** | ✅ | ❌² | ❌² |
| **P10** | **0** | 12, one per victim | 0 | **0** | ✅ | ✅ | ✅ |
| **P11** | **0** | 9: 15 executors, 6 victims gave 2 and 3 gave 1 | 0 | **0** | ✅ | ✅ | ✅ |
| **P12** before the fix (`01a46e466`) | **3** | 0 | – | 3 | ✅ | ✅ | ✅ |
| **P12** | **0** | 6, each victim 3 → 1 | 0 | **0** | ✅ | ✅ | ✅ |
| REQ S1–S3 | – | – | – | – | ✅ | ✅ | ✅ |
| REQ S4 | **0** | 2 | 0 | **0** | ✅ | ✅ | ✅ |
| REQ S5 | **0** | 2 | 0 | **0** | ✅ | ✅ | ✅ |

PR-1..PR-8 hold in every scenario, as in r3. REQ S6 (plain Job, static SparkApplication, elastic Job)
passes R-9 and R-10.

- **Many victims (P10, P11).** The spread rule held at scale: with a shortfall of 12 across 12 victims,
  each gave exactly one executor in a single decision; with 15 across 9, every victim gave one before any
  gave a second.
- **Configurable preemption (P12).** On `01a46e466` the preemptor was admitted by evicting 3 labeled
  applications whole ("preemption config batch-pool rule urgent-takes-from-batch-pool"), although all 6
  labeled applications had opted in and had 2 executors above their floor each. The unlabeled
  low-priority applications were untouched in both runs: the `candidateSelectors`, not priority, chose
  the victims. With the fix, the same 12 slots came from 2 executors of each labeled application. See
  bug 6.
- **REQ S3's R-12** reported 3s of idle capacity against a 2s threshold. The same value appears on
  `096f6ff72`, before partial preemption existed, so it is the elastic scale-up's existing timing, not a
  regression. REQ S4/S5's R-1/R-4 show the reclaim hold of §3.2 (36–38s).

---

## 4. Bugs found on the cluster

| # | Found in | Symptom | Cause | Fix |
|---|---|---|---|---|
| 1 | r0 P1 | A one-executor scale-up evicted a whole low-priority application | With every victim at its floor, a scale-up fell back to whole eviction | `505348177`, narrowed in `18abe488b` (row 5) |
| 2 | r0 P1 | Two "reclaim complete" events for one reclaim | A pass that read a stale Workload repeated the completion | `505348177`: a stale pass only cleans up |
| 3 | r1 P3, r1 P2 | SC-2 broken (4 executors on a grant of 2, 13 live Pods in 12 slots). A reclaim/ungate loop that ended in a timeout | The ungater read the old grant in the gap between the reclaimed Pods stopping and the grant being lowered | `a1ac5c3af`: cap ungating at the reclaim target while one is outstanding |
| 4 | r1 P5 | A completed application kept 7 slots for 6 minutes | **Already in Kueue.** A finished elastic job finished only the Workload it picked: the pending scale-up slice, not the admitted one. Reproduced with the gate off | `a901d16b3`: finish every slice of a finished elastic job |
| 5 | scenario design (PR-7) | Fix 1 also stopped any elastic integration's scale-up from preempting victims that never opted in | The rule applied to every victim | `18abe488b`: the scale-up fallback (`WholeOnlyUndeclared`) spares only opted-in victims |
| 6 | r4 P12 | With preemption chosen only by a `PreemptionConfig`, opted-in victims were evicted whole | The partial planner drew only on the classical candidates; the config's candidates reached only the whole-eviction fallback | `adc8c5cbd`: the planner also takes the config's `Always` candidates, and its `InsufficientQuota` candidates once the baseline is at its floor |
| 7 | r4 P10 | The preemptor's `PartiallyPreemptedWorkload` event named only 1 of 12 victims | `events/v1` dedups on the regarding and related objects and the reason, not the message | `38480e1cb`: the victim is the event's related object |

**Analyzer corrections made along the way.** None of these change the results above:

- Events outlive a run (1h TTL) and runs reuse application names, so events now count only if they name
  one of the run's own Workloads.
- Each application's terminal time is its first terminal state.
- `edge-analyze.py`'s `suspends=` counts both SUSPENDING and SUSPENDED, so it reads about twice the
  number of restarts.
- (r4) `pp-analyze.py`'s spread line now reads the victims' `PartiallyPreempted` events, which bug 7
  had made the only complete record.
- (r4) The capture records each event's watch type and object UID. An event expiring after its 1h TTL
  arrives as a deletion, and was being counted as a new one; `pp-analyze.py` skips deletions and events
  of applications outside the run, and reports restarts as extra driver Pods. The earlier "restarts
  (PENDING_RERUN)" total also counted each application's first start.

---

## 5. Caveats

- **A Wi-Fi drop invalidated the first r3 run of REQ S4 and S5.** At 16:34:40 UTC the node running every
  Spark Pod lost its Wi-Fi connection and went `NodeStatusUnknown`. Its Pods were evicted 300s later, and
  the Kueue controller was rescheduled onto another node. No Kueue errors were logged.

  Both scenarios were rerun once the node was back, on the same image (captures `REQ-1012-1301-*`), and
  pass every check. The archived S4/S5 results are the reruns.
- **One executor failure each in r2 P1 and the S5 rerun.** In r2 P1, Spark's own Dynamic Allocation
  scale-down removed an executor 3s after it started, before it registered with the driver; no reclaim was
  involved. The S5 rerun's failure was on the high-priority application, which is never a reclaim victim,
  so it has the same Spark-side cause. It is one failure
  against a limit of 60, and is the design's §8 risk arising from Spark itself.
- **r2 P3's capture watch restarted once.** P3R is the clean rerun.
- **r2 P5 exercised no reclaim.** The static victims finished before the preemptor needed room. P5B is
  the contended version.

---

## 6. Reproducing

1. Install Kueue built from the branch with `--feature-gates=SparkPartialPreemption=true` alongside the
   elastic gates, the Kubeflow Spark Operator, and [`setup/queues.yaml`](./setup/queues.yaml).
2. Copy [`scripts/`](./scripts/) into a working directory and create `runs/` beside the scripts.
3. Run the suites:
   - `./run-pp-suite.sh [P1..P5]` for the main scenarios
   - `./run-pp-tailored.sh [P3R P5B P6 P7a P7b P8 P9 REQ]` for the requirement scenarios and the EQM suite
   - `./run-req-suite.sh` for the EQM suite alone
   - `./run-pp-many.sh [P10 P11]` and `./run-pp-configurable.sh` (P12), with
     [`setup/queues-tres.yaml`](./setup/queues-tres.yaml) and
     [`setup/preemption-config-p12.yaml`](./setup/preemption-config-p12.yaml) copied beside the scripts.
     P12 also needs the `ConfigurablePreemptions` gate, the `PreemptionConfig` CRD
     (`config/components/crd/alpha/bases/`) and `integrations.labelKeysToCopy: [preemption-pool,
     preemption-role]` in Kueue's configuration. It sets and restores `cluster-queue-tres`'s preemption
     policy itself.

   Each scenario's manifest is in [`scenarios/`](./scenarios/). The suites generate them with
   `gen-apps.py` and apply quotas and ClusterQueue annotations as recorded in each header.
4. Regenerate the committed summaries from local captures with
   `./archive-results.sh runs ../results [NAME_REGEX]`, e.g. `'^r4-'` for one round.

   The raw captures are not committed. They contain full controller and driver logs, and cluster
   addresses.
