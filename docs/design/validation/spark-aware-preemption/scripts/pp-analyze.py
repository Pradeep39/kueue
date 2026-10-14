#!/usr/bin/env python3
"""Partial-preemption scorecard for one capture.sh run directory.

  pp-analyze.py [--floor N] [--expect-whole N] [--victims PREFIX] RUN_DIR [RUN_DIR...]

With --floor/--expect-whole it also prints requirement verdicts (design doc §4: PR-1..PR-6),
the spread of each partial-preemption decision, and post-completion quota leaks.

Per run: whole vs partial preemptions, reclaim outcomes, restarts (PENDING_RERUN), executor Pods
deleted, per-app completion, and from driver logs: decommissions and executor failures.
"""
import collections
import glob
import json
import os
import re
import sys
from datetime import datetime


def ts(s):
    return datetime.fromisoformat(s.replace("Z", "+00:00")).timestamp()


def jsonl(path):
    if not os.path.exists(path):
        return
    with open(path) as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            try:
                yield json.loads(line)
            except json.JSONDecodeError:
                continue


def verdict(name, ok, detail):
    print(f"   {'PASS' if ok else 'FAIL'}  {name:58} {detail}")


def analyze(d, args=None):
    start = float(open(f"{d}/start.epoch").read()) if os.path.exists(f"{d}/start.epoch") else None
    owner = {}  # workload name -> app
    for w in jsonl(f"{d}/watch/workloads.jsonl"):
        if w.get("name") and w.get("owner"):
            owner[w["name"]] = w["owner"]

    # This run's applications. Application-level events (the operator's) name the application,
    # and events outlive a run (1h TTL), so only those naming one of these count.
    run_apps = {s["name"] for s in jsonl(f"{d}/watch/sparkapps.jsonl") if s.get("name")}

    decisions = collections.defaultdict(list)  # (preemptor workload, time) -> [(victim app, pods)]
    ev = collections.Counter()
    per_app = collections.defaultdict(collections.Counter)
    seen = set()
    for e in jsonl(f"{d}/watch/events.jsonl"):
        r = e.get("reason")
        if not r:
            continue
        # A DELETED notification is an old event expiring (1h TTL), not a new occurrence. Captures
        # before the watch type was recorded show these with the expiry time; see run_apps below.
        if e.get("type") == "DELETED":
            continue
        obj0 = e.get("obj") or ""
        if e.get("kind") == "SparkApplication" and obj0 not in run_apps:
            continue
        key = (e.get("obj"), r, e.get("msg"), e.get("count"))
        if key in seen:  # the watch can replay an event on resync
            continue
        seen.add(key)
        # Events outlive a run (1h TTL) and runs reuse application names, so an event counts only
        # if it names a Workload of this run: as its object, or inside a SparkApplication event.
        obj = e.get("obj") or ""
        if obj.startswith("sparkapplication-") and obj not in owner:
            continue
        wm = re.search(r"Workload \S+?/(sparkapplication-\S+?):", e.get("msg") or "")
        if wm and wm.group(1) not in owner:
            continue
        if start and e.get("t") and ts(e["t"]) < start - 5:
            continue
        app = owner.get(obj, obj)
        # Decisions come from the victims' PartiallyPreempted events, one per victim. The preemptor's
        # PartiallyPreemptedWorkload events are no use for this: events/v1 dedups on the regarding
        # object and reason, not the message, so a preemptor taking from N victims in one cycle
        # records one event (kept as a series) naming only the first.
        if r == "PartiallyPreempted" and e.get("msg"):
            m = re.search(r"Partially preempted (\d+) Pod\(s\) of PodSet \S+, from \d+ to \d+, for workload \S+?/(\S+?)\.", e["msg"])
            if m:
                decisions[(m.group(2), e.get("t"))].append((app, int(m.group(1))))
        if r in ("Preempted", "PartiallyPreempted", "PartialPreemptionComplete", "PartialPreemptionTimedOut",
                 "SparkApplicationPendingRerun", "PreemptedWorkload", "PartiallyPreemptedWorkload"):
            ev[r] += 1
            per_app[app][r] += 1

    # Executor Pods deleted (first time each Pod is seen deleting), per app; driver Pods created.
    deleting, drivers = set(), collections.defaultdict(set)
    exec_deleted = collections.Counter()
    for p in jsonl(f"{d}/watch/pods.jsonl"):
        if p.get("type") == "RESYNC" or not p.get("name"):
            continue
        if p.get("role") == "driver":
            drivers[p["app"]].add(p.get("created"))
        if p.get("role") == "executor" and p.get("deleting") and p["name"] not in deleting:
            deleting.add(p["name"])
            exec_deleted[p["app"]] += 1

    # Application end state and time.
    final = {}
    for s in jsonl(f"{d}/watch/sparkapps.jsonl"):
        if s.get("name"):
            prev = final.get(s["name"])
            # Keep the first terminal state: a watch re-list replays it with a later timestamp.
            if prev and prev[0] in ("COMPLETED", "FAILED"):
                continue
            final[s["name"]] = (s.get("state"), s.get("t"))

    # Driver logs: decommission hand-offs and executor failures.
    decom = collections.Counter()
    failed = collections.Counter()
    migrated = collections.Counter()
    reasons = {}      # app -> last ExecutorMonitor remove-reason tally
    decom_on = {}     # app -> whether Spark added the decommission preStop hook
    for f in glob.glob(f"{d}/logs/driver-*.log"):
        app = re.sub(r"^driver-|-driver\.log$", "", os.path.basename(f))
        for line in open(f, errors="replace"):
            low = line.lower()
            m = re.search(r"gracefully decommissioned: (\d+), decommission unfinished: (\d+), driver killed: (\d+), unexpectedly exited: (\d+)", line)
            if m:
                reasons[app] = tuple(int(x) for x in m.groups())
            if "Adding decommission script to lifecycle" in line:
                decom_on[app] = True
            elif "Decommissioning not enabled" in line:
                decom_on.setdefault(app, False)
            if "new failed executors" in low:
                m = re.search(r"(\d+) new failed executors", line)
                failed[app] += int(m.group(1)) if m else 1
            if "migrat" in low and "block" in low:
                migrated[app] += 1

    apps = sorted(set(final) | set(drivers))
    print(f"== {os.path.basename(d.rstrip('/'))}")
    print(f"   whole preemptions {ev['Preempted']}, partial preemptions {ev['PartiallyPreempted']}, "
          f"reclaims completed {ev['PartialPreemptionComplete']}, timed out {ev['PartialPreemptionTimedOut']}, "
          f"restarts (extra driver Pods) {sum(max(0, len(v) - 1) for v in drivers.values())}, "
          f"PENDING_RERUN events {ev['SparkApplicationPendingRerun']} (one per app is its first start)")
    print(f"   {'app':10} {'state':12} {'end(s)':>6} {'drivers':>7} {'whole':>5} {'partial':>7} {'reruns':>6} "
          f"{'execDel':>7} {'decom':>5} {'graceful/unfinished/killed/unexpected':>38} {'failedEx':>8}")
    for a in apps:
        state, t = final.get(a, ("-", None))
        end = f"{ts(t) - start:.0f}" if (t and start and state in ("COMPLETED", "FAILED")) else "-"
        c = per_app[a]
        print(f"   {a:10} {str(state):12} {end:>6} {len(drivers[a]):>7} {c['Preempted']:>5} {c['PartiallyPreempted']:>7} "
              f"{c['SparkApplicationPendingRerun']:>6} {exec_deleted[a]:>7} {('on' if decom_on.get(a) else 'off'):>5} "
              f"{'/'.join(map(str, reasons.get(a, ('-',)*4))):>38} {failed[a]:>8}")


    if args is None or (args.floor is None and args.expect_whole is None and not args.expect_unchanged and args.min_whole is None):
        return
    vic = [a for a in apps if a.startswith(args.victims)]
    print("   -- requirements")
    whole = ev["Preempted"]
    exp = args.expect_whole if args.expect_whole is not None else 0
    if args.expect_unchanged:
        verdict("PR-7 victims not opted in are preempted as before", whole >= 1 and ev["PartiallyPreempted"] == 0,
                f"{whole} whole evictions, {ev['PartiallyPreempted']} partial (want >=1 whole, 0 partial)")
    elif args.min_whole is not None:
        verdict("PR-1 whole evictions only for a first admission", whole >= args.min_whole,
                f"{whole} whole evictions (want >= {args.min_whole})")
    else:
        verdict("PR-1 whole evictions only where expected", whole == exp, f"{whole} whole evictions, expected {exp}")
    bad = {a: len(drivers[a]) for a in apps if len(drivers[a]) != 1 + per_app[a]["Preempted"]}
    verdict("PR-2 driver never reduced (1 driver per life)", not bad,
            "every app kept one driver per admission" if not bad else f"extra drivers {bad}")
    if args.floor is not None:
        low = {}
        for w in jsonl(f"{d}/watch/workloads.jsonl"):
            a = w.get("owner")
            c = w.get("cond") or {}
            g = (w.get("granted") or {}).get("executor")
            if a in vic and g is not None and c.get("QuotaReserved", {}).get("s") == "True" and c.get("Finished", {}).get("s") != "True":
                low[a] = min(low.get(a, g), g)
        okf = all(v >= args.floor for v in low.values())
        verdict(f"PR-3 victim grant never below the floor ({args.floor})", okf, f"lowest executor grant per victim {low}")
    tout = ev["PartialPreemptionTimedOut"]
    verdict("PR-4 reclaims released quota after Pods stopped (no timeout)", tout == 0 or args.expect_timeout,
            f"{ev['PartialPreemptionComplete']} completed, {tout} timed out" + (" (timeouts expected)" if args.expect_timeout else ""))
    pre = {a: per_app[a]["PreemptedWorkload"] + per_app[a]["PartiallyPreemptedWorkload"] for a in vic}
    verdict("PR-6 low-priority (grow-back) slices never preempt", not any(pre.values()), f"preemptions issued by victims {pre}")
    # Quota leak after completion: time from an app completing until all its Workloads are finished.
    states, last = collections.defaultdict(dict), {}
    for w in jsonl(f"{d}/watch/workloads.jsonl"):
        if w.get("owner") and w.get("type") != "RESYNC":
            c = w.get("cond") or {}
            held = c.get("QuotaReserved", {}).get("s") == "True" and c.get("Finished", {}).get("s") != "True" and w.get("type") != "DELETED"
            states[w["owner"]][w["name"]] = held
            if not any(states[w["owner"]].values()):
                last.setdefault(w["owner"], []).append(w["t"])
    leaks = {}
    end = float(open(f"{d}/end.epoch").read()) if os.path.exists(f"{d}/end.epoch") else None
    for a, (st, t) in final.items():
        if st in ("COMPLETED", "FAILED") and t:
            after = [x for x in last.get(a, []) if x >= t]
            if any(states[a].values()) and not after:
                leaks[a] = f">{(end - ts(t)):.0f}s (never released)"
            elif after:
                gap = ts(after[0]) - ts(t)
                if gap > 5:
                    leaks[a] = f"{gap:.0f}s"
    verdict("Quota released when an application finishes (<=5s)", not leaks, f"leaks {leaks}" if leaks else "all slices finished promptly")
    if decisions:
        per_victim_max = max(n for v in decisions.values() for _, n in v)
        multi = sum(1 for v in decisions.values() if len({a for a, _ in v}) > 1)
        dist = collections.Counter(n for v in decisions.values() for _, n in v)
        print(f"   INFO  spread: {len(decisions)} partial decisions, {multi} spanning >1 victim; Pods per victim per decision {dict(sorted(dist.items()))} (max {per_victim_max})")


import argparse
ap = argparse.ArgumentParser()
ap.add_argument("dirs", nargs="+")
ap.add_argument("--floor", type=int)
ap.add_argument("--expect-whole", type=int)
ap.add_argument("--expect-timeout", action="store_true")
ap.add_argument("--victims", default="als-lo")
ap.add_argument("--expect-unchanged", action="store_true", help="victims not opted in: want whole, no partial")
ap.add_argument("--min-whole", type=int, help="want at least N whole evictions")
a = ap.parse_args()
for d in a.dirs:
    analyze(d, a)
