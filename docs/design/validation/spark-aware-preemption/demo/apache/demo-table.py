#!/usr/bin/env python3
"""Live table for run-partial-preemption-spread.sh: per app, executors running / granted / gated / being deleted.

  demo-table.py PODS_JSON WORKLOADS_JSON [QUOTA [EVENTS_JSON SINCE]]

With EVENTS_JSON and SINCE (UTC), rows are coloured: the admitted high-priority app (the preemptor)
in green, every app preempted whole or in part since SINCE (a preemptee) in yellow. NO_COLOR turns
the colours off.
"""
import json, os, re, sys, collections
def items(path):
    try:
        return json.load(open(path))["items"]
    except (OSError, ValueError, KeyError):
        return []
pods = items(sys.argv[1])
wls = items(sys.argv[2])
c = collections.defaultdict(collections.Counter)
for p in pods:
    app = p["metadata"].get("labels", {}).get("spark.operator/spark-app-name", "?")
    if p["metadata"]["labels"].get("spark-role") != "executor":
        c[app]["driver"] += 1 if p["status"].get("phase") == "Running" else 0
        continue
    if p["status"].get("phase") in ("Succeeded", "Failed"):
        continue
    if p["metadata"].get("deletionTimestamp"):
        c[app]["deleting"] += 1
    elif any(g["name"].startswith("kueue.x-k8s.io/") for g in p["spec"].get("schedulingGates") or []):
        c[app]["gated"] += 1
    elif p["status"].get("phase") == "Running":
        c[app]["running"] += 1
    else:
        c[app]["pending"] += 1
grant, prio, pp = {}, {}, {}
for w in wls:
    owner = next((o["name"] for o in w["metadata"].get("ownerReferences", []) if o.get("controller")), None)
    if not owner:
        continue
    prio.setdefault(owner, w["spec"].get("priorityClassRef", {}).get("name") or w["spec"].get("priorityClassName") or "")
    adm = (w.get("status") or {}).get("admission")
    conds = {x["type"]: x["status"] for x in (w.get("status") or {}).get("conditions", [])}
    if adm and conds.get("Admitted") == "True" and conds.get("Finished") != "True":
        for ps in adm.get("podSetAssignments", []):
            if ps["name"] == "executor":
                grant[owner] = ps.get("count")
        ann = w["metadata"].get("annotations", {})
        if "kueue.x-k8s.io/partial-preemption-target" in ann:
            pp[owner] = ann["kueue.x-k8s.io/partial-preemption-target"]
PREEMPTOR, PREEMPTEE, RESET = ("", "", "") if os.environ.get("NO_COLOR") else ("\033[1;32m", "\033[33m", "\033[0m")
preempted = set()
if len(sys.argv) > 5:
    since = sys.argv[5]
    for e in items(sys.argv[4]):
        t = e.get("lastTimestamp") or e.get("eventTime") or ""
        if t[:19] >= since[:19] and e["involvedObject"].get("kind") == "Workload" and e["reason"] in ("Preempted", "PartiallyPreempted"):
            preempted.add(re.sub(r"^sparkapplication-|-[0-9a-f]{5}$", "", e["involvedObject"]["name"]))
# An app evicted whole has no Pods and no grant; keep its row while its Workload exists.
apps = sorted(set(c) | set(grant) | (preempted & set(prio)), key=lambda a: (a == "als-hi", len(a), a))
def color(a):
    if len(sys.argv) <= 5:
        return ""
    if prio.get(a) == "high" and a in grant:
        return PREEMPTOR
    return PREEMPTEE if a in preempted else ""
print(f"  {"app":10} {"priority":8} {"driver":>6} {"running":>7} {"granted":>7} {"gated":>5} {"deleting":>8}  reclaim")
used = 0
for a in apps:
    k = c[a]; g = grant.get(a)
    used += (g or 0) + (1 if g is not None else 0)
    note = f"-> {pp[a]}" if a in pp else ""
    col = color(a)
    print(f"  {col}{a:10} {prio.get(a, ""):8} {k["driver"]:>6} {k["running"]:>7} {("-" if g is None else g):>7} {k["gated"]:>5} {k["deleting"]:>8}  {note}{RESET if col else ""}")
quota = sys.argv[3] if len(sys.argv) > 3 else "36"
print(f"  slots granted: {used} of {quota}")
if len(sys.argv) > 5 and (preempted or any(prio.get(a) == "high" and a in grant for a in apps)):
    print(f"  {PREEMPTOR}green: preemptor (the urgent app){RESET}   {PREEMPTEE}yellow: preemptee (gave capacity back){RESET}")
