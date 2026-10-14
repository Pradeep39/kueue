#!/usr/bin/env python3
"""Live table for run-p11-demo.sh: per app, executors running / granted / gated / being deleted.

  p11-table.py PODS_JSON WORKLOADS_JSON
"""
import json, sys, collections
def items(path):
    try:
        return json.load(open(path))["items"]
    except (OSError, ValueError, KeyError):
        return []
pods = items(sys.argv[1])
wls = items(sys.argv[2])
c = collections.defaultdict(collections.Counter)
for p in pods:
    app = p["metadata"].get("labels", {}).get("sparkoperator.k8s.io/app-name", "?")
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
apps = sorted(set(c) | set(grant), key=lambda a: (a == "als-hi", len(a), a))
print(f"  {"app":10} {"priority":8} {"driver":>6} {"running":>7} {"granted":>7} {"gated":>5} {"deleting":>8}  reclaim")
used = 0
for a in apps:
    k = c[a]; g = grant.get(a)
    used += (g or 0) + (1 if g is not None else 0)
    note = f"-> {pp[a]}" if a in pp else ""
    print(f"  {a:10} {prio.get(a, ""):8} {k["driver"]:>6} {k["running"]:>7} {("-" if g is None else g):>7} {k["gated"]:>5} {k["deleting"]:>8}  {note}")
print(f"  slots granted: {used} of 36")
