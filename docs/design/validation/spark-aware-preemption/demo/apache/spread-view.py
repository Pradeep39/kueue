#!/usr/bin/env python3
"""Live view for run-partial-preemption-spread.sh (demo parts 3 and 4): one row per low-priority app,
with its driver, a dot per executor (running, waiting for quota, stopping), how many executors it gave
to the urgent app, and what it is doing now.

  spread-view.py PODS_JSON WORKLOADS_JSON EVENTS_JSON QUOTA EXECS URGENT_EXECS TRIGGER [TAGS]

TRIGGER is the UTC time the urgent app was submitted ("" before that); only reclaims after it count.
TAGS (optional, part 5) is "app=text,app=text...": an extra column after the app name, e.g. its
priority and whether a PreemptionConfig rule makes it a candidate.
"""
import collections
import json
import re
import sys

pods_json, wls_json, events_json, quota, execs, urgent_execs, trigger = sys.argv[1:8]
tags = dict(t.split("=", 1) for t in (sys.argv[8] if len(sys.argv) > 8 else "").split(",") if "=" in t)
T = max([len(v) for v in tags.values()] + [len("candidate")]) if tags else 0
quota, execs, urgent_execs = int(quota), int(execs), int(urgent_execs)
W = max(len("executors"), 2 * execs + 4)   # width of the executor-dots column


def items(path):
    try:
        return json.load(open(path))["items"]
    except (OSError, ValueError, KeyError):
        return []


pods, wls, events = items(pods_json), items(wls_json), items(events_json)

c = collections.defaultdict(collections.Counter)
for p in pods:
    labels = p["metadata"].get("labels", {})
    app = labels.get("spark.operator/spark-app-name", "?")
    phase = p["status"].get("phase")
    if labels.get("spark-role") == "driver":
        c[app]["driver"] += 1 if phase == "Running" else 0
        c[app]["driver_done"] += 1 if phase == "Succeeded" else 0
        continue
    if phase in ("Succeeded", "Failed"):
        continue
    if p["metadata"].get("deletionTimestamp"):
        c[app]["stopping"] += 1
    elif any(g["name"].startswith("kueue.x-k8s.io/") for g in p["spec"].get("schedulingGates") or []):
        c[app]["waiting"] += 1
    elif phase == "Running":
        c[app]["running"] += 1
    else:
        c[app]["starting"] += 1

granted, urgent_admitted = 0, False
finished, active = set(), set()
for w in wls:
    owner = next((o["name"] for o in w["metadata"].get("ownerReferences", []) if o.get("controller")), "")
    st = w.get("status") or {}
    conds = {x["type"]: x["status"] for x in st.get("conditions", [])}
    if conds.get("Finished") == "True":
        finished.add(owner)
    else:
        active.add(owner)
    if st.get("admission") and conds.get("Admitted") == "True" and conds.get("Finished") != "True":
        for ps in st["admission"].get("podSetAssignments", []):
            granted += ps.get("count") or 0
        if owner == "als-hi":
            urgent_admitted = True

gave, evicted = collections.Counter(), collections.Counter()
for e in events:
    t = e.get("lastTimestamp") or e.get("eventTime") or ""
    if not trigger or t[:19] < trigger[:19] or e["involvedObject"].get("kind") != "Workload":
        continue
    app = re.sub(r"^sparkapplication-|-[0-9a-f]{5}$", "", e["involvedObject"]["name"])
    m = re.search(r"Partially preempted (\d+) Pod", e.get("message") or "")
    if e["reason"] == "PartiallyPreempted" and m:
        gave[app] += int(m.group(1))
    elif e["reason"] == "Preempted":
        evicted[app] += 1

u = c.get("als-hi", collections.Counter())
if not trigger:
    urgent = "not submitted yet"
elif u["driver_done"] or ("als-hi" in finished and "als-hi" not in active):
    urgent = "finished"
elif not urgent_admitted:
    urgent = "waiting for quota"
else:
    urgent = f"running, {u['running']} of {urgent_execs} executors"
print(f"  urgent app (high priority, needs {urgent_execs + 1} slots): {urgent:30} queue: {granted} of {quota} slots granted")
print()
print(f"  {'app':9} " + (f"{'candidate':{T}}  " if tags else "") + f"{'driver':6}  {'executors':{W}} {'gave':>4}  status")
victims = sorted((a for a in c if a.startswith("als-lo")), key=lambda a: int(a[6:]))
for a in victims:
    k = c[a]
    dots = " ".join(["●"] * k["running"] + ["○"] * k["starting"] + ["◌"] * k["waiting"] + ["✕"] * k["stopping"])
    drv = "✓" if k["driver"] else ("done" if k["driver_done"] else "✗")
    g = gave[a]
    if evicted[a]:
        status = "EVICTED WHOLE"
    elif k["driver_done"] or (a in finished and a not in active):
        status = "finished"
    elif k["stopping"]:
        status = f"giving {k['stopping']} back (decommissioning)"
    elif k["waiting"] and g:
        status = f"waiting to get {k['waiting']} executor{'s' * (k['waiting'] > 1)} back (grow-back)"
    elif k["waiting"]:
        status = "waiting for quota"
    elif g and k["running"] >= execs:
        # The reclaim event lands a moment before Kueue deletes the Pods: until the urgent app is
        # done, a full app that gave executors is about to give them back, not grown back.
        status = "back to full size" if urgent == "finished" else f"giving {g} back (reclaim decided)"
    elif g:
        status = f"running on {k['running']} (gave {g})"
    else:
        status = "running"
    print(f"  {a:9} " + (f"{tags.get(a, ''):{T}}  " if tags else "") + f"{drv:6}  {dots:{W}} {(g if trigger else '-'):>4}  {status}")
print()
print("  ● running   ○ starting   ◌ waiting for quota   ✕ stopping (reclaimed)")
