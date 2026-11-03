#!/usr/bin/env python3
"""Live view for run-da-varying-max.sh (demo part 7): one row per Dynamic Allocation app sharing one queue,
with its maximum, its driver, a dot per executor (running, starting, waiting for quota, stopping), how many
Workload slices Kueue created as it resized, and what it is doing now.

  da-view.py PODS_JSON WORKLOADS_JSON CLUSTERQUEUE_JSON QUOTA MAXES

MAXES is "app=n,app=n...": each app's spark.dynamicAllocation.maxExecutors, in display order.
The headline shows the slots the apps' Pods hold (drivers and executors that are neither gated nor
finished, including those still terminating), and the CPU Kueue has reserved in the ClusterQueue
(status.flavorsReservation; 1 slot = 1 CPU). While Kueue replaces one Workload slice of an app with the
next, its reservation briefly counts both slices, so it can read above the quota although the Pods don't.
Optional environment: VIEW_APP_LABEL, the Pod label naming the app (default the Kubeflow Spark Operator's
sparkoperator.k8s.io/app-name); VIEW_STATE, a JSON file in which the peaks seen so far are kept for the
summary. NO_COLOR (any value) turns colours off.
"""
import collections
import json
import os
import sys

pods_json, wls_json, cq_json, quota, maxes = sys.argv[1:6]
quota = int(quota)
maxes = {k: int(v) for k, v in (t.split("=", 1) for t in maxes.split(",") if "=" in t)}
APP_LABEL = os.environ.get("VIEW_APP_LABEL") or "sparkoperator.k8s.io/app-name"
STATE = os.environ.get("VIEW_STATE")
W = max(len("executors"), 2 * max(maxes.values() or [1]) + 4)   # executor-dots column
AT_MAX, WAITING, RESET = ("", "", "") if os.environ.get("NO_COLOR") else ("\033[1;32m", "\033[33m", "\033[0m")


def items(path):
    try:
        return json.load(open(path))["items"]
    except (OSError, ValueError, KeyError):
        return []


c = collections.defaultdict(collections.Counter)
for p in items(pods_json):
    labels = p["metadata"].get("labels", {})
    app = labels.get(APP_LABEL, "?")
    phase = p["status"].get("phase")
    if labels.get("spark-role") == "driver":
        c[app]["driver"] += 1 if phase == "Running" else 0
        c[app]["held"] += 1 if phase not in ("Succeeded", "Failed") else 0
        c[app]["driver_done"] += 1 if phase == "Succeeded" else 0
        continue
    if phase in ("Succeeded", "Failed"):
        continue
    gated = any(g["name"].startswith("kueue.x-k8s.io/") for g in p["spec"].get("schedulingGates") or [])
    c[app]["held"] += 0 if gated else 1
    if p["metadata"].get("deletionTimestamp"):
        c[app]["stopping"] += 1
    elif gated:
        c[app]["waiting"] += 1
    elif phase == "Running":
        c[app]["running"] += 1
    else:
        c[app]["starting"] += 1

slices, finished, active = collections.Counter(), set(), set()
for w in items(wls_json):
    owner = next((o["name"] for o in w["metadata"].get("ownerReferences", []) if o.get("controller")), "")
    slices[owner] += 1
    st = w.get("status") or {}
    conds = {x["type"]: x["status"] for x in st.get("conditions", [])}
    if conds.get("Finished") == "True":
        finished.add(owner)
        continue
    active.add(owner)

held = sum(c[a]["held"] for a in maxes)
reserved = 0.0
try:
    for f in json.load(open(cq_json)).get("status", {}).get("flavorsReservation") or []:
        for r in f.get("resources", []):
            if r["name"] == "cpu":
                q = str(r.get("total", "0"))
                reserved += float(q[:-1]) / 1000 if q.endswith("m") else float(q)
except (OSError, ValueError):
    pass

state = {}
if STATE:
    try:
        state = json.load(open(STATE))
    except (OSError, ValueError):
        state = {}
peak = state.setdefault("peak_running", {})
waited = state.setdefault("waited", {})
state["peak_held"] = max(state.get("peak_held", 0), held)
state["peak_reserved"] = max(state.get("peak_reserved", 0), reserved)
if held > quota:
    state["held_over_quota"] = state.get("held_over_quota", 0) + 1
if reserved > quota:
    state["reserved_over_quota"] = state.get("reserved_over_quota", 0) + 1

print(f"  {len(maxes)} apps with Dynamic Allocation sharing one queue: Pods hold {held} of {quota} slots,"
      f" Kueue reserved {reserved:g}  (1 slot = driver or executor = 1 CPU)")
print()
print(f"  {'app':8} {'max':>3}  {'driver':6}  {'executors':{W}} {'slices':>6}  status")
for a, mx in maxes.items():
    k = c[a]
    done = k["driver_done"] or (a in finished and a not in active)
    dots = " ".join(["●"] * k["running"] + ["○"] * k["starting"] + ["◌"] * k["waiting"] + ["✕"] * k["stopping"])
    drv = "✓" if k["driver"] else ("done" if done else "○")
    peak[a] = max(peak.get(a, 0), k["running"])
    if k["waiting"]:
        waited[a] = True
    color = ""
    if done:
        status = f"finished (peaked at {peak[a]} of {mx})"
    elif not k["driver"] and not slices[a]:
        status = "submitted"
    elif not k["driver"]:
        status = "starting the driver"
    elif k["waiting"]:
        status = f"waiting for quota: {k['waiting']} more executor{'s' * (k['waiting'] > 1)} gated"
        color = WAITING
    elif k["stopping"]:
        status = f"scaling down: {k['stopping']} idle executor{'s' * (k['stopping'] > 1)} removed"
    elif k["starting"]:
        status = f"scaling up: {k['starting']} starting"
    elif k["running"] >= mx:
        status = f"at its maximum of {mx}"
        color = AT_MAX
    else:
        status = f"running on {k['running']} of up to {mx}"
    text = f"{a:8} {mx:>3}  {drv:6}  {dots:{W}} {slices[a]:>6}  {status}"
    print(f"  {color}{text}{RESET if color else ''}")
print()
print("  ● running   ○ starting   ◌ waiting for quota (gated)   ✕ stopping (removed by Dynamic Allocation)")
print(f"  {AT_MAX}green: at its maximum{RESET}   {WAITING}yellow: executors waiting for quota{RESET}")
if STATE:
    json.dump(state, open(STATE, "w"))
