#!/usr/bin/env python3
"""Live view for run-shuffle-resiliency.sh: one row per victim executor with its shuffle data, its state
and, once Kueue reclaims it, what it handed over to the surviving executors (from its own log).

  shuffle-view.py RUN_DIR PODS_JSON DU_FILE URGENT_STATE URGENT_EXECUTORS VICTIM_STATE THRESH_MIB [URGENT_DRIVER_PHASE]

RUN_DIR holds exec-logs/<pod>.log and driver.log, streamed by the runner. DU_FILE has one
"<pod> <KiB of shuffle files>" line per running executor. It prints the table, then one status line,
and exits 0 once every running executor holds at least THRESH_MIB (the "ready" hint).
Once the urgent app's driver exists (URGENT_DRIVER_PHASE set, so Kueue admitted it), it gets a row in
green (the preemptor), and the reclaimed executors are shown in yellow (preemptees). NO_COLOR turns the
colours off.
"""
import json
import os
import re
import sys

run, pods_json, du_file, urgent_state, urgent_execs, victim_state, thresh = sys.argv[1:8]
urgent_driver = sys.argv[8] if len(sys.argv) > 8 else ""
thresh = int(thresh)
PREEMPTOR, PREEMPTEE, RESET = ("", "", "") if os.environ.get("NO_COLOR") else ("\033[1;32m", "\033[33m", "\033[0m")


def read(path):
    try:
        with open(path, errors="replace") as f:
            return f.read()
    except OSError:
        return ""


try:
    pods = json.load(open(pods_json))["items"]
except (OSError, ValueError, KeyError):
    pods = []
du = {}
for line in read(du_file).splitlines():
    parts = line.split()
    if len(parts) == 2 and parts[1].isdigit():
        du[parts[0]] = int(parts[1]) // 1024

live = {}
for p in pods:
    name = p["metadata"]["name"]
    if p["metadata"].get("labels", {}).get("spark-role") != "executor":
        continue
    m = re.search(r"-exec-(\d+)$", name)
    if not m:
        continue
    phase = p["status"].get("phase")
    if phase in ("Succeeded", "Failed"):
        continue
    state = "decommissioning" if p["metadata"].get("deletionTimestamp") else ("running" if phase == "Running" else "starting")
    live[int(m.group(1))] = (name, p["spec"].get("nodeName") or "-", state)

# Every executor that ever logged, so exited executors keep their row.
logged = {}
for f in os.listdir(os.path.join(run, "exec-logs")) if os.path.isdir(os.path.join(run, "exec-logs")) else []:
    m = re.search(r"-exec-(\d+)\.log$", f)
    if m:
        logged[int(m.group(1))] = os.path.join(run, "exec-logs", f)


def handed_over(path):
    text = read(path)
    if "Decommission executor" not in text and "Starting block migration" not in text:
        return ""
    sizes, blocks = 0.0, 0
    for m in re.finditer(r"Migrated migrate_shuffle_\S+ \(size: ([\d.]+) (KiB|MiB|B)\)", text):
        blocks += 1
        v = float(m.group(1))
        sizes += v / 1024 if m.group(2) == "KiB" else (v if m.group(2) == "MiB" else v / 1048576)
    rdd = len(re.findall(r"Block rdd_\S+ migrated successfully", text))
    if not blocks and not rdd:
        return "decommission started"
    out = f"{blocks} shuffle blocks · {sizes:.0f} MiB"
    if rdd:
        out += f" · {rdd} RDD blocks"
    return out + " → survivors"


print(f"  pr-victim: SparkPageRank, shuffle-heavy, floor 2 ({victim_state.lower() or 'starting'})"
      f"{'':8}urgent app: {urgent_state}")
print()
print(f"  {'executor':9} {'node':12} {'shuffle MiB':>11}  {'state':16} handed over")
if urgent_driver:
    print(f"  {PREEMPTOR}{'urgent':9} {'-':12} {'-':>11}  {'preemptor':16} driver {urgent_driver.lower()}, "
          f"{urgent_execs or 0} executor(s) running{RESET}")
ready = bool(live)
for eid in sorted(set(live) | set(logged)):
    if eid in live:
        name, node, state = live[eid]
        mib = du.get(name)
        shown = "-" if mib is None else str(mib)
        if state != "running" or mib is None or mib < thresh:
            ready = False
    else:
        node, state, shown = "-", "exited", "-"
    note = handed_over(logged[eid]) if eid in logged else ""
    if state == "exited" and note:
        state = "reclaimed, exited"
    col = PREEMPTEE if (note or state == "decommissioning") else ""
    print(f"  {col}{eid:<9} {node:12} {shown:>11}  {state:16} {note}{RESET if col else ''}")

drv = read(os.path.join(run, "driver.log"))
relocated = drv.count("Updating map output")
fetch = len(re.findall(r"FetchFailed|fetch failure", drv))
resub = len(re.findall(r"Resubmitting", drv))
print()
print(f"  driver: map outputs relocated {relocated} · fetch failures {fetch} · stages resubmitted {resub}")
if urgent_driver:
    print(f"  {PREEMPTOR}green: preemptor (the urgent app){RESET}   {PREEMPTEE}yellow: preemptee (reclaimed executors){RESET}")
sys.exit(0 if ready else 1)
