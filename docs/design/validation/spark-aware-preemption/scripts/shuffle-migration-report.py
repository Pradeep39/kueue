#!/usr/bin/env python3
"""Report for one run-shuffle-migration.sh run: did reclaimed executors migrate their shuffle data?

  shuffle-migration-report.py RUN_DIR
"""
import collections
import glob
import json
import os
import re
import sys

d = sys.argv[1]


def lines(path):
    try:
        with open(path, errors="replace") as f:
            return f.read().splitlines()
    except OSError:
        return []


def strip(line):
    # "2026-…-05:00 26/10/13 01:02:03 INFO Class: msg" -> "01:02:03 INFO Class: msg"
    m = re.search(r"\d\d/\d\d/\d\d (\d\d:\d\d:\d\d) (\w+) (.*)", line)
    return f"{m.group(1)} {m.group(2)} {m.group(3)}" if m else line


print(f"== {os.path.basename(d.rstrip('/'))}")
runner = lines(f"{d}/runner.log")
for l in runner:
    if "TRIGGER" in l or "applied" in l or "never" in l or "before the threshold" in l:
        print("  " + l)
trig = lines(f"{d}/shuffle-at-trigger.txt")
if trig:
    print("  shuffle MiB per executor at trigger:" + trig[0])

# Which executors did Kueue reclaim? Spark logs the decommission on the driver.
drv = lines(f"{d}/driver.log")
decom_ids = []
for l in drv:
    m = re.search(r"Decommission executors: ([\d, ]+)", l)
    if m:
        decom_ids += [x.strip() for x in m.group(1).split(",") if x.strip()]
print(f"\n  executors Spark was told to decommission: {', '.join(decom_ids) or 'none'}")

# Per executor: its own log, streamed until it exited.
print("\n  per decommissioned executor (from its own log):")
print(f"  {'executor':10} {'decom-signal':>12} {'shuffle-migrated':>16} {'rdd-migrated':>12} {'finished':>9} {'exit':>20}")
exec_logs = {re.sub(r".*-exec-(\d+)\.log$", r"\1", p): p for p in glob.glob(f"{d}/exec-logs/*.log")}
detail = {}
for eid in decom_ids:
    L = lines(exec_logs.get(eid, ""))
    sig = sum(1 for l in L if re.search(r"SIGPWR|Decommission(ing)? self|decommissionSelf|Received decommission", l, re.I))
    shuf = sum(1 for l in L if re.search(r"Migrated migrate_shuffle_", l))
    rdd = sum(1 for l in L if re.search(r"Block rdd_\S+ migrated successfully", l))
    fin = any(re.search(r"Finished decommissioning|decommission.*finished|all blocks.*migrated|No running tasks", l, re.I) for l in L)
    tail = [strip(l) for l in L if re.search(r"Executor self-exiting|exiting|SIGTERM|Shutdown hook called|shutdown", l, re.I)]
    print(f"  {eid:10} {sig:>12} {shuf:>16} {rdd:>12} {('yes' if fin else 'no'):>9} {(tail[-1][9:29] if tail else '-'):>20}")
    detail[eid] = [strip(l) for l in L if re.search(r"decommission|migrat|BlockManagerDecommissioner|exiting", l, re.I)]

# Driver: relocation of map outputs, recomputation, failures.
reloc = [strip(l) for l in drv if "Updating map output" in l]
resub = [strip(l) for l in drv if re.search(r"Resubmitt|FetchFailed|fetch failure|Lost task|TaskKilled", l)]
lostf = [strip(l) for l in drv if "Shuffle files lost for executor" in l]
reasons = [strip(l) for l in drv if "Remove reason statistics" in l]
decom_after = re.findall(r"Executor (\d+) on \S+ is decommissioned after ([\d.]+) s", "\n".join(drv))
failed = [strip(l) for l in drv if "new failed executors" in l]
done = [strip(l) for l in drv if re.search(r"Job \d+ finished|Job \d+ failed|Exception in thread", l)]
print("\n  driver:")
print(f"    map outputs relocated to another executor ('Updating map output'): {len(reloc)}")
print(f"    'Shuffle files lost for executor' lines: {len(lostf)}  (logged on every executor loss; harmless when outputs were relocated first)")
print(f"    resubmitted stages / fetch failures / lost or killed tasks: {len(resub)}")
print(f"    executor failures counted: {len(failed)}")
if decom_after:
    print("    time from decommission signal to exit, per executor: " + ", ".join(f"{e}: {t}s" for e, t in decom_after))
if reasons:
    print(f"    last remove-reason tally: {reasons[-1].split('Remove reason statistics: ')[-1]}")
if done:
    print(f"    last job line: {done[-1][:140]}")
for l in resub[:6]:
    print(f"      {l[:160]}")

print("\n  sample executor lines (first decommissioned executor):")
if decom_ids:
    for l in detail.get(decom_ids[0], [])[:14]:
        print(f"    {l[:170]}")
print("\n  sample driver relocation lines:")
for l in reloc[:5]:
    print(f"    {l[:170]}")
