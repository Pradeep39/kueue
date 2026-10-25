#!/usr/bin/env bash
# The short partial-preemption demo for the Apache Spark Operator (spark.apache.org/v1 SparkApplications):
# the same six parts as ../kubeflow/demo-short.sh (see ../DEMO-SPREAD-SHUFFLE.md), in namespace batch-ns-dos.
# ../demo-short.sh --operator apache runs this script.
#
#   ./demo-short.sh preflight      check the cluster, Kueue, queues and the Spark operator, and set up
#                                  whatever is missing (gates, labelKeysToCopy, queues, CRD, watched
#                                  namespace), restarting Kueue or the operator after asking (YES=1: don't ask)
#   ./demo-short.sh preflight --check   the same checks, changing nothing
#   ./demo-short.sh 1              whole-workload preemption today: apps evicted whole, again and again
#   ./demo-short.sh 2              shuffle resiliency: reclaimed executors move their shuffle data
#   ./demo-short.sh 3              spread: six apps each give exactly one executor, then grow back
#   ./demo-short.sh 4              round-robin: five apps each give one before any gives a second
#   ./demo-short.sh 5              configurable preemption: classic preemption off, a PreemptionConfig
#                                  rule picks the victims by label, not priority
#   ./demo-short.sh 6 [--floor N]  default preemption: static apps of mixed size, no floor, so Kueue's
#                                  greedy selection evicts whole apps; --floor N turns on partial
#                                  preemption for the same apps and spreads the loss
#   ./demo-short.sh all            parts 1 to 6 in order (6 without, then with --floor 1), pausing
#                                  between them (~20 min of cluster time)
#   ./demo-short.sh reset          delete this demo's apps and put cluster-queue-tres back to 36 slots
#   ./demo-short.sh clear-events   delete the preemption and partial-preemption events from earlier runs
#   ./demo-short.sh yaml N [--list]  the YAML the latest run of part N applied (--list: all its runs)
#   ./demo-short.sh logs           the reclaimed executors' decommission logs from the latest part-2 run
#                                  (add a runs/SHUF-<stamp> path only to look at an older run)
#
# Settings: SPARK_VERSION (default 3.4.3), REFRESH seconds between screen refreshes (default 10),
# EVICTIONS for part 1 (default 3), THRESH for part 2 (MiB of
# shuffle per executor before the urgent app arrives, default 40). Each part pauses for Enter before the
# urgent app arrives; in part 1, Enter during the evictions ends it early.
#
# It only ever deletes the apps it creates. If anything else is in the namespace, the parts refuse to
# start until it is gone.
set -uo pipefail
HERE="$(cd "$(dirname "$0")" && pwd)"
# setup_file NAME: the setup YAML NAME, in this script's directory or the nearest parent that has it
# (directly, or in its setup/ directory), so it is found in the ~/kueue-gated-repro and the repo layouts.
setup_file() { local d=$HERE n c; for n in 0 1 2 3 4; do for c in "$d/$1" "$d/setup/$1"; do [ -f "$c" ] && { echo "$c"; return; }; done; d=$(dirname "$d"); done; echo "$HERE/$1"; }
SCRIPTS="$HERE"   # the Apache runners live beside this script
NS=batch-ns-dos
SA=sparkapplications.spark.apache.org
export SPARK_VERSION=${SPARK_VERSION:-3.4.3}
DEMO_APPS='^(workload-[ab]|pr-victim|urgent|als-(lo[0-9]+|hi)|plain-hi)$'

say()   { printf '\n\033[1;36m▶ %s\033[0m\n' "$*"; }
pause() { printf '\n\033[1m⏸  %s — press Enter\033[0m ' "$*"; read -r _; }

demo_apps()  { kubectl -n $NS get $SA -o name 2>/dev/null | sed 's#.*/##' | grep -E  "$DEMO_APPS" || true; }
other_apps() { kubectl -n $NS get $SA -o name 2>/dev/null | sed 's#.*/##' | grep -vE "$DEMO_APPS" || true; }

# wait_clean: each part deletes its apps in the background when it ends; wait for them to go, and warn
# about apps that are not this demo's (the part will refuse to start while they are there).
wait_clean() {
  for _ in $(seq 1 40); do [ -z "$(demo_apps)" ] && break; sleep 3; done
  local others; others=$(other_apps)
  if [ -n "$others" ]; then
    echo "  ⚠️  $NS has SparkApplications this demo did not create: $(echo $others | tr '\n' ' ')"
    echo "     The part will refuse to start. Remove them first if they are yours to remove."
  fi
}

preflight() {
  local mode=${1:-} ok=1
  if [ "$mode" = --check ]; then say "Preflight (check only)"; else say "Preflight: check, and set up whatever is missing"; fi
  kubectl get nodes --no-headers 2>/dev/null | awk '$2!="Ready"{print "  ⚠️  node "$1" is "$2}'
  if [ -z "$(demo_apps)$(other_apps)" ]; then echo "  ✅ $NS has no SparkApplications"
  else echo "  ⚠️  $NS has SparkApplications: $(demo_apps; other_apps | tr '\n' ' ')"; fi
  for s in run-whole-workload-preemption.sh run-partial-preemption-spread.sh run-shuffle-resiliency.sh shuffle-view.py spread-view.py run-configurable-preemption.sh run-default-preemption.sh gen-apache-apps.py demo-table.py; do
    [ -e "$SCRIPTS/$s" ] || { echo "  ❌ $SCRIPTS/$s missing"; ok=0; }
  done
  echo "  Spark image for the demo apps: docker.io/library/spark:$SPARK_VERSION"
  # Kueue, queues and the Spark operator: ensure-demo-env.sh checks them and, without --check, sets up
  # what is missing (it asks before restarting Kueue or the operator; YES=1 skips the question).
  "$(setup_file ensure-demo-env.sh)" apache $mode || ok=0
  [ $ok = 1 ]
}

part_1() { say "Part 1 of 6: whole-workload preemption, today's behaviour (Spark $SPARK_VERSION)"; wait_clean
  PART=part1 EVICTIONS=${EVICTIONS:-3} "$SCRIPTS/run-whole-workload-preemption.sh"; }
part_2() { say "Part 2 of 6: shuffle resiliency (Spark $SPARK_VERSION)"; wait_clean
  PART=part2 THRESH=${THRESH:-40} "$SCRIPTS/run-shuffle-resiliency.sh"; }
part_3() { say "Part 3 of 6: spread the loss (Spark $SPARK_VERSION)"; wait_clean
  PART=part3 CAPTURE=0 "$SCRIPTS/run-partial-preemption-spread.sh" p10s; }
part_4() { say "Part 4 of 6: round-robin, then grow-back (Spark $SPARK_VERSION)"; wait_clean
  PART=part4 CAPTURE=0 "$SCRIPTS/run-partial-preemption-spread.sh" p11s; }
part_5() { say "Part 5 of 6: configurable preemption picks the victims (Spark $SPARK_VERSION)"; wait_clean
  PART=part5 "$SCRIPTS/run-configurable-preemption.sh"; }
part_6() { say "Part 6 of 6: $([ "${1:-}" = --floor ] && echo "the same apps with a floor of ${2:-}: partial preemption" || echo "default preemption, greedy candidate selection") (Spark $SPARK_VERSION)"; wait_clean
  local floor=""; case "${1:-}" in --floor) floor=${2:-} ;; --floor=*) floor=${1#--floor=} ;; esac
  PART=part6${floor:+-floor$floor} "$SCRIPTS/run-default-preemption.sh" "$@"; }

# show_yaml N [--list]: the YAML the latest run of part N applied, file by file in the order it was
# applied (each runner saves it under runs/applied/, see applied-yaml.sh). --list: every run of part N,
# newest first, with its files.
show_yaml() {
  local n=${1:-} dirs d f
  case "$n" in [1-6]) ;; *) echo "usage: ./demo-short.sh yaml N [--list]   (N = 1 to 6)" >&2; return 2 ;; esac
  dirs=$(ls -1dt "$SCRIPTS/runs/applied/part$n-"* 2>/dev/null)
  [ -n "$dirs" ] || { echo "  no run of part $n has applied anything yet (looked in $SCRIPTS/runs/applied)" >&2; return 1; }
  if [ "${2:-}" = --list ]; then
    for d in $dirs; do echo "$d"; for f in "$d"/*.yaml; do [ -f "$f" ] && echo "  $(basename "$f")"; done; done
    return 0
  fi
  d=$(head -1 <<<"$dirs")
  echo "# Part $n, latest run: $d"
  for f in "$d"/*.yaml; do
    [ -f "$f" ] || continue
    printf '\n# ==> %s\n' "$(basename "$f")"
    cat "$f"
  done
}

run_all() {
  part_1; pause "Next: part 2, shuffle resiliency"
  part_2; pause "Next: part 3, spread the loss"
  part_3; pause "Next: part 4, round-robin"
  part_4; pause "Next: part 5, configurable preemption"
  part_5; pause "Next: part 6, default preemption (no floor)"
  part_6; pause "Next: part 6 again, with a floor of 1"
  part_6 --floor 1; say "Demo complete"
}

reset() {
  say "Reset: deleting this demo's apps, putting cluster-queue-tres back to 36 slots"
  local apps; apps=$(demo_apps)
  [ -n "$apps" ] && kubectl -n $NS delete $SA $apps --wait=true >/dev/null 2>&1
  kubectl patch clusterqueue cluster-queue-tres --type=json -p '[
    {"op":"replace","path":"/spec/resourceGroups/0/flavors/0/resources/0/nominalQuota","value":"36"},
    {"op":"replace","path":"/spec/resourceGroups/0/flavors/0/resources/1/nominalQuota","value":"32256Mi"}]' >/dev/null 2>&1
  rmdir "$SCRIPTS"/runs/.whole-workload-preemption.lock "$SCRIPTS"/runs/.shuffle-migration.lock "$SCRIPTS"/runs/.partial-preemption-spread.lock "$SCRIPTS"/runs/.configurable-preemption.lock "$SCRIPTS"/runs/.default-preemption.lock 2>/dev/null
  local others; others=$(other_apps)
  [ -n "$others" ] && echo "  Left alone (not this demo's): $(echo $others | tr '\n' ' ')"
  echo "  Done."
}

# clear_events: delete the whole and partial preemption events in the namespace (what the eviction and
# reclaim lists show). Other events (Pods, the Spark Operator, admissions) are left alone.
PREEMPTION_REASONS="Preempted PreemptedWorkload EvictedDueToPreempted PartiallyPreempted PartiallyPreemptedWorkload PartialPreemptionComplete PartialPreemptionTimedOut PartialPreemptionAbandoned"
clear_events() {
  say "Clearing preemption and partial-preemption events in $NS"
  local r n total=0
  for r in $PREEMPTION_REASONS; do
    n=$(kubectl -n $NS get events --field-selector reason=$r --no-headers 2>/dev/null | wc -l | tr -d ' ')
    [ "$n" = 0 ] && continue
    kubectl -n $NS delete events --field-selector reason=$r ${DRY_RUN:+--dry-run=server} >/dev/null 2>&1 && total=$((total + n))
    printf '  %-28s %s\n' "$r" "$n"
  done
  echo "  ${DRY_RUN:+(dry run) }$total event(s) deleted"
}

# logs [RUN]: for the latest part-2 run (or RUN), each executor Kueue reclaimed, from its own log,
# which the runner streams from start to exit: the decommission signal, the shuffle and RDD blocks it
# migrated, and when it exited.
show_logs() {
  local run=${1:-$(ls -td "$SCRIPTS"/runs/SHUF-* 2>/dev/null | head -1)}
  [ -d "$run/exec-logs" ] || { echo "  no part-2 run found (looked for $SCRIPTS/runs/SHUF-*)"; return 1; }
  run=$(cd "$run" && pwd)
  say "Reclaimed executors in $(basename "$run") (full logs: $run/exec-logs/)"
  python3 -I - "$run" <<'PY'
import glob, os, re, sys
run = sys.argv[1]
driver = open(os.path.join(run, "driver.log"), errors="replace").read() if os.path.exists(os.path.join(run, "driver.log")) else ""
ids = []
for m in re.finditer(r"Decommission executors: ([\d, ]+)", driver):
    ids += [x.strip() for x in m.group(1).split(",") if x.strip() and x.strip() not in ids]
if not ids:
    print("  the driver log names no decommissioned executors")
def hms(line):
    m = re.search(r"\d\d/\d\d/\d\d (\d\d:\d\d:\d\d)", line)
    return m.group(1) if m else "--:--:--"
for eid in ids:
    path = next(iter(glob.glob(os.path.join(run, "exec-logs", f"*-exec-{eid}.log"))), None)
    if not path:
        print(f"\n  executor {eid}: no log captured"); continue
    lines = open(path, errors="replace").read().splitlines()
    first = lambda pat: next((l for l in lines if re.search(pat, l)), None)
    shuffle = [l for l in lines if "Migrated migrate_shuffle_" in l]
    rdd = [l for l in lines if re.search(r"Block rdd_\S+ migrated successfully", l)]
    sizes = [float(m.group(1)) * (1 / 1024 if m.group(2) == "KiB" else 1)
             for l in shuffle for m in [re.search(r"\(size: ([\d.]+) (KiB|MiB)\)", l)] if m]
    sig, added, stop, exitl = (first(r"Decommission executor \d+"), first(r"local shuffles are added"),
                               first(r"Stop shuffle block migration"), first(r"Shutdown hook called"))
    print(f"\n  executor {eid}  ({os.path.basename(path)})")
    if sig:   print(f"    {hms(sig)}  decommission signal received")
    if added: print(f"    {hms(added)}  {re.sub(r'.*BlockManagerDecommissioner: ', '', added)}")
    if shuffle:
        print(f"    {hms(shuffle[0])}–{hms(shuffle[-1])}  migrated {len(shuffle)} shuffle blocks"
              + (f" ({sum(sizes):.0f} MiB)" if sizes else "") + " to surviving executors")
    if rdd:   print(f"    {hms(rdd[-1])}  migrated {len(rdd)} cached RDD blocks")
    if exitl or stop:
        print(f"    {hms(exitl or stop)}  executor exited (it waits for Spark's next migration round, 30s by default)")
PY
}

case "${1:-}" in
  clear-events) clear_events ;;
  logs) show_logs "${2:-}" ;;
  yaml) shift; show_yaml "$@" ;;
  preflight) preflight "${2:-}" ;;
  1) part_1 ;;
  2) part_2 ;;
  3) part_3 ;;
  4) part_4 ;;
  5) part_5 ;;
  6) shift; part_6 "$@" ;;
  all) run_all ;;
  reset) reset ;;
  *) sed -n '2,/^set -uo pipefail/p' "$0" | sed '$d' | sed 's/^# \{0,1\}//' ;;
esac
