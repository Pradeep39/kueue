#!/usr/bin/env bash
# The short partial-preemption demo: the four scenarios in DEMO-SPREAD-SHUFFLE.md and SPEAKER-NOTES.md.
#
#   ./demo-short.sh preflight      check the cluster, Kueue, queues and namespace (read-only)
#   ./demo-short.sh 1              whole-workload preemption today: apps evicted whole, again and again
#   ./demo-short.sh 2              shuffle resiliency: reclaimed executors move their shuffle data
#   ./demo-short.sh 3              spread: six apps each give exactly one executor, then grow back
#   ./demo-short.sh 4              round-robin: five apps each give one before any gives a second
#   ./demo-short.sh 5              configurable preemption: classic preemption off, a PreemptionConfig
#                                  rule picks the victims by label, not priority
#   ./demo-short.sh all            parts 1 to 5 in order, pausing between them (~12 min of cluster time)
#   ./demo-short.sh reset          delete this demo's apps and put cluster-queue-tres back to 36 slots
#   ./demo-short.sh clear-events   delete the preemption and partial-preemption events from earlier runs
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
# setup_file NAME: a setup YAML beside this script, one level up (~/kueue-gated-repro layout), or in
# ../setup (repo layout).
setup_file() { local d; for d in "$HERE" "$HERE/.." "$HERE/../setup"; do [ -f "$d/$1" ] && { echo "$d/$1"; return; }; done; echo "$HERE/../setup/$1"; }
SCRIPTS="$HERE/.."                                                  # ~/kueue-gated-repro layout
[ -f "$HERE/../scripts/run-shuffle-migration.sh" ] && SCRIPTS="$HERE/../scripts"   # repo layout
NS=batch-ns-uno
SA=sparkapplications.sparkoperator.k8s.io
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
  say "Preflight (read-only)"
  local img args ok=1
  img=$(kubectl -n kueue-system get deploy kueue-controller-manager -o jsonpath='{.spec.template.spec.containers[0].image}' 2>/dev/null)
  args=$(kubectl -n kueue-system get deploy kueue-controller-manager -o jsonpath='{.spec.template.spec.containers[0].args}' 2>/dev/null)
  echo "  Kueue image: ${img:-not found}"
  if [[ "$args" == *SparkPartialPreemption=true* ]]; then echo "  ✅ SparkPartialPreemption gate on"; else echo "  ❌ SparkPartialPreemption gate is OFF"; ok=0; fi
  if [[ "$args" == *ConfigurablePreemptions=true* ]]; then echo "  ✅ ConfigurablePreemptions gate on (part 5)"; else echo "  ⚠️  ConfigurablePreemptions gate is off: part 5 will refuse to start"; fi
  kubectl -n kueue-system get cm kueue-manager-config -o jsonpath='{.data.controller_manager_config\.yaml}' 2>/dev/null | grep -q -- '- preemption-pool$' ||
    echo "  ⚠️  integrations.labelKeysToCopy lacks preemption-pool/preemption-role: part 5 will refuse to start"
  kubectl -n kueue-system get pods --no-headers 2>/dev/null | awk '$3!="Running"{print "  ⚠️  "$1" is "$3}'
  kubectl get nodes --no-headers 2>/dev/null | awk '$2!="Ready"{print "  ⚠️  node "$1" is "$2}'
  for cq in cluster-queue-uno cluster-queue-dos cluster-queue-tres cluster-queue-cfg; do
    local q; q=$(kubectl get clusterqueue $cq -o jsonpath='{.spec.resourceGroups[0].flavors[0].resources[0].nominalQuota}' 2>/dev/null)
    if [ -n "$q" ]; then echo "  ✅ $cq: $q CPU"; elif [ $cq = cluster-queue-cfg ]; then echo "  ⚠️  $cq missing (part 5 applies $(setup_file queues-cfg.yaml))"
    else echo "  ❌ $cq missing: kubectl apply -f $(setup_file queues-setup.yaml)"; ok=0; fi
  done
  [ "$(kubectl get clusterqueue cluster-queue-tres -o jsonpath='{.spec.resourceGroups[0].flavors[0].resources[0].nominalQuota}' 2>/dev/null)" = 36 ] ||
    echo "  ⚠️  cluster-queue-tres is not at 36 slots: ./demo-short.sh reset puts it back"
  if [ -z "$(demo_apps)$(other_apps)" ]; then echo "  ✅ $NS has no SparkApplications"
  else echo "  ⚠️  $NS has SparkApplications: $(demo_apps; other_apps | tr '\n' ' ')"; fi
  for s in run-whole-workload-preemption.sh run-shuffle-migration.sh run-partial-preemption-spread.sh run-shuffle-resiliency.sh run-configurable-preemption.sh shuffle-view.py gen-apps.py gen-apps-label.py demo-table.py capture.sh; do
    [ -e "$SCRIPTS/$s" ] || { echo "  ❌ $SCRIPTS/$s missing"; ok=0; }
  done
  for y in queues-setup.yaml queues-cfg.yaml; do
    [ -f "$(setup_file $y)" ] || { echo "  ❌ setup file $y not found (looked beside the scripts and in ../setup)"; ok=0; }
  done
  echo "  Spark image for the demo apps: docker.io/library/spark:$SPARK_VERSION"
  [ $ok = 1 ] && echo "  Ready." || echo "  Fix the ❌ items before starting."
}

part_1() { say "Part 1 of 5: whole-workload preemption, today's behaviour (Spark $SPARK_VERSION)"; wait_clean
  EVICTIONS=${EVICTIONS:-3} "$SCRIPTS/run-whole-workload-preemption.sh"; }
part_2() { say "Part 2 of 5: shuffle resiliency (Spark $SPARK_VERSION)"; wait_clean
  THRESH=${THRESH:-40} "$SCRIPTS/run-shuffle-resiliency.sh"; }
part_3() { say "Part 3 of 5: spread the loss (Spark $SPARK_VERSION)"; wait_clean
  CAPTURE=0 "$SCRIPTS/run-partial-preemption-spread.sh" p10s; }
part_4() { say "Part 4 of 5: round-robin, then grow-back (Spark $SPARK_VERSION)"; wait_clean
  CAPTURE=0 "$SCRIPTS/run-partial-preemption-spread.sh" p11s; }
part_5() { say "Part 5 of 5: configurable preemption picks the victims (Spark $SPARK_VERSION)"; wait_clean
  "$SCRIPTS/run-configurable-preemption.sh"; }

run_all() {
  part_1; pause "Next: part 2, shuffle resiliency"
  part_2; pause "Next: part 3, spread the loss"
  part_3; pause "Next: part 4, round-robin"
  part_4; pause "Next: part 5, configurable preemption"
  part_5; say "Demo complete"
}

reset() {
  say "Reset: deleting this demo's apps, putting cluster-queue-tres back to 36 slots"
  local apps; apps=$(demo_apps)
  [ -n "$apps" ] && kubectl -n $NS delete $SA $apps --wait=true >/dev/null 2>&1
  kubectl patch clusterqueue cluster-queue-tres --type=json -p '[
    {"op":"replace","path":"/spec/resourceGroups/0/flavors/0/resources/0/nominalQuota","value":"36"},
    {"op":"replace","path":"/spec/resourceGroups/0/flavors/0/resources/1/nominalQuota","value":"32256Mi"}]' >/dev/null 2>&1
  rmdir "$SCRIPTS"/runs/.whole-workload-preemption.lock "$SCRIPTS"/runs/.shuffle-migration.lock "$SCRIPTS"/runs/.partial-preemption-spread.lock "$SCRIPTS"/runs/.configurable-preemption.lock 2>/dev/null
  local others; others=$(other_apps)
  [ -n "$others" ] && echo "  Left alone (not this demo's): $(echo $others | tr '\n' ' ')"
  echo "  Done."
}

# clear_events: delete the whole and partial preemption events in the namespace (what the watch view and
# the eviction/reclaim lists show), and start the watch view's event list from now. Other events (Pods,
# the Spark Operator, admissions) are left alone.
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
  [ -z "${DRY_RUN:-}" ] && date -u +%Y-%m-%dT%H:%M:%SZ > "$HERE/.demo-start"
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
  preflight) preflight ;;
  1) part_1 ;;
  2) part_2 ;;
  3) part_3 ;;
  4) part_4 ;;
  5) part_5 ;;
  all) run_all ;;
  reset) reset ;;
  *) sed -n '2,/^set -uo pipefail/p' "$0" | sed '$d' | sed 's/^# \{0,1\}//' ;;
esac
