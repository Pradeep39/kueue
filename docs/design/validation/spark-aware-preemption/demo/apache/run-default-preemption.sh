#!/usr/bin/env bash
# Apache Spark Operator copy of the shared scripts/run-default-preemption.sh (spark.apache.org/v1 apps in
# batch-ns-dos). Demo part 6: Kueue's default preemption and its greedy candidate selection, then the
# same apps with a partial-preemption floor.
#
# Five low-priority static apps (no Dynamic Allocation) of mixed size fill cluster-queue-tres, admitted
# one at a time so their admission order is fixed:
#   als-lo1 6 executors (admitted 1st)   als-lo2 1   als-lo3 3   als-lo4 1   als-lo5 2 (admitted 5th, newest)
# Then a high-priority static app needing 6 slots arrives.
#
#   (default)   no floor. A static app's floor is then its whole executor count, so nothing is
#               reclaimable and Kueue preempts the classic way: candidates ordered by priority, then
#               newest admitted first, removed one by one until the urgent app fits, then put back
#               (newest-but-one first) where it still fits. Size plays no part, so it evicts whole apps
#               and frees more than it needs.
#   --floor N   spark.kueue.preemption.minExecutors N on every app: partial preemption takes the
#               6 slots as executors, spread across the apps above their floor. Nobody is evicted.
#
#   [AUTO=1] [FORCE=1] [GROWBACK=0] run-default-preemption.sh [--floor N]
#     AUTO=1        no Enter prompt
#     FORCE=1       delete other SparkApplications in batch-ns-dos before starting (otherwise refuse)
#     GROWBACK=0    stop once the urgent app runs, instead of waiting for every app to be back at full size
#     VICTIM_ITERS / URGENT_ITERS   SparkALS iterations (default 100 / 4: the victims outlive the urgent app;
#                   the demo deletes them when it ends)
#     REFRESH=N     seconds between screen refreshes (default 10)
#   The queue is sized to exactly what the apps fill (18 slots) and put back to 36 on exit.
set -uo pipefail
HERE="$(cd "$(dirname "$0")" && pwd)"
# setup_file NAME: the setup YAML NAME, in this script's directory or the nearest parent that has it
# (directly, or in its setup/ directory), so it is found in the ~/kueue-gated-repro and the repo layouts.
setup_file() { local d=$HERE n c; for n in 0 1 2 3 4; do for c in "$d/$1" "$d/setup/$1"; do [ -f "$c" ] && { echo "$c"; return; }; done; d=$(dirname "$d"); done; echo "$HERE/$1"; }
NS=batch-ns-dos; SA=sparkapplications.spark.apache.org; CQ=cluster-queue-tres; LQ=batch-queue-tres
APP_LABEL=spark.operator/spark-app-name
gen() { python3 -I "$HERE/gen-apache-apps.py" "$@"; }
ARGS_FLAG=--args

FLOOR=""
case "${1:-}" in
  --floor) [[ "${2:-}" =~ ^[0-9]+$ ]] || { echo "--floor needs a number" >&2; exit 2; }; FLOOR=$2 ;;
  --floor=*) FLOOR=${1#--floor=}; [[ "$FLOOR" =~ ^[0-9]+$ ]] || { echo "--floor needs a number" >&2; exit 2; } ;;
  -h|--help) sed -n "2,/^set -uo pipefail/p" "$0" | sed "\$d" | sed "s/^# \{0,1\}//"; exit 0 ;;
  "") ;;
  *) echo "unknown argument '$1' (try --help)" >&2; exit 2 ;;
esac
SIZES=(6 1 3 1 2)
VICTIMS=${#SIZES[@]}; URGENT_EXECS=5
QUOTA=0; for n in "${SIZES[@]}"; do QUOTA=$((QUOTA + n + 1)); done
VICTIM_ARGS="1500 1500 12 ${VICTIM_ITERS:-100} 200"
URGENT_ARGS="1500 1500 12 ${URGENT_ITERS:-4} 200"
ORD=(1st 2nd 3rd 4th 5th)
TAGS=$(for i in $(seq 1 $VICTIMS); do t=${ORD[$((i - 1))]}; [ $i = 1 ] && t="$t (oldest)"; [ $i = $VICTIMS ] && t="$t (newest)"; echo "als-lo$i=$t"; done | paste -sd, -)
export VIEW_SIZES=$(for i in $(seq 1 $VICTIMS); do echo "als-lo$i=${SIZES[$((i - 1))]}"; done | paste -sd, -)
export VIEW_TAG_HEADER=admitted
if [ -n "$FLOOR" ]; then MODE="partial preemption, floor $FLOOR"; else MODE="default preemption, no floor"; fi

say() { printf '\n\033[1;36m▶ %s\033[0m\n' "$*"; }

table() {
  local tmp; tmp=$(mktemp -d)
  kubectl -n $NS get pods -l spark-role -o json > "$tmp/pods.json" 2>/dev/null
  kubectl -n $NS get workloads -o json > "$tmp/wls.json" 2>/dev/null
  kubectl -n $NS get events -o json > "$tmp/events.json" 2>/dev/null
  echo "  mode: $MODE"
  python3 -I "$HERE/spread-view.py" "$tmp/pods.json" "$tmp/wls.json" "$tmp/events.json" \
    $QUOTA 1 $URGENT_EXECS "${TRIGGER:-}" "$TAGS"
  rm -rf "$tmp"
}

live() { # live UNTIL_CMD [PROMPT]
  while true; do
    clear; date +%H:%M:%S; table
    [ -n "${2:-}" ] && { echo; printf '\033[1m  %s\033[0m\n' "$2"; }
    [ -n "${1:-}" ] && eval "$1" && return 0
    if [ -n "${2:-}" ] && [ -t 0 ] && [ "${AUTO:-0}" != 1 ]; then
      read -r -t "${REFRESH:-10}" _ && return 0
    else
      sleep "${REFRESH:-10}"
    fi
  done
}

set_quota() { # set_quota SLOTS: one slot = 1 CPU + 896Mi
  kubectl patch clusterqueue $CQ --type=json -p "[
    {\"op\":\"replace\",\"path\":\"/spec/resourceGroups/0/flavors/0/resources/0/nominalQuota\",\"value\":\"$1\"},
    {\"op\":\"replace\",\"path\":\"/spec/resourceGroups/0/flavors/0/resources/1/nominalQuota\",\"value\":\"$(( $1 * 896 ))Mi\"}]" >/dev/null
}

admitted() { # admitted APP: its Workload holds quota
  kubectl -n $NS get workloads -o json 2>/dev/null | python3 -I -c '
import json, sys
for w in json.load(sys.stdin)["items"]:
    if any(o.get("name") == sys.argv[1] for o in w["metadata"].get("ownerReferences", [])):
        c = {x["type"]: x["status"] for x in (w.get("status") or {}).get("conditions", [])}
        if c.get("Admitted") == "True" and c.get("Finished") != "True":
            sys.exit(0)
sys.exit(1)' "$1"
}
running_execs() { # running_execs APP
  kubectl -n $NS get pods -l "$APP_LABEL=$1,spark-role=executor" --field-selector=status.phase=Running --no-headers 2>/dev/null | wc -l | tr -d ' '
}
driver_running() {
  [ "$(kubectl -n $NS get pods -l "$APP_LABEL=$1,spark-role=driver" --field-selector=status.phase=Running --no-headers 2>/dev/null | wc -l | tr -d ' ')" -ge 1 ]
}
all_full() { # every victim runs its driver and all its executors, or has finished
  local i
  for i in $(seq 1 $VICTIMS); do
    app_finished als-lo$i && continue
    driver_running als-lo$i && [ "$(running_execs als-lo$i)" -ge "${SIZES[$((i - 1))]}" ] || return 1
  done
}
urgent_running() { [ "$(running_execs als-hi)" -ge $URGENT_EXECS ]; }
app_finished() { # app_finished APP
  local st; st=$(kubectl -n $NS get $SA "$1" -o jsonpath='{.status.currentState.currentStateSummary}' 2>/dev/null)
  case "$st" in Succeeded|Failed|ResourceReleased|TerminatedWithoutReleaseResources|SchedulingFailure|DriverEvicted|DriverStartTimedOut|ExecutorsStartTimedOut|DriverReadyTimedOut) return 0 ;; esac
  return 1
}
urgent_finished() { app_finished als-hi; }
growback_done() {
  [ "$(date +%s)" -ge "$DEADLINE" ] && { echo "  (stopped waiting after ${GROWBACK_TIMEOUT:-480}s)"; return 0; }
  urgent_finished && all_full
}

summary() {
  say "Summary ($MODE)"
  kubectl -n $NS get events -o json 2>/dev/null | python3 -I -c '
import json, sys, re, collections
since, sizes, urgent_slots = sys.argv[1], [int(x) for x in sys.argv[2].split(",")], int(sys.argv[3])
apps = [f"als-lo{i}" for i in range(1, len(sizes) + 1)]
size = dict(zip(apps, sizes))
gave, whole = collections.Counter(), collections.Counter()
for e in json.load(sys.stdin)["items"]:
    t = e.get("lastTimestamp") or e.get("eventTime") or ""
    if t[:19] < since[:19] or e["involvedObject"].get("kind") != "Workload":
        continue
    app = re.sub(r"^sparkapplication-|-[0-9a-f]{5}$", "", e["involvedObject"]["name"])
    m = re.search(r"Partially preempted (\d+) Pod", e.get("message") or "")
    if e["reason"] == "PartiallyPreempted" and m:
        gave[app] += int(m.group(1))
    elif e["reason"] == "Preempted" and app in size:
        whole[app] += 1
order = list(reversed(apps))   # same priority, so newest admitted first
print("  candidate order (lowest priority first, then newest admitted first): " + ", ".join(order))
evicted = [a for a in order if whole[a]]
if evicted:
    lost = sum(size[a] for a in evicted)
    freed = sum(size[a] + 1 for a in evicted)
    execs = lambda n: f"{n} executor" + ("s" if n > 1 else "")
    print("  evicted whole: " + ", ".join(f"{a} ({execs(size[a])} + driver)" for a in evicted))
    print(f"  lost: {lost} executors and {len(evicted)} drivers; {freed} slots freed for a {urgent_slots}-slot app")
    oldest = max(order.index(a) for a in evicted)
    spared = [a for a in order[:oldest] if not whole[a]]
    if spared:
        print("  put back by the greedy pass: " + ", ".join(spared)
              + " (newer than an evicted app, so removed first while searching, but the urgent app fit without evicting it)")
    print("  every evicted app waits for quota, then restarts from scratch")
if gave:
    print(f"  reclaimed: {sum(gave.values())} executors from {len(gave)} apps: "
          + ", ".join(f"{a} {gave[a]} of {size[a]}" for a in apps if gave[a]))
    kept = [a for a in apps if not gave[a] and not whole[a]]
    if kept:
        print("  gave nothing: " + ", ".join(kept))
print(f"  apps evicted whole: {sum(whole.values())}; drivers lost: {sum(whole.values())}; restarts: {sum(whole.values())}")
' "$TRIGGER" "$(IFS=,; echo "${SIZES[*]}")" $((URGENT_EXECS + 1))
  [ -n "${URGENT_UP:-}" ] && echo "  urgent app running $(( URGENT_UP - TRIGGER_AT ))s after it was submitted"
  [ -n "${GROWN_AT:-}" ] && echo "  every app back at full size (or finished) $(( GROWN_AT - TRIGGER_AT ))s after the urgent app arrived"
}

# ---- preflight
say "Preflight"
echo "  Kueue: $(kubectl -n kueue-system get deploy kueue-controller-manager -o jsonpath='{.spec.template.spec.containers[0].image}')"
if [ -n "$FLOOR" ]; then
  [[ "$(kubectl -n kueue-system get deploy kueue-controller-manager -o jsonpath='{.spec.template.spec.containers[0].args}')" == *SparkPartialPreemption=true* ]] ||
    { echo "  ❌ SparkPartialPreemption gate is off: ./demo-short.sh preflight sets it" >&2; exit 1; }
fi
if ! kubectl get clusterqueue $CQ >/dev/null 2>&1 || ! kubectl -n $NS get localqueue $LQ >/dev/null 2>&1; then
  echo "  $CQ / $LQ missing: applying $(setup_file queues-setup.yaml)"
  kubectl apply -f "$(setup_file queues-setup.yaml)" >/dev/null || exit 1
fi
echo "  $CQ: classic preemption withinClusterQueue=$(kubectl get clusterqueue $CQ -o jsonpath='{.spec.preemption.withinClusterQueue}'); mode: $MODE"
mkdir -p "$HERE/runs"
LOCK="$HERE/runs/.default-preemption.lock"
mkdir "$LOCK" 2>/dev/null || { echo "  refusing to start: another run-default-preemption.sh is in progress (remove $LOCK if not)" >&2; exit 3; }
cleanup() { [ "${QUOTA_SET:-0}" = 1 ] && set_quota 36; rmdir "$LOCK" 2>/dev/null; }
trap cleanup EXIT
others=$(kubectl -n $NS get $SA -o name 2>/dev/null | sed 's#.*/##' | grep -vE '^als-(lo[0-9]+|hi)$' || true)
if [ -n "$others" ] && [ "${FORCE:-0}" != 1 ]; then
  echo "  refusing to start: $NS has other SparkApplications ($(echo $others | tr '\n' ' ')). Rerun with FORCE=1 to delete them." >&2
  exit 3
fi

say "Cleaning $NS"
kubectl -n $NS delete $SA --all --wait=true >/dev/null 2>&1
for _ in $(seq 1 60); do [ "$(kubectl -n $NS get workloads,pods --no-headers 2>/dev/null | wc -l | tr -d ' ')" = 0 ] && break; sleep 3; done
set_quota $QUOTA; QUOTA_SET=1

say "Admitting $VICTIMS low-priority static apps one at a time (executors: ${SIZES[*]}), $([ -n "$FLOOR" ] && echo "floor $FLOOR" || echo "no floor") ($QUOTA of $QUOTA slots)"
for i in $(seq 1 $VICTIMS); do
  gen als-lo$i:low --queue $LQ --static-elastic "${SIZES[$((i - 1))]}" ${FLOOR:+--floor $FLOOR} $ARGS_FLAG "$VICTIM_ARGS" | kubectl apply -f - >/dev/null
  for _ in $(seq 1 60); do admitted als-lo$i && break; sleep 1; done
  echo "  als-lo$i admitted ($(date +%H:%M:%S)), ${SIZES[$((i - 1))]} executor(s)"
  sleep 2   # admission times one second apart at least, so "newest first" is unambiguous
done
live all_full
[ -t 0 ] || AUTO=1
[ "${AUTO:-0}" = 1 ] && sleep 5 || live "" "⏸  Queue is full. Press Enter to submit the high-priority app (static, needs $((URGENT_EXECS + 1)) slots)"
TRIGGER=$(date -u +%Y-%m-%dT%H:%M:%SZ); TRIGGER_AT=$(date +%s)
gen als-hi:high --queue $LQ --static $URGENT_EXECS $ARGS_FLAG "$URGENT_ARGS" | kubectl apply -f - >/dev/null
DEADLINE=$(( $(date +%s) + 300 ))
live 'urgent_running || [ "$(date +%s)" -ge "$DEADLINE" ]'
urgent_running && URGENT_UP=$(date +%s)
echo; echo "  ✅ als-hi is running all $URGENT_EXECS executors."

if [ "${GROWBACK:-1}" = 1 ]; then
  sleep 3
  DEADLINE=$(( $(date +%s) + ${GROWBACK_TIMEOUT:-480} ))
  live growback_done
  all_full && GROWN_AT=$(date +%s)
  clear; date +%H:%M:%S; table
fi

summary
if [ "${KEEP:-0}" != 1 ]; then
  kubectl -n $NS get $SA -o name 2>/dev/null | grep -E '/als-(lo[0-9]+|hi)$' | xargs -r kubectl -n $NS delete --wait=false >/dev/null 2>&1
  echo; echo "  Demo apps deleted (in the background). KEEP=1 leaves them running."
fi
