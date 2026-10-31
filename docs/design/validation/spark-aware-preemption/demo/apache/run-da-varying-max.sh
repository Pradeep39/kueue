#!/usr/bin/env bash
# Demo part 7: Spark Dynamic Allocation apps of different maximum sizes share one queue. Each app grows
# from its minimum towards its own maxExecutors as its driver asks for executors, and Kueue admits every
# scale-up as a new Workload slice only when it fits. The queue is smaller than all the maximums together,
# so executors wait gated for quota, and the apps that finish first hand their quota to the others.
#
#   [AUTO=1] [FORCE=1] run-da-varying-max.sh
#   APPS="name:max:iterations,..." QUOTA=N run-da-varying-max.sh   any other mix
#     APPS          default "als-da2:2:4,als-da4:4:6,als-da8:8:10": maxExecutors 2, 4 and 8; the smaller
#                   apps run fewer SparkALS iterations, so they finish first
#     QUOTA=N       slots in the queue for the run (default 12, below the 17 the apps need at their
#                   maximums); one slot is 1 CPU + 896Mi, a driver or an executor
#     MIN=N         spark.dynamicAllocation.minExecutors of every app (default 1)
#     FORCE=1       delete other SparkApplications in batch-ns-dos before starting (otherwise refuse)
#     REFRESH=N     seconds between screen refreshes (default 10)
#     TIMEOUT=N     stop waiting for the apps to finish after N seconds (default 900)
#   The queue is put back to 36 slots on exit.
#   run-da-varying-max.sh watch      the live table only, e.g. on a second screen
#
# Needs: Kueue with ElasticJobsViaWorkloadSlicesForApacheSparkApplication=true, cluster-queue-tres
# (../queues-setup.yaml), batch-queue-tres in batch-ns-dos (queues-apache.yaml), and gen-apache-apps.py
# beside it.
set -uo pipefail
HERE="$(cd "$(dirname "$0")" && pwd)"
. "$HERE/applied-yaml.sh"   # record_apply: saves each applied manifest
setup_file() { local d=$HERE n c; for n in 0 1 2 3 4; do for c in "$d/$1" "$d/setup/$1"; do [ -f "$c" ] && { echo "$c"; return; }; done; d=$(dirname "$d"); done; echo "$HERE/$1"; }
NS=batch-ns-dos; SA=sparkapplications.spark.apache.org; CQ=cluster-queue-tres; LQ=batch-queue-tres
APP_LABEL=spark.operator/spark-app-name; GATE=ElasticJobsViaWorkloadSlicesForApacheSparkApplication
APPS=${APPS:-als-da2:2:4,als-da4:4:6,als-da8:8:10}; QUOTA=${QUOTA:-12}; MIN=${MIN:-1}
MAXES=$(echo "$APPS" | tr ',' '\n' | awk -F: '{printf "%s%s=%s", (NR>1?",":""), $1, $2}')
NAMES=$(echo "$APPS" | tr ',' '\n' | cut -d: -f1)
NEED=$(echo "$APPS" | tr ',' '\n' | awk -F: '{s += $2 + 1} END {print s}')
STATE=$(mktemp)

say()   { printf '\n\033[1;36m▶ %s\033[0m\n' "$*"; }

table() {
  local tmp; tmp=$(mktemp -d)
  kubectl -n $NS get pods -l spark-role -o json > "$tmp/pods.json" 2>/dev/null
  kubectl -n $NS get workloads -o json > "$tmp/wls.json" 2>/dev/null
  kubectl get clusterqueue $CQ -o json > "$tmp/cq.json" 2>/dev/null
  VIEW_APP_LABEL=$APP_LABEL VIEW_STATE=$STATE python3 -I "$HERE/da-view.py" "$tmp/pods.json" "$tmp/wls.json" "$tmp/cq.json" "$QUOTA" "$MAXES"
  rm -rf "$tmp"
}

live() { # live UNTIL_CMD — redraw every REFRESH seconds (default 10) until UNTIL_CMD succeeds (or forever if empty)
  while true; do
    clear; date +%H:%M:%S; table
    [ -n "${1:-}" ] && eval "$1" && return 0
    sleep "${REFRESH:-10}"
  done
}

# all_done: every app's driver has finished (or the deadline passed)
all_done() {
  [ "$(date +%s)" -ge "$DEADLINE" ] && { echo "  (stopped waiting after ${TIMEOUT:-900}s)"; return 0; }
  local a st
  for a in $NAMES; do
    st=$(kubectl -n $NS get $SA "$a" -o jsonpath='{.status.currentState.currentStateSummary}' 2>/dev/null)
    case "$st" in Succeeded|Failed|ResourceReleased|TerminatedWithoutReleaseResources|SchedulingFailure|DriverEvicted|DriverStartTimedOut|ExecutorsStartTimedOut|DriverReadyTimedOut) ;; *) return 1 ;; esac
  done
}

summary() {
  say "Summary"
  local slices; slices=$(kubectl -n $NS get workloads -o json 2>/dev/null)
  python3 -I - "$STATE" "$MAXES" "$QUOTA" "$slices" <<'PY'
import json, sys, collections
state = json.load(open(sys.argv[1])); maxes = dict(t.split("=") for t in sys.argv[2].split(",")); quota = int(sys.argv[3])
try:
    wls = json.loads(sys.argv[4])["items"]
except ValueError:
    wls = []
n = collections.Counter(next((o["name"] for o in w["metadata"].get("ownerReferences", []) if o.get("controller")), "") for w in wls)
for a, mx in maxes.items():
    p = state.get("peak_running", {}).get(a, 0)
    w = "waited for quota" if state.get("waited", {}).get(a) else "never waited for quota"
    print(f"  {a:8} max {mx:>2}: peaked at {p} running executor{'s' * (p != 1)}, {n[a]} Workload slice{'s' * (n[a] != 1)}, {w}")
held, res = state.get("peak_held", 0), state.get("peak_reserved", 0)
print(f"  queue: the Pods held at most {held} of {quota} slots"
      + (f", above the quota in {state['held_over_quota']} redraws" if state.get("held_over_quota") else ", never above the quota"))
print(f"  Kueue's reservation peaked at {res:g} CPU"
      + (f", above the quota in {state['reserved_over_quota']} redraws: while it replaces an app's Workload slice with the next,"
         " Kueue briefly counts both slices; the Pods never used that quota" if state.get("reserved_over_quota") and not state.get("held_over_quota") else "")
      + (f", above the quota in {state['reserved_over_quota']} redraws" if state.get("reserved_over_quota") and state.get("held_over_quota") else ""))
PY
}

[ "${1:-}" = "watch" ] && { DEADLINE=$(( $(date +%s) + 86400 )); live ""; exit 0; }
case "${1:-}" in -h|--help) sed -n 2,22p "$0" | sed 's/^# \{0,1\}//'; exit 0 ;; esac
[ -n "${1:-}" ] && { echo "unknown argument '$1' (try --help)" >&2; exit 2; }
[ "$QUOTA" -le 36 ] || { echo "QUOTA must be at most 36" >&2; exit 2; }

# ---- preflight
say "Preflight"
args=$(kubectl -n kueue-system get deploy kueue-controller-manager -o jsonpath='{.spec.template.spec.containers[0].args}')
echo "  Kueue: $(kubectl -n kueue-system get deploy kueue-controller-manager -o jsonpath='{.spec.template.spec.containers[0].image}')"
[[ "$args" == *$GATE=true* ]] || { echo "  ❌ $GATE gate is off" >&2; exit 1; }
if ! kubectl get clusterqueue $CQ >/dev/null 2>&1 || ! kubectl -n $NS get localqueue $LQ >/dev/null 2>&1; then
  echo "  $CQ / $LQ missing: applying $(setup_file queues-setup.yaml) and $(setup_file queues-apache.yaml)"
  kubectl apply -f "$(setup_file queues-setup.yaml)" >/dev/null || exit 1
  kubectl apply -f "$(setup_file queues-apache.yaml)" >/dev/null || exit 1
fi
mkdir -p "$HERE/runs"
LOCK="$HERE/runs/.da-varying-max.lock"
mkdir "$LOCK" 2>/dev/null || { echo "  refusing to start: another run-da-varying-max.sh is in progress (remove $LOCK if not)" >&2; exit 3; }
set_quota() {
  kubectl patch clusterqueue $CQ --type=json -p "[
    {\"op\":\"replace\",\"path\":\"/spec/resourceGroups/0/flavors/0/resources/0/nominalQuota\",\"value\":\"$1\"},
    {\"op\":\"replace\",\"path\":\"/spec/resourceGroups/0/flavors/0/resources/1/nominalQuota\",\"value\":\"$(( $1 * 896 ))Mi\"}]" >/dev/null
}
cleanup() {
  [ "${QUOTA_SET:-0}" = 1 ] && set_quota 36
  rmdir "$LOCK" 2>/dev/null; rm -f "$STATE"
}
trap cleanup EXIT
mine=$(echo "$NAMES" | paste -sd'|' -)
others=$(kubectl -n $NS get $SA -o name 2>/dev/null | sed 's#.*/##' | grep -vE "^($mine)$" || true)
if [ -n "$others" ] && [ "${FORCE:-0}" != 1 ]; then
  echo "  refusing to start: $NS has other SparkApplications ($(echo $others | tr '\n' ' ')). Rerun with FORCE=1 to delete them." >&2
  exit 3
fi

say "Cleaning $NS"
kubectl -n $NS delete $SA --all --wait=true >/dev/null 2>&1
for _ in $(seq 1 60); do [ "$(kubectl -n $NS get workloads,pods --no-headers 2>/dev/null | wc -l | tr -d ' ')" = 0 ] && break; sleep 3; done

say "Submitting $(echo "$NAMES" | wc -l | tr -d ' ') apps with Dynamic Allocation (minExecutors $MIN, maxExecutors $(echo "$MAXES" | sed 's/[a-z0-9-]*=//g; s/,/, /g')), $NEED slots at their maximums, in a $QUOTA-slot queue"
set_quota $QUOTA; QUOTA_SET=1
for spec in $(echo "$APPS" | tr ',' ' '); do
  IFS=: read -r name mx iters <<<"$spec"
  python3 -I "$HERE/gen-apache-apps.py" "$name:low" --queue $LQ --min-executors "$MIN" --max-executors "$mx" \
    --args "1500 1500 12 $iters 200" | record_apply "$name"
done
DEADLINE=$(( $(date +%s) + ${TIMEOUT:-900} ))
live all_done
clear; date +%H:%M:%S; table   # end on the finished state
summary
if [ "${KEEP:-0}" != 1 ]; then
  for a in $NAMES; do kubectl -n $NS delete $SA "$a" --wait=false >/dev/null 2>&1; done
  echo; echo "  Demo apps deleted (in the background). KEEP=1 leaves them running."
fi
