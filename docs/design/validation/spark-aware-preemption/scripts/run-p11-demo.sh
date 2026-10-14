#!/usr/bin/env bash
# Demo of the many-victims scenarios: small low-priority Spark apps fill a 36-slot ClusterQueue,
# then a high-priority app arrives. Partial preemption trims executors across every victim, and no
# app is evicted or restarted.
#
#   p10  12 victims x (driver + 2 executors); the urgent app needs 12 slots: exactly 1 from each
#   p11  9 victims x (driver + 3 executors); the urgent app needs 15 slots: 1 from each, then a
#        second from 6 of them (round-robin) — the default
#
#   [AUTO=1] [FORCE=1] [CAPTURE=0] run-p11-demo.sh [p10|p11]
#   VICTIMS=N EXECS=N URGENT_EXECS=N run-p11-demo.sh   any other shape (victims x (EXECS+1) <= 36)
#     AUTO=1     submit the urgent app as soon as every victim executor runs (no Enter prompt)
#     FORCE=1    delete other SparkApplications in batch-ns-uno before starting (otherwise refuse)
#     CAPTURE=0  skip the background capture (then no report at the end)
#   run-p11-demo.sh watch      the live table only, e.g. on a second screen
#
# Needs: Kueue with SparkPartialPreemption=true, cluster-queue-tres and batch-queue-tres
# (queues-setup.yaml), and gen-apps.py / capture.sh / req-check.py / pp-analyze.py beside it.
set -uo pipefail
HERE="$(cd "$(dirname "$0")" && pwd)"
NS=batch-ns-uno; SA=sparkapplications.sparkoperator.k8s.io; CQ=cluster-queue-tres; LQ=batch-queue-tres
FLOOR=1
case "${1:-p11}" in
  p10) VICTIMS=${VICTIMS:-12}; EXECS=${EXECS:-2}; URGENT_EXECS=${URGENT_EXECS:-11} ;;
  p11|watch|-h|--help) VICTIMS=${VICTIMS:-9}; EXECS=${EXECS:-3}; URGENT_EXECS=${URGENT_EXECS:-14} ;;
  *) echo "unknown argument '$1' (try --help)" >&2; exit 2 ;;
esac
SCENARIO=$(echo "${1:-p11}" | tr a-z A-Z)
VICTIM_ARGS="1500 1500 12 40 200"   # long enough that victims keep running well past the reclaim
URGENT_ARGS="1500 1500 12 15 200"

say()   { printf '\n\033[1;36m▶ %s\033[0m\n' "$*"; }
pause() { printf '\n\033[1m⏸  %s — press Enter\033[0m ' "$*"; read -r _; }

# table: one line per app — executors running / gated (waiting for quota) / being deleted, and the
# Kueue grant of the executor PodSet on the app's admitted Workload slice.
table() {
  # The JSON goes through files: as arguments it outgrows the argument-size limit at ~40 Pods.
  local tmp; tmp=$(mktemp -d)
  kubectl -n $NS get pods -l spark-role -o json > "$tmp/pods.json" 2>/dev/null
  kubectl -n $NS get workloads -o json > "$tmp/wls.json" 2>/dev/null
  python3 -I "$HERE/p11-table.py" "$tmp/pods.json" "$tmp/wls.json"
  rm -rf "$tmp"
}

# events since the trigger: one line per reclaim decision on each victim
reclaims() {
  kubectl -n $NS get events -o json 2>/dev/null | python3 -I -c '
import json, sys, re
since = sys.argv[1]
for e in sorted(json.load(sys.stdin)["items"], key=lambda e: e.get("lastTimestamp") or e.get("eventTime") or ""):
    t = e.get("lastTimestamp") or e.get("eventTime") or ""
    if t < since or e["reason"] not in ("PartiallyPreempted", "Preempted"):
        continue
    m = re.search(r"Partially preempted (\d+) Pod\(s\) of PodSet \S+, from (\d+) to (\d+)", e.get("message") or "")
    app = re.sub(r"^sparkapplication-|-[0-9a-f]{5}$", "", e["involvedObject"]["name"])
    what = f"gave {m.group(1)} executor(s), {m.group(2)} -> {m.group(3)}" if m else "EVICTED WHOLE"
    print(f"  {t[11:19]}  {app:10} {what}")
' "$1"
}

live() { # live UNTIL_CMD — redraw every 3s until UNTIL_CMD succeeds (or forever if empty)
  while true; do
    clear; date +%H:%M:%S; table
    [ -n "${TRIGGER:-}" ] && { echo; echo "  reclaims since the urgent app arrived:"; reclaims "$TRIGGER"; }
    [ -n "${1:-}" ] && eval "$1" && return 0
    sleep 3
  done
}

all_victims_running() {
  [ "$(kubectl -n $NS get pods -l spark-role=executor --field-selector=status.phase=Running --no-headers 2>/dev/null | wc -l | tr -d ' ')" -ge $((VICTIMS * EXECS)) ]
}
urgent_running() {
  [ "$(kubectl -n $NS get pods -l sparkoperator.k8s.io/app-name=als-hi,spark-role=executor --field-selector=status.phase=Running --no-headers 2>/dev/null | wc -l | tr -d ' ')" -ge $URGENT_EXECS ]
}

[ "${1:-}" = "watch" ] && { live ""; exit 0; }
case "${1:-}" in -h|--help) sed -n 2,19p "$0" | sed 's/^# \{0,1\}//'; exit 0 ;; esac
if [ $((VICTIMS * (EXECS + 1))) -gt 36 ] || [ $((URGENT_EXECS + 1)) -gt 36 ]; then
  echo "VICTIMS x (EXECS + 1) and URGENT_EXECS + 1 must each fit the 36-slot queue" >&2; exit 2
fi

# ---- preflight
say "Preflight"
args=$(kubectl -n kueue-system get deploy kueue-controller-manager -o jsonpath='{.spec.template.spec.containers[0].args}')
echo "  Kueue: $(kubectl -n kueue-system get deploy kueue-controller-manager -o jsonpath='{.spec.template.spec.containers[0].image}')"
[[ "$args" == *SparkPartialPreemption=true* ]] || { echo "  ❌ SparkPartialPreemption gate is off" >&2; exit 1; }
if ! kubectl get clusterqueue $CQ >/dev/null 2>&1 || ! kubectl -n $NS get localqueue $LQ >/dev/null 2>&1; then
  echo "  $CQ / $LQ missing: applying queues-setup.yaml"
  kubectl apply -f "$HERE/queues-setup.yaml" >/dev/null || exit 1
fi
echo "  $CQ: $(kubectl get clusterqueue $CQ -o jsonpath='{.spec.resourceGroups[0].flavors[0].resources[0].nominalQuota}') CPU, preemption $(kubectl get clusterqueue $CQ -o jsonpath='{.spec.preemption.withinClusterQueue}')"
mkdir -p "$HERE/runs"
LOCK="$HERE/runs/.p11-demo.lock"
mkdir "$LOCK" 2>/dev/null || { echo "  refusing to start: another run-p11-demo.sh is in progress (remove $LOCK if not)" >&2; exit 3; }
CAP=""
cleanup() { [ -n "$CAP" ] && kill "$CAP" 2>/dev/null; rmdir "$LOCK" 2>/dev/null; }
trap cleanup EXIT
others=$(kubectl -n $NS get $SA -o name 2>/dev/null | sed 's#.*/##' | grep -vE '^als-(lo[0-9]+|hi)$' || true)
if [ -n "$others" ] && [ "${FORCE:-0}" != 1 ]; then
  echo "  refusing to start: $NS has other SparkApplications ($(echo $others | tr '\n' ' ')). Rerun with FORCE=1 to delete them." >&2
  exit 3
fi

say "Cleaning $NS"
kubectl -n $NS delete $SA --all --wait=true >/dev/null 2>&1
for _ in $(seq 1 60); do [ "$(kubectl -n $NS get workloads,pods --no-headers 2>/dev/null | wc -l | tr -d ' ')" = 0 ] && break; sleep 3; done

OUT="$HERE/runs/$SCENARIO-DEMO-$(date +%m%d-%H%M)"
if [ "${CAPTURE:-1}" = 1 ]; then
  OUT_DIR="$OUT" "$HERE/capture.sh" -n $NS -q $CQ -d 900 > "$OUT.capture.log" 2>&1 &
  CAP=$!
  echo "  capturing to $OUT (15 minutes)"
  sleep 10
fi

victims=$(python3 -c "print(','.join(f'als-lo{i}:low' for i in range(1, $VICTIMS + 1)))")
say "$SCENARIO: submitting $VICTIMS low-priority apps, driver + $EXECS executors each, floor $FLOOR ($((VICTIMS * (EXECS + 1))) of 36 slots)"
python3 -I "$HERE/gen-apps.py" "$victims" --queue $LQ --static-elastic $EXECS --floor $FLOOR --als-args "$VICTIM_ARGS" | kubectl apply -f - >/dev/null
live all_victims_running

if [ "${AUTO:-0}" = 1 ]; then sleep 5; else pause "Queue is full. Submit the high-priority app (driver + $URGENT_EXECS executors = $((URGENT_EXECS + 1)) slots)"; fi
TRIGGER=$(date -u +%Y-%m-%dT%H:%M:%SZ)
python3 -I "$HERE/gen-apps.py" als-hi:high --queue $LQ --static $URGENT_EXECS --als-args "$URGENT_ARGS" | kubectl apply -f - >/dev/null
live urgent_running
echo; echo "  ✅ als-hi is running all $URGENT_EXECS executors; every victim kept its driver."

if [ -n "$CAP" ]; then
  say "Watching until the capture ends (Ctrl-C to stop early; the report commands are below)"
  echo "  python3 -I $HERE/req-check.py $OUT --slots 36 --preemption"
  echo "  python3 -I $HERE/pp-analyze.py --floor $FLOOR --expect-whole 0 $OUT"
  sleep 5
  live '! kill -0 $CAP 2>/dev/null'
  wait "$CAP" 2>/dev/null; CAP=""
  "$HERE/analyze.sh" "$OUT" >/dev/null 2>&1
  say "Report"
  python3 -I "$HERE/req-check.py" "$OUT" --slots 36 --preemption | grep -E 'SC-1|SC-2|capture'
  python3 -I "$HERE/pp-analyze.py" --floor $FLOOR --expect-whole 0 "$OUT"
fi
echo; echo "  Reset afterwards with: kubectl -n $NS delete $SA --all"
