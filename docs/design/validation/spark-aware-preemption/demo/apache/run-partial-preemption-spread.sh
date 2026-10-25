#!/usr/bin/env bash
# Demo of the many-victims scenarios: small low-priority Spark apps fill cluster-queue-tres, then a
# high-priority app arrives. Partial preemption trims executors across every victim, no app is
# evicted or restarted, and when the urgent app finishes the victims grow back.
#
#   p10   12 victims x (driver + 2 executors); the urgent app needs 12 slots: exactly 1 from each
#   p11   9 victims x (driver + 3 executors); the urgent app needs 15 slots: 1 from each, then a
#         second from 6 of them (round-robin) — the default
#   p10s  6 victims x (driver + 2); urgent needs 6: exactly 1 from each (18-slot queue)
#   p11s  5 victims x (driver + 3); urgent needs 8: 1 from each, then a second from 3 (20-slot queue)
#
#   [AUTO=1] [FORCE=1] [GROWBACK=0] run-partial-preemption-spread.sh [p10|p11|p10s|p11s]
#   VICTIMS=N EXECS=N URGENT_EXECS=N run-partial-preemption-spread.sh   any other shape (victims x (EXECS+1) <= 36)
#     AUTO=1        submit the urgent app as soon as every victim executor runs (no Enter prompt)
#     FORCE=1       delete other SparkApplications in batch-ns-dos before starting (otherwise refuse)
#     GROWBACK=0    stop once the urgent app runs, instead of waiting for the victims to grow back
#     VICTIM_ITERS / URGENT_ITERS   SparkALS iterations (default 10 / 4: ~4 min / ~1 min jobs)
#     REFRESH=N     seconds between screen refreshes (default 10)
#   The queue is sized to exactly what the victims fill, and put back to 36 slots on exit.
#   run-partial-preemption-spread.sh watch      the live table only, e.g. on a second screen
#
# Needs: Kueue with SparkPartialPreemption=true, cluster-queue-tres (../queues-setup.yaml) and
# batch-queue-tres in batch-ns-dos (queues-apache.yaml).
set -uo pipefail
HERE="$(cd "$(dirname "$0")" && pwd)"
. "$HERE/applied-yaml.sh"   # record_apply: saves each applied manifest
# setup_file NAME: the setup YAML NAME, in this script's directory or the nearest parent that has it
# (directly, or in its setup/ directory), so it is found in the ~/kueue-gated-repro and the repo layouts.
setup_file() { local d=$HERE n c; for n in 0 1 2 3 4; do for c in "$d/$1" "$d/setup/$1"; do [ -f "$c" ] && { echo "$c"; return; }; done; d=$(dirname "$d"); done; echo "$HERE/$1"; }
NS=batch-ns-dos; SA=sparkapplications.spark.apache.org; CQ=cluster-queue-tres; LQ=batch-queue-tres
FLOOR=1
case "${1:-p11}" in
  p10) VICTIMS=${VICTIMS:-12}; EXECS=${EXECS:-2}; URGENT_EXECS=${URGENT_EXECS:-11} ;;
  p11|watch|-h|--help) VICTIMS=${VICTIMS:-9}; EXECS=${EXECS:-3}; URGENT_EXECS=${URGENT_EXECS:-14} ;;
  p10s) VICTIMS=${VICTIMS:-6}; EXECS=${EXECS:-2}; URGENT_EXECS=${URGENT_EXECS:-5} ;;
  p11s) VICTIMS=${VICTIMS:-5}; EXECS=${EXECS:-3}; URGENT_EXECS=${URGENT_EXECS:-7} ;;
  *) echo "unknown argument '$1' (try --help)" >&2; exit 2 ;;
esac
SCENARIO=$(echo "${1:-p11}" | tr a-z A-Z)
# Victims outlive the urgent app, so the demo ends with them growing back.
VICTIM_ARGS="1500 1500 12 ${VICTIM_ITERS:-10} 200"
URGENT_ARGS="1500 1500 12 ${URGENT_ITERS:-4} 200"
QUOTA=$((VICTIMS * (EXECS + 1)))   # slots: the victims fill the queue exactly

say()   { printf '\n\033[1;36m▶ %s\033[0m\n' "$*"; }
pause() { printf '\n\033[1m⏸  %s — press Enter\033[0m ' "$*"; read -r _; }

# table: one line per app — executors running / gated (waiting for quota) / being deleted, and the
# Kueue grant of the executor PodSet on the app's admitted Workload slice.
table() {
  # The JSON goes through files: as arguments it outgrows the argument-size limit at ~40 Pods.
  local tmp; tmp=$(mktemp -d)
  kubectl -n $NS get pods -l spark-role -o json > "$tmp/pods.json" 2>/dev/null
  kubectl -n $NS get workloads -o json > "$tmp/wls.json" 2>/dev/null
  kubectl -n $NS get events -o json > "$tmp/events.json" 2>/dev/null
  python3 -I "$HERE/spread-view.py" "$tmp/pods.json" "$tmp/wls.json" "$tmp/events.json" \
    "${QUOTA:-36}" "${EXECS:-3}" "${URGENT_EXECS:-1}" "${TRIGGER:-}"
  rm -rf "$tmp"
}

# events since the trigger: one line per reclaim decision on each victim
reclaims() {
  kubectl -n $NS get events -o json 2>/dev/null | python3 -I -c '
import json, sys, re
from datetime import datetime
def hms(t):  # event times are UTC; show them in local time like the rest of the screen
    return datetime.fromisoformat(t[:19] + "+00:00").astimezone().strftime("%H:%M:%S")
since = sys.argv[1]
for e in sorted(json.load(sys.stdin)["items"], key=lambda e: e.get("lastTimestamp") or e.get("eventTime") or ""):
    t = e.get("lastTimestamp") or e.get("eventTime") or ""
    if t[:19] < since[:19] or e["reason"] not in ("PartiallyPreempted", "Preempted"):
        continue
    m = re.search(r"Partially preempted (\d+) Pod\(s\) of PodSet \S+, from (\d+) to (\d+)", e.get("message") or "")
    app = re.sub(r"^sparkapplication-|-[0-9a-f]{5}$", "", e["involvedObject"]["name"])
    what = f"gave {m.group(1)} executor(s), {m.group(2)} -> {m.group(3)}" if m else "EVICTED WHOLE"
    print(f"  {hms(t)}  {app:10} {what}")
' "$1"
}

live() { # live UNTIL_CMD [PROMPT] — redraw every REFRESH seconds (default 10) until UNTIL_CMD succeeds
  # (or forever if empty). With a PROMPT, it is shown under the table and Enter also ends the wait.
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

# summary: what the demo showed, from the reclaim events since the urgent app arrived
summary() {
  say "Summary"
  kubectl -n $NS get events -o json 2>/dev/null | python3 -I -c '
import json, sys, re, collections
since = sys.argv[1]
gave, whole = collections.Counter(), 0
for e in json.load(sys.stdin)["items"]:
    t = e.get("lastTimestamp") or e.get("eventTime") or ""
    if t[:19] < since[:19] or e["involvedObject"].get("kind") != "Workload":
        continue
    m = re.search(r"Partially preempted (\d+) Pod", e.get("message") or "")
    if e["reason"] == "PartiallyPreempted" and m:
        gave[re.sub(r"^sparkapplication-|-[0-9a-f]{5}$", "", e["involvedObject"]["name"])] += int(m.group(1))
    elif e["reason"] == "Preempted":
        whole += 1
dist = collections.Counter(gave.values())
parts = [str(n) + (" apps" if n > 1 else " app") + f" gave {k}" for k, n in sorted(dist.items())]
print(f"  {sum(gave.values())} executors reclaimed from {len(gave)} apps: " + ", ".join(parts))
if len(dist) > 1 and max(gave.values()) - min(gave.values()) == 1:
    print(f"  round-robin: every app gave {min(gave.values())} before any app gave {max(gave.values())}")
print(f"  apps evicted whole: {whole}; drivers lost: {whole}; restarts: {whole}")
' "$TRIGGER"
  [ -n "${URGENT_UP:-}" ] && echo "  urgent app running $(( URGENT_UP - TRIGGER_AT ))s after it was submitted (the reclaimed executors' 30s grace period, then teardown)"
  [ -n "${GROWN_AT:-}" ] && echo "  every app back to $EXECS executors $(( GROWN_AT - TRIGGER_AT ))s after the urgent app arrived, without a restart"
}

all_victims_running() {
  [ "$(kubectl -n $NS get pods -l spark-role=executor --field-selector=status.phase=Running --no-headers 2>/dev/null | wc -l | tr -d ' ')" -ge $((VICTIMS * EXECS)) ]
}
# growback_done: the urgent app has finished and every victim still running is back to EXECS
# running executors (or the deadline passed).
growback_done() {
  [ "$(date +%s)" -ge "$DEADLINE" ] && { echo "  (stopped waiting for grow-back after ${GROWBACK_TIMEOUT:-420}s)"; return 0; }
  local st; st=$(kubectl -n $NS get $SA als-hi -o jsonpath='{.status.currentState.currentStateSummary}' 2>/dev/null)
  case "$st" in Succeeded|Failed|ResourceReleased|TerminatedWithoutReleaseResources|SchedulingFailure|DriverEvicted|DriverStartTimedOut|ExecutorsStartTimedOut|DriverReadyTimedOut) ;; *) return 1 ;; esac
  kubectl -n $NS get pods -l spark-role=executor --field-selector=status.phase=Running \
    -o jsonpath='{range .items[*]}{.metadata.labels.spark\.operator/spark-app-name}{"\n"}{end}' 2>/dev/null |
    awk -v want=$EXECS -v total=$VICTIMS '/^als-lo/ {n[$1]++} END {short=0; for (a in n) if (n[a] < want) short++; exit (short > 0)}'
}
urgent_running() {
  [ "$(kubectl -n $NS get pods -l spark.operator/spark-app-name=als-hi,spark-role=executor --field-selector=status.phase=Running --no-headers 2>/dev/null | wc -l | tr -d ' ')" -ge $URGENT_EXECS ]
}

[ "${1:-}" = "watch" ] && { live ""; exit 0; }
case "${1:-}" in -h|--help) sed -n 2,19p "$0" | sed 's/^# \{0,1\}//'; exit 0 ;; esac
if [ $QUOTA -gt 36 ] || [ $((URGENT_EXECS + 1)) -gt $QUOTA ]; then
  echo "VICTIMS x (EXECS + 1) must be at most 36, and the urgent app (URGENT_EXECS + 1) must fit in it" >&2; exit 2
fi

# ---- preflight
say "Preflight"
args=$(kubectl -n kueue-system get deploy kueue-controller-manager -o jsonpath='{.spec.template.spec.containers[0].args}')
echo "  Kueue: $(kubectl -n kueue-system get deploy kueue-controller-manager -o jsonpath='{.spec.template.spec.containers[0].image}')"
[[ "$args" == *SparkPartialPreemption=true* ]] || { echo "  ❌ SparkPartialPreemption gate is off" >&2; exit 1; }
if ! kubectl get clusterqueue $CQ >/dev/null 2>&1 || ! kubectl -n $NS get localqueue $LQ >/dev/null 2>&1; then
  echo "  $CQ / $LQ missing: applying $(setup_file queues-setup.yaml) and $(setup_file queues-apache.yaml)"
  kubectl apply -f "$(setup_file queues-setup.yaml)" >/dev/null || exit 1
  kubectl apply -f "$(setup_file queues-apache.yaml)" >/dev/null || exit 1
fi
echo "  $CQ: $(kubectl get clusterqueue $CQ -o jsonpath='{.spec.resourceGroups[0].flavors[0].resources[0].nominalQuota}') CPU, preemption $(kubectl get clusterqueue $CQ -o jsonpath='{.spec.preemption.withinClusterQueue}')"
mkdir -p "$HERE/runs"
LOCK="$HERE/runs/.partial-preemption-spread.lock"
mkdir "$LOCK" 2>/dev/null || { echo "  refusing to start: another run-partial-preemption-spread.sh is in progress (remove $LOCK if not)" >&2; exit 3; }
# set_quota SLOTS: one slot = 1 CPU + 896Mi
set_quota() {
  kubectl patch clusterqueue $CQ --type=json -p "[
    {\"op\":\"replace\",\"path\":\"/spec/resourceGroups/0/flavors/0/resources/0/nominalQuota\",\"value\":\"$1\"},
    {\"op\":\"replace\",\"path\":\"/spec/resourceGroups/0/flavors/0/resources/1/nominalQuota\",\"value\":\"$(( $1 * 896 ))Mi\"}]" >/dev/null
}
cleanup() {
  [ "${QUOTA_SET:-0}" = 1 ] && set_quota 36
  rmdir "$LOCK" 2>/dev/null
}
trap cleanup EXIT
others=$(kubectl -n $NS get $SA -o name 2>/dev/null | sed 's#.*/##' | grep -vE '^als-(lo[0-9]+|hi)$' || true)
if [ -n "$others" ] && [ "${FORCE:-0}" != 1 ]; then
  echo "  refusing to start: $NS has other SparkApplications ($(echo $others | tr '\n' ' ')). Rerun with FORCE=1 to delete them." >&2
  exit 3
fi

say "Cleaning $NS"
kubectl -n $NS delete $SA --all --wait=true >/dev/null 2>&1
for _ in $(seq 1 60); do [ "$(kubectl -n $NS get workloads,pods --no-headers 2>/dev/null | wc -l | tr -d ' ')" = 0 ] && break; sleep 3; done


victims=$(python3 -c "print(','.join(f'als-lo{i}:low' for i in range(1, $VICTIMS + 1)))")
say "$SCENARIO: submitting $VICTIMS low-priority apps, driver + $EXECS executors each, floor $FLOOR (the queue is sized to $QUOTA slots)"
set_quota $QUOTA; QUOTA_SET=1
python3 -I "$HERE/gen-apache-apps.py" "$victims" --queue $LQ --static-elastic $EXECS --floor $FLOOR --args "$VICTIM_ARGS" | record_apply victims
live all_victims_running
[ -t 0 ] || AUTO=1
if [ "${AUTO:-0}" = 1 ]; then sleep 5; else
  live "" "⏸  Queue is full ($QUOTA of $QUOTA slots). Press Enter to submit the high-priority app (needs $((URGENT_EXECS + 1)) slots)"
fi
TRIGGER=$(date -u +%Y-%m-%dT%H:%M:%SZ); TRIGGER_AT=$(date +%s)
python3 -I "$HERE/gen-apache-apps.py" als-hi:high --queue $LQ --static $URGENT_EXECS --args "$URGENT_ARGS" | record_apply urgent
live urgent_running
URGENT_UP=$(date +%s)
echo; echo "  ✅ als-hi is running all $URGENT_EXECS executors; every victim kept its driver."

if [ "${GROWBACK:-1}" = 1 ]; then
  sleep 3
  DEADLINE=$(( $(date +%s) + ${GROWBACK_TIMEOUT:-420} ))
  live growback_done
  GROWN_AT=$(date +%s)
  clear; date +%H:%M:%S; table   # end on the grown-back state
  echo; echo "  ✅ als-hi finished and the victims grew back to $EXECS executors each, with no restart."
fi

summary
if [ "${KEEP:-0}" != 1 ]; then
  kubectl -n $NS get $SA -o name 2>/dev/null | grep -E '/als-(lo[0-9]+|hi)$' | xargs -r kubectl -n $NS delete --wait=false >/dev/null 2>&1
  echo; echo "  Demo apps deleted (in the background). KEEP=1 leaves them running."
fi
