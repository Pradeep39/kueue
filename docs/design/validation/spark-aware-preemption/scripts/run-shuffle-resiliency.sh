#!/usr/bin/env bash
# Demo: partial preemption keeps a shuffle-heavy job's work. A low-priority SparkPageRank app (10
# executors, floor 2, opted in) builds shuffle data on every executor; when a high-priority app arrives,
# Kueue reclaims a few executors, Spark's graceful decommissioning hands their shuffle data to the
# survivors, and the job finishes normally with nothing recomputed. One live screen shows it all.
#
#   [AUTO=1] [FORCE=1] [THRESH=40] [REFRESH=6] run-shuffle-resiliency.sh
#     AUTO=1     submit the urgent app automatically once every executor holds THRESH MiB
#     FORCE=1    delete other SparkApplications in batch-ns-uno before starting (otherwise refuse)
#     THRESH=N   MiB of shuffle per executor for the "ready" hint (default 40)
#     REFRESH=N  seconds between screen refreshes (default 10)
#   Press Enter to submit the urgent app (any time; the hint says when the data is there), and again
#   to end early. Logs stay in runs/SHUF-DEMO-<stamp>/ (exec-logs/, driver.log) for demo-short.sh logs.
#
# Runs on cluster-queue-uno (the 12-slot cohort with cluster-queue-dos). The victim is the one from
# run-shuffle-migration.sh (its --yaml mode), so the two stay identical.
set -uo pipefail
HERE="$(cd "$(dirname "$0")" && pwd)"
NS=batch-ns-uno; SA=sparkapplications.sparkoperator.k8s.io
VICTIM=pr-victim; THRESH=${THRESH:-40}; REFRESH=${REFRESH:-10}

say() { printf '\n\033[1;36m▶ %s\033[0m\n' "$*"; }
case "${1:-}" in -h|--help) sed -n 2,16p "$0" | sed 's/^# \{0,1\}//'; exit 0 ;; "") ;; *) echo "unknown argument '$1' (try --help)" >&2; exit 2 ;; esac
[ -t 0 ] || AUTO=1

mkdir -p "$HERE/runs"
LOCK="$HERE/runs/.shuffle-resiliency.lock"
mkdir "$LOCK" 2>/dev/null || { echo "refusing to start: another run-shuffle-resiliency.sh is in progress (remove $LOCK if not)" >&2; exit 3; }
OUT="$HERE/runs/SHUF-DEMO-$(date +%m%d-%H%M)"; mkdir -p "$OUT/exec-logs"
cleanup() {
  touch "$OUT/.stop"
  # Reap the streamers quietly: bash otherwise reports each one as "Terminated".
  { local j; for j in $(jobs -p); do kill "$j"; done; wait; } 2>/dev/null
  pkill -f "logs -f $VICTIM-" 2>/dev/null
  rmdir "$LOCK" 2>/dev/null
}
trap cleanup EXIT

others=$(kubectl -n $NS get $SA -o name 2>/dev/null | sed 's#.*/##' | grep -vxE "$VICTIM|urgent" || true)
if [ -n "$others" ] && [ "${FORCE:-0}" != 1 ]; then
  echo "refusing to start: $NS has other SparkApplications ($(echo $others | tr '\n' ' ')). Rerun with FORCE=1 to delete them." >&2
  exit 3
fi
say "Cleaning $NS"
kubectl -n $NS delete $SA --all --wait=true >/dev/null 2>&1
for _ in $(seq 1 60); do [ "$(kubectl -n $NS get workloads,pods --no-headers 2>/dev/null | wc -l | tr -d ' ')" = 0 ] && break; sleep 3; done

say "Submitting $VICTIM: SparkPageRank, 10 executors, opted in to partial preemption with a floor of 2"
"$HERE/run-shuffle-migration.sh" --yaml > "$OUT/victim.yaml"
kubectl apply -f "$OUT/victim.yaml" >/dev/null || exit 1

# Stream the driver's and every executor's log from start to exit, so a reclaimed executor's hand-off
# is on disk after its Pod is gone. A log file's existence marks a Pod as followed (bash 3.2: no maps).
(
  while [ ! -f "$OUT/.stop" ]; do
    for p in $(kubectl -n $NS get pods -l "sparkoperator.k8s.io/app-name=$VICTIM" --field-selector=status.phase=Running -o name 2>/dev/null); do
      p=${p#pod/}
      case "$p" in
        *-driver) f="$OUT/driver.log"; c=spark-kubernetes-driver ;;
        *) f="$OUT/exec-logs/$p.log"; c=spark-kubernetes-executor ;;
      esac
      [ -e "$f" ] && continue
      kubectl -n $NS logs -f "$p" -c $c > "$f" 2>&1 &
    done
    sleep 2
  done
) &

# view: refresh the table. Shuffle MiB per executor via kubectl exec + du, all executors in parallel.
view() {
  local tmp pids=""; tmp=$(mktemp -d)
  kubectl -n $NS get pods -l "sparkoperator.k8s.io/app-name=$VICTIM" -o json > "$tmp/pods.json" 2>/dev/null
  for p in $(kubectl -n $NS get pods -l "sparkoperator.k8s.io/app-name=$VICTIM,spark-role=executor" --field-selector=status.phase=Running -o name 2>/dev/null); do
    p=${p#pod/}
    ( kib=$(kubectl -n $NS exec "$p" -c spark-kubernetes-executor -- sh -c \
        'find /var/data -name "shuffle_*" -type f -exec du -k {} + 2>/dev/null | awk "{s+=\$1} END{printf \"%d\", s}"' 2>/dev/null)
      [ -n "$kib" ] && echo "$p $kib" > "$tmp/$p.du" ) &
    pids="$pids $!"
  done
  [ -n "$pids" ] && wait $pids   # only the du probes: a bare wait would also wait on the log streamer
  cat "$tmp"/*.du > "$tmp/du" 2>/dev/null
  local ust uex vst
  ust=$(kubectl -n $NS get $SA urgent -o jsonpath='{.status.applicationState.state}' 2>/dev/null)
  uex=$(kubectl -n $NS get pods -l sparkoperator.k8s.io/app-name=urgent,spark-role=executor --field-selector=status.phase=Running --no-headers 2>/dev/null | wc -l | tr -d ' ')
  vst=$(kubectl -n $NS get $SA $VICTIM -o jsonpath='{.status.applicationState.state}' 2>/dev/null)
  local urg="not submitted"
  [ -n "${TRIGGER:-}" ] && urg="${ust:-pending}${uex:+, $uex executors running}"
  [ "$ust" = RUNNING ] && [ -z "${URGENT_UP:-}" ] && URGENT_UP=$(date +%s)
  VICTIM_STATE=$vst
  clear; date +%H:%M:%S
  python3 -I "$HERE/shuffle-view.py" "$OUT" "$tmp/pods.json" "$tmp/du" "$urg" "$uex" "$vst" "$THRESH"
  READY=$?
  rm -rf "$tmp"
}

# wait_or_enter: true if Enter was pressed during the refresh interval (interactive only)
wait_or_enter() {
  if [ "${AUTO:-0}" != 1 ]; then read -r -t "$REFRESH" _ && return 0; else sleep "$REFRESH"; fi
  return 1
}

# Phase 1: shuffle data builds up; Enter (or AUTO at the threshold) submits the urgent app.
STARTED=$(date +%s)
while true; do
  view
  case "$VICTIM_STATE" in COMPLETED|FAILED) echo; echo "  $VICTIM finished before the urgent app was submitted: rerun and press Enter sooner."; exit 1 ;; esac
  echo
  if [ "$READY" = 0 ]; then
    printf '\033[1m  ✅ every executor holds at least %s MiB of shuffle data — press Enter to submit the high-priority app\033[0m\n' "$THRESH"
    [ "${AUTO:-0}" = 1 ] && break
  else
    printf '\033[1m  building shuffle data (%ss) — press Enter to submit the high-priority app\033[0m\n' $(( $(date +%s) - STARTED ))
  fi
  wait_or_enter && break
done

TRIGGER=$(date -u +%Y-%m-%dT%H:%M:%SZ); TRIGGER_AT=$(date +%s)
python3 -I "$HERE/gen-apps.py" urgent:high --partial --cap 2 --idle 5s | kubectl apply -f - >/dev/null

# Phase 2: the reclaim and the hand-off, until the victim finishes (Enter ends early).
while true; do
  view
  echo
  case "$VICTIM_STATE" in COMPLETED|FAILED) break ;; esac
  printf '\033[1m  urgent app submitted %ss ago — watch the "handed over" column; press Enter to end here\033[0m\n' $(( $(date +%s) - TRIGGER_AT ))
  [ $(( $(date +%s) - TRIGGER_AT )) -ge 480 ] && break
  wait_or_enter && break
done

say "Summary"
reclaimed=$(grep -ho "Decommission executors: [0-9, ]*" "$OUT/driver.log" 2>/dev/null | sed 's/.*: //' | tr ',' '\n' | tr -d ' ' | grep . | sort -un | tr '\n' ' ')
echo "  executors Kueue reclaimed: ${reclaimed:-none}"
echo "  $VICTIM: ${VICTIM_STATE:-unknown}$([ "$VICTIM_STATE" = COMPLETED ] && echo ' — the job finished normally')"
fetch=$(grep -cE "FetchFailed|fetch failure" "$OUT/driver.log" 2>/dev/null || true)
resub=$(grep -c "Resubmitting" "$OUT/driver.log" 2>/dev/null || true)
echo "  fetch failures: ${fetch:-0}, stages resubmitted: ${resub:-0} (nothing was recomputed)"
[ -n "${URGENT_UP:-}" ] && echo "  urgent app running $(( URGENT_UP - TRIGGER_AT ))s after it was submitted, while $VICTIM kept running on the executors it kept"
echo "  per-executor timelines: ./demo-short.sh logs   (full logs in $OUT)"
kubectl -n $NS delete $SA $VICTIM urgent --ignore-not-found --wait=false >/dev/null 2>&1
