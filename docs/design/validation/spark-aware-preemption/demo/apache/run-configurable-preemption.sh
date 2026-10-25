#!/usr/bin/env bash
# Apache Spark Operator copy of ../run-configurable-preemption.sh (spark.apache.org/v1 apps in
# batch-ns-dos). Demo part 5: partial preemption when a PreemptionConfig, not priority, picks the victims.
# cluster-queue-quatro has classic preemption off (withinClusterQueue and reclaimWithinCohort Never), the
# recommended setup with configurable preemption. Its PreemptionConfig rule (queues-quatro.yaml) lets
# apps labeled preemption-role=urgent take capacity from apps labeled preemption-pool=batch.
#
#   als-lo1..4  default priority, preemption-pool=batch, driver + 3 executors, floor 1  (16 slots)
#   als-lo5..6  low priority, no pool label,             driver + 3 executors, floor 1  ( 8 slots)
#   1. plain-hi  high priority, no preemption-role label, driver + 3: the queue is full and no rule
#                matches it, so it waits — priority alone preempts nothing. It is then withdrawn.
#   2. als-hi    high priority, preemption-role=urgent,   driver + 3: the rule matches, and the pool
#                apps give one executor each. als-lo5/6 are lower priority but outside the pool, so
#                they keep all theirs. No app is evicted whole, and the pool apps grow back after.
#
#   [AUTO=1] [FORCE=1] [GROWBACK=0] [CONTROL=0] run-configurable-preemption.sh
#     AUTO=1        no Enter prompts
#     FORCE=1       delete other SparkApplications in batch-ns-dos before starting (otherwise refuse)
#     GROWBACK=0    stop once the urgent app runs, instead of waiting for the pool apps to grow back
#     CONTROL=0     skip step 1 (the high-priority app without the urgent label)
#     CONTROL_WAIT=N  seconds step 1 shows plain-hi waiting (default 40)
#     VICTIM_ITERS / URGENT_ITERS   SparkALS iterations (default 12 / 4)
#     REFRESH=N     seconds between screen refreshes (default 10)
#   run-configurable-preemption.sh watch      the live table only, e.g. on a second screen
#
# Needs: Kueue with SparkPartialPreemption=true and ConfigurablePreemptions=true, the PreemptionConfig
# CRD, integrations.labelKeysToCopy [preemption-pool, preemption-role], and ../queues-quatro.yaml applied
# (applied here if cluster-queue-quatro is missing).
set -uo pipefail
HERE="$(cd "$(dirname "$0")" && pwd)"
# setup_file NAME: the setup YAML NAME, in this script's directory or the nearest parent that has it
# (directly, or in its setup/ directory), so it is found in the ~/kueue-gated-repro and the repo layouts.
setup_file() { local d=$HERE n c; for n in 0 1 2 3 4; do for c in "$d/$1" "$d/setup/$1"; do [ -f "$c" ] && { echo "$c"; return; }; done; d=$(dirname "$d"); done; echo "$HERE/$1"; }
NS=batch-ns-dos; SA=sparkapplications.spark.apache.org; CQ=cluster-queue-quatro; LQ=batch-queue-quatro
APP_LABEL=spark.operator/spark-app-name
POOL="als-lo1 als-lo2 als-lo3 als-lo4"; OUTSIDE="als-lo5 als-lo6"
VICTIMS=6; EXECS=3; URGENT_EXECS=3; FLOOR=1; QUOTA=24
VICTIM_ARGS="1500 1500 12 ${VICTIM_ITERS:-12} 200"
URGENT_ARGS="1500 1500 12 ${URGENT_ITERS:-4} 200"
TAGS=$( { for a in $POOL; do echo "$a=pool · default"; done; for a in $OUTSIDE; do echo "$a=no · low"; done; } | paste -sd, -)

say()   { printf '\n\033[1;36m▶ %s\033[0m\n' "$*"; }

gen()  { python3 -I "$HERE/gen-apache-apps.py" "$@"; }

table() {
  local tmp; tmp=$(mktemp -d)
  kubectl -n $NS get pods -l spark-role -o json > "$tmp/pods.json" 2>/dev/null
  kubectl -n $NS get workloads -o json > "$tmp/wls.json" 2>/dev/null
  kubectl -n $NS get events -o json > "$tmp/events.json" 2>/dev/null
  python3 -I "$HERE/spread-view.py" "$tmp/pods.json" "$tmp/wls.json" "$tmp/events.json" \
    $QUOTA $EXECS $URGENT_EXECS "${TRIGGER:-}" "$TAGS"
  [ -n "${CONTROL_AT:-}" ] && control_line "$tmp/wls.json"
  rm -rf "$tmp"
}

# control_line: step 1's high-priority app without the urgent label — admitted or waiting.
control_line() {
  python3 -I -c '
import json, sys
st = "withdrawn"
for w in json.load(open(sys.argv[1]))["items"]:
    if any(o.get("name") == "plain-hi" for o in w["metadata"].get("ownerReferences", [])):
        conds = {c["type"]: c["status"] for c in (w.get("status") or {}).get("conditions", [])}
        st = "ADMITTED" if conds.get("Admitted") == "True" else "waiting: no rule matches it, classic preemption is off"
print(f"\n  control app plain-hi (high priority, no preemption-role label): {st}")
' "$1" 2>/dev/null
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

# preemptions SINCE: "<whole> <partial>" preemption events on Workloads since SINCE (UTC)
preemptions() {
  kubectl -n $NS get events -o json 2>/dev/null | python3 -I -c '
import json, sys
since = sys.argv[1]
n = {"Preempted": 0, "PartiallyPreempted": 0}
for e in json.load(sys.stdin)["items"]:
    t = e.get("lastTimestamp") or e.get("eventTime") or ""
    if t[:19] >= since[:19] and e["involvedObject"].get("kind") == "Workload" and e["reason"] in n:
        n[e["reason"]] += 1
print(n["Preempted"], n["PartiallyPreempted"])
' "$1"
}

summary() {
  say "Summary"
  [ -n "${CONTROL_RESULT:-}" ] && echo "  step 1: $CONTROL_RESULT"
  kubectl -n $NS get events -o json 2>/dev/null | python3 -I -c '
import json, sys, re, collections
since, pool, outside = sys.argv[1], sys.argv[2].split(), sys.argv[3].split()
gave, whole = collections.Counter(), collections.Counter()
for e in json.load(sys.stdin)["items"]:
    t = e.get("lastTimestamp") or e.get("eventTime") or ""
    if t[:19] < since[:19] or e["involvedObject"].get("kind") != "Workload":
        continue
    app = re.sub(r"^sparkapplication-|-[0-9a-f]{5}$", "", e["involvedObject"]["name"])
    m = re.search(r"Partially preempted (\d+) Pod", e.get("message") or "")
    if e["reason"] == "PartiallyPreempted" and m:
        gave[app] += int(m.group(1))
    elif e["reason"] == "Preempted":
        whole[app] += 1
fmt = lambda apps: ", ".join(f"{a} {gave[a]}" for a in apps)
print(f"  step 2: pool apps (preemption-pool=batch, default priority) gave {sum(gave[a] for a in pool)} executors: {fmt(pool)}")
print(f"          apps outside the pool (low priority) gave {sum(gave[a] for a in outside)}: {fmt(outside)}")
print(f"  apps evicted whole: {sum(whole.values())}; drivers lost: {sum(whole.values())}; restarts: {sum(whole.values())}")
ok = all(gave[a] == 0 for a in outside) and sum(gave[a] for a in pool) > 0 and not whole
print("  ✅ the PreemptionConfig chose the victims, not priority" if ok else "  ❌ victims did not follow the PreemptionConfig rule: see the events above")
' "$TRIGGER" "$POOL" "$OUTSIDE"
  [ -n "${URGENT_UP:-}" ] && echo "  urgent app running $(( URGENT_UP - TRIGGER_AT ))s after it was submitted (the reclaimed executors' 30s grace period, then teardown)"
  [ -n "${GROWN_AT:-}" ] && echo "  every app back to $EXECS executors $(( GROWN_AT - TRIGGER_AT ))s after the urgent app arrived, without a restart"
}

all_victims_running() {
  [ "$(kubectl -n $NS get pods -l spark-role=executor --field-selector=status.phase=Running --no-headers 2>/dev/null | grep -c '^als-lo')" -ge $((VICTIMS * EXECS)) ]
}
control_waited() { [ "$(date +%s)" -ge "$CONTROL_UNTIL" ] || ! plain_hi_pending; }
plain_hi_pending() {
  [ -z "$(kubectl -n $NS get workloads -o json 2>/dev/null | python3 -I -c '
import json, sys
for w in json.load(sys.stdin)["items"]:
    if any(o.get("name") == "plain-hi" for o in w["metadata"].get("ownerReferences", [])):
        if any(c["type"] == "Admitted" and c["status"] == "True" for c in (w.get("status") or {}).get("conditions", [])):
            print("admitted")
')" ]
}
urgent_running() {
  [ "$(kubectl -n $NS get pods -l $APP_LABEL=als-hi,spark-role=executor --field-selector=status.phase=Running --no-headers 2>/dev/null | wc -l | tr -d ' ')" -ge $URGENT_EXECS ]
}
urgent_finished() {
  local st; st=$(kubectl -n $NS get $SA als-hi -o jsonpath='{.status.currentState.currentStateSummary}' 2>/dev/null)
  case "$st" in Succeeded|Failed|ResourceReleased|TerminatedWithoutReleaseResources|SchedulingFailure|DriverEvicted|DriverStartTimedOut|ExecutorsStartTimedOut|DriverReadyTimedOut) return 0 ;; esac
  return 1
}
growback_done() {
  [ "$(date +%s)" -ge "$DEADLINE" ] && { echo "  (stopped waiting for grow-back after ${GROWBACK_TIMEOUT:-420}s)"; return 0; }
  urgent_finished || return 1
  kubectl -n $NS get pods -l spark-role=executor --field-selector=status.phase=Running \
    -o jsonpath="{range .items[*]}{.metadata.labels.${APP_LABEL//./\\.}}{\"\\n\"}{end}" 2>/dev/null |
    awk -v want=$EXECS '/^als-lo/ {n[$1]++} END {short=0; for (a in n) if (n[a] < want) short++; exit (short > 0)}'
}

[ "${1:-}" = "watch" ] && { live ""; exit 0; }
case "${1:-}" in -h|--help) sed -n 2,23p "$0" | sed 's/^# \{0,1\}//'; exit 0 ;; "") ;; *) echo "unknown argument '$1' (try --help)" >&2; exit 2 ;; esac

# ---- preflight
say "Preflight"
args=$(kubectl -n kueue-system get deploy kueue-controller-manager -o jsonpath='{.spec.template.spec.containers[0].args}')
echo "  Kueue: $(kubectl -n kueue-system get deploy kueue-controller-manager -o jsonpath='{.spec.template.spec.containers[0].image}')"
ok=1
for g in SparkPartialPreemption ConfigurablePreemptions; do
  [[ "$args" == *$g=true* ]] || { echo "  ❌ $g gate is off" >&2; ok=0; }
done
cfg=$(kubectl -n kueue-system get cm kueue-manager-config -o jsonpath='{.data.controller_manager_config\.yaml}')
for k in preemption-pool preemption-role; do
  grep -q -- "- $k\$" <<<"$cfg" || { echo "  ❌ integrations.labelKeysToCopy does not list $k: the labels would not reach the Workloads" >&2; ok=0; }
done
kubectl get crd preemptionconfigs.kueue.x-k8s.io >/dev/null 2>&1 || { echo "  ❌ PreemptionConfig CRD is not installed" >&2; ok=0; }
[ $ok = 1 ] || { echo "  ./demo-short.sh preflight sets these up (it restarts Kueue after asking)" >&2; exit 1; }
if ! kubectl get clusterqueue $CQ >/dev/null 2>&1 || ! kubectl -n $NS get localqueue $LQ >/dev/null 2>&1; then
  echo "  $CQ / $LQ missing: applying $(setup_file queues-quatro.yaml)"
  kubectl apply -f "$(setup_file queues-quatro.yaml)" >/dev/null || exit 1
fi
echo "  $CQ: $(kubectl get clusterqueue $CQ -o jsonpath='{.spec.resourceGroups[0].flavors[0].resources[0].nominalQuota}') CPU, classic preemption withinClusterQueue=$(kubectl get clusterqueue $CQ -o jsonpath='{.spec.preemption.withinClusterQueue}') reclaimWithinCohort=$(kubectl get clusterqueue $CQ -o jsonpath='{.spec.preemption.reclaimWithinCohort}'), PreemptionConfig $(kubectl get clusterqueue $CQ -o jsonpath='{.metadata.annotations.kueue\.x-k8s\.io/preemption-config-name}')"
mkdir -p "$HERE/runs"
LOCK="$HERE/runs/.configurable-preemption.lock"
mkdir "$LOCK" 2>/dev/null || { echo "  refusing to start: another run-configurable-preemption.sh is in progress (remove $LOCK if not)" >&2; exit 3; }
trap 'rmdir "$LOCK" 2>/dev/null' EXIT
others=$(kubectl -n $NS get $SA -o name 2>/dev/null | sed 's#.*/##' | grep -vE '^(als-(lo[0-9]+|hi)|plain-hi)$' || true)
if [ -n "$others" ] && [ "${FORCE:-0}" != 1 ]; then
  echo "  refusing to start: $NS has other SparkApplications ($(echo $others | tr '\n' ' ')). Rerun with FORCE=1 to delete them." >&2
  exit 3
fi

say "Cleaning $NS"
kubectl -n $NS delete $SA --all --wait=true >/dev/null 2>&1
for _ in $(seq 1 60); do [ "$(kubectl -n $NS get workloads,pods --no-headers 2>/dev/null | wc -l | tr -d ' ')" = 0 ] && break; sleep 3; done

say "Submitting 4 pool apps (preemption-pool=batch, default priority) and 2 low-priority apps outside the pool, driver + $EXECS executors each, floor $FLOOR ($QUOTA of $QUOTA slots)"
gen "$(echo $POOL | sed 's/ /:default,/g'):default" --queue $LQ --static-elastic $EXECS --floor $FLOOR --args "$VICTIM_ARGS" --label preemption-pool=batch | kubectl apply -f - >/dev/null
gen "$(echo $OUTSIDE | sed 's/ /:low,/g'):low" --queue $LQ --static-elastic $EXECS --floor $FLOOR --args "$VICTIM_ARGS" | kubectl apply -f - >/dev/null
live all_victims_running
[ -t 0 ] || AUTO=1

if [ "${CONTROL:-1}" = 1 ]; then
  [ "${AUTO:-0}" = 1 ] && sleep 5 || live "" "⏸  Queue is full. Press Enter to submit a high-priority app WITHOUT the preemption-role=urgent label"
  CONTROL_AT=$(date -u +%Y-%m-%dT%H:%M:%SZ); CONTROL_UNTIL=$(( $(date +%s) + ${CONTROL_WAIT:-40} ))
  gen plain-hi:high --queue $LQ --static $URGENT_EXECS --args "$URGENT_ARGS" | kubectl apply -f - >/dev/null
  live control_waited
  read -r w p <<<"$(preemptions "$CONTROL_AT")"
  if plain_hi_pending && [ "$w" = 0 ] && [ "$p" = 0 ]; then
    CONTROL_RESULT="✅ plain-hi (high priority, no rule matches) waited ${CONTROL_WAIT:-40}s and preempted nothing"
  else
    CONTROL_RESULT="❌ plain-hi was admitted or preempted something ($w whole, $p partial preemptions): classic preemption is not off"
  fi
  echo; echo "  $CONTROL_RESULT; withdrawing it"
  kubectl -n $NS delete $SA plain-hi --wait=true >/dev/null 2>&1
  CONTROL_AT=""
fi

[ "${AUTO:-0}" = 1 ] && sleep 3 || live "" "⏸  Press Enter to submit the urgent app: high priority, preemption-role=urgent (needs $((URGENT_EXECS + 1)) slots)"
TRIGGER=$(date -u +%Y-%m-%dT%H:%M:%SZ); TRIGGER_AT=$(date +%s)
gen als-hi:high --queue $LQ --static $URGENT_EXECS --args "$URGENT_ARGS" --label preemption-role=urgent | kubectl apply -f - >/dev/null
live urgent_running
URGENT_UP=$(date +%s)
echo; echo "  ✅ als-hi is running all $URGENT_EXECS executors; every app kept its driver."

if [ "${GROWBACK:-1}" = 1 ]; then
  sleep 3
  DEADLINE=$(( $(date +%s) + ${GROWBACK_TIMEOUT:-420} ))
  live growback_done
  GROWN_AT=$(date +%s)
  clear; date +%H:%M:%S; table
  echo; echo "  ✅ als-hi finished and the pool apps grew back to $EXECS executors each, with no restart."
fi

summary
if [ "${KEEP:-0}" != 1 ]; then
  kubectl -n $NS get $SA -o name 2>/dev/null | grep -E '/(als-(lo[0-9]+|hi)|plain-hi)$' | xargs -r kubectl -n $NS delete --wait=false >/dev/null 2>&1
  echo; echo "  Demo apps deleted (in the background). KEEP=1 leaves them running."
fi
