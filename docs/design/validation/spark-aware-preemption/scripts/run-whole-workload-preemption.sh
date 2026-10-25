#!/usr/bin/env bash
# Demo of today's behaviour, before partial preemption: two shuffle-heavy SparkPageRank apps that have
# NOT opted in to partial preemption share a 12-slot queue with a high-priority app using Dynamic
# Allocation. Each time the urgent app is admitted or scales up, Kueue evicts a whole low-priority app:
# its driver dies, its shuffle data is lost, and when the urgent app scales down the victim is
# readmitted and starts over from scratch, only to be evicted again.
#
#   [AUTO=1] [FORCE=1] [DURATION=240] run-whole-workload-preemption.sh
#     AUTO=1       submit the urgent app as soon as both victims run (no Enter prompt)
#     FORCE=1      delete other SparkApplications in batch-ns-uno before starting (otherwise refuse)
#     DURATION=N   stop watching N seconds after the urgent app arrives (default 240), or earlier
#                  when the urgent app finishes
#     EVICTIONS=N  stop as soon as N whole evictions have happened (e.g. EVICTIONS=3)
#     REFRESH=N    seconds between screen refreshes (default 10)
#   To end early by hand: press Enter while the evictions are showing, or Ctrl-C. Either way the
#   summary is printed and the demo apps are deleted.
#     ITERS=N / URGENT_ITERS=N   PageRank iterations per victim run (default 100) / urgent SparkALS
#                  iterations (default 10)
#   cluster-queue-tres is sized to 12 slots for the run and put back to 36 on exit.
#
# Run it before run-shuffle-migration.sh to show the same kind of work being thrown away, then kept.
set -uo pipefail
HERE="$(cd "$(dirname "$0")" && pwd)"
# setup_file NAME: the setup YAML NAME, in this script's directory or the nearest parent that has it
# (directly, or in its setup/ directory), so it is found in the ~/kueue-gated-repro and the repo layouts.
setup_file() { local d=$HERE n c; for n in 0 1 2 3 4; do for c in "$d/$1" "$d/setup/$1"; do [ -f "$c" ] && { echo "$c"; return; }; done; d=$(dirname "$d"); done; echo "$HERE/$1"; }
NS=batch-ns-uno; SA=sparkapplications.sparkoperator.k8s.io; CQ=cluster-queue-tres; LQ=batch-queue-tres
VICTIMS="workload-a workload-b"; EXECS=4; QUOTA=12
ITERS=${ITERS:-100}
URGENT_ARGS="1500 1500 12 ${URGENT_ITERS:-10} 200"
GEN='awk "BEGIN{srand(42); for(i=0;i<2000000;i++) print int(rand()*200000), int(rand()*200000)}" > /data/edges.txt'

say()   { printf '\n\033[1;36m▶ %s\033[0m\n' "$*"; }
pause() { printf '\n\033[1m⏸  %s — press Enter\033[0m ' "$*"; read -r _; }

# victim_yaml NAME: a static SparkPageRank app on its own generated graph, without the elastic opt-in,
# so Kueue can only preempt it whole.
victim_yaml() {
  python3 -I "$HERE/gen-apps.py" "$1:low" --queue $LQ --static $EXECS \
    --main org.apache.spark.examples.SparkPageRank --als-args "file:///data/edges.txt $ITERS" | python3 -I -c '
import sys
y = sys.stdin.read()
gen = sys.argv[1]
y = y.replace("    \"spark.kubernetes.executor.podNamePrefix\"",
  "    \"spark.sql.files.maxPartitionBytes\": \"2m\"\n"
  "    \"spark.default.parallelism\": \"40\"\n"
  "    \"spark.driver.extraJavaOptions\": \"-Xss64m\"\n"
  "    \"spark.executor.extraJavaOptions\": \"-Xss64m\"\n"
  "    \"spark.kubernetes.executor.podNamePrefix\"", 1)
init = ("    initContainers:\n    - name: gen-edges\n      image: docker.io/library/spark:4.0.1\n"
        "      command: [\"sh\", \"-c\", %r]\n      volumeMounts: [{name: data, mountPath: /data}]\n"
        "      securityContext: {runAsUser: 185, runAsNonRoot: true, allowPrivilegeEscalation: false, "
        "capabilities: {drop: [ALL]}, seccompProfile: {type: RuntimeDefault}}\n"
        "    volumeMounts: [{name: data, mountPath: /data}]\n") % gen
y = y.replace("  driver:\n", "  volumes: [{name: data, emptyDir: {}}]\n  driver:\n" + init, 1)
y = y.replace("  executor:\n", "  executor:\n" + init, 1)
print(y)' "$GEN"
}

set_quota() {
  kubectl patch clusterqueue $CQ --type=json -p "[
    {\"op\":\"replace\",\"path\":\"/spec/resourceGroups/0/flavors/0/resources/0/nominalQuota\",\"value\":\"$1\"},
    {\"op\":\"replace\",\"path\":\"/spec/resourceGroups/0/flavors/0/resources/1/nominalQuota\",\"value\":\"$(( $1 * 896 ))Mi\"}]" >/dev/null
}

# history: per app, how many times Kueue admitted and evicted it since the demo started, and the
# eviction lines themselves.
history() {
  kubectl -n $NS get events -o json 2>/dev/null | python3 -I -c '
import json, sys, re, collections
from datetime import datetime
def hms(t):  # event times are UTC; show them in local time like the rest of the screen
    return datetime.fromisoformat(t[:19] + "+00:00").astimezone().strftime("%H:%M:%S")
since = sys.argv[1]
adm, ev, lines = collections.Counter(), collections.Counter(), []
for e in sorted(json.load(sys.stdin)["items"], key=lambda e: e.get("lastTimestamp") or e.get("eventTime") or ""):
    t = e.get("lastTimestamp") or e.get("eventTime") or ""
    if t[:19] < since[:19] or e["involvedObject"].get("kind") != "Workload":
        continue
    app = re.sub(r"^sparkapplication-|-[0-9a-f]{5}$", "", e["involvedObject"]["name"])
    if e["reason"] == "Admitted":
        adm[app] += 1
    elif e["reason"] == "Preempted":
        ev[app] += 1
        lines.append(f"  {hms(t)}  {app:9} EVICTED WHOLE: driver killed, shuffle data lost")
print("  app        admitted  evicted whole")
for a in sorted(set(adm) | set(ev)):
    if a.startswith("workload-"):
        print(f"  {a:10} {adm[a]:>8}  {ev[a]:>13}")
print()
print("\n".join(lines[-8:]) or "  (no evictions yet)")
' "$START"
}

table() {
  local tmp; tmp=$(mktemp -d)
  kubectl -n $NS get pods -l spark-role -o json > "$tmp/pods.json" 2>/dev/null
  kubectl -n $NS get workloads -o json > "$tmp/wls.json" 2>/dev/null
  kubectl -n $NS get events -o json > "$tmp/events.json" 2>/dev/null
  python3 -I "$HERE/demo-table.py" "$tmp/pods.json" "$tmp/wls.json" $QUOTA "$tmp/events.json" "$START" | grep -v "^  als-"
  rm -rf "$tmp"
}

# shuffle_view: each running executor of the two apps with the MiB of shuffle files on its disk
# (kubectl exec + du, all executors in parallel), a total per app, and a log of the shuffle data each
# eviction threw away: an app's last total before its executors disappeared.
SHUFFLE_STATE=$(mktemp -d)
shuffle_view() {
  local tmp; tmp=$(mktemp -d)
  kubectl -n $NS get pods -l spark-role=executor --field-selector=status.phase=Running \
    -o jsonpath='{range .items[*]}{.metadata.name}{" "}{.metadata.labels.sparkoperator\.k8s\.io/app-name}{" "}{.spec.nodeName}{" "}{.metadata.deletionTimestamp}{"\n"}{end}' 2>/dev/null |
    awk '$2 ~ /^workload-/' > "$tmp/pods"
  while read -r pod app node del; do
    ( kib=$(kubectl -n $NS exec "$pod" -c spark-kubernetes-executor -- sh -c \
        'find /var/data -name "shuffle_*" -type f -exec du -k {} + 2>/dev/null | awk "{s+=\$1} END{printf \"%d\", s}"' 2>/dev/null)
      echo "$pod $app $node ${kib:-0} ${del:+terminating}" > "$tmp/$pod.out" ) &
  done < "$tmp/pods"
  wait
  printf '  %-26s %-12s %12s\n' "executor" "node" "shuffle MiB"
  local app total line
  for app in workload-a workload-b; do
    total=0
    for f in "$tmp"/*.out; do
      [ -e "$f" ] || continue
      read -r pod a node kib state < "$f"
      [ "$a" = "$app" ] || continue
      printf '  %-26s %-12s %12d %s\n' "$pod" "$node" $(( kib / 1024 )) "$state"
      total=$(( total + kib / 1024 ))
    done
    printf '  %-26s %-12s %12d\n' "$app total" "" $total
    # Running executors gone after holding shuffle data: the eviction threw that data away.
    local prev; prev=$(cat "$SHUFFLE_STATE/$app" 2>/dev/null || echo 0)
    if [ "$total" = 0 ] && [ "$prev" -gt 0 ] &&
       [ "$(kubectl -n $NS get $SA $app -o jsonpath='{.status.applicationState.state}' 2>/dev/null)" != COMPLETED ]; then
      echo "  $(date +%H:%M:%S)  $app lost ${prev} MiB of shuffle data" >> "$SHUFFLE_STATE/lost.log"
    fi
    echo $total > "$SHUFFLE_STATE/$app"
  done
  if [ -s "$SHUFFLE_STATE/lost.log" ]; then echo; cat "$SHUFFLE_STATE/lost.log"; fi
  rm -rf "$tmp"
}

live() { # live UNTIL_CMD [stoppable [PROMPT]] — with "stoppable", Enter ends the wait early
  while true; do
    clear; date +%H:%M:%S; table; echo; shuffle_view; echo; history
    [ "${2:-}" = stoppable ] && echo && printf '\033[1m  %s\033[0m\n' "${3:-(press Enter to end the demo here)}"
    eval "$1" && return 0
    if [ "${2:-}" = stoppable ] && [ -t 0 ]; then
      read -r -t "${REFRESH:-10}" _ && return 0
    else
      sleep "${REFRESH:-10}"
    fi
  done
}

# evictions: whole evictions of the demo's apps since it started
evictions() {
  kubectl -n $NS get events -o json 2>/dev/null | python3 -I -c '
import json, sys
since = sys.argv[1]
print(sum(1 for e in json.load(sys.stdin)["items"]
          if e["reason"] == "Preempted" and e["involvedObject"].get("kind") == "Workload"
          and "-workload-" in e["involvedObject"]["name"]
          and (e.get("lastTimestamp") or e.get("eventTime") or "")[:19] >= since[:19]))' "$START"
}

summary() {
  say "Summary"
  history | head -4
  [ -s "$SHUFFLE_STATE/lost.log" ] && { echo; echo "  Shuffle data thrown away by the evictions:"; cat "$SHUFFLE_STATE/lost.log"; }
  echo
  echo "  Every eviction killed a driver and threw away that app's shuffle data; each readmission"
  echo "  started the job over from scratch."
  kubectl -n $NS delete $SA --all --wait=false >/dev/null 2>&1
  echo "  Demo apps deleted (in the background)."
}

victims_running() {
  [ "$(kubectl -n $NS get pods -l spark-role=executor --field-selector=status.phase=Running --no-headers 2>/dev/null | grep -c '^workload-')" -ge $((2 * EXECS)) ]
}
urgent_done_or_timeout() {
  [ "$(date +%s)" -ge "$DEADLINE" ] && return 0
  [ -n "${EVICTIONS:-}" ] && [ "$(evictions)" -ge "$EVICTIONS" ] && return 0
  local st; st=$(kubectl -n $NS get $SA urgent -o jsonpath='{.status.applicationState.state}' 2>/dev/null)
  [ "$st" = COMPLETED ] || [ "$st" = FAILED ]
}

case "${1:-}" in -h|--help) sed -n 2,17p "$0" | sed 's/^# \{0,1\}//'; exit 0 ;; "") ;; *) echo "unknown argument '$1' (try --help)" >&2; exit 2 ;; esac

say "Preflight"
echo "  Kueue: $(kubectl -n kueue-system get deploy kueue-controller-manager -o jsonpath='{.spec.template.spec.containers[0].image}')"
kubectl get clusterqueue $CQ >/dev/null 2>&1 || kubectl apply -f "$(setup_file queues-setup.yaml)" >/dev/null || exit 1
mkdir -p "$HERE/runs"
LOCK="$HERE/runs/.whole-workload-preemption.lock"
mkdir "$LOCK" 2>/dev/null || { echo "  refusing to start: another run-whole-workload-preemption.sh is in progress (remove $LOCK if not)" >&2; exit 3; }
trap 'set_quota 36; rmdir "$LOCK" 2>/dev/null; rm -rf "$SHUFFLE_STATE"' EXIT
others=$(kubectl -n $NS get $SA -o name 2>/dev/null | sed 's#.*/##' | grep -vxE 'workload-a|workload-b|urgent' || true)
if [ -n "$others" ] && [ "${FORCE:-0}" != 1 ]; then
  echo "  refusing to start: $NS has other SparkApplications ($(echo $others | tr '\n' ' ')). Rerun with FORCE=1 to delete them." >&2
  exit 3
fi

say "Cleaning $NS"
kubectl -n $NS delete $SA --all --wait=true >/dev/null 2>&1
for _ in $(seq 1 60); do [ "$(kubectl -n $NS get workloads,pods --no-headers 2>/dev/null | wc -l | tr -d ' ')" = 0 ] && break; sleep 3; done
set_quota $QUOTA

START=$(date -u +%Y-%m-%dT%H:%M:%SZ)
say "Submitting two low-priority SparkPageRank apps (driver + $EXECS executors each), NOT opted in to partial preemption"
for v in $VICTIMS; do victim_yaml $v | kubectl apply -f - >/dev/null; done
live victims_running

# Keep refreshing (shuffle data growing) until Enter; the apps finish after ~2.5 minutes on their own.
VICTIMS_UP=$(date +%s)
building() {
  local up=$(( $(date +%s) - VICTIMS_UP ))
  echo "  apps running for ${up}s (they finish on their own after ~150s: submit before then)"
  [ "${AUTO:-0}" = 1 ] && [ $up -ge 20 ]
}
[ -t 0 ] || AUTO=1   # no terminal to press Enter in: submit after 20s, as with AUTO=1
if [ "${AUTO:-0}" = 1 ]; then
  live building
else
  live building stoppable "⏸  Both apps are building shuffle data. Press Enter to submit the high-priority app (Dynamic Allocation)"
fi
python3 -I "$HERE/gen-apps.py" urgent:high --queue $LQ --partial --cap 2 --idle 5s --als-args "$URGENT_ARGS" | kubectl apply -f - >/dev/null
DEADLINE=$(( $(date +%s) + ${DURATION:-240} ))
# From here on, Ctrl-C ends the demo with the summary rather than leaving the apps running.
trap 'trap - INT; echo; summary; exit 0' INT
live urgent_done_or_timeout stoppable
trap - INT
summary
