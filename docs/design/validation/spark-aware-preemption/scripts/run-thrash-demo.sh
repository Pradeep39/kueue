#!/usr/bin/env bash
# Demo of today's behaviour, before partial preemption: two shuffle-heavy SparkPageRank apps that have
# NOT opted in to partial preemption share a 12-slot queue with a high-priority app using Dynamic
# Allocation. Each time the urgent app is admitted or scales up, Kueue evicts a whole low-priority app:
# its driver dies, its shuffle data is lost, and when the urgent app scales down the victim is
# readmitted and starts over from scratch, only to be evicted again (thrashing).
#
#   [AUTO=1] [FORCE=1] [DURATION=240] run-thrash-demo.sh
#     AUTO=1       submit the urgent app as soon as both victims run (no Enter prompt)
#     FORCE=1      delete other SparkApplications in batch-ns-uno before starting (otherwise refuse)
#     DURATION=N   stop watching N seconds after the urgent app arrives (default 240), or earlier
#                  when the urgent app finishes
#     ITERS=N / URGENT_ITERS=N   PageRank iterations per victim run (default 100) / urgent SparkALS
#                  iterations (default 10)
#   cluster-queue-tres is sized to 12 slots for the run and put back to 36 on exit.
#
# Run it before run-shuffle-migration.sh to show the same kind of work being thrown away, then kept.
set -uo pipefail
HERE="$(cd "$(dirname "$0")" && pwd)"
NS=batch-ns-uno; SA=sparkapplications.sparkoperator.k8s.io; CQ=cluster-queue-tres; LQ=batch-queue-tres
VICTIMS="thrash-a thrash-b"; EXECS=4; QUOTA=12
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
since = sys.argv[1]
adm, ev, lines = collections.Counter(), collections.Counter(), []
for e in sorted(json.load(sys.stdin)["items"], key=lambda e: e.get("lastTimestamp") or e.get("eventTime") or ""):
    t = e.get("lastTimestamp") or e.get("eventTime") or ""
    if t < since or e["involvedObject"].get("kind") != "Workload":
        continue
    app = re.sub(r"^sparkapplication-|-[0-9a-f]{5}$", "", e["involvedObject"]["name"])
    if e["reason"] == "Admitted":
        adm[app] += 1
    elif e["reason"] == "Preempted":
        ev[app] += 1
        lines.append(f"  {t[11:19]}  {app:9} EVICTED WHOLE: driver killed, shuffle data lost")
print("  app        admitted  evicted whole")
for a in sorted(set(adm) | set(ev)):
    if a.startswith("thrash-"):
        print(f"  {a:10} {adm[a]:>8}  {ev[a]:>13}")
print()
print("\n".join(lines[-8:]) or "  (no evictions yet)")
' "$START"
}

table() {
  local tmp; tmp=$(mktemp -d)
  kubectl -n $NS get pods -l spark-role -o json > "$tmp/pods.json" 2>/dev/null
  kubectl -n $NS get workloads -o json > "$tmp/wls.json" 2>/dev/null
  python3 -I "$HERE/p11-table.py" "$tmp/pods.json" "$tmp/wls.json" $QUOTA | grep -v "^  als-"
  rm -rf "$tmp"
}

live() { # live UNTIL_CMD
  while true; do
    clear; date +%H:%M:%S; table; echo; history
    eval "$1" && return 0
    sleep 3
  done
}

victims_running() {
  [ "$(kubectl -n $NS get pods -l spark-role=executor --field-selector=status.phase=Running --no-headers 2>/dev/null | grep -c '^thrash-')" -ge $((2 * EXECS)) ]
}
urgent_done_or_timeout() {
  [ "$(date +%s)" -ge "$DEADLINE" ] && return 0
  local st; st=$(kubectl -n $NS get $SA urgent -o jsonpath='{.status.applicationState.state}' 2>/dev/null)
  [ "$st" = COMPLETED ] || [ "$st" = FAILED ]
}

case "${1:-}" in -h|--help) sed -n 2,17p "$0" | sed 's/^# \{0,1\}//'; exit 0 ;; "") ;; *) echo "unknown argument '$1' (try --help)" >&2; exit 2 ;; esac

say "Preflight"
echo "  Kueue: $(kubectl -n kueue-system get deploy kueue-controller-manager -o jsonpath='{.spec.template.spec.containers[0].image}')"
kubectl get clusterqueue $CQ >/dev/null 2>&1 || kubectl apply -f "$HERE/queues-setup.yaml" >/dev/null || exit 1
mkdir -p "$HERE/runs"
LOCK="$HERE/runs/.thrash-demo.lock"
mkdir "$LOCK" 2>/dev/null || { echo "  refusing to start: another run-thrash-demo.sh is in progress (remove $LOCK if not)" >&2; exit 3; }
trap 'set_quota 36; rmdir "$LOCK" 2>/dev/null' EXIT
others=$(kubectl -n $NS get $SA -o name 2>/dev/null | sed 's#.*/##' | grep -vxE 'thrash-a|thrash-b|urgent' || true)
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

if [ "${AUTO:-0}" = 1 ]; then sleep 20; else pause "Both apps are building shuffle data. Submit the high-priority app (Dynamic Allocation)"; fi
python3 -I "$HERE/gen-apps.py" urgent:high --queue $LQ --partial --cap 2 --idle 5s --als-args "$URGENT_ARGS" | kubectl apply -f - >/dev/null
DEADLINE=$(( $(date +%s) + ${DURATION:-240} ))
live urgent_done_or_timeout

say "Summary"
history | head -4
echo
echo "  Every eviction killed a driver and threw away that app's shuffle data; each readmission"
echo "  started the job over from scratch."
kubectl -n $NS delete $SA --all --wait=false >/dev/null 2>&1
echo "  Demo apps deleted (in the background)."
