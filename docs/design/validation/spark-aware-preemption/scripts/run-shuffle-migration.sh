#!/usr/bin/env bash
# Shuffle-migration experiment: is shuffle data moved off an executor that partial preemption reclaims?
#
# A low-priority SparkPageRank (official Spark example, iterative join + reduceByKey, so shuffle data
# accumulates on every executor) runs on a generated 2M-edge graph. When EVERY victim executor holds
# at least THRESH_MB of shuffle files (measured with du in the executor), a high-priority app is
# submitted, Kueue reclaims executors, and their own logs (streamed from start to exit) show whether
# Spark's decommissioner migrated the blocks.
#   [EXTRA_CONF=key=value,key=value] run-shuffle-migration.sh [THRESH_MB]
set -uo pipefail
HERE="$(cd "$(dirname "$0")" && pwd)"
NS=batch-ns-uno; SA=sparkapplications.sparkoperator.k8s.io
usage() {
  cat <<USAGE
usage: [EXTRA_CONF=key=value,...] [FORCE=1] run-shuffle-migration.sh [THRESH_MB]
  THRESH_MB  MiB of shuffle files every victim executor must hold before the preemptor is
             submitted (default 15). This runs the experiment; it takes no application name.
  FORCE=1    delete other SparkApplications in $NS before starting (otherwise refuse).
To analyse a finished run:   shuffle-migration-report.py runs/SHUF-<stamp>
To see shuffle data now:     shuffle-per-executor.sh <app> [namespace] [--watch]
USAGE
}
case "${1:-15}" in
  --yaml) ;;
  -h|--help) usage; exit 0 ;;
  *[!0-9]*|"") echo "run-shuffle-migration.sh: THRESH_MB must be a whole number of MiB, got '${1}'" >&2; usage >&2; exit 2 ;;
esac
THRESH_MB=${1:-15}
STAMP=$(date +%m%d-%H%M); OUT="$HERE/runs/SHUF-$STAMP"
VICTIM=pr-victim; EXECS=10; FLOOR=2; ITERS=50
GEN='awk "BEGIN{srand(42); for(i=0;i<2000000;i++) print int(rand()*200000), int(rand()*200000)}" > /data/edges.txt'

log() { printf '%s %s\n' "$(date +%H:%M:%S)" "$*" | tee -a "$OUT/runner.log"; }

victim_yaml() {
  python3 -I "$HERE/gen-apps.py" "$VICTIM:low" --static-elastic $EXECS --floor $FLOOR \
    --main org.apache.spark.examples.SparkPageRank --als-args "file:///data/edges.txt $ITERS" | python3 -I -c '
import sys, re
y = sys.stdin.read()
gen = sys.argv[1]
y = y.replace("    \"spark.kubernetes.executor.podNamePrefix\"",
  "    \"spark.kubernetes.executor.terminationGracePeriodSeconds\": \"30s\"\n"
  "    \"spark.sql.files.maxPartitionBytes\": \"2m\"\n"
  "    \"spark.default.parallelism\": \"40\"\n"
  + "".join("    \"%s\": \"%s\"\n" % tuple(kv.split("=", 1)) for kv in sys.argv[2].split(",") if kv)
  + "    \"spark.kubernetes.executor.podNamePrefix\"", 1)
init = ("    initContainers:\n    - name: gen-edges\n      image: docker.io/library/spark:4.0.1\n"
        "      command: [\"sh\", \"-c\", %r]\n      volumeMounts: [{name: data, mountPath: /data}]\n"
        "      securityContext: {runAsUser: 185, runAsNonRoot: true, allowPrivilegeEscalation: false, "
        "capabilities: {drop: [ALL]}, seccompProfile: {type: RuntimeDefault}}\n"
        "    volumeMounts: [{name: data, mountPath: /data}]\n") % gen
y = y.replace("  driver:\n", "  volumes: [{name: data, emptyDir: {}}]\n  driver:\n" + init, 1)
y = y.replace("  executor:\n", "  executor:\n" + init, 1)
print(y)' "$GEN" "${EXTRA_CONF:-}"
}

[ "${1:-}" = "--yaml" ] && { victim_yaml; exit 0; }
mkdir -p "$OUT/exec-logs"
echo "$(victim_yaml)" > "$OUT/victim.yaml"
python3 -I "$HERE/gen-apps.py" urgent:high --partial --cap 2 --idle 5s > "$OUT/urgent.yaml"
kubectl apply --dry-run=client -f "$OUT/victim.yaml" >/dev/null || { echo "victim.yaml invalid"; exit 1; }

others=$(kubectl -n $NS get $SA -o name 2>/dev/null | sed 's#.*/##' | grep -vxE "$VICTIM|urgent" || true)
if [ -n "$others" ] && [ "${FORCE:-0}" != 1 ]; then
  echo "refusing to start: $NS has other SparkApplications ($(echo $others | tr '\n' ' ')); this experiment deletes everything in $NS. Rerun with FORCE=1 to allow it." >&2
  exit 3
fi
# One run at a time: a second run's cleanup would delete the first run's apps mid-experiment.
LOCK="$HERE/runs/.shuffle-migration.lock"
if ! mkdir "$LOCK" 2>/dev/null; then
  echo "refusing to start: another run-shuffle-migration.sh is in progress (remove $LOCK if it is not)" >&2
  exit 3
fi
trap 'rmdir "$LOCK" 2>/dev/null' EXIT
log "cleaning namespace"
kubectl -n $NS delete $SA --all --wait=true >/dev/null 2>&1
for _ in $(seq 1 60); do [ "$(kubectl -n $NS get workloads,pods --no-headers 2>/dev/null | wc -l | tr -d ' ')" = 0 ] && break; sleep 3; done

OUT_DIR="$OUT/capture" "$HERE/capture.sh" -n $NS -q cluster-queue-uno -d 600 > "$OUT/capture.log" 2>&1 &
CAP=$!
sleep 10
kubectl apply -f "$OUT/victim.yaml" >/dev/null && log "victim $VICTIM applied ($EXECS executors, floor $FLOOR, PageRank $ITERS iterations, grace 30s)"

# Stream every victim executor's log from the moment it runs, so its decommission is recorded.
# (No associative arrays: macOS ships bash 3.2. A log file's existence marks a Pod as followed.)
(
  while kill -0 $CAP 2>/dev/null; do
    for p in $(kubectl -n $NS get pods -l "sparkoperator.k8s.io/app-name=$VICTIM,spark-role=executor" --field-selector=status.phase=Running -o name 2>/dev/null); do
      p=${p#pod/}; [ -e "$OUT/exec-logs/$p.log" ] && continue
      kubectl -n $NS logs -f "$p" -c spark-kubernetes-executor > "$OUT/exec-logs/$p.log" 2>&1 &
    done
    sleep 2
  done
) &
STREAMER=$!

# shuffle_mb POD: MiB of shuffle files under the executor's block-manager dirs.
shuffle_mb() {
  kubectl -n $NS exec "$1" -c spark-kubernetes-executor -- sh -c \
    'find /var/data -name "shuffle_*" -type f -exec du -k {} + 2>/dev/null | awk "{s+=\$1} END{printf \"%d\", s/1024}"' 2>/dev/null || echo -1
}

log "waiting until every executor holds >= ${THRESH_MB} MiB of shuffle files"
TRIGGERED=0
for _ in $(seq 1 120); do
  pods=$(kubectl -n $NS get pods -l "sparkoperator.k8s.io/app-name=$VICTIM,spark-role=executor" --field-selector=status.phase=Running -o name 2>/dev/null | sed 's#pod/##')
  n=$(echo "$pods" | grep -c . || true)
  line=""; min=999999
  for p in $pods; do m=$(shuffle_mb "$p"); line="$line ${p#$VICTIM-exec-}:${m}"; [ "$m" -lt "$min" ] && min=$m; done
  log "executors=$n shuffle MiB per executor:${line:- none}"
  if [ "$n" -ge $((EXECS - 1)) ] && [ "$min" -ge "$THRESH_MB" ]; then
    echo "$line" > "$OUT/shuffle-at-trigger.txt"
    kubectl apply -f "$OUT/urgent.yaml" >/dev/null && log "TRIGGER: urgent applied; min per-executor shuffle ${min} MiB"
    TRIGGERED=1; break
  fi
  state=$(kubectl -n $NS get $SA $VICTIM -o jsonpath='{.status.applicationState.state}' 2>/dev/null)
  case "$state" in COMPLETED|FAILED) log "victim $state before the threshold was reached"; break;; esac
  sleep 5
done
[ $TRIGGERED = 1 ] || log "never triggered"

wait $CAP
kill $STREAMER 2>/dev/null; pkill -f "logs -f $VICTIM-exec" 2>/dev/null
cp "$OUT/capture/logs/driver-$VICTIM-driver.log" "$OUT/driver.log" 2>/dev/null || kubectl -n $NS logs "$VICTIM-driver" > "$OUT/driver.log" 2>/dev/null
log "capture done; analysing"
python3 -I "$HERE/shuffle-migration-report.py" "$OUT" | tee "$OUT/report.txt"
