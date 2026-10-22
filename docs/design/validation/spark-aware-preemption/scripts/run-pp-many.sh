#!/usr/bin/env bash
# Many small low-priority victims on a standalone 36-slot ClusterQueue (queues-tres.yaml): a
# high-priority static app is admitted by reclaiming a fraction of each victim's executors.
#   run-pp-many.sh [P10 P11]   (default: both)
set -uo pipefail
HERE="$(cd "$(dirname "$0")" && pwd)"
cd "$HERE/runs"
NS=batch-ns-uno; SA=sparkapplications.sparkoperator.k8s.io; CQ=cluster-queue-tres
STAMP=$(date +%m%d-%H%M)
SEL="${*:-P10 P11}"
want() { [[ " $SEL " == *" $1 "* ]]; }
clean() {
  kubectl -n $NS delete $SA --all --wait=true >/dev/null 2>&1
  for _ in $(seq 1 60); do
    [ "$(kubectl -n $NS get workloads,pods --no-headers 2>/dev/null | wc -l | tr -d ' ')" = 0 ] && return 0
    sleep 3
  done
  echo "WARNING: namespace not clean after 180s"
}
names() { local out=""; for i in $(seq 1 "$1"); do out="$out${out:+,}als-lo$i:low"; done; echo "$out"; }
# admitted_all N: wait until N low-priority apps have a running driver and all their executors.
wait_running() {
  local want=$1 execs=$2
  for _ in $(seq 1 60); do
    local n
    n=$(kubectl -n $NS get pods -l spark-role=executor --field-selector=status.phase=Running --no-headers 2>/dev/null | wc -l | tr -d ' ')
    [ "$n" -ge $((want * execs)) ] && { echo "   $n executors running at $(date +%H:%M:%S)"; return 0; }
    sleep 5
  done
  echo "   WARNING: only $n of $((want * execs)) executors running"
}
# scenario NAME DUR VICTIMS EXECS FLOOR HI_EXECS
scenario() {
  local label=$1 name="PPM-$STAMP-$1" dur=$2 victims=$3 execs=$4 floor=$5 hi=$6
  echo; echo "=================== $label  ($(date +%H:%M:%S))"
  echo "   $victims victims x (driver + $execs executors, floor $floor) = $((victims * (execs + 1))) slots; preemptor: driver + $hi executors"
  clean; rm -rf "$name"
  OUT_DIR=$PWD/$name ../capture.sh -n $NS -q $CQ -d "$dur" > "$name.capture.log" 2>&1 &
  local cap=$!
  sleep 12
  python3 -I ../gen-apps.py "$(names "$victims")" --queue batch-queue-tres --static-elastic "$execs" --floor "$floor" \
    --als-args "1500 1500 12 40 200" | kubectl apply -f - >/dev/null
  wait_running "$victims" "$execs"
  sleep 10
  python3 -I ../gen-apps.py als-hi:high --queue batch-queue-tres --static "$hi" | kubectl apply -f - >/dev/null
  echo "   preemptor applied at $(date +%H:%M:%S)"
  wait $cap
  ../analyze.sh "$PWD/$name" >/dev/null 2>&1
  python3 -I ../req-check.py "$PWD/$name" --slots 36 --preemption | grep -E 'SC-1|SC-2|capture' | cut -c1-190
  python3 -I ../pp-analyze.py --floor "$floor" --expect-whole 0 "$PWD/$name"
}

echo "Kueue under test: $(kubectl -n kueue-system get deploy kueue-controller-manager -o jsonpath='{.spec.template.spec.containers[0].image}')"
kubectl apply -f "$HERE/queues-tres.yaml" >/dev/null && echo "   $CQ: 36 cpu / 32256Mi (36 slots), standalone"
# P10: 12 victims x 3 slots fill the queue; the preemptor needs 12, so each victim gives exactly 1 of its 2 executors.
want P10 && scenario P10-12-victims-1-each 540 12 2 1 11
# P11: 9 victims x 4 slots; the preemptor needs 15: every victim gives 1, then 6 give a second (round-robin).
want P11 && scenario P11-9-victims-round-robin 540 9 3 1 14
clean
echo; echo "many-victims done $(date +%H:%M:%S)"
