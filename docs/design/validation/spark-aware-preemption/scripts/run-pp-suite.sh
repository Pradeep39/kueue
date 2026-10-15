#!/usr/bin/env bash
# Partial-preemption scenarios against whatever Kueue is deployed (spark-aware-preemption branch).
# Mirrors run-req-suite.sh's S4/S5 preemption scenarios so results compare with the baseline,
# plus floor, decommission-off, and static-elastic variants. Prints req-check + pp-analyze per run.
#   run-pp-suite.sh [SCENARIO...]     (default: all)
set -uo pipefail
HERE="$(cd "$(dirname "$0")" && pwd)"
cd "$HERE/runs"
NS=batch-ns-uno
SA=sparkapplications.sparkoperator.k8s.io
STAMP=$(date +%m%d-%H%M)

clean() {
  kubectl -n $NS delete $SA --all --wait=true >/dev/null 2>&1
  for _ in $(seq 1 60); do
    [ "$(kubectl -n $NS get workloads,pods --no-headers 2>/dev/null | wc -l | tr -d ' ')" = 0 ] && return 0
    sleep 3
  done
  echo "WARNING: namespace not clean after 180s"
}

# quota <cpu-per-cq> : both ClusterQueues get <cpu> CPU and cpu*896Mi memory, borrowing limit = nominal
quota() {
  local cpu=$1 mem=$(( $1 * 896 ))Mi
  for cq in cluster-queue-uno cluster-queue-dos; do
    kubectl patch clusterqueue $cq --type=json -p "[
      {\"op\":\"replace\",\"path\":\"/spec/resourceGroups/0/flavors/0/resources/0/nominalQuota\",\"value\":\"$cpu\"},
      {\"op\":\"replace\",\"path\":\"/spec/resourceGroups/0/flavors/0/resources/0/borrowingLimit\",\"value\":\"$cpu\"},
      {\"op\":\"replace\",\"path\":\"/spec/resourceGroups/0/flavors/0/resources/1/nominalQuota\",\"value\":\"$mem\"},
      {\"op\":\"replace\",\"path\":\"/spec/resourceGroups/0/flavors/0/resources/1/borrowingLimit\",\"value\":\"$mem\"}]" >/dev/null
  done
  echo "   quota: $cpu cpu / $mem per ClusterQueue ($(( 2 * cpu )) slots in the cohort)"
}

# scenario <name> <duration> <slots> <wave1> <wave1 flags> <wave2> <wave2 flags> <delay> <req-check flags>
scenario() {
  local label=$1 name="PP-$STAMP-$1" dur=$2 slots=$3 w1=$4 f1=$5 w2=$6 f2=$7 delay=$8 checks=$9
  echo; echo "=================== $label  ($(date +%H:%M:%S))"
  echo "   wave1 $w1 [$f1]   wave2 +${delay}s $w2 [$f2]"
  clean; rm -rf "$name"
  OUT_DIR=$PWD/$name ../capture.sh -n $NS -q cluster-queue-uno -d "$dur" > "$name.capture.log" 2>&1 &
  local cap=$!
  sleep 12
  python3 -I ../gen-apps.py "$w1" $f1 | kubectl apply -f - >/dev/null
  sleep "$delay"; python3 -I ../gen-apps.py "$w2" $f2 | kubectl apply -f - >/dev/null
  wait $cap
  ../analyze.sh "$PWD/$name" >/dev/null 2>&1
  python3 -I ../req-check.py "$PWD/$name" --slots "$slots" $checks | cut -c1-190
  python3 -I ../pp-analyze.py "$PWD/$name"
}

want() { [ $# -eq 0 ] || [[ " ${SELECTED} " == *" $1 "* ]]; }
SELECTED="${*:-}"
KUEUE_IMAGE=$(kubectl -n kueue-system get deploy kueue-controller-manager -o jsonpath='{.spec.template.spec.containers[0].image}')
echo "Kueue under test: $KUEUE_IMAGE"

DA="--idle 5s"
# P1/P2/P3: the baseline's 12-slot cohort, so results compare with REQ-1011-1054-S4/S5.
if [ -z "$SELECTED" ] || want P1; then quota 6
  scenario P1-S4-floor-min 480 12 als-lo1:low,als-lo2:low "--partial --cap 2 $DA" als-hi:high "--partial --cap 2 $DA" 40 "--cap 2 --partial --preemption"; fi
if [ -z "$SELECTED" ] || want P2; then quota 6
  scenario P2-S4-floor1 480 12 als-lo1:low,als-lo2:low "--partial --cap 2 --floor 1 $DA" als-hi:high "--partial --cap 2 $DA" 40 "--cap 2 --partial --preemption"; fi
if [ -z "$SELECTED" ] || want P3; then quota 6
  scenario P3-S5-floor1 480 12 als-lo1:low,als-lo2:low "--floor 1 $DA" als-hi:high "$DA" 40 "--preemption"; fi
if [ -z "$SELECTED" ] || want P4; then quota 6
  scenario P4-S4-floor1-nodecom 480 12 als-lo1:low,als-lo2:low "--partial --cap 2 --floor 1 --no-decom $DA" als-hi:high "--partial --cap 2 $DA" 40 "--cap 2 --partial --preemption"; fi
# P5: static victims need room: two static apps of 8 executors are 18 slots, so a 24-slot cohort.
if [ -z "$SELECTED" ] || want P5; then quota 12
  scenario P5-static-victims 480 24 als-lo1:low,als-lo2:low "--static-elastic 8 --floor 2" als-hi:high "--partial --cap 2 $DA" 40 "--preemption"; fi
quota 6
clean
echo; echo "suite done $(date +%H:%M:%S)"
