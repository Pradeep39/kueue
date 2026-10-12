#!/usr/bin/env bash
# Tailored partial-preemption requirement scenarios (design doc §4 PR-1..PR-8), then the EQM
# requirements suite (R-1..R-13) on the same image.
#   run-pp-tailored.sh [P6 P7a P7b P8 P9 REQ]   (default: all)
set -uo pipefail
HERE="$(cd "$(dirname "$0")" && pwd)"
cd "$HERE/runs"
NS=batch-ns-uno
SA=sparkapplications.sparkoperator.k8s.io
STAMP=$(date +%m%d-%H%M)
SEL="${*:-P3R P5B P6 P7a P7b P8 P9 REQ}"
want() { [[ " $SEL " == *" $1 "* ]]; }

clean() {
  kubectl -n $NS delete $SA --all --wait=true >/dev/null 2>&1
  kubectl -n $NS delete jobs --all --wait=true >/dev/null 2>&1
  for _ in $(seq 1 60); do
    [ "$(kubectl -n $NS get workloads,pods --no-headers 2>/dev/null | wc -l | tr -d ' ')" = 0 ] && return 0
    sleep 3
  done
  echo "WARNING: namespace not clean after 180s"
}
quota() {
  local cpu=$1 mem=$(( $1 * 896 ))Mi
  for cq in cluster-queue-uno cluster-queue-dos; do
    kubectl patch clusterqueue $cq --type=json -p "[
      {\"op\":\"replace\",\"path\":\"/spec/resourceGroups/0/flavors/0/resources/0/nominalQuota\",\"value\":\"$cpu\"},
      {\"op\":\"replace\",\"path\":\"/spec/resourceGroups/0/flavors/0/resources/0/borrowingLimit\",\"value\":\"$cpu\"},
      {\"op\":\"replace\",\"path\":\"/spec/resourceGroups/0/flavors/0/resources/1/nominalQuota\",\"value\":\"$mem\"},
      {\"op\":\"replace\",\"path\":\"/spec/resourceGroups/0/flavors/0/resources/1/borrowingLimit\",\"value\":\"$mem\"}]" >/dev/null
  done
  echo "   quota: $cpu cpu / $mem per ClusterQueue ($(( 2 * cpu )) slots)"
}
# cqann KEY VALUE|-  : set or remove an annotation on both ClusterQueues
cqann() {
  for cq in cluster-queue-uno cluster-queue-dos; do
    if [ "$2" = "-" ]; then kubectl annotate clusterqueue $cq "$1-" >/dev/null 2>&1
    else kubectl annotate --overwrite clusterqueue $cq "$1=$2" >/dev/null; fi
  done
  echo "   ClusterQueue annotation $1=${2/-/(removed)}"
}
# scenario NAME DUR SLOTS W1 "F1" W2 "F2" DELAY "REQCHECK" "PPANALYZE"
scenario() {
  local label=$1 name="PPT-$STAMP-$1" dur=$2 slots=$3 w1=$4 f1=$5 w2=$6 f2=$7 delay=$8 checks=$9 ppa=${10}
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
  python3 -I ../req-check.py "$PWD/$name" --slots "$slots" $checks | grep -E 'SC-1|SC-2|capture' | cut -c1-190
  python3 -I ../pp-analyze.py $ppa "$PWD/$name"
}

echo "Kueue under test: $(kubectl -n kueue-system get deploy kueue-controller-manager -o jsonpath='{.spec.template.spec.containers[0].image}')"
DA="--idle 5s"
# P3R: clean rerun of P3 (atomic scale-up, floor 1); the 0731 capture's watch restarted.
if want P3R; then quota 6
  scenario P3R-S5-floor1 480 12 als-lo1:low,als-lo2:low "--floor 1 $DA" als-hi:high "$DA" 40 "--preemption" "--expect-whole 0 --floor 1"; fi
# P5B: static victims under contention: 18 slots exactly filled by the two static apps, so the
# high-priority application's first admission and growth must reclaim from them (floor 2).
if want P5B; then quota 9
  scenario P5B-static-contended 480 18 als-lo1:low,als-lo2:low "--static-elastic 8 --floor 2" als-hi:high "--partial --cap 2 $DA" 15 "--preemption" "--expect-whole 0 --floor 2"; fi
# P6 PR-7: victims that did not opt in (static, non-elastic) are preempted whole, as before.
if want P6; then quota 6
  scenario P6-not-opted-in 420 12 als-lo1:low,als-lo2:low "--static 4" als-hi:high "--partial --cap 2 $DA" 40 "--preemption" "--expect-unchanged"; fi
# P7a fallback Wait: victims at their floor (floor = count), a first admission that needs room waits.
if want P7a; then quota 6; cqann kueue.x-k8s.io/partial-preemption-fallback Wait
  scenario P7a-fallback-wait 420 12 als-lo1:low,als-lo2:low "--static-elastic 5 --floor 5" als-hi:high "--partial --cap 2 $DA" 40 "--preemption" "--expect-whole 0 --floor 5"
  cqann kueue.x-k8s.io/partial-preemption-fallback -; fi
# P7b fallback Evict (default): the same first admission evicts a whole application, by design.
if want P7b; then quota 6
  scenario P7b-fallback-evict 420 12 als-lo1:low,als-lo2:low "--static-elastic 5 --floor 5" als-hi:high "--partial --cap 2 $DA" 40 "--preemption" "--min-whole 1"; fi
# P8 spread: four victims; reclaims should be spread one executor per victim.
if want P8; then quota 12
  scenario P8-spread-4-victims 480 24 als-lo1:low,als-lo2:low,als-lo3:low,als-lo4:low "--partial --cap 2 --floor 1 $DA" als-hi:high "--partial --cap 2 $DA" 40 "--cap 2 --partial --preemption" "--expect-whole 0 --floor 1"; fi
# P9 timeout: a 1s reclaim timeout forces the deadline path; the grant is lowered anyway (bounded wait).
if want P9; then quota 6; cqann kueue.x-k8s.io/partial-preemption-timeout 1s
  scenario P9-timeout-1s 420 12 als-lo1:low,als-lo2:low "--partial --cap 2 --floor 1 $DA" als-hi:high "--partial --cap 2 $DA" 40 "--cap 2 --partial --preemption" "--expect-whole 0 --floor 1 --expect-timeout"
  cqann kueue.x-k8s.io/partial-preemption-timeout -; fi
quota 6; clean
# REQ: EQM requirements R-1..R-13 (S1-S6) with the partial-preemption gate on.
if want REQ; then echo; echo "=================== REQ suite ($(date +%H:%M:%S))"; bash ../run-req-suite.sh; fi
echo; echo "tailored done $(date +%H:%M:%S)"
