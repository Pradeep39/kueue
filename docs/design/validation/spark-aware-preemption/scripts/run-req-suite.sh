#!/usr/bin/env bash
# Requirements suite (R-1..R-13, SC-1/SC-2) against whatever Kueue is deployed. Runs S1-S5 through
# capture.sh + req-check.py, then S6 (R-9/R-10), and prints one verdict table per scenario.
set -uo pipefail
HERE="$(cd "$(dirname "$0")" && pwd)"
cd "$HERE/runs"
NS=batch-ns-uno
SA=sparkapplications.sparkoperator.k8s.io
STAMP=$(date +%m%d-%H%M)

clean() {
  kubectl -n $NS delete $SA --all --wait=true >/dev/null 2>&1
  kubectl -n $NS delete jobs --all --wait=true >/dev/null 2>&1
  for _ in $(seq 1 60); do
    [ "$(kubectl -n $NS get workloads,pods --no-headers 2>/dev/null | wc -l | tr -d ' ')" = 0 ] && return 0
    sleep 3
  done
  echo "WARNING: namespace not clean after 180s"
}

# scenario <name> <duration> <wave1> <wave2|-> <delay> <req-check flags> -- <gen-apps flags>
scenario() {
  local label=$1 name="REQ-$STAMP-$1" dur=$2 w1=$3 w2=$4 delay=$5 checks=$6; shift 7
  echo; echo "=================== $label  ($(date +%H:%M:%S))  gen: $*  check: ${checks:-none}"
  clean; rm -rf "$name"
  OUT_DIR=$PWD/$name ../capture.sh -n $NS -q cluster-queue-uno -d "$dur" > "$name.capture.log" 2>&1 &
  local cap=$!
  sleep 12
  python3 -I ../gen-apps.py "$w1" "$@" | kubectl apply -f - >/dev/null
  if [ "$w2" != "-" ]; then sleep "$delay"; python3 -I ../gen-apps.py "$w2" "$@" | kubectl apply -f - >/dev/null; fi
  wait $cap
  kubectl -n $NS get $SA --no-headers 2>/dev/null | awk '{print "   app " $1 " " $3}'
  ../analyze.sh "$PWD/$name" >/dev/null 2>&1
  python3 -I ../req-check.py "$PWD/$name" $checks | cut -c1-190
}

KUEUE_IMAGE=$(kubectl -n kueue-system get deploy kueue-controller-manager -o jsonpath='{.spec.template.spec.containers[0].image}')
echo "Kueue under test: $KUEUE_IMAGE"
scenario S1-equal-partial-cap2   360 als-g1:default,als-g2:default,als-g3:default - 0  "--cap 2 --partial"             -- --partial --cap 2 --idle 10s
scenario S2-equal-control        360 als-g1:default,als-g2:default,als-g3:default - 0  ""                               -- --idle 10s
scenario S3-aggressive-partial   360 als-g1:default,als-g2:default,als-g3:default - 0  "--cap 2 --partial"             -- --partial --cap 2 --idle 5s
scenario S4-preempt-partial-cap2 480 als-lo1:low,als-lo2:low als-hi:high 40           "--cap 2 --partial --preemption" -- --partial --cap 2 --idle 5s
scenario S5-preempt-atomic       480 als-lo1:low,als-lo2:low als-hi:high 40           "--preemption"                   -- --idle 5s
echo; echo "=================== S6-non-elastic-and-batch-elastic  ($(date +%H:%M:%S))"
clean
bash ../run-s6.sh
clean
echo; echo "suite done $(date +%H:%M:%S)"
