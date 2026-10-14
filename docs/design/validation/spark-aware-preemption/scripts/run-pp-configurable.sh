#!/usr/bin/env bash
# P12: partial preemption when classic preemption is off (withinClusterQueue and
# reclaimWithinCohort Never) and a PreemptionConfig's candidateSelectors pick the victims.
# Needs the ConfigurablePreemptions gate, the PreemptionConfig CRD, and labelKeysToCopy
# [preemption-pool, preemption-role] in Kueue's configuration.
set -uo pipefail
HERE="$(cd "$(dirname "$0")" && pwd)"
cd "$HERE/runs"
NS=batch-ns-uno; SA=sparkapplications.sparkoperator.k8s.io; CQ=cluster-queue-tres
STAMP=$(date +%m%d-%H%M); name="PPC-$STAMP-P12-configurable-pool"
clean() {
  kubectl -n $NS delete $SA --all --wait=true >/dev/null 2>&1
  for _ in $(seq 1 60); do
    [ "$(kubectl -n $NS get workloads,pods --no-headers 2>/dev/null | wc -l | tr -d ' ')" = 0 ] && return 0
    sleep 3
  done
  echo "WARNING: namespace not clean after 180s"
}
echo "Kueue under test: $(kubectl -n kueue-system get deploy kueue-controller-manager -o jsonpath='{.spec.template.spec.containers[0].image}')"
kubectl apply -f "$HERE/queues-tres.yaml" >/dev/null
kubectl apply -f "$HERE/preemption-config-p12.yaml" >/dev/null
kubectl patch clusterqueue $CQ --type=merge -p '{"metadata":{"annotations":{"kueue.x-k8s.io/preemption-config-name":"batch-pool"}},"spec":{"preemption":{"withinClusterQueue":"Never","reclaimWithinCohort":"Never"}}}' >/dev/null
echo "   $CQ: preemption $(kubectl get clusterqueue $CQ -o jsonpath='{.spec.preemption}'), preemption-config-name=batch-pool"

echo; echo "=================== P12-configurable-pool  ($(date +%H:%M:%S))"
echo "   als-lo1..6: default priority, preemption-pool=batch, driver + 3 executors, floor 1 (24 slots)"
echo "   als-lo7..10: low priority, unlabeled, driver + 2 executors, floor 1 (12 slots)"
echo "   als-hi: high priority, preemption-role=urgent, static driver + 11 executors (12 slots)"
clean; rm -rf "$name"
OUT_DIR=$PWD/$name ../capture.sh -n $NS -q $CQ -d 540 > "$name.capture.log" 2>&1 &
cap=$!
sleep 12
ALS="1500 1500 12 40 200"
python3 -I ../gen-apps-label.py als-lo1:default,als-lo2:default,als-lo3:default,als-lo4:default,als-lo5:default,als-lo6:default \
  --queue batch-queue-tres --static-elastic 3 --floor 1 --als-args "$ALS" --label preemption-pool=batch | kubectl apply -f - >/dev/null
python3 -I ../gen-apps.py als-lo7:low,als-lo8:low,als-lo9:low,als-lo10:low \
  --queue batch-queue-tres --static-elastic 2 --floor 1 --als-args "$ALS" | kubectl apply -f - >/dev/null
for _ in $(seq 1 60); do
  n=$(kubectl -n $NS get pods -l spark-role=executor --field-selector=status.phase=Running --no-headers 2>/dev/null | wc -l | tr -d ' ')
  [ "$n" -ge 26 ] && break; sleep 5
done
echo "   $n executors running at $(date +%H:%M:%S)"
sleep 10
python3 -I ../gen-apps-label.py als-hi:high --queue batch-queue-tres --static 11 --label preemption-role=urgent | kubectl apply -f - >/dev/null
echo "   preemptor applied at $(date +%H:%M:%S)"
wait $cap
../analyze.sh "$PWD/$name" >/dev/null 2>&1
python3 -I ../req-check.py "$PWD/$name" --slots 36 --preemption | grep -E 'SC-1|SC-2|capture' | cut -c1-190
python3 -I ../pp-analyze.py --floor 1 "$PWD/$name"
clean
kubectl patch clusterqueue $CQ --type=json -p '[{"op":"remove","path":"/metadata/annotations/kueue.x-k8s.io~1preemption-config-name"}]' >/dev/null
kubectl patch clusterqueue $CQ --type=merge -p '{"spec":{"preemption":{"withinClusterQueue":"LowerPriority","reclaimWithinCohort":"Never"}}}' >/dev/null
echo "P12 done $(date +%H:%M:%S); $CQ restored to classic preemption"
