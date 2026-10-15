#!/usr/bin/env bash
# S6 — R-9 (non-elastic behaviour unchanged) and R-10 (existing elastic integrations unaffected).
#   a plain batch/v1 Job       : admitted whole; its status.admission stays immutable
#   an elastic batch/v1 Job    : scale up -> replacement slice admitted; scale down -> grant lowered in place
#   a static SparkApplication  : admitted at spark.executor.instances, not elastic
# Prints PASS/FAIL lines; exit status is the number of failures.
set -uo pipefail
NS=batch-ns-uno; LQ=batch-queue-uno; FAILS=0
pass() { printf 'PASS  %s\n' "$*"; }
fail() { printf 'FAIL  %s\n' "$*"; FAILS=$((FAILS+1)); }
wl_for() { kubectl -n $NS get workloads -o json | jq -r --arg o "$1" '[.items[] | select(.metadata.ownerReferences[0].name==$o)] | sort_by(.metadata.creationTimestamp) | .[] | "\(.metadata.name) \(.spec.podSets[0].count) \(.status.admission.podSetAssignments[0].count // "-") \([.status.conditions[]? | select(.type=="QuotaReserved" and .status=="True")] | length) \([.status.conditions[]? | select(.type=="Finished" and .status=="True")] | length) \(.metadata.annotations["kueue.x-k8s.io/workload-slice-replacement-for"] // "-")"'; }
job() { # job <name> <parallelism> <elastic:true|false>
  cat <<EOF
apiVersion: batch/v1
kind: Job
metadata:
  name: $1
  namespace: $NS
  labels: {kueue.x-k8s.io/queue-name: $LQ}
$( [ "$3" = true ] && printf '  annotations: {kueue.x-k8s.io/elastic-job: "true"}' )
spec:
  parallelism: $2
  $( [ "$3" = true ] || printf 'completions: %s' "$2" )
  template:
    spec:
      restartPolicy: Never
      containers:
      - name: c
        image: busybox:1.36
        command: ["sh", "-c", "sleep 150"]
        resources: {requests: {cpu: "1", memory: 256Mi}}
EOF
}

kubectl -n $NS delete job s6-plain s6-elastic --ignore-not-found --wait=true >/dev/null 2>&1
kubectl -n $NS delete sparkapplications.sparkoperator.k8s.io s6-static --ignore-not-found --wait=true >/dev/null 2>&1
job s6-plain 2 false | kubectl apply -f - >/dev/null
job s6-elastic 2 true | kubectl apply -f - >/dev/null
python3 -I "$(dirname "$0")/gen-apps.py" s6-static:- --static 2 | kubectl apply -f - >/dev/null
sleep 25

# --- R-9: plain Job admitted whole, admission immutable
read -r name spec granted qr _ _ <<<"$(wl_for s6-plain | head -1)"
[ "${qr:-0}" = 1 ] && [ "$granted" = 2 ] && pass "R-9 plain Job admitted whole ($name: granted $granted of $spec)" || fail "R-9 plain Job not admitted as expected: $(wl_for s6-plain)"
if out=$(kubectl -n $NS patch workload "$name" --subresource=status --type=json --dry-run=server \
    -p '[{"op":"replace","path":"/status/admission/podSetAssignments/0/count","value":1}]' 2>&1); then
  fail "R-9 plain Job's admission accepted a change (should be immutable): $out"
else
  pass "R-9 plain Job's admission is immutable (webhook: ${out:0:110})"
fi

# --- R-9: static SparkApplication admitted at its declared count, not elastic
sa=$(kubectl -n $NS get workloads -o json | jq -r '[.items[] | select(.metadata.ownerReferences[0].name=="s6-static")][0] | "\(.spec.podSets[1].count) \(.status.admission.podSetAssignments[1].count // "-") \(.metadata.annotations["kueue.x-k8s.io/elastic-job"] // "none")"')
[ "$sa" = "2 2 none" ] && pass "R-9 static SparkApplication admitted at spark.executor.instances, not elastic ($sa)" || fail "R-9 static SparkApplication: spec/granted/elastic = $sa"

# --- R-10: elastic Job scale up
kubectl -n $NS patch job s6-elastic --type=merge -p '{"spec":{"parallelism":4}}' >/dev/null
sleep 20
up=$(wl_for s6-elastic)
if echo "$up" | awk '$6!="-" && $2==4 && $4==1 {f=1} END{exit !f}'; then pass "R-10 elastic Job scale-up admitted as a replacement slice (4)"; else fail "R-10 elastic Job scale-up: $up"; fi
ungated=$(kubectl -n $NS get pods -l job-name=s6-elastic -o json | jq '[.items[] | select((.spec.schedulingGates // []) | length == 0) | select(.status.phase | IN("Succeeded","Failed") | not)] | length')
[ "$ungated" = 4 ] && pass "R-10 elastic Job runs 4 ungated Pods after the grant" || fail "R-10 elastic Job ungated Pods = $ungated, want 4"

# --- R-10 + R-4: elastic Job scale down lowers the grant in place
kubectl -n $NS patch job s6-elastic --type=merge -p '{"spec":{"parallelism":1}}' >/dev/null
sleep 20
down=$(wl_for s6-elastic | awk '$5==0')
if echo "$down" | awk '$2==1 && $3==1 {f=1} END{exit !f}'; then pass "R-10/R-4 elastic Job scale-down lowered the grant to 1"; else fail "R-10/R-4 elastic Job scale-down: active slice spec/granted = $down"; fi

kubectl -n $NS delete job s6-plain s6-elastic --ignore-not-found >/dev/null 2>&1
kubectl -n $NS delete sparkapplications.sparkoperator.k8s.io s6-static --ignore-not-found >/dev/null 2>&1
exit $FAILS
