#!/usr/bin/env bash
# Capture the observability needed to tie gate-blocked executor Pods (design doc §14.1) to
# reconcile-conflict churn and admission wait.
#
# Start this BEFORE `kubectl apply -f apps.yaml`, then leave it running until the apps finish
# (or Ctrl-C — the after-state and Prometheus pull still run).
#
#   ./capture.sh -n batch-ns-uno -q cluster-queue-uno -d 900
#   ./analyze.sh gated-churn-YYYYmmdd-HHMMSS
#
# What it records, and why each is needed:
#   watch/pods.jsonl        every executor/driver Pod transition, incl. gate on/off — exact
#                           gated-count timeline, not a 2s sample
#   watch/workloads.jsonl   every Workload write: spec vs granted executor count, conditions,
#                           and the field manager of the latest write (who wrote it)
#   watch/sparkapps.jsonl   every SparkApplication write, same attribution
#   watch/events.jsonl      namespace events (Kueue Pending/QuotaReserved/Admitted, etc.)
#   logs/kueue.log          Kueue controller, --timestamps; conflicts are logged at error level,
#                           so they appear at any -v. Slice-accounting lines need -v=3.
#   logs/spark-operator.log the operator also writes SparkApplications and can lose conflicts
#   logs/driver-<pod>.log   Spark's own DA decisions ("Requesting N new executors", idle kills)
#   samples.jsonl           2s ClusterQueue usage/reservation/pending
#   prom/*.json             apiserver 409s by resource, controller-runtime reconcile results,
#                           workqueue depth/retries, Kueue admission wait — if Prometheus exists
#
# Requires kubectl and jq. Prometheus is optional (auto-detected via svc/prometheus-operated).
set -uo pipefail

NS="batch-ns-uno"
CQ="cluster-queue-uno"
KUEUE_NS="kueue-system"
DURATION=900
INTERVAL=2

while getopts "n:q:k:d:i:h" opt; do
  case $opt in
    n) NS="$OPTARG" ;;
    q) CQ="$OPTARG" ;;
    k) KUEUE_NS="$OPTARG" ;;
    d) DURATION="$OPTARG" ;;
    i) INTERVAL="$OPTARG" ;;
    h) sed -n '2,29p' "$0"; exit 0 ;;
    *) exit 1 ;;
  esac
done

# Both operators' CRDs can be installed, and the bare name "sparkapplications" resolves to
# spark.apache.org — so always use the fully-qualified Kubeflow resource.
SA="sparkapplications.sparkoperator.k8s.io"

for bin in kubectl jq; do
  command -v "$bin" >/dev/null || { echo "$bin required" >&2; exit 1; }
done

OUT="${OUT_DIR:-$(pwd)/gated-churn-$(date +%Y%m%d-%H%M%S)}"
mkdir -p "$OUT"/{watch,logs,raw,prom}
STOP="$OUT/.stop"
echo "namespace=$NS clusterqueue=$CQ kueue-ns=$KUEUE_NS duration=${DURATION}s interval=${INTERVAL}s"
echo "output -> $OUT"

START_EPOCH=$(date +%s)
echo "$START_EPOCH" > "$OUT/start.epoch"

# ---------------------------------------------------------------- context, once
{
  echo "### captured $(date -u +%Y-%m-%dT%H:%M:%SZ)"
  echo "### kueue deployment (check the log level: -v=3 / --zap-log-level=3 for slice lines)"
  kubectl -n "$KUEUE_NS" get deploy -o json 2>/dev/null \
    | jq -r '.items[] | "\(.metadata.name) replicas=\(.spec.replicas): \(.spec.template.spec.containers[].image)\n  args: \(.spec.template.spec.containers[].args // [] | join(" "))"'
  echo
  echo "### kueue config (feature gates, frameworks, metrics)"
  kubectl -n "$KUEUE_NS" get cm -o json 2>/dev/null \
    | jq -r '.items[] | select(.data | tostring | test("featureGates|integrations")) | "--- \(.metadata.name) ---\n\(.data | to_entries[] | .value)"' \
    | grep -iE "featureGates|ElasticJobs|SparkApplication|frameworks|sparkoperator|metrics|enableClusterQueueResources|^ *-" | head -40
  echo
  echo "### SparkApplication DA settings at start"
  kubectl -n "$NS" get "$SA" -o json 2>/dev/null \
    | jq -r '.items[] | "\(.metadata.name): \(.spec.sparkConf // {} | with_entries(select(.key|test("dynamicAllocation|executor.instances"))) | tojson)"'
} > "$OUT/context.txt" 2>&1
kubectl get clusterqueue "$CQ" -o yaml > "$OUT/raw/clusterqueue-initial.yaml" 2>&1

# ------------------------------------------------------- background streams
PIDS=()
cleanup() {
  touch "$STOP"
  # wait inside the redirect so bash's "Terminated" job notices are swallowed too
  for p in ${PIDS[@]+"${PIDS[@]}"}; do
    { pkill -TERM -P "$p"; kill "$p"; wait "$p"; } 2>/dev/null
  done
  PIDS=()
  for f in "$OUT"/logs/.driver-*.pid; do
    [ -f "$f" ] || continue
    p=$(cat "$f"); pkill -TERM -P "$p" 2>/dev/null; kill "$p" 2>/dev/null; rm -f "$f"
  done
}
trap cleanup EXIT

# watch_loop <outfile> <jq filter> <kubectl args...>
# Restarts the watch if the server closes it; replayed ADDED events are harmless because the
# analysis counts only MODIFIED/DELETED as writes.
watch_loop() {
  local out="$1" filter="$2"; shift 2
  while [ ! -f "$STOP" ]; do
    # Marks a (re)start: the watch replays current state as ADDED, but any deletion during the
    # gap is lost, so analyzers must drop this stream's state here and rebuild from the replay.
    printf '{"t":"%s","type":"RESYNC"}\n' "$(date -u +%Y-%m-%dT%H:%M:%SZ)" >> "$out"
    # ERROR watch events carry a metav1.Status, not the resource; skip them, and wrap the
    # filter in try so one odd object cannot kill the stream (it used to, silently, for
    # minutes until the loop restarted).
    kubectl "$@" -w --output-watch-events -o json 2>>"$OUT/raw/watch-errors.txt" \
      | jq -c --unbuffered "select(.type != \"ERROR\" and .object.kind != \"Status\") | try ($filter) catch (\"watch filter error: \" + tostring | stderr | empty)" \
        >> "$out" 2>>"$OUT/raw/watch-errors.txt"
    sleep 1
  done
}

LASTMGR='(.object.metadata.managedFields // [] | sort_by(.time // "") | last | "\(.manager // "-")/\(.subresource // "")")'

( watch_loop "$OUT/watch/pods.jsonl" "{
    t: (now | todate), type,
    name: .object.metadata.name,
    app: (.object.metadata.labels[\"sparkoperator.k8s.io/app-name\"] // \"-\"),
    role: (.object.metadata.labels[\"spark-role\"] // \"-\"),
    phase: (.object.status.phase // \"-\"),
    gated: ((.object.spec.schedulingGates // []) | map(.name) | any(startswith(\"kueue.x-k8s.io/\"))),
    deleting: (.object.metadata.deletionTimestamp != null),
    created: .object.metadata.creationTimestamp,
    mem: (.object.spec.containers[0].resources.requests.memory // null)
  }" -n "$NS" get pods -l spark-role ) &
PIDS+=($!)

( watch_loop "$OUT/watch/workloads.jsonl" "{
    t: (now | todate), type,
    name: .object.metadata.name,
    rv: .object.metadata.resourceVersion,
    owner: (.object.metadata.ownerReferences // [] | map(select(.controller==true)) | (.[0].name // \"-\")),
    replaces: (.object.metadata.annotations[\"kueue.x-k8s.io/workload-slice-replacement-for\"] // null),
    created: .object.metadata.creationTimestamp,
    spec: ([.object.spec.podSets[]? | {(.name): .count}] | add // {}),
    granted: ([.object.status.admission.podSetAssignments[]? | {(.name): .count}] | add // {}),
    cond: ([.object.status.conditions[]? | {(.type): {s: .status, r: .reason, lt: .lastTransitionTime}}] | add // {}),
    writer: $LASTMGR
  }" -n "$NS" get workloads ) &
PIDS+=($!)

( watch_loop "$OUT/watch/sparkapps.jsonl" "{
    t: (now | todate), type,
    name: .object.metadata.name,
    rv: .object.metadata.resourceVersion,
    gen: .object.metadata.generation,
    suspend: (.object.spec.suspend // false),
    state: (.object.status.applicationState.state // \"-\"),
    executors: (.object.status.executorState // {} | length),
    writer: $LASTMGR
  }" -n "$NS" get "$SA" ) &
PIDS+=($!)

( watch_loop "$OUT/watch/events.jsonl" "{
    t: (now | todate), type,
    kind: .object.involvedObject.kind, obj: .object.involvedObject.name,
    uid: (.object.involvedObject.uid // null),
    reason: .object.reason, etype: .object.type, count: (.object.count // 1),
    msg: .object.message
  }" -n "$NS" get events ) &
PIDS+=($!)

# follow_logs <outfile> <kubectl logs args...> — restarted if the stream drops (pod restart).
follow_logs() {
  local out="$1"; shift
  while [ ! -f "$STOP" ]; do
    kubectl logs "$@" --timestamps --since=10s -f >> "$out" 2>>"$OUT/raw/log-errors.txt"
    sleep 2
  done
}
( follow_logs "$OUT/logs/kueue.log" -n "$KUEUE_NS" deploy/kueue-controller-manager --all-containers ) &
PIDS+=($!)

# The Kubeflow chart labels its Deployments app.kubernetes.io/name=spark-operator; the Apache
# operator's chart uses spark-kubernetes-operator, so this does not pick it up. Override with
# SPARK_OP_NS if the chart was installed with a nameOverride.
SPARK_NS="${SPARK_OP_NS:-$(kubectl get deploy -A -l app.kubernetes.io/name=spark-operator \
  -o jsonpath='{.items[0].metadata.namespace}' 2>/dev/null)}"
kubectl get deploy -A -l app.kubernetes.io/name=spark-operator --no-headers >> "$OUT/context.txt" 2>&1
if [ -n "$SPARK_NS" ]; then
  ( follow_logs "$OUT/logs/spark-operator.log" -n "$SPARK_NS" -l app.kubernetes.io/name=spark-operator \
      --all-containers --prefix --max-log-requests 10 ) &
  PIDS+=($!)
  echo "kubeflow spark operator logs from namespace $SPARK_NS"
else
  echo "WARNING: no Kubeflow spark-operator Deployment found — operator logs not captured (set SPARK_OP_NS)"
fi

# Drivers come and go (PENDING_RERUN makes a new one), so they are attached from the sample loop.
attach_drivers() {
  kubectl -n "$NS" get pods -l spark-role=driver --field-selector=status.phase=Running \
    -o jsonpath='{range .items[*]}{.metadata.name}{"\n"}{end}' 2>/dev/null \
  | while read -r pod; do
      [ -z "$pod" ] && continue
      [ -f "$OUT/logs/driver-$pod.log" ] && continue
      : > "$OUT/logs/driver-$pod.log"
      ( kubectl -n "$NS" logs "$pod" --timestamps -f >> "$OUT/logs/driver-$pod.log" 2>&1 ) &
      echo $! > "$OUT/logs/.driver-$pod.pid"
    done
}

# ------------------------------------------------------------------ sample loop
SAMPLES="$OUT/samples.jsonl"
: > "$SAMPLES"
trap 'echo; echo "interrupted — finishing capture"; touch "$STOP"' INT TERM
printf 'sampling: '
END=$(( START_EPOCH + DURATION ))
i=0
while [ ! -f "$STOP" ] && [ "$(date +%s)" -lt "$END" ]; do
  i=$(( i + 1 ))
  attach_drivers
  kubectl get clusterqueue "$CQ" -o json 2>/dev/null | jq -c --arg t "$(date -u +%Y-%m-%dT%H:%M:%SZ)" '{
      t: $t,
      nominal:  ([((.spec.resourceGroups)//[])[].flavors[] as $f | $f.resources[] | {"\($f.name)/\(.name)": .nominalQuota}] | add // {}),
      usage:    ([((.status.flavorsUsage)//[])[] as $f | $f.resources[] | {"\($f.name)/\(.name)": .total}] | add // {}),
      reserved: ([((.status.flavorsReservation)//[])[] as $f | $f.resources[] | {"\($f.name)/\(.name)": .total}] | add // {}),
      pending: (.status.pendingWorkloads // 0),
      reserving: (.status.reservingWorkloads // 0),
      admitted: (.status.admittedWorkloads // 0)
    }' >> "$SAMPLES" 2>/dev/null
  [ $(( i % 15 )) -eq 0 ] && printf '%ss ' "$(( $(date +%s) - START_EPOCH ))"
  sleep "$INTERVAL"
done
touch "$STOP"
trap - INT TERM
END_EPOCH=$(date +%s)
echo "$END_EPOCH" > "$OUT/end.epoch"
echo; echo "sampling done ($(( END_EPOCH - START_EPOCH ))s)"

# ------------------------------------------------------------------- after-state
kubectl -n "$NS" get workloads -o yaml > "$OUT/raw/workloads-final.yaml" 2>&1
kubectl -n "$NS" get "$SA" -o yaml > "$OUT/raw/sparkapplications-final.yaml" 2>&1
kubectl get clusterqueue "$CQ" -o yaml > "$OUT/raw/clusterqueue-final.yaml" 2>&1
kubectl -n "$NS" get pods -o wide > "$OUT/raw/pods-final.txt" 2>&1
cleanup
trap - EXIT

# ------------------------------------------------------------------- Prometheus
PROM_NS="$(kubectl get svc -A --field-selector metadata.name=prometheus-operated \
  -o jsonpath='{.items[0].metadata.namespace}' 2>/dev/null)"
if [ -n "$PROM_NS" ]; then
  echo "pulling Prometheus range data from $PROM_NS/prometheus-operated"
  PORT=19090
  kubectl -n "$PROM_NS" port-forward svc/prometheus-operated "$PORT:9090" >/dev/null 2>&1 &
  PF=$!
  sleep 3
  STEP=5
  prom() {  # prom <name> <promql>
    curl -sG "http://127.0.0.1:$PORT/api/v1/query_range" \
      --data-urlencode "query=$2" --data-urlencode "start=$START_EPOCH" \
      --data-urlencode "end=$END_EPOCH" --data-urlencode "step=$STEP" > "$OUT/prom/$1.json" \
      || echo "prom query $1 failed" >&2
  }
  K="namespace=\"$KUEUE_NS\""
  # 409 = optimistic-lock conflict, counted by the apiserver for every client, so it does not
  # depend on any controller choosing to log the error.
  prom apiserver_409          'sum by (group, resource, verb) (rate(apiserver_request_total{code="409", resource=~"workloads|sparkapplications|pods"}[30s]))'
  prom apiserver_writes       'sum by (group, resource, verb) (rate(apiserver_request_total{verb=~"UPDATE|PATCH|APPLY|update|patch|apply", resource=~"workloads|sparkapplications|pods"}[30s]))'
  prom reconcile_by_result    "sum by (controller, result) (rate(controller_runtime_reconcile_total{$K}[30s]))"
  prom reconcile_errors       "sum by (controller) (rate(controller_runtime_reconcile_errors_total{$K}[30s]))"
  prom workqueue_depth        "max by (name) (workqueue_depth{$K})"
  prom workqueue_retries      "sum by (name) (rate(workqueue_retries_total{$K}[30s]))"
  prom pending_workloads      "sum by (status) (kueue_pending_workloads{cluster_queue=\"$CQ\"})"
  prom admission_wait_p95     "histogram_quantile(0.95, sum by (le) (rate(kueue_admission_wait_time_seconds_bucket{cluster_queue=\"$CQ\"}[1m])))"
  kill $PF 2>/dev/null
else
  echo "no svc/prometheus-operated found — skipping metrics (logs + watches still cover conflicts)"
fi

echo
echo "captured. now run:  ./analyze.sh $OUT"
