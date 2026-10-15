#!/usr/bin/env bash
# Partial-preemption demo for Lens + KueueViz. See DEMO.md for the talk track.
#
#   ./demo.sh preflight        check the cluster, image, gate, queues and KueueViz
#   ./demo.sh watch            live terminal view (put it on a second screen)
#   ./demo.sh A|B|C|D          run a scenario (it pauses before the urgent app arrives)
#   ./demo.sh reset            delete the demo apps and restore the baseline queues
#
#   A  today's behaviour: victims that did not opt in are evicted whole and restart
#   B  partial preemption: executors are trimmed, drivers survive, executors grow back
#   C  the protected floor: victims stop at minExecutors; the urgent app waits instead
#   D  spread: four batch apps, each reclaim takes one executor from a different app
#
# The short demo (DEMO-SPREAD-SHUFFLE.md, SPEAKER-NOTES.md), on Spark $SPARK_VERSION (default 3.4.3):
#   ./demo.sh 1 | whole        whole-workload preemption today: apps evicted whole, again and again
#   ./demo.sh 2 | shuffle      partial preemption: reclaimed executors move their shuffle data
#   ./demo.sh shuffle-watch    shuffle MiB per executor for part 2 (run it in a second terminal)
#   ./demo.sh 3 | spread       p10s: six apps each give exactly one executor, then grow back
#   ./demo.sh 4 | roundrobin   p11s: five apps each give one before any gives a second
#   ./demo.sh all              parts 1 to 4 in order, pausing between them
set -uo pipefail
HERE="$(cd "$(dirname "$0")" && pwd)"
GEN="python3 -I $HERE/../scripts/gen-apps.py"
[ -f "$HERE/../scripts/gen-apps.py" ] || GEN="python3 -I $HERE/../gen-apps.py"
NS=batch-ns-uno
SA=sparkapplications.sparkoperator.k8s.io
CQS="cluster-queue-uno cluster-queue-dos"
KUEUEVIZ=${KUEUEVIZ:-}   # your KueueViz frontend URL, e.g. http://<lb-ip>:8080
# Longer jobs than the validation runs, so there is time to point at things.
BATCH_ARGS="1500 1500 12 30 200"
URGENT_ARGS="1500 1500 12 20 200"

say()   { printf '\n\033[1;36m▶ %s\033[0m\n' "$*"; }
look()  { printf '  \033[33m👀 %s\033[0m\n' "$*"; }
pause() { printf '\n\033[1m⏸  %s — press Enter\033[0m ' "$*"; read -r _; }

quota() { # quota CPU_PER_CQ
  local cpu=$1 mem=$(( $1 * 896 ))Mi
  for cq in $CQS; do
    kubectl patch clusterqueue "$cq" --type=json -p "[
      {\"op\":\"replace\",\"path\":\"/spec/resourceGroups/0/flavors/0/resources/0/nominalQuota\",\"value\":\"$cpu\"},
      {\"op\":\"replace\",\"path\":\"/spec/resourceGroups/0/flavors/0/resources/0/borrowingLimit\",\"value\":\"$cpu\"},
      {\"op\":\"replace\",\"path\":\"/spec/resourceGroups/0/flavors/0/resources/1/nominalQuota\",\"value\":\"$mem\"},
      {\"op\":\"replace\",\"path\":\"/spec/resourceGroups/0/flavors/0/resources/1/borrowingLimit\",\"value\":\"$mem\"}]" >/dev/null
  done
  echo "  cohort capacity: $(( 2 * cpu )) slots (1 slot = 1 CPU + 896Mi)"
}

reset() {
  say "Resetting: deleting demo apps, restoring the 12-slot baseline"
  # The watch view shows only events after this moment; events from earlier runs (same app names)
  # stay in the API for an hour.
  date -u +%Y-%m-%dT%H:%M:%SZ > "$HERE/.demo-start"
  kubectl -n $NS delete $SA --all --wait=true >/dev/null 2>&1
  for cq in $CQS; do
    kubectl annotate clusterqueue "$cq" kueue.x-k8s.io/partial-preemption-fallback- kueue.x-k8s.io/partial-preemption-timeout- >/dev/null 2>&1
  done
  quota 6
  for _ in $(seq 1 60); do
    [ "$(kubectl -n $NS get workloads,pods --no-headers 2>/dev/null | wc -l | tr -d ' ')" = 0 ] && { echo "  namespace clean"; return 0; }
    sleep 3
  done
  echo "  WARNING: namespace not clean after 180s"
}

preflight() {
  say "Preflight"
  local img args
  img=$(kubectl -n kueue-system get deploy kueue-controller-manager -o jsonpath='{.spec.template.spec.containers[0].image}')
  args=$(kubectl -n kueue-system get deploy kueue-controller-manager -o jsonpath='{.spec.template.spec.containers[0].args}')
  echo "  Kueue image: $img"
  [[ "$args" == *SparkPartialPreemption=true* ]] && echo "  ✅ SparkPartialPreemption gate on" || echo "  ❌ SparkPartialPreemption gate is OFF"
  kubectl get nodes --no-headers | awk '$2!="Ready"{print "  ⚠️  node "$1" is "$2}'
  kubectl -n kueue-system get pods --no-headers | awk '$3!="Running"{print "  ⚠️  "$1" is "$3}'
  for cq in $CQS; do
    echo "  $cq: $(kubectl get clusterqueue "$cq" -o jsonpath='{.spec.resourceGroups[0].flavors[0].resources[0].nominalQuota}') CPU, annotations: $(kubectl get clusterqueue "$cq" -o jsonpath='{.metadata.annotations}')"
  done
  [ "$(kubectl -n $NS get $SA --no-headers 2>/dev/null | wc -l | tr -d ' ')" = 0 ] && echo "  ✅ namespace $NS has no SparkApplications" || echo "  ⚠️  leftover SparkApplications: run ./demo.sh reset"
  local code
  [ -z "$KUEUEVIZ" ] && { echo "  ℹ️  set KUEUEVIZ=<frontend URL> to check KueueViz"; return 0; }
  code=$(curl -s -o /dev/null -m 3 -w '%{http_code}' "$KUEUEVIZ" 2>/dev/null)
  case "$code" in
    200) echo "  ✅ KueueViz up at $KUEUEVIZ" ;;
    000|"") echo "  ⚠️  KueueViz not reachable at $KUEUEVIZ" ;;
    *) echo "  ℹ️  KueueViz answered HTTP $code at $KUEUEVIZ (a proxy on this machine may block LAN addresses; open it in your browser)" ;;
  esac
  kubectl -n kueue-system get pods -l app=kueueviz-frontend --no-headers 2>/dev/null | awk '{print "  KueueViz frontend pod: "$3}' 
  echo "  Lens: open the cluster, set the namespace filter to $NS"
}

# wait_running APP... : until every app is RUNNING with >= MIN executors running (MIN env, default 3)
wait_running() {
  local min=${MIN:-3}
  printf '  waiting for %s to be running with >= %s executors each ' "$*" "$min"
  for _ in $(seq 1 120); do
    local ok=1
    for a in "$@"; do
      n=$(kubectl -n $NS get pods -l "sparkoperator.k8s.io/app-name=$a,spark-role=executor" --field-selector=status.phase=Running --no-headers 2>/dev/null | wc -l | tr -d ' ')
      [ "${n:-0}" -ge "$min" ] || ok=0
    done
    [ $ok = 1 ] && { echo "✅"; return 0; }
    printf '.'; sleep 3
  done
  echo " (timed out; continuing)"
}

apply() { # apply NAME:PRIORITY[,...] [gen-apps flags]
  local apps=$1; shift
  $GEN "$apps" "$@" | kubectl apply -f - >/dev/null && echo "  applied: ${apps//,/ }"
}

scenario_A() {
  say "Scenario A — today's behaviour: whole-application eviction"
  echo "  Two low-priority static batch apps that have NOT opted into partial preemption"
  echo "  (no elastic-job annotation), then a high-priority Dynamic Allocation app."
  reset
  apply batch-a:low,batch-b:low --static 4 --als-args "$BATCH_ARGS"
  MIN=4 wait_running batch-a batch-b
  look "Lens → Pods: batch-a/batch-b each have 1 driver + 4 executors Running"
  look "KueueViz → Cohort / ClusterQueues: 10 of 12 slots used"
  pause "Launch the urgent app"
  apply urgent:high --partial --cap 2 --als-args "$URGENT_ARGS"
  look "Lens → Pods: a whole batch app disappears — its DRIVER is deleted, not just executors"
  look "Lens → SparkApplications: the victim goes SUSPENDED → PENDING_RERUN → RUNNING (it restarts from scratch)"
  look "Lens → Events: 'Preempted' on the victim's Workload"
  look "KueueViz → Workloads: the victim's Workload is evicted and re-queued"
  echo "  Let it run as long as you like; Ctrl-C returns to the shell. Then ./demo.sh B"
}

scenario_B() {
  say "Scenario B — partial preemption"
  echo "  Two low-priority Dynamic Allocation apps that opted in (elastic, floor 1),"
  echo "  then the same high-priority app."
  reset
  apply batch-a:low,batch-b:low --partial --cap 2 --floor 1 --als-args "$BATCH_ARGS"
  MIN=4 wait_running batch-a batch-b
  look "Lens → SparkApplication batch-a → spec.sparkConf: spark.decommission.enabled=true and"
  look "       spark.kubernetes.executor.terminationGracePeriodSeconds=30s were added by Kueue's webhook"
  look "Lens → Workload of batch-a → annotations: partial-preemption-podset=executor, min-count=1"
  pause "Launch the urgent app"
  apply urgent:high --partial --cap 2 --als-args "$URGENT_ARGS"
  look "Lens → Events: 'PartiallyPreempted … N Pod(s) of PodSet executor, from X to Y' — only executors"
  look "Lens → Pods: reclaimed executors go Terminating (up to 30s: they hand off shuffle data first);"
  look "       the batch DRIVERS stay Running the whole time"
  look "Lens → Pods: Spark recreates executors; they sit Pending with status SchedulingGated"
  look "Lens → Workload of the victim: annotation partial-preemption-target=Y, condition PartiallyPreempted"
  look "Lens → batch-a driver logs: 'Decommission executors' / 'gracefully decommissioned'"
  look "KueueViz → ClusterQueues: usage moves to the urgent app without anything restarting"
  look "When urgent finishes: the gated executors are released and the batch apps grow back"
}

scenario_C() {
  say "Scenario C — the protected floor"
  echo "  Same as B, but the floor is minExecutors (3): Kueue may only take executors above it."
  reset
  apply batch-a:low,batch-b:low --partial --cap 2 --als-args "$BATCH_ARGS"
  MIN=4 wait_running batch-a batch-b
  pause "Launch the urgent app"
  apply urgent:high --partial --cap 2 --als-args "$URGENT_ARGS"
  look "Lens → Pods: each batch app is trimmed down to exactly 3 running executors, never fewer"
  look "Lens → Workload of urgent (pending scale-up slice) → condition QuotaReserved=False, message:"
  look "       'scaling up a running workload never preempts whole workloads that opted into partial preemption…'"
  look "KueueViz → Workloads: urgent's scale-up waits instead of evicting a batch app"
  look "Point: the batch apps keep their guaranteed floor; nothing restarts"
}

scenario_D() {
  say "Scenario D — spreading the loss across victims"
  echo "  Four low-priority apps in a 24-slot cohort, then the high-priority app."
  reset
  quota 12
  apply batch-a:low,batch-b:low,batch-c:low,batch-d:low --partial --cap 2 --floor 1 --als-args "$BATCH_ARGS"
  MIN=3 wait_running batch-a batch-b batch-c batch-d
  pause "Launch the urgent app"
  apply urgent:high --partial --cap 2 --als-args "$URGENT_ARGS"
  look "Lens → Events (filter 'PartiallyPreempted'): each reclaim takes ONE executor, rotating across"
  look "       batch-a, batch-b, batch-c, batch-d — nobody loses many while others lose none"
  look "KueueViz → Workloads: four victims each a little smaller, all still Admitted"
  echo "  Run ./demo.sh reset afterwards to restore the 12-slot baseline."
}

watch_view() {
  while true; do
    local pods wls apps
    pods=$(kubectl -n $NS get pods -o json 2>/dev/null)
    wls=$(kubectl -n $NS get workloads -o json 2>/dev/null)
    apps=$(kubectl -n $NS get $SA -o json 2>/dev/null)
    clear
    printf '\033[1mPartial-preemption demo — %s   (Ctrl-C to quit)\033[0m\n\n' "$(date +%H:%M:%S)"
    jq -rn --argjson p "$pods" --argjson w "$wls" --argjson a "$apps" -f "$HERE/watch.jq" | column -t -s $'\t'
    echo
    kubectl get clusterqueue -o json | jq -r '
      [.items[] | select(.metadata.name | test("^cluster-queue-(uno|dos)$"))] as $cqs
      | ([$cqs[].status.flavorsUsage[]?.resources[]? | select(.name == "cpu") | .total | tonumber] | add // 0) as $used
      | ([$cqs[].spec.resourceGroups[0].flavors[0].resources[0].nominalQuota | tonumber] | add) as $cap
      | "Cohort: \($used) of \($cap) slots in use   (" + ([$cqs[] | "\(.metadata.name): \(([.status.flavorsUsage[]?.resources[]? | select(.name == "cpu") | .total] | first) // "0")"] | join(", ")) + ")"'
    echo; printf '\033[1mPreemption events this scenario (UTC)\033[0m\n'
    local since; since=$(cat "$HERE/.demo-start" 2>/dev/null || echo "1970-01-01T00:00:00Z")
    kubectl -n $NS get events -o json 2>/dev/null | jq -r --arg since "$since" '
      def when: ((.lastTimestamp // .eventTime // .metadata.creationTimestamp // "") | .[0:19] + "Z");
      [.items[]
        | select(.reason | test("^(Preempted|PartiallyPreempted|PartialPreemptionComplete|PartialPreemptionTimedOut)$"))
        | select((.involvedObject.name + " " + .message) | test("batch-|urgent"))
        | select(when >= $since)]
      | sort_by(when) | .[-8:][]
      | "  \(when[11:19])  \(.reason | .[0:26])  \(.involvedObject.name | sub("^sparkapplication-"; "") | .[0:16])  \(.message | gsub("batch-ns-uno/sparkapplication-"; "") | .[0:100])"'
    sleep 2
  done
}

# ---- the short demo: parts 1-4 run the validation scripts in $SCRIPTS
SCRIPTS="$HERE/.."
[ -f "$HERE/../scripts/run-shuffle-migration.sh" ] && SCRIPTS="$HERE/../scripts"
export SPARK_VERSION=${SPARK_VERSION:-3.4.3}

# Apps the short demo creates. Anything else in the namespace belongs to someone else: the parts
# never delete it (no FORCE), so the scripts refuse to start until it is gone.
DEMO_APPS='^(workload-[ab]|pr-victim|urgent|als-(lo[0-9]+|hi))$'
wait_clean() { # each part deletes its own apps in the background; wait for those to go
  local mine others
  for _ in $(seq 1 40); do
    mine=$(kubectl -n $NS get $SA -o name 2>/dev/null | sed 's#.*/##' | grep -cE "$DEMO_APPS" || true)
    [ "$mine" = 0 ] && break
    sleep 3
  done
  others=$(kubectl -n $NS get $SA -o name 2>/dev/null | sed 's#.*/##' | grep -vE "$DEMO_APPS" || true)
  if [ -n "$others" ]; then
    echo "  ⚠️  $NS has SparkApplications this demo did not create: $(echo $others | tr '\n' ' ')"
    echo "     The part will refuse to start. Delete them yourself (./demo.sh reset) if they are yours to remove."
  fi
}
part_1() { say "Part 1: whole-workload preemption (Spark $SPARK_VERSION)"; wait_clean
  EVICTIONS=${EVICTIONS:-3} "$SCRIPTS/run-whole-workload-preemption.sh"; }
part_2() { say "Part 2: shuffle resiliency (Spark $SPARK_VERSION). In a second terminal: ./demo.sh shuffle-watch"; wait_clean
  STOP_AFTER_VICTIM=1 "$SCRIPTS/run-shuffle-migration.sh" "${THRESH:-40}"
  kubectl -n $NS delete $SA --all --wait=false >/dev/null 2>&1; }
part_3() { say "Part 3: spread the loss, p10s (Spark $SPARK_VERSION)"; wait_clean
  CAPTURE=0 "$SCRIPTS/run-partial-preemption-spread.sh" p10s; }
part_4() { say "Part 4: round-robin, p11s (Spark $SPARK_VERSION)"; wait_clean
  CAPTURE=0 "$SCRIPTS/run-partial-preemption-spread.sh" p11s; }
run_all() {
  part_1; pause "Next: part 2, shuffle resiliency (start ./demo.sh shuffle-watch in the second terminal)"
  part_2; pause "Next: part 3, spread the loss"
  part_3; pause "Next: part 4, round-robin"
  part_4; say "Demo complete"
}

case "${1:-}" in
  1|whole) part_1 ;;
  2|shuffle) part_2 ;;
  shuffle-watch) "$SCRIPTS/shuffle-per-executor.sh" pr-victim "$NS" --watch ;;
  3|spread) part_3 ;;
  4|roundrobin) part_4 ;;
  all) run_all ;;
  preflight) preflight ;;
  reset) reset ;;
  watch) watch_view ;;
  A) scenario_A ;; B) scenario_B ;; C) scenario_C ;; D) scenario_D ;;
  *) sed -n '2,/^set -uo pipefail/p' "$0" | sed '$d' | sed 's/^# \{0,1\}//' ;;
esac
