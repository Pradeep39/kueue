#!/usr/bin/env bash
# Print the shuffle data each executor of a Spark application holds on local disk.
#
#   shuffle-per-executor.sh APP [NAMESPACE] [--watch]
#
# APP is the SparkApplication name (the sparkoperator.k8s.io/app-name label). Spark writes shuffle
# output as shuffle_<shuffleId>_<mapId>_0.data (+ .index) files under the executor's block-manager
# directories (/var/data/spark-*/blockmgr-* on Kubernetes); this sums them per executor with
# kubectl exec, so it needs nothing beyond kubectl access to the namespace.
set -uo pipefail
APP=${1:?usage: shuffle-per-executor.sh APP [NAMESPACE] [--watch]}
NS=${2:-batch-ns-uno}
[ "${2:-}" = "--watch" ] && NS=batch-ns-uno
WATCH=0; [[ " $* " == *" --watch "* ]] && WATCH=1

once() {
  printf '%-28s %-12s %8s %10s %12s\n' EXECUTOR NODE FILES "SHUFFLE MiB" "ACTIVE?"
  local total=0
  for pod in $(kubectl -n "$NS" get pods -l "sparkoperator.k8s.io/app-name=$APP,spark-role=executor" \
                 --field-selector=status.phase=Running -o name 2>/dev/null | sed 's#pod/##' | sort -V); do
    # The Pod can exit between the list and here (a reclaimed executor finishing); skip it.
    meta=$(kubectl -n "$NS" get pod "$pod" -o jsonpath='{.spec.nodeName} {.metadata.deletionTimestamp}' 2>/dev/null) || continue
    read -r node del <<<"$meta"
    read -r files kib < <(kubectl -n "$NS" exec "$pod" -c spark-kubernetes-executor -- sh -c \
      'find /var/data -name "shuffle_*" -type f -exec du -k {} + 2>/dev/null | awk "{n++; s+=\$1} END{printf \"%d %d\", n, s}"' 2>/dev/null || echo "- 0")
    mib=$(( ${kib:-0} / 1024 )); total=$(( total + mib ))
    printf '%-28s %-12s %8s %10s %12s\n' "$pod" "$node" "${files:--}" "$mib" "$([ -n "$del" ] && echo decommissioning || echo yes)"
  done
  echo "total: ${total} MiB"
}

if [ $WATCH = 1 ]; then
  while true; do clear; date +%H:%M:%S; once; sleep "${REFRESH:-6}"; done
else
  once
fi
