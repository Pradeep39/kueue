#!/usr/bin/env bash
# workload-slice-quota-e2e: end-to-end tests for kubernetes-sigs/kueue#18396. A workload slice
# and its replacement must not both be charged to the ClusterQueue during an elastic scale-up.
#
#   ./workload-slice-quota-e2e.sh preflight            check the cluster and Kueue, create nothing
#   ./workload-slice-quota-e2e.sh edge                 deterministic cache edge cases (fails on a Kueue with the bug)
#   ./workload-slice-quota-e2e.sh job [--trials N]     elastic batch Job scale-up, N trials (default 3)
#   ./workload-slice-quota-e2e.sh all [--trials N]     preflight, edge, job
#   ./workload-slice-quota-e2e.sh clean                delete everything the kit created
#
# Environment: KUEUE_NAMESPACE (default kueue-system).
set -uo pipefail
HERE="$(cd "$(dirname "$0")" && pwd)"
KUEUE_NS=${KUEUE_NAMESPACE:-kueue-system}
CMD=${1:-help}; shift || true

die() { echo "preflight: FAIL: $*" >&2; exit 2; }

preflight() {
  command -v kubectl >/dev/null || die "kubectl not found"
  command -v python3 >/dev/null || die "python3 not found"
  kubectl version -o json >/dev/null 2>&1 || die "cannot reach the cluster with the current kubectl context"
  echo "context:      $(kubectl config current-context)"
  kubectl get crd workloads.kueue.x-k8s.io >/dev/null 2>&1 || die "Kueue CRDs not installed"
  local img args
  img=$(kubectl -n "$KUEUE_NS" get deploy kueue-controller-manager -o jsonpath='{.spec.template.spec.containers[0].image}' 2>/dev/null)
  echo "Kueue image:  ${img:-unknown (set KUEUE_NAMESPACE if Kueue is not in kueue-system)}"
  args=$(kubectl -n "$KUEUE_NS" get deploy kueue-controller-manager -o jsonpath='{.spec.template.spec.containers[0].args}' 2>/dev/null)
  case "$args" in *ElasticJobsViaWorkloadSlices=false*) die "the ElasticJobsViaWorkloadSlices feature gate is off";; esac
  local cfg
  cfg=$(kubectl -n "$KUEUE_NS" get cm kueue-manager-config -o jsonpath='{.data.controller_manager_config\.yaml}' 2>/dev/null)
  if [ -n "$cfg" ]; then
    echo "$cfg" | grep -q '"batch/job"\|batch/job' || die "the batch/job integration is not enabled in kueue-manager-config"
  else
    echo "warning:      cannot read kueue-manager-config; assuming the batch/job integration is enabled"
  fi
  kubectl auth can-i patch workloads.kueue.x-k8s.io --subresource=status -n default >/dev/null 2>&1 \
    || die "the edge test needs permission to patch workloads/status"
  for ns in wl-slice-quota-job wl-slice-quota-edge wl-slice-quota-edge-2; do
    kubectl get ns "$ns" >/dev/null 2>&1 && echo "note:         namespace $ns exists from an earlier run; it is reused"
  done
  echo "preflight: PASS"
}

clean() {
  kubectl delete ns wl-slice-quota-job wl-slice-quota-edge wl-slice-quota-edge-2 --ignore-not-found --wait=true >/dev/null
  kubectl delete clusterqueue wl-slice-quota-job wl-slice-quota-edge --ignore-not-found >/dev/null
  kubectl delete resourceflavor wl-slice-quota --ignore-not-found >/dev/null
  echo "clean: done"
}

out() { local d="$HERE/out/$(date +%Y%m%d-%H%M%S)-$1"; mkdir -p "$d"; echo "$d"; }

case "$CMD" in
  preflight) preflight ;;
  edge) preflight >/dev/null || exit 2; python3 "$HERE/lib/edge_test.py" --out "$(out edge)" ;;
  job) preflight >/dev/null || exit 2; python3 "$HERE/lib/job_test.py" --out "$(out job)" "$@" ;;
  all)
    preflight || exit 2
    echo; python3 "$HERE/lib/edge_test.py" --out "$(out edge)"; e=$?
    echo; python3 "$HERE/lib/job_test.py" --out "$(out job)" "$@"; j=$?
    echo; echo "summary: edge $([ $e = 0 ] && echo PASS || echo FAIL), job $([ $j = 0 ] && echo PASS || echo FAIL)"
    exit $((e || j)) ;;
  clean) clean ;;
  *) sed -n '2,12p' "$0" | sed 's/^# \{0,1\}//'; exit 2 ;;
esac
