#!/usr/bin/env bash
# The short partial-preemption demo (DEMO-SPREAD-SHUFFLE.md, SPEAKER-NOTES.md), on either Spark operator.
#
#   ./demo-short.sh [--operator kubeflow|apache] COMMAND [ARGS]
#
# Runs the chosen operator's kit with the same COMMAND and ARGS:
#   kubeflow   kubeflow/demo-short.sh   Kubeflow Spark Operator (sparkoperator.k8s.io), namespace batch-ns-uno
#   apache     apache/demo-short.sh     Apache Spark Operator (spark.apache.org/v1), namespace batch-ns-dos
#
# The operator comes from, in order: --operator, the OPERATOR environment variable, OPERATOR= in
# demo-short.conf beside this script, then kubeflow.
#
# Commands (the same in both kits):
#   preflight [--check]   check the cluster and set up what is missing (--check: report only)
#   1 2 3 4 5             one part: whole eviction, shuffle resiliency, spread, round-robin,
#                         configurable preemption
#   6 [--floor N]         default preemption and its greedy candidate selection; with --floor N the
#                         same apps get partial preemption and the loss is spread
#   7                     Dynamic Allocation apps of different maximum sizes sharing one queue
#   all                   parts 1 to 7 (6 without, then with --floor 1), pausing between them
#   yaml N [--list]       the YAML the latest run of part N applied (--list: all its runs)
#   reset | clear-events | logs
#   help                  this text and the chosen kit's full help
set -uo pipefail
HERE="$(cd "$(dirname "$0")" && pwd)"

op=""
case "${1:-}" in
  --operator) [ $# -ge 2 ] || { echo "demo-short.sh: --operator needs kubeflow or apache" >&2; exit 2; }
              op=$2; shift 2 ;;
  --operator=*) op=${1#--operator=}; shift ;;
esac
# demo-short.conf is read, not sourced: only its OPERATOR= line counts.
conf_op=$(sed -n 's/^[[:space:]]*OPERATOR[[:space:]]*=[[:space:]]*"\{0,1\}\([a-z]*\)"\{0,1\}[[:space:]]*$/\1/p' "$HERE/demo-short.conf" 2>/dev/null | tail -1)
op=${op:-${OPERATOR:-${conf_op:-kubeflow}}}
case "$op" in
  kubeflow|apache) ;;
  *) echo "demo-short.sh: unknown operator '$op' (use --operator kubeflow|apache, OPERATOR=..., or demo-short.conf)" >&2; exit 2 ;;
esac
KIT="$HERE/$op/demo-short.sh"
[ -x "$KIT" ] || { echo "demo-short.sh: $KIT is missing" >&2; exit 1; }

case "${1:-}" in
  ""|help|-h|--help)
    sed -n '2,/^set -uo pipefail/p' "$0" | sed '$d' | sed 's/^# \{0,1\}//'
    echo; echo "Operator: $op ($op/demo-short.sh). Its help:"; echo
    exec "$KIT" ;;
esac
echo "Operator: $op ($op/demo-short.sh)"
# exec, not source: the kit finds its runners and setup files from its own path.
exec "$KIT" "$@"
