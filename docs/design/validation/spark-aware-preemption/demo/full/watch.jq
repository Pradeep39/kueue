# Input: $p (pods), $w (workloads), $a (SparkApplications). Output: TSV rows.
def gated: ((.spec.schedulingGates // []) | length) > 0;
def deleting: .metadata.deletionTimestamp != null;
def admitted: ([.status.conditions[]? | select(.type == "QuotaReserved" and .status == "True")] | length) > 0
  and ([.status.conditions[]? | select(.type == "Finished" and .status == "True")] | length) == 0;
def ann($k): (.metadata.annotations // {})[$k] // "";

["APP", "PRIO", "STATE", "DRIVER", "EXEC_RUNNING", "GATED", "TERMINATING", "GRANT", "FLOOR", "RECLAIM_TO", "GROW_BACK_TO"],
($a.items[] | . as $app
  | $app.metadata.name as $n
  | ($p.items | map(select(.metadata.labels["sparkoperator.k8s.io/app-name"] == $n))) as $mine
  | ($mine | map(select(.metadata.labels["spark-role"] == "driver")) | first) as $drv
  | ($mine | map(select(.metadata.labels["spark-role"] == "executor"))) as $ex
  | ($w.items | map(select(.metadata.ownerReferences[0].name == $n and admitted)) | first) as $wl
  | [ $n,
      ($app.metadata.labels["kueue.x-k8s.io/priority-class"] // "-"),
      ($app.status.applicationState.state // "-"),
      (if $drv == null then "-" elif ($drv | deleting) then "TERMINATING" else $drv.status.phase end),
      ($ex | map(select(.status.phase == "Running" and (deleting | not) and (gated | not))) | length),
      ($ex | map(select(gated and (deleting | not))) | length),
      ($ex | map(select(deleting)) | length),
      (if $wl == null then "-" else ([$wl.status.admission.podSetAssignments[] | select(.name == "executor") | .count] | first // "-") end),
      (if $wl == null then "-" else ($wl | ann("kueue.x-k8s.io/partial-preemption-min-count")) end),
      (if $wl == null then "" else ($wl | ann("kueue.x-k8s.io/partial-preemption-target")) end),
      (if $wl == null then "" else ($wl | ann("kueue.x-k8s.io/partial-preemption-restore-count")) end)
    ] | map(tostring))
| @tsv
