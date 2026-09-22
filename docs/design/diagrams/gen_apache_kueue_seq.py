#!/usr/bin/env python3
"""Sequence diagrams for the Kueue-side Apache SparkApplication integration
(`pkg/controller/jobs/apachesparkapplication`, PR #28) under Dynamic Allocation scaling.

This is the **Kueue-owned Workload** design - the counterpart to gen_apache_da_seq.py, which
traces the operator-owned alternative. Read side by side they show where the work moves.

Reference handling is deliberately split, because the two halves have different lifetimes:

* Symbols in the **generic** Kueue packages are still live code, so they are resolved from source
  at generation time through gen_seq.py's SYMBOLS table and cannot rot.
* Symbols in the **apachesparkapplication package are pinned**, because that package is removed by
  PR #37. Resolving them from source would fail the build on any branch where the removal has
  landed, and a line number for deleted code is a historical fact rather than a live one. They are
  pinned to f1d6092ab, the last commit where the package existed.
"""

from gen_seq import call, event, note, ref, render, self_, seq

# Pinned refs into the removed package. commit f1d6092ab, branch
# docs-apache-integration-and-diagram-refs - the last state before PR #37 deleted it.
PINNED_COMMIT = "f1d6092ab"
A = {
    "isTrackedExecutorPod": "pod_handler.go:64",
    "schedule": "pod_handler.go:126",
    "isVerifiedLiveExecutor": "podset.go:444",
    "liveExecutorCount": "podset.go:461",
    "computeLiveExecutorCount": "podset.go:486",
    "clampToDynamicAllocationBounds": "podset.go:378",
    "staticExecutorCount": "podset.go:297",
    "initialExecutorCount": "podset.go:399",
    "totalMemoryBytes": "podset.go:130",
    "workloadSequenceNumber": "podset.go:528",
    "PodSets": "controller.go:263",
    "GetWorkloadNameExtraPart": "controller.go:180",
    "RunWithPodSetsInfo": "controller.go:315",
    "PodLabelSelector": "controller.go:259",
    "Suspend": "controller.go:200",
    "Default": "webhook.go:89",
    "validateElasticJob": "webhook.go:184",
}

SPARK_T = ("#f2f4f6", "#c2cad3")     # Spark driver pod / control plane
OPERATOR_T = ("#fdf1e0", "#dcb375")  # spark-kubernetes-operator pod, Java
KUEUE_T = ("#e6eff6", "#8fb2cb")     # kueue-controller-manager pod, Go

SIDE_LEGEND = [
    (*SPARK_T, "Spark driver pod / kube control plane"),
    (*OPERATOR_T, "spark-kubernetes-operator pod (Java)"),
    (*KUEUE_T, "kueue-controller-manager pod (Go)"),
]

SUB = ("Kueue-owned Workload - pkg/controller/jobs/apachesparkapplication (PR #28, removed by #37) "
       "· apachesparkapplication refs pinned to " + PINNED_COMMIT)

UP_LANES = [
    ("Spark driver (DA)", "SPARK · driver pod · ExecutorAllocationManager", SPARK_T),
    ("kube-apiserver", "CLUSTER · control plane", SPARK_T),
    ("executorPodHandler", "KUEUE · apachesparkapplication_executor_pod_handler.go", KUEUE_T),
    ("JobReconciler", "KUEUE · " + ref("ensureOneWorkload"), KUEUE_T),
    ("PodSets / count", "KUEUE · apachesparkapplication_podset.go", KUEUE_T),
    ("workloadslicing", "KUEUE · workloadslicing/workloadslicing.go", KUEUE_T),
    ("Scheduler + flavorassigner", "KUEUE · scheduler/scheduler.go", KUEUE_T),
    ("scheduler cache", "KUEUE · cache/scheduler/clusterqueue.go", KUEUE_T),
    ("elasticJobUngater", "KUEUE · elasticjobs/elastic_job_ungater.go", KUEUE_T),
]

UP = seq([
    note("Precondition: the executor pod template already carries kueue.x-k8s.io/elastic-job as a "
         "scheduling gate, injected once at CR create by the integration's own mutating webhook "
         f"({A['Default']}), with {A['validateElasticJob']} refusing an elastic app that lacks it. "
         "Every executor the driver creates from that template is born gated. The Apache operator "
         "plays no part in scaling at all - it only needs to honour spec.suspend, which "
         f"{A['Suspend']} sets for the initial admission."),
    call(0, 1, "create executor Pods (born gated)"),
    event(1, 2, "Pod CREATE event"),
    self_(2, "isTrackedExecutorPod - label match on the Apache "
             "app-name + spark-role labels. Executor Pods are owned "
             "by the DRIVER Pod, so there is no OwnerReference chain",
          A["isTrackedExecutorPod"]),
    self_(2, "schedule - trailing-edge debounce 5s, maxWait 30s. "
             "The ceiling matters: operator status churn can land more "
             "often than the quiet window",
          A["schedule"]),
    call(2, 3, "enqueue reconcile.Request for the SparkApplication"),
    call(3, 4, "PodSets(ctx, client)", A["PodSets"]),
    call(4, 1, "List Pods by " + A["PodLabelSelector"]),
    self_(4, "isVerifiedLiveExecutor - non-terminal Pods count, "
             "INCLUDING still-gated ones (defect 3)",
          A["isVerifiedLiveExecutor"]),
    self_(4, "clampToDynamicAllocationBounds - conf keys first, then "
             "instanceConfig. Lower bound stops a mid-startup read "
             "dismantling a just-granted gang",
          A["clampToDynamicAllocationBounds"]),
    note("This is the whole inference: no DA event, no call from Spark into Kueue. The count is "
         f"re-derived from live Pod objects every debounced reconcile ({A['computeLiveExecutorCount']}) "
         f"and cached for the pass ({A['liveExecutorCount']}). Before any Pod exists it falls back to "
         f"{A['initialExecutorCount']}, a MAX over instances/initialExecutors/minExecutors. Memory is "
         f"Spark's own base+overhead arithmetic from sparkConf ({A['totalMemoryBytes']}), never the "
         "pod template - Spark overwrites the template's cpu and memory before creating the pod."),
    call(4, 3, "executor PodSet Count = N_live"),
    call(3, 5, "EnsureWorkloadSlices(podSets, ...)", ref("EnsureWorkloadSlices")),
    self_(5, "ScaledUp() -> a NEW slice, never an in-place grow",
          ref("ScaledUp")),
    call(5, 1, "create Workload slice + replacement-for annotation; name from "
               "GetWorkloadNameExtraPart, a per-job sequence number because DA "
               "scaling never bumps Generation",
         A["GetWorkloadNameExtraPart"] + " / " + A["workloadSequenceNumber"]),
    event(1, 6, "pending Workload observed"),
    self_(6, "ReplacedWorkloadSlice / FindReplacedSliceTarget - the "
             "predecessor becomes the preemption target",
          ref("FindReplacedSliceTarget_call")),
    self_(6, "Assignment.append charges the snapshot only the DELTA "
             "vs the replaced slice", ref("Assignment.append")),
    call(6, 1, "Assignment.ToAPI - FULL count written to status.admission",
         ref("Assignment.ToAPI")),
    call(6, 7, "AddOrUpdateWorkload", ref("AddOrUpdateWorkload")),
    self_(7, "sliceChainKey / reconcileSliceGroup - only the chain "
             "tip is charged (ns + slice name + owning job UID)",
          ref("reconcileSliceGroup")),
    call(6, 1, "replaceOldWorkloadSlice - Finish the predecessor",
         ref("replaceOldWorkloadSlice")),
    event(1, 8, "Workload update event"),
    self_(8, "podsToUngate - room = granted - alreadyUngated, capped "
             "by the GRANT and not the request",
          ref("podsToUngate")),
    call(8, 1, "remove the gate from exactly `room` Pods; kube-scheduler "
               "then places them"),
    call(7, 1, "Cache.Usage -> ClusterQueue.status.flavorsUsage, and the "
               "cohort's borrowing headroom with it",
         ref("flavorsUsage")),
])

DOWN_LANES = [
    ("Spark driver (DA)", "SPARK · driver pod · ExecutorAllocationManager", SPARK_T),
    ("kube-apiserver", "CLUSTER · control plane", SPARK_T),
    ("executorPodHandler", "KUEUE · apachesparkapplication_executor_pod_handler.go", KUEUE_T),
    ("JobReconciler", "KUEUE · " + ref("ensureOneWorkload"), KUEUE_T),
    ("PodSets / count", "KUEUE · apachesparkapplication_podset.go", KUEUE_T),
    ("workloadslicing", "KUEUE · workloadslicing/workloadslicing.go", KUEUE_T),
    ("Workload webhook", "KUEUE · webhooks/workload_webhook.go", KUEUE_T),
    ("scheduler cache", "KUEUE · cache/scheduler + workload/workload.go", KUEUE_T),
]

DOWN = seq([
    call(0, 1, "delete executor Pods (executorIdleTimeout elapsed)"),
    event(1, 2, "Pod DELETE / UPDATE event"),
    call(2, 3, "debounced enqueue - same handler as scale-up",
         A["schedule"]),
    call(3, 4, "PodSets(ctx, client)", A["PodSets"]),
    self_(4, "isVerifiedLiveExecutor - a Pod with a DeletionTimestamp "
             "STILL counts until Succeeded/Failed: its containers run "
             "and it holds node resources until then",
          A["isVerifiedLiveExecutor"]),
    call(4, 3, "executor PodSet Count = N_live (lower)"),
    call(3, 5, "EnsureWorkloadSlices(podSets, ...)", ref("EnsureWorkloadSlices")),
    self_(5, "ScaledDown() -> in-place patch. No new slice, and the "
             "scheduler is never involved", ref("ScaledDown")),
    call(5, 1, "updatePodSetCountsWithRetry - lower spec.podSets[].count",
         ref("updatePodSetCountsWithRetry")),
    call(5, 1, "scaleDownAdmission - lower the granted count, rescale "
               "ResourceUsage proportionally (it is the podSet TOTAL, not "
               "per-pod), truncate TopologyAssignment",
         ref("scaleDownAdmission")),
    call(1, 6, "admission mutation must pass validation"),
    self_(6, "validateAdmissionUpdate - the narrow decrease-only, "
             "elastic-only exception. Plain batch/v1 Job admission "
             "stays fully immutable",
          ref("validateAdmissionUpdate")),
    event(1, 7, "Workload update"),
    self_(7, "totalRequestsFromAdmission - charges min(spec.count, "
             "granted), so a stale high grant cannot inflate usage",
          ref("totalRequestsFromAdmission")),
    call(7, 1, "flavorsUsage drops; the freed quota returns to the "
               "ClusterQueue and becomes lendable in the cohort again",
         ref("flavorsUsage")),
    note("The asymmetry this pair exists to show: scale-UP creates a Workload and traverses the "
         "full scheduler and preemption path, because growing a grant needs a capacity check. "
         "Scale-DOWN is two in-place patches and never reaches the scheduler, because giving quota "
         "back cannot fail. Both are driven by the same debounced Pod watch, which is the only "
         "signal Kueue has. Note the Apache operator appears in neither flow."),
])

if __name__ == "__main__":
    render("apache-kueue-da-upscale",
           "Kueue-owned Workload: Apache SparkApplication Dynamic Allocation scale-UP",
           SUB, UP_LANES, UP, extra_legend=SIDE_LEGEND)
    render("apache-kueue-da-downscale",
           "Kueue-owned Workload: Apache SparkApplication Dynamic Allocation scale-DOWN",
           SUB, DOWN_LANES, DOWN, extra_legend=SIDE_LEGEND)
