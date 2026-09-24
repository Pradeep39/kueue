#!/usr/bin/env python3
"""Sequence diagrams for the Kueue-side Apache SparkApplication integration
(`pkg/controller/jobs/apachesparkapplication`, PR #28) under Dynamic Allocation scaling.

This is the **Kueue-owned Workload** design - the counterpart to gen_apache_da_seq.py, which
traces the operator-owned alternative. Read side by side they show where the work moves.

Every code reference is resolved from the source at generation time - both the generic Kueue
symbols (through gen_seq.py's SYMBOLS) and the apachesparkapplication ones (through A_SYMBOLS
below, resolved by the same machinery). An earlier draft pinned the Apache refs to a commit,
because the package was about to be deleted; that removal was abandoned, so pinning would now
just rot, which is the exact problem the resolver exists to prevent.
"""

from gen_seq import call, event, note, ref, render, self_, seq

# apachesparkapplication symbols, resolved from source like the generic ones. A renamed symbol
# fails the build with the key that no longer matches, rather than emitting a wrong number.
AP = "pkg/controller/jobs/apachesparkapplication"
A_SYMBOLS = {
    "isTrackedExecutorPod": (
        f"{AP}/apachesparkapplication_executor_pod_handler.go",
        r"^func isTrackedExecutorPod", "pod_handler.go"),
    "schedule": (
        f"{AP}/apachesparkapplication_executor_pod_handler.go",
        r"^func \(h \*executorPodHandler\) schedule\(", "pod_handler.go"),
    "isVerifiedLiveExecutor": (
        f"{AP}/apachesparkapplication_podset.go",
        r"^func isVerifiedLiveExecutor", "podset.go"),
    "liveExecutorCount": (
        f"{AP}/apachesparkapplication_podset.go",
        r"^func \(j \*SparkApplication\) liveExecutorCount\(", "podset.go"),
    "computeLiveExecutorCount": (
        f"{AP}/apachesparkapplication_podset.go",
        r"^func \(j \*SparkApplication\) computeLiveExecutorCount\(", "podset.go"),
    "clampToDynamicAllocationBounds": (
        f"{AP}/apachesparkapplication_podset.go",
        r"^func \(j \*SparkApplication\) clampToDynamicAllocationBounds\(", "podset.go"),
    "initialExecutorCount": (
        f"{AP}/apachesparkapplication_podset.go",
        r"^func \(j \*SparkApplication\) initialExecutorCount\(", "podset.go"),
    "totalMemoryBytes": (
        f"{AP}/apachesparkapplication_podset.go",
        r"^func \(j \*SparkApplication\) totalMemoryBytes\(", "podset.go"),
    "workloadSequenceNumber": (
        f"{AP}/apachesparkapplication_podset.go",
        r"^func \(j \*SparkApplication\) workloadSequenceNumber\(", "podset.go"),
    "PodSets": (
        f"{AP}/apachesparkapplication_controller.go",
        r"^func \(j \*SparkApplication\) PodSets\(", "controller.go"),
    "GetWorkloadNameExtraPart": (
        f"{AP}/apachesparkapplication_controller.go",
        r"^func \(j \*SparkApplication\) GetWorkloadNameExtraPart\(", "controller.go"),
    "PodLabelSelector": (
        f"{AP}/apachesparkapplication_controller.go",
        r"^func \(j \*SparkApplication\) PodLabelSelector\(", "controller.go"),
    "Suspend": (
        f"{AP}/apachesparkapplication_controller.go",
        r"^func \(j \*SparkApplication\) Suspend\(", "controller.go"),
    "Default": (
        f"{AP}/apachesparkapplication_webhook.go",
        r"^func \(w \*SparkApplicationWebhook\) Default\(", "webhook.go"),
    "validateElasticJob": (
        f"{AP}/apachesparkapplication_webhook.go",
        r"^func validateElasticJob\(", "webhook.go"),
}


def aref(key):
    """An apachesparkapplication reference, resolved from source."""
    return ref(key, A_SYMBOLS)


SPARK_T = ("#f2f4f6", "#c2cad3")     # Spark driver pod / control plane
OPERATOR_T = ("#fdf1e0", "#dcb375")  # spark-kubernetes-operator pod, Java
KUEUE_T = ("#e6eff6", "#8fb2cb")     # kueue-controller-manager pod, Go

SIDE_LEGEND = [
    (*SPARK_T, "Spark driver pod / kube control plane"),
    (*OPERATOR_T, "spark-kubernetes-operator pod (Java)"),
    (*KUEUE_T, "kueue-controller-manager pod (Go)"),
]

SUB = ("Kueue-owned Workload · pkg/controller/jobs/apachesparkapplication (PR #28) · the Apache "
       "operator only has to honour spec.suspend")

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
         f"({aref('Default')}), with {aref('validateElasticJob')} refusing an elastic app that lacks it. "
         "Every executor the driver creates from that template is born gated. The Apache operator "
         "plays no part in scaling at all - it only needs to honour spec.suspend, which "
         f"{aref('Suspend')} sets for the initial admission."),
    call(0, 1, "create executor Pods (born gated)"),
    event(1, 2, "Pod CREATE event"),
    self_(2, "isTrackedExecutorPod - label match on the Apache "
             "app-name + spark-role labels. Executor Pods are owned "
             "by the DRIVER Pod, so there is no OwnerReference chain",
          aref('isTrackedExecutorPod')),
    self_(2, "schedule - trailing-edge debounce 5s, maxWait 30s. "
             "The ceiling matters: operator status churn can land more "
             "often than the quiet window",
          aref('schedule')),
    call(2, 3, "enqueue reconcile.Request for the SparkApplication"),
    call(3, 4, "PodSets(ctx, client)", aref('PodSets')),
    call(4, 1, "List Pods by " + aref('PodLabelSelector')),
    self_(4, "isVerifiedLiveExecutor - non-terminal Pods count, "
             "INCLUDING still-gated ones (defect 3)",
          aref('isVerifiedLiveExecutor')),
    self_(4, "clampToDynamicAllocationBounds - conf keys first, then "
             "instanceConfig. Lower bound stops a mid-startup read "
             "dismantling a just-granted gang",
          aref('clampToDynamicAllocationBounds')),
    note("This is the whole inference: no DA event, no call from Spark into Kueue. The count is "
         f"re-derived from live Pod objects every debounced reconcile ({aref('computeLiveExecutorCount')}) "
         f"and cached for the pass ({aref('liveExecutorCount')}). Before any Pod exists it falls back to "
         f"{aref('initialExecutorCount')}, a MAX over instances/initialExecutors/minExecutors. Memory is "
         f"Spark's own base+overhead arithmetic from sparkConf ({aref('totalMemoryBytes')}), never the "
         "pod template - Spark overwrites the template's cpu and memory before creating the pod."),
    call(4, 3, "executor PodSet Count = N_live"),
    call(3, 5, "EnsureWorkloadSlices(podSets, ...)", ref("EnsureWorkloadSlices")),
    self_(5, "ScaledUp() -> a NEW slice, never an in-place grow",
          ref("ScaledUp")),
    call(5, 1, "create Workload slice + replacement-for annotation; name from "
               "GetWorkloadNameExtraPart, a per-job sequence number because DA "
               "scaling never bumps Generation",
         aref('GetWorkloadNameExtraPart') + " / " + aref('workloadSequenceNumber')),
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
         aref('schedule')),
    call(3, 4, "PodSets(ctx, client)", aref('PodSets')),
    self_(4, "isVerifiedLiveExecutor - a Pod with a DeletionTimestamp "
             "STILL counts until Succeeded/Failed: its containers run "
             "and it holds node resources until then",
          aref('isVerifiedLiveExecutor')),
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
