#!/usr/bin/env python3
"""Sequence diagram for the *proposed* Architecture A: the Apache Spark operator owning the
Kueue Workload for a Dynamic Allocation application.

Unlike gen_seq.py this describes code that does not exist yet, so nothing here is resolved
from source. Every lane is annotated with where it is physically deployed and whether it is
NEW Java to write, an existing component that must CHANGE, or usable as-is.

See docs/design/apache-sparkapplication-integration-design.md sections 2b-2d.
"""

from gen_seq import call, event, note, render, self_, seq

# Lane headers are tinted by the process the component physically runs in, because that is the
# whole point of this diagram: the work is split across two deployments that ship separately.
SPARK_T = ("#f2f4f6", "#c2cad3")     # the Spark driver pod / the control plane - neutral
OPERATOR_T = ("#fdf1e0", "#dcb375")  # spark-kubernetes-operator pod, Java
KUEUE_T = ("#e6eff6", "#8fb2cb")     # kueue-controller-manager pod, Go

SIDE_LEGEND = [
    (*SPARK_T, "Spark driver pod / kube control plane"),
    (*OPERATOR_T, "spark-kubernetes-operator pod (Java)"),
    (*KUEUE_T, "kueue-controller-manager pod (Go)"),
]

# Lane sublabels repeat the side in text, so the diagram survives being printed greyscale,
# and carry whether the component is NEW Java to write, must CHANGE, or is usable as-is.
A_LANES = [
    ("Spark driver (DA)", "SPARK · driver pod · as-is · ExecutorAllocationManager", SPARK_T),
    ("kube-apiserver", "CLUSTER · control plane", SPARK_T),
    ("executor Pod watch", "OPERATOR · Java · NEW · label-keyed watch + debounce", OPERATOR_T),
    ("SparkAppReconciler", "OPERATOR · Java · CHANGE · reconcilesteps/AppInitStep.java",
     OPERATOR_T),
    ("live executor count", "OPERATOR · Java · NEW · count derivation + DA clamp", OPERATOR_T),
    ("Workload factory", "OPERATOR · Java · CHANGE · kueue/KueueWorkloadFactory.java + Utils",
     OPERATOR_T),
    ("Scheduler + flavorassigner", "KUEUE · Go · as-is · scheduler/scheduler.go", KUEUE_T),
    ("scheduler cache", "KUEUE · Go · as-is · cache/scheduler/clusterqueue.go", KUEUE_T),
    ("elasticJobUngater", "KUEUE · Go · as-is · elasticjobs/elastic_job_ungater.go", KUEUE_T),
]

A = seq([
    note("Precondition, and the first thing that has to be built: the executor pod template must "
         "already carry kueue.x-k8s.io/elastic-job as a scheduling gate, plus the PodSet label and "
         "the workload-slice-name annotation. In the Kueue-owned design a Kueue webhook injects "
         "these. Here no Kueue webhook sees the CR, so the OPERATOR must inject them when it "
         "builds the executor pod template. Without the gate there is no admission control at "
         "all: the driver creates executors and kube-scheduler places them."),
    call(0, 1, "create executor Pods (must be born gated)"),
    event(1, 2, "Pod CREATE / UPDATE / DELETE event"),
    self_(2, "match on spark-app-name + spark-role labels. Executor "
             "Pods are owned by the DRIVER Pod, so there is no "
             "OwnerReference chain to watch",
          "NEW · cf. isTrackedExecutorPod"),
    self_(2, "trailing-edge debounce 5s, maxWait 30s",
          "NEW · cf. executorPodHandler.schedule"),
    call(2, 3, "enqueue reconcile for the SparkApplication"),
    call(3, 4, "derive the executor count"),
    call(4, 1, "List Pods by app-name + spark-role label"),
    self_(4, "count non-terminal Pods INCLUDING still-gated ones. "
             "Excluding gated Pods deadlocks scale-up detection",
          "NEW · cf. isVerifiedLiveExecutor"),
    self_(4, "clamp to instanceConfig / spark.dynamicAllocation "
             "min+maxExecutors. The lower bound stops a mid-startup "
             "reconcile dismantling a just-granted gang",
          "NEW · cf. clampToDynamicAllocationBounds"),
    call(4, 3, "executor PodSet count = N_live"),
    call(3, 5, "build the desired Workload"),
    self_(5, "stamp workload-slice-name and replacement-for "
             "annotations. Without them Kueue sees unrelated "
             "Workloads: no predecessor, no delta charging, no netting",
          "NEW · in KueueWorkloadFactory"),
    note("The load-bearing CHANGE. requestAdmission today returns early once the Workload is "
         "admitted, and its only reaction to a changed podSet is to DELETE and recreate it while "
         "still pending. Neither is the slice protocol. It has to create a new Workload slice "
         "that names its predecessor, and leave the predecessor in place for the scheduler."),
    call(5, 1, "create the replacement Workload slice"),
    event(1, 6, "pending Workload observed"),
    self_(6, "ReplacedWorkloadSlice / FindReplacedSliceTarget find "
             "the predecessor. Annotation-driven, so this works "
             "unchanged for an operator-created Workload",
          "as-is"),
    self_(6, "Assignment.append charges the snapshot only the DELTA "
             "vs the replaced slice", "as-is"),
    call(6, 1, "ToAPI — FULL count written to status.admission", "as-is"),
    call(6, 7, "AddOrUpdateWorkload", "as-is"),
    self_(7, "sliceChainKey / reconcileSliceGroup charge only the "
             "chain tip. Key is namespace + slice name + owning job "
             "UID, so the ownerReference must be right",
          "as-is"),
    call(6, 1, "Finish the predecessor slice", "as-is"),
    event(1, 8, "Workload update event"),
    self_(8, "podsToUngate — room = granted - alreadyUngated. Reads "
             "the PodSet label and slice annotation OFF THE PODS, "
             "which is why the operator must stamp them",
          "as-is"),
    call(8, 1, "remove the gate from exactly `room` Pods; "
               "kube-scheduler then places them", "as-is"),
    note("Scale-down is deliberately not drawn. It is two in-place patches - spec.podSets[].count "
         "and status.admission - and the second is only permitted by the decrease-only, "
         "elastic-only exception in Kueue's workload webhook. That exception already exists and is "
         "keyed off the WORKLOAD's kueue.x-k8s.io/elastic-job annotation, not off any job, so the "
         "operator can perform both patches with no further Kueue change. What it must NOT do is "
         "delete and recreate: that drops the predecessor the scheduler needs in order to charge "
         "only the delta."),
])

if __name__ == "__main__":
    render("apache-da-architecture-a",
           "PROPOSED: operator-owned Workload for a Dynamic Allocation SparkApplication",
           "Architecture A — apache/spark-kubernetes-operator + Pradeep39/kueue · "
           "lane tint and sublabel give the deployment side; sublabel also gives change status",
           A_LANES, A, extra_legend=SIDE_LEGEND)
