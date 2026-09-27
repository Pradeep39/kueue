# KEP-77: Dynamically Sized Jobs

<!-- toc -->
- [Summary](#summary)
- [Motivation](#motivation)
  - [Goals](#goals)
  - [Non-Goals](#non-goals)
- [Proposal](#proposal)
  - [User Stories](#user-stories)
    - [Story 1 - batchv1/Job scale-down](#story-1---batchv1job-scale-down)
    - [Story 2 - batchv1/Job scale-up](#story-2---batchv1job-scale-up)
    - [Story 3 – batch/v1.Job in Multi-Cluster Configuration](#story-3--batchv1job-in-multi-cluster-configuration)
    - [Story 4 - SparkApplication with Spark Dynamic Allocation](#story-4---sparkapplication-with-spark-dynamic-allocation)
  - [Notes/Constraints/Caveats (Optional)](#notesconstraintscaveats-optional)
- [Design Details](#design-details)
  - [Enablement](#enablement)
    - [Features](#features)
    - [WorkloadSliceAnnotation](#workloadsliceannotation)
  - [Processing of Workload Slices](#processing-of-workload-slices)
    - [Creation](#creation)
      - [Naming](#naming)
    - [Scheduling and Preemption](#scheduling-and-preemption)
      - [Preemption Disambiguation.](#preemption-disambiguation)
      - [Resource Flavors](#resource-flavors)
    - [Garbage Collection of Preempted Workload Slices](#garbage-collection-of-preempted-workload-slices)
  - [Pod Scheduling Gates](#pod-scheduling-gates)
  - [Topology Aware Scheduling Integration](#topology-aware-scheduling-integration)
    - [Pod Identification Across Slices](#pod-identification-across-slices)
    - [Scale Up](#scale-up)
    - [Scale Down](#scale-down)
  - [Limitations and Incompatibilities](#limitations-and-incompatibilities)
    - [PartialAdmission](#partialadmission)
- [Phases](#phases)
  - [Alpha: Phase 1 - batchv1/Job WorkloadSlices Support in Single-Cluster Configuration.](#alpha-phase-1---batchv1job-workloadslices-support-in-single-cluster-configuration)
    - [Scale Down](#scale-down-1)
    - [Scale Up](#scale-up-1)
  - [Alpha: Phase 2 – RayCluster WorkloadSlice Support in Single-Cluster Configuration](#alpha-phase-2--raycluster-workloadslice-support-in-single-cluster-configuration)
    - [Scale Down](#scale-down-2)
    - [Scale Up](#scale-up-2)
  - [Alpha: Phase 3 – Enabling Workload Slicing for batch/v1.Job in Multi-Cluster Configuration](#alpha-phase-3--enabling-workload-slicing-for-batchv1job-in-multi-cluster-configuration)
  - [Beta: Phase 4 – SparkApplication WorkloadSlice Support with Spark Dynamic Allocation](#beta-phase-4--sparkapplication-workloadslice-support-with-spark-dynamic-allocation)
    - [Scale Down](#scale-down-3)
    - [Scale Up](#scale-up-3)
    - [Scale Up Beyond Available Quota](#scale-up-beyond-available-quota)
- [Additional Details](#additional-details)
  - [Test Plan](#test-plan)
    - [Unit Tests](#unit-tests)
    - [Integration tests](#integration-tests)
    - [Scale-down](#scale-down-4)
    - [Scale-Down](#scale-down-5)
    - [Resource Flavor Handling](#resource-flavor-handling)
  - [Graduation Criteria](#graduation-criteria)
  - [PodScheduling Readiness and ResourceQuota](#podscheduling-readiness-and-resourcequota)
- [Alternatives](#alternatives)
<!-- /toc -->

## Summary

Enable dynamic resizing of Kueue-managed workloads by supporting in-place horizontal scaling of job parallelism.
The primary goal is to allow dynamic adjustment of job parallelism without requiring suspension and requeueing, thereby improving scheduling efficiency and minimizing disruption during resource fluctuations.

In addition, we aim to extend dynamic resizing support to RayCluster workloads. 
While the initial focus is on batch/v1.Job and RayCluster, the long-term objective is to generalize support for dynamically sized (elastic) workloads across other Kueue-integrated resource types.

## Motivation

Kueue currently lacks native support for resizing jobs. Any change in job size leads to the recreation of the associated Workload, resulting in job suspension and requeueing. 
This disrupts execution and hinders usability for elastic workloads like RayCluster, which rely on in-place autoscaling. 

To support such scenarios, Kueue must gracefully handle horizontal scale-up and scale-down operations without disrupting admitted jobs or re-acquiring quota.

### Goals

- Gracefully handle resize operations for Kueue-managed jobs by updating quota usage without suspending the job or terminating running pods.
  
### Non-Goals

- Vertical scaling of workloads – Kueue will only handle resize operations that scale Pods horizontally.
- Orchestration or initiation of resizing for the parent workload objects (e.g., `batch/v1.Job`, `RayJob`, etc.)
- Partial Preemption.

## Proposal

Update the Job framework reconciler and introduce new controllers to orchestrate dynamic resizing of jobs. We are only interested in horizontal scaling of jobs (e.g. scaling more replicas). At a high level, this will be accomplished by:
- Creating WorkloadSlice objects that represent incremental scale up of jobs. This gives us per-replica control of workload admission. Workload Slices will be garbage collected and consolidated with their parent workloads after successful admission.
  - Adding default scheduling gates to control the scheduling of new pods based on their admission status.
  - Dynamically adjust quotas in ClusterQueue based on scaling events.

For the MVP, dynamic resizing will be available through an opt-in mechanism for selected, supported Kueue frameworks—starting with batch/v1.Job and, optionally, RayCluster workloads with autoscaling enabled. 
Additionally, WorkloadSlice will be supported in the multiKueue context as part of the MVP scope.

### User Stories

#### Story 1 - batchv1/Job scale-down

1. The user creates a `batch/v1.Job` with workload-slice enablement explicitly opted in.
2. Kueue admits the Job based on its requested resources, creating the corresponding Workload and Job's Pods. The Pods begin running as usual.
3. The user updates the Job by reducing its parallelism, triggering a scale-down event:
   1. The associated Workload is updated to reflect the new, lower pod count in its spec. 
   2. The local queue’s capacity is updated accordingly to reflect the reduced resource usage. 
   3. One running Job pod is terminated, while the remaining Pods continue running uninterrupted.

#### Story 2 - batchv1/Job scale-up
1. The user creates a `batch/v1.Job` with workload-slice enablement explicitly opted in.
2. Kueue admits the Job based on its requested resources, creating the corresponding Workload and Job’s Pods. The Pods begin running as usual.
3. The user updates the Job by increasing its parallelism, triggering a scale-up event:
   1. A new WorkloadSlice is created to represent the additional pod capacity requested.
   2. The new Pods are created in a gated state (via PodSchedulingGates) and held from scheduling until the slice is admitted. 
   3. Kueue evaluates available resources; if sufficient, the slice is admitted and the scheduling gates on the new Pods are removed. 
   4. The newly admitted Pods begin scheduling and running, resulting in the Job operating with the increased parallelism level.

#### Story 3 – batch/v1.Job in Multi-Cluster Configuration

This story mirrors the behaviors in Stories 1 and 2 but applies them within a multi-cluster (MultiKueue) setup, 
where WorkloadSlices are propagated, admitted, and scheduled across multiple clusters.

#### Story 4 - SparkApplication with Spark Dynamic Allocation

Stories 1-3 are all driven by a user editing the job: parallelism or replica count changes, and Kueue reacts to the
spec update. Spark [Dynamic Allocation](https://spark.apache.org/docs/latest/job-scheduling.html#dynamic-resource-allocation)
inverts that. The Spark driver's `ExecutorAllocationManager` decides on its own when to add or remove executors, and it
creates and deletes executor Pods directly against the API server. It never updates the `SparkApplication`, and it never
calls Kueue. There is therefore no spec change to observe, and the size a user declared is not the size that is running.

1. The user creates a `SparkApplication` with workload-slice enablement explicitly opted in and
   `spark.dynamicAllocation.enabled=true`.
2. Kueue admits the application based on its declared initial executor count, creating the corresponding Workload. The
   driver starts and creates its initial executor Pods.
3. The driver's Dynamic Allocation decides more executors are needed and creates their Pods, triggering a scale-up
   event that **no spec change accompanies**:
   1. Kueue observes the new executor Pods and re-derives the executor count from the Pods that actually exist.
   2. A new WorkloadSlice is created for the higher count, and the newly created Pods stay gated until it is admitted.
   3. On admission the gates are removed on exactly as many Pods as the grant covers, and the previous slice is
      finished.
4. The driver's Dynamic Allocation later decides some executors are idle and deletes their Pods, triggering a
   scale-down:
   1. Kueue observes the deletions, re-derives the lower count, and lowers the existing slice in place - both its
      requested count and its grant - returning the quota to the ClusterQueue and its cohort.
   2. The remaining executors continue running uninterrupted, and the driver is never disturbed.

Because the size is inferred rather than declared, an executor Pod is counted from the moment it exists until it
reaches a terminal phase - including while it is still gated, and including while it is terminating but not yet gone.
A Pod that is gated is precisely the evidence that Dynamic Allocation wants to grow; a Pod that is terminating still
holds node resources. Counting either one differently would make the ClusterQueue disagree with the cluster.

See [SparkApplication Dynamic Allocation: design](sparkapplication-dynamic-allocation.md) for how the count is derived
and applied, including the sequence diagrams for both directions.

### Notes/Constraints/Caveats (Optional)

If Kueue needs to preempt a resized Job, it will preempt the entire Job as a single unit, regardless of whether the Job has undergone a scale-up operation.

## Design Details

To support horizontal scaling of Jobs, we will introduce the concept of a "WorkloadSlice". A WorkloadSlice is a Workload object with an owner reference to the original Workload for a Job. 
Workload Slices represent per-replica changes to a Job that were not initially accounted for when the Job was created. **It's an internal state, not an extra CRD.**

The benefit of Workload Slices is that Kueue can evaluate admission on a per-replica basis without changing the existing semantics of the Workload API. 
Once a WorkloadSlice is admitted, it will replace the original/old WorkloadSlice. 
The replaced slice will be marked as finished.
- Each WorkloadSlice inherits the resource flavor assignment of the original admitted slice, enforcing flavor consistency across scale-ups. 
- In MultiKueue, Workload Slices would go into both clusters (management and workload) in a multi-cluster environment.
- Workload Slices will be created by Kueue and use identical PodTemplate.
- Workload Slices will belong to the same resource flavor as the top-level Workload that was initially admitted (for more details see "Resource Flavors" subsection)


### Enablement

WorkloadSlices in Kueue are enabled through a combination of a Kubernetes feature gate and an opt-in annotation on individual Workload objects. 
At the cluster level, the WorkloadSlices feature must be enabled via the corresponding Kueue feature gate, which controls whether the controller logic for slicing is active. 

Once the feature gate is enabled, individual Workload objects can opt into slicing by including the `kueue.x-k8s.io/elastic-job: "true"` annotation. 
When both conditions are met, Kueue treats the Workload as eligible for partitioning into one or more WorkloadSlice objects, enabling fine-grained scheduling and 
execution across multiple clusters or within a single cluster. If the feature gate is disabled or the annotation is omitted (or set to "false"), 
the system defaults to the traditional single-Workload scheduling path for full backward compatibility.

#### Features
```golang
// owner: @ichekrygin
// kep: https://github.com/kubernetes-sigs/kueue/tree/main/keps/77-dynamically-sized-jobs
//
// ElasticJobsViaWorkloadSlices enables support for horizontal-scale in jobs.
ElasticJobsViaWorkloadSlices featuregate.Feature = "ElasticJobsViaWorkloadSlices"
```

#### WorkloadSliceAnnotation
```golang
// Workload slicing can be enabled for a specific integration job instance,
// provided that the integration supports workload slicing.
const (
  // EnabledAnnotationKey refers to the annotation key present on Job's that support
  // workload slicing.
  // This annotation is alpha-level.
  EnabledAnnotationKey = "kueue.x-k8s.io/elastic-job"
)
```

### Processing of Workload Slices

#### Creation
The creation of WorkloadSlices in Kueue is initiated when a Kueue Job with slicing enabled, via both annotation and feature gate, is admitted for execution. 
Upon admission, the `jobframework.JobReconciler` evaluates the Job’s PodSets and resource requirements, then creates a new or additional (slice) Workload object 
that represents a partition of the original workload. Each slice contains a `PodSet.Count` identical to the Job’s definition, along with scheduling metadata needed for handling preemption of the old workload (slice).

At most two active workload slices can exist for any single Job in Kueue: the current admitted slice and a newly created slice during a scaling operation. 
This ensures a controlled transition between slices while maintaining consistency and avoiding resource contention and race conditions.

To facilitate precise resource scheduling and effective workload preemption, every new job configured with slice enablement will be appropriately annotated using `EnabledAnnotationKey/Value` mentioned above.
Furthermore, each individual workload slice will record the identity of any preempting slice it encounters.

```golang

const (
    // WorkloadSliceReplacementFor is the annotation key used to capture an "old" workload slice key
    // that will be preempted by the "new", e.g., this workload slice with annotation.
    WorkloadSliceReplacementFor = "kueue.x-k8s.io/workload-slice-replacement-for"
)
```

##### Naming
Currently, Kueue uses a deterministic Workload naming strategy by generating consistent names for the same Job instance via `GetWorkloadNameForOwnerWithGVK`.
As a result, Workload names are not unique across different revisions of the same owner object (e.g., a Job), which makes this approach unsuitable for representing Workload slices.

To address this, a new naming strategy will be introduced specifically for Workload slices, incorporating the owner object's generation value to ensure name uniqueness across revisions.
For example, `bachv1/Job`: 
```yaml
annotations:
  kueue.x-k8s.io/elastic-job: "true"
creationTimestamp: "2025-06-12T21:32:41Z"
generation: 1
labels:
  kueue.x-k8s.io/queue-name: demo
name: demo-slice
namespace: demo
resourceVersion: "190175"
uid: 78d1e61a-c399-4c09-800f-a0bbfb5fe71e
```

will result in a Workload name similar to: `job-demo-slice-75e0c`
Whereas, changing `generation: 2`: will result in a different name similar to: `job-demo-slice-e7080`

#### Scheduling and Preemption
Kueue’s scheduler and preemptor components will be augmented to support workload slice processing, specifically by enforcing 
preemption of the old workload slice regardless of queue capacity to make room for the new slice. 

Scheduling assignment is based on pod count capacity, accounting for the existing workload slice's allocated pods. 
For example, scaling a job up from `3` pods to `10` pods results in the creation of a new Workload representing all `10` pods; 
however, the scheduling assignment will reflect only the `7` additional pods needed to admit the new slice, since `3` pods are already accounted for by the existing slice.

While preemption and admission are not strictly atomic operations, they occur within the same scheduling cycle, specifically within the same closure of workload processing. Barring transient errors, for every candidate workload, preemption is immediately followed by admission. We could think of this as quasi-atomic behavior from a scheduling perspective.

What’s important to emphasize is that while the workload object is preempted, the pods associated with the old workload are not. More importantly, those existing pods are effectively absorbed into the new workload.

In the example at hand, preempting an old workload with 3 running pods and admitting a new workload requesting 10 pods (7 of which are pending and schedule-gated), the new workload will effectively consist of 3 running pods and 7 un-gated pods. This transition avoids disruption while maintaining quota integrity.

##### Preemption Disambiguation.

Here, we’re using the term “preemption” to describe the process of replacing an old workload slice with a new one. 
At the same time, Kueue already uses the concept of workload preemption to mean evicting a workload and deferring its scheduling until capacity becomes available.

In the context of workload slices, it may be more helpful to think of this replacement process as **aggregation** rather than preemption. 
Aggregation involves terminating the old slice and activating the new one. In the strict sense, neither eviction nor preemption (as traditionally defined) actually occurs during this process.

As such, we mark the old slice with the `Finished` condition. This ensures it is removed from the in-memory ClusterQueue snapshot, and its resource usage is released and attributed to the new slice that replaces it.

A Workload that is preempted by a WorkloadSlice will be correctly marked with a Finished status condition, including an appropriate reason and message.
```yaml
- type: Finished
  status: True
  reason: WorkloadSliceReplaced
  message: 'Removed to accommodate a workload (UID: a112b551-a595-4356-875d-e075aee52bac, JobUID: 88d348ca-317a-47a0-bd92-786f95f2608e) due to workload slice aggregation'
  observedGeneration: 1
  lastTransitionTime: "2025-07-08T20:09:24Z"
```

##### Resource Flavors

In the context of WorkloadSlice scheduling, when a workload qualifies for multiple resource flavors, i.e., more than one flavor can satisfy the resource requirements and selector constraints, the scheduler must determine which flavor to assign. 
Kueue currently does not support combining resources from multiple flavors within a single PodSet; however, multiple PodSets within the same workload may be admitted using different resource flavors.

Flavor selection is influenced by factors such as quota availability, taint tolerations, node label selectors, and, where applicable, topology-aware scheduling policies.

When scaling up a previously admitted workload slice, the current implementation enforces a sticky flavor constraint, meaning that the new slice must reuse the originally assigned flavor, even if other eligible flavors have available capacity. 
This approach avoids unpredictable flavor switching but may lead to the new slice remaining in a Pending state if the original flavor lacks sufficient resources.

Future enhancements may introduce support for controlled flavor reassignment or user-defined policies to express preferences or tolerances for such transitions.

#### Garbage Collection of Preempted Workload Slices
Currently, a Workload can become outdated, marked by Kueue as "out-of-sync", and subsequently transitioned to the "Finished" state.
This also applies to outdated Workload slices: when multiple successive updates are issued for a given Job, any superseded slices are marked as "Finished" to reflect their obsolescence.

Preempted (i.e., aggregated) Workload slices are marked as Finished.
Under the current design proposal, all finished workload slices are retained indefinitely and are not garbage collected.
To address potential resource buildup, a `PreemptedWorkloadSliceHistory` mechanism whether as a configuration option or an API field on the Workload, could be introduced to limit the number of retained inactive slices, 
similar to how `revisionHistoryLimit` is used in Kubernetes Deployments to manage ReplicaSet history.

Preempted Workload slices that fail admission requirements are not marked as Finished, as they were never actually started (i.e., never admitted or executed). 

Similar to current functionality, all Workloads associated with a Job (including slices) will be deleted when the parent Job is removed.

### Pod Scheduling Gates

Workload slices operate independently of the `spec.suspend` mechanism where applicable, relying instead on pod scheduling gates for admission control. 
This approach is automatically enabled for supported Job types through defaulting. When a Workload (either initial or a slice) is created, all associated pods are initialized with scheduling gates, effectively "gated" from scheduling. 
These gates are removed only upon the admission of the corresponding Workload, ensuring controlled and deliberate execution.

```golang
const (
...
  // WorkloadSliceSchedulingGate is the name of the scheduling gate applied to Pods
  // to delay their scheduling until the associated workload slice has been admitted.
  // This gate ensures that Pods do not begin scheduling prematurely, maintaining
  // proper sequencing in workload processing.
  // This scheduling gate is alpha-level.
  ElasticJobSchedulingGate = "kueue.x-k8s.io/elastic-job"
)
```

### Topology Aware Scheduling Integration

When the ElasticJobsViaWorkloadSlicesWithTAS feature gate is enabled (requires
both ElasticJobsViaWorkloadSlices and TopologyAwareScheduling), Kueue attempts
to preserve topology locality across workload slice transitions during scaling
operations.

For the initial iteration (v0.17), only **unconstrained** topology mode is supported for scaling
operations. Jobs with `kueue.x-k8s.io/elastic-job: "true"` that also specify required or preferred
topology annotations are rejected at job creation via webhook validation. Support for
required/preferred modes will be added in future iterations. During node repair (unhealthy nodes),
both scale-up and scale-down operations are blocked until the nodes recover.

#### Pod Identification Across Slices

The `kueue.x-k8s.io/workload` annotation changes with each workload slice, which prevents
TAS from finding all pods belonging to a job across slice transitions. To address this,
a `kueue.x-k8s.io/workload-slice-name` annotation is added to pods when both features are
enabled. This annotation contains the origin workload name in the slice chain (following
`WorkloadSliceReplacementFor` annotations to find the root) and remains stable across slice
transitions, allowing `TopologyUngater` to find all pods belonging to the job. The namespace
scope in pod lookups ensures uniqueness across different namespaces.

```golang
const (
    // WorkloadSliceNameAnnotation identifies the original workload name in a slice chain.
    WorkloadSliceNameAnnotation = "kueue.x-k8s.io/workload-slice-name"
)
```

#### Scale Up

When a new workload slice replaces an old one during scale-up, the scheduler extracts
the `TopologyAssignment` from the old slice and passes it to TAS. TAS attempts to place
new pods in the same topology domain as existing pods. If the previous assignment is
stale (e.g., nodes are unhealthy), TAS blocks the scale-up operation until the nodes recover.

**Note:** This blocking behavior is a tentative plan for Alpha and will be re-evaluated before Beta.

#### Scale Down

During scale-down, the workload slice retains the existing topology assignment for the
remaining pods. If unhealthy nodes are detected during the scale-down operation, the
operation is blocked to prevent violating the expected minimum replica count. For example,
if a job has 3 replicas with minCount=2 and one node becomes unhealthy (leaving 2 active
pods), allowing scale-down to 2 replicas could result in only 1 active pod.

**Note:** This blocking behavior is a tentative plan for Alpha and will be re-evaluated before Beta.

See [KEP-2724: Topology Aware Scheduling](../2724-topology-aware-scheduling#support-for-elastic-workloads) for TAS details.

### Limitations and Incompatibilities

This section captures all known limitations and incompatibilities introduced by, or resulting from, enabling the ElasticJobs feature.

#### PartialAdmission

A given Job instance cannot have both `PartialAdmission` and `ElasticJob` enabled. This is a **per-Job limitation**, not a global feature-level conflict. It is entirely valid to have both features enabled in the system, just not simultaneously on the same Job.

**Rationale**

* `PartialAdmission` depends on the static nature of a workload. Specifically, it assumes, reasonably, given the current model, that workloads are immutable in terms of `podSets[].count`. Based on this assumption, it adjusts the Job’s spec to reflect the minimum allowed parallelism at scheduling time. This value is fixed and will not change, even if the ClusterQueue’s capacity increases later.

* In practice, `PartialAdmission` takes ownership of `job.spec.parallelism`, and Kueue enforces this immutability through the admission webhook. For example:

  ```text
  admission webhook "vjob.kb.io" denied the request: spec.parallelism: Forbidden: cannot change when partial admission is enabled and the job is not suspended
  ```

**Validation**

To avoid delayed or implicit validation errors, any Kueue-integrated Job that supports `ElasticJobs` should include admission validation logic to reject the following annotation combination:

```yaml
annotations:
  kueue.x-k8s.io/dynamically-sized-job: "true"
  kueue.x-k8s.io/job-min-parallelism: "1"
```

## Phases

### Alpha: Phase 1 - batchv1/Job WorkloadSlices Support in Single-Cluster Configuration.

Scaling up and down for `batch/v1.Job` will be the initial phase of the MVP, as it builds on stable and well-understood Kubernetes components without requiring additional integration work.

#### Scale Down

1. Create a Job with workload-slice enablement and a parallelism value greater than 1. 
2. Observe that a corresponding Workload is created and admitted, and that the Job's pods are in the Running state, matching the specified parallelism. 
3. Update the Job by reducing its parallelism value. 
4. Confirm that the existing Workload is updated accordingly, with WorkloadSpec.PodSets[0].Count reflecting the new parallelism value. 
5. Observe that the number of running pods is reduced, while the remaining pods continue to run uninterrupted.

#### Scale Up

1. Create a `batch/v1.Job` with workload-slice enablement and an initial `parallelism` value greater than 0.
2. Verify that a corresponding `Workload` is created and admitted, and that the Job’s pods are in the `Running` state, matching the specified parallelism.
3. Increase the Job’s `parallelism` value to trigger a scale-up event.
4. Confirm that a new `Workload` slice is created and admitted, reflecting the increased pod count, while the previous slice is marked as `Finished`.
5. Observe that the total number of running pods increases to match the updated parallelism, with the original pods continuing to run without disruption.

### Alpha: Phase 2 – RayCluster WorkloadSlice Support in Single-Cluster Configuration
This phase mirrors the steps from Phase 1 but is adapted specifically for RayCluster workloads, taking into account their autoscaling behavior and internal lifecycle management.


#### Scale Down

1. Create a `RayCluster` with workload-slice enablement, consisting of a head node and a worker group with 2 replicas. Both the head node and each replica in the worker group request 1 CPU core for a total of 3 CPU cores.
2. Verify that a corresponding `Workload` is created, containing 2 `PodSets`, and that it is admitted. You should observe 3 pods in the `Running` state: one for the head node and two for the two worker group replicas.
3. Update the job by reducing the number of replicas in the worker group from 2 to 1.
4. Confirm that the existing `Workload` is updated accordingly:
   - `WorkloadSpec.PodSets[0].Count` (for the head node) remains equal to 1.
   - `WorkloadSpec.PodSets[1].Count` (for the worker group) is equal to 1 (down from 2).
5. Observe that the number of running pods is reduced, while the remaining pods continue to run uninterrupted.
6. Also observe that the ClusterQueue reflects the 2 CPUs that the RayCluster uses.

#### Scale Up

1. Create a `RayCluster` with workload-slice enablement, consisting of a head node and a worker group with 1 replica. Both the head node and each replica in the worker group request 1 CPU core.
2. Verify that a corresponding `Workload` is created and admitted, and that there are 2 pods belonging to the `RayCluster` in the Running state: one for the head node and one for the worker group replica.
3. Update the job by increasing the number of replicas in the worker group from 1 to 2.
4. Confirm that a new `Workload` slice is created and admitted, reflecting the increased pod count, while the previous slice is marked as `Finished`. The new `Workload` reflects the current state of the `RayCluster`:
   - `WorkloadSpec.PodSets[0].Count` (for the head node) is equal to 1.
   - `WorkloadSpec.PodSets[1].Count` (for the worker group) is equal to 2.
5. Observe that the total number of running pods increases to 3 to match the updated replica count, with the original pods continuing to run without disruption.
6. Also observe that the ClusterQueue reflects the 3 CPUs that the RayCluster uses.

### Alpha: Phase 3 – Enabling Workload Slicing for batch/v1.Job in Multi-Cluster Configuration
In this phase, Workload Slicing support for batch/v1.Job will be extended to multi-cluster environments using Kueue’s MultiKueue architecture. 
When a job is scaled in the management cluster, its corresponding WorkloadSlice will be propagated to the appropriate workload cluster(s), respecting existing cluster assignment and resource flavor constraints. 
Each WorkloadSlice will be subject to independent admission in the target cluster, and only after successful admission will scheduling gates be lifted to allow pod execution. 
Slice preemption, quota accounting, and garbage collection must be coordinated across clusters to ensure consistency and avoid orphaned resources. 
This phase will validate correctness and stability of the slicing mechanism in a federated deployment model.

### Beta: Phase 4 – SparkApplication WorkloadSlice Support with Spark Dynamic Allocation

This phase extends workload slicing to `SparkApplication` (`sparkoperator.k8s.io/v1beta2`) under Spark Dynamic
Allocation. It differs from Phases 1-3 in kind, not just in CRD: there is no spec change to react to. The Spark driver
creates and deletes executor Pods itself, so the desired size must be **inferred from the executor Pods that exist**
rather than read from the object.

That inversion drives four design consequences, each of which is a verification point below:

- **The signal is a Pod watch, not a spec update.** Executor Pods carry the Spark Operator's owning-application label,
  which is the only thing linking them to the `SparkApplication` - they are owned by the *driver Pod*, so there is no
  OwnerReference chain and the standard ownership-based watch cannot be used.
- **The count is re-derived, never accumulated.** Every reconcile lists the live executor Pods and counts them afresh.
  An event is only a trigger to go and look, so a coalesced or missed event cannot corrupt the count.
- **Gated and terminating Pods both count.** A gated Pod is the evidence that Dynamic Allocation wants to grow, so
  excluding gated Pods would mean a scale-up is never detected. A terminating Pod still holds node resources until it
  reaches a terminal phase.
- **The derived count is clamped to Dynamic Allocation's own bounds.** The lower bound is load-bearing: a reconcile
  landing while the driver is still creating its initial executors observes a transient prefix of them, which is
  indistinguishable from a real scale-down, and acting on it would dismantle the gang that was just admitted.

Slice names cannot be derived from `metadata.generation`, as they are for the earlier phases, because Dynamic
Allocation scaling never modifies the `SparkApplication` and so never bumps it. A monotonic per-job sequence number is
used instead.

[SparkApplication Dynamic Allocation: design](sparkapplication-dynamic-allocation.md) covers the mechanism behind these
points - the Pod watch and why `Owns()` cannot be used, the clamp, the slice-chain quota accounting that keeps
overlapping slices from being double-counted, the decrease-only admission exception the scale-down path needs, and the
one known limitation - with a sequence diagram for each direction.

#### Scale Down

1. Create a `SparkApplication` with workload-slice enablement, `spark.dynamicAllocation.enabled=true`, and bounds that
   permit shrinking (for example `minExecutors: 1`, `initialExecutors: 3`). Set a short
   `spark.dynamicAllocation.executorIdleTimeout` so idle executors are reclaimed promptly.
2. Verify that a corresponding `Workload` is created and admitted with two `PodSets` - `driver` at count 1 and
   `executor` at the initial count - and that the driver plus that many executor Pods reach `Running`.
3. Let the application go idle so that Dynamic Allocation reclaims executors. Do **not** modify the
   `SparkApplication`.
4. Confirm that the existing `Workload` is updated in place rather than replaced:
   - `WorkloadSpec.PodSets[executor].Count` drops to the new live count.
   - The admitted count in `status.admission` drops to match, with its recorded resource usage rescaled
     proportionally.
5. Observe that the remaining executors continue running uninterrupted and the driver is not restarted.
6. Also observe that the ClusterQueue's reported usage falls to reflect the smaller executor count, and that the freed
   quota becomes lendable again within the cohort.

#### Scale Up

1. Create a `SparkApplication` as above, with `maxExecutors` set above the initial count and enough ClusterQueue quota
   to satisfy it.
2. Verify that a corresponding `Workload` is created and admitted, and that the driver plus the initial executors reach
   `Running`.
3. Submit work that makes Dynamic Allocation request more executors. Again, do **not** modify the `SparkApplication`.
4. Confirm that the newly created executor Pods are held by the elastic scheduling gate, and that a new `Workload`
   slice is created for the higher count while the previous slice remains until the new one is admitted.
5. Confirm that on admission the gate is removed from exactly as many Pods as the new grant covers - not from every
   Pod the driver created - and that the previous slice is then marked `Finished`.
6. Observe that the ClusterQueue is charged the higher count exactly once: while both slices exist, only the latest is
   counted, so the queue never reports usage for the superseded slice as well.
7. Observe that the original executors continue running without disruption throughout.

#### Scale Up Beyond Available Quota

1. With the same setup, constrain the ClusterQueue so that Dynamic Allocation's `maxExecutors` cannot be satisfied.
2. Submit work that makes Dynamic Allocation request more executors than the quota allows.
3. Confirm that the replacement slice is not admitted, the surplus executor Pods remain gated, and the application
   continues running at its currently granted size rather than failing.
4. Confirm that the ClusterQueue never reports usage above its nominal quota at any point.

## Additional Details

### Test Plan

<!--
**Note:** *Not required until targeted at a release.*
The goal is to ensure that we don't accept enhancements with inadequate testing.

All code is expected to have adequate tests (eventually with coverage
expectations). Please adhere to the [Kubernetes testing guidelines][testing-guidelines]
when drafting this test plan.

[testing-guidelines]: https://git.k8s.io/community/contributors/devel/sig-testing/testing.md
-->

[x] I/we understand the owners of the involved components may require updates to
existing tests to make this code solid enough prior to committing the changes necessary
to implement this enhancement.

#### Unit Tests

<!--
In principle every added code should have complete unit test coverage, so providing
the exact set of tests will not bring additional value.
However, if complete unit test coverage is not possible, explain the reason of it
together with explanation why this is acceptable.
-->

<!--
Additionally, try to enumerate the core package you will be touching
to implement this enhancement and provide the current unit coverage for those
in the form of:
- <package>: <date> - <current test coverage>

This can inform certain test coverage improvements that we want to do before
extending the production code to implement this enhancement.
-->
The code will adhere to regular best practices for unit tests and coverage.

#### Integration tests

Integration tests will be executed against mocked clients for Jobs 
that will provide predefined responses and allow to test various scenarios, 
including situations like:

#### Scale-down
* Job scale down, cluster/local queue correctly reflect resource flavor used capacity.

Here’s a revised and polished version of your text with improved grammar, structure, and clarity:

---

#### Integration Tests

Integration tests will be executed against mocked clients for `batchv1/Job` resources. These mocks will simulate predefined responses, enabling validation of various scheduling and workload slice scenarios, including the following cases:

---

#### Scale-Down

* A `Job` is scaled down.
* The ClusterQueue and/or local queue correctly reflect the reduced resource usage for the assigned resource flavor.

---

#### Scale-Up

* A `batchv1/Job` is updated to increase its `parallelism` value.
* New pods are created in the `ScheduleGated` state.
* A new `WorkloadSlice` is created and admitted.
* The previous `WorkloadSlice` is marked as "Finished."
* `ScheduleGates` are removed from the new pods, allowing them to transition to the running state.

---

#### Resource Flavor Handling

* Kueue is configured with two resource flavors, each with different CPU capacities.
* A `batchv1/Job` is created that matches the selection criteria for both flavors.
* The job is initially admitted using the flavor with the smaller capacity.
* The job is later updated to increase `parallelism` beyond the capacity of the originally assigned flavor, but within the capacity of the alternative flavor.
* New pods are created in the `ScheduleGated` state.
* The new `WorkloadSlice` remains in a `Pending` state due to the sticky flavor constraint. The status condition resembles the following:

```yaml
- lastTransitionTime: "2025-06-12T21:32:56Z"
  message: "couldn't assign flavors to pod set main: couldn't change flavor from: smaller-flavor to: larger-flavor, previously considered podsets requests (0) + current podset request (1100m) > maximum capacity (1)
  observedGeneration: 1
  reason: Pending
  status: "False"
  type: QuotaReserved 
```

### Graduation Criteria
<!--

Clearly define what it means for the feature to be implemented and
considered stable.

If the feature you are introducing has high complexity, consider adding graduation
milestones with these graduation criteria:
- [Maturity levels (`alpha`, `beta`, `stable`)][maturity-levels]
- [Feature gate][feature gate] lifecycle
- [Deprecation policy][deprecation-policy]

[feature gate]: https://git.k8s.io/community/contributors/devel/sig-architecture/feature-gates.md
[maturity-levels]: https://git.k8s.io/community/contributors/devel/sig-architecture/api_changes.md#alpha-beta-and-stable-versions
[deprecation-policy]: https://kubernetes.io/docs/reference/using-api/deprecation-policy/
-->

Here’s a structured and detailed **Graduation Criteria** section for KEP-77: *Dynamically Sized Jobs*, tailored to your current implementation of `WorkloadSlice` and phased MVP plan.

---

### Graduation Criteria

#### Alpha

* [x] Feature is gated by the `ElasticJobsViaWorkloadSlices` feature gate.
* [x] WorkloadSlice is implemented as an internal Kueue construct (no new CRDs).
* [x] Scale-up events result in the creation of new `WorkloadSlice`s; scale-downs result in updates to the original `Workload`.
* [x] Pod-level gating using `PodSchedulingGates` is integrated with slice admission.
* [x] Sticky flavor behavior is enforced: scale-ups must reuse the original assigned flavor.
* [x] Superseded slices are marked as `Finished` once the new slice is admitted.
* [x] Support for `batch/v1.Job` in both single-cluster and multi-cluster (multiKueue) configurations.
* [x] Integration tests cover basic scale-up, scale-down, and resource flavor constraints.
* [x] Feature opt-in is supported via workload annotation (`kueue.x-k8s.io/elastic-job: "true"`).
* [x] Documented enablement steps, slice behavior, and known limitations.
* [x] Kueue-native Deployment support leveraging WorkloadSlices.
* [x] Topology Aware Scheduling integration: elastic workloads with unconstrained topology preserve topology assignment during scale-up. Scale-down and required/preferred modes are not yet supported.

#### Beta

* [x] Feature is enabled by default in Kueue, still guarded by the feature gate for opt-out.
* [x] Webhook validation to reject elastic annotation on unsupported frameworks.
* [x] Webhook validation to reject PartialAdmission and Elastic on the same job.
* [x] Dedicated controller for removing scheduling gates on admitted workload slices.
* [x] At least one additional framework beyond `batch/v1.Job` integrates the WorkloadSlice flow.
* [x] Metrics for slice lifecycle transitions (`replaced_workload_slices_total`).
* [ ] Documentation with examples for users and integrators.
* [x] E2e tests for scale-up, scale-down, slice replacement, sticky flavor.
* [x] Old workload slice finished only after replacement slice is admitted.

#### GA (Stable)

* [ ] Feature is permanently enabled and no longer gated.
* [ ] Proven stability under production-scale workloads.
* [ ] API guarantees around slice naming, replacement markers, and flavor enforcement documented and stable.
* [ ] No known correctness issues across single- and multi-cluster environments.
* [ ] Dynamic resizing enabled for all Kueue-managed workloads (JobSet, Kubeflow jobs, etc.).
* [ ] Garbage collection of finished workload slices.
* [ ] Multi-cluster e2e coverage and support.
* [ ] Re-evaluate the WorkloadSlice implementation considering alternatives ([#5897](https://github.com/kubernetes-sigs/kueue/issues/5897)).
* [ ] Re-evaluate PartialAdmission and Elastic combination.
* [ ] Re-evaluate integration frameworks leveraging `ElasticJobsViaWorkloadSlices`.
* [ ] Re-evaluate MultiKueue JobAdapter Sync routine for elastic-specific functionality ([#7065](https://github.com/kubernetes-sigs/kueue/issues/7065)).
* [ ] Re-evaluate cache-level quota accounting during slice replacement to avoid temporary double-counting.

### `ElasticJobsViaWorkloadSlicesWithTAS` Graduation Criteria

TAS integration is gated separately via `ElasticJobsViaWorkloadSlicesWithTAS`
(requires both `ElasticJobsViaWorkloadSlices` and `TopologyAwareScheduling`).

#### Alpha

* [x] Unconstrained topology mode supported for scale-up.
* [x] Webhook validation to reject elastic jobs with required/preferred topology annotations.
* [x] Integration tests for elastic workloads with unconstrained topology.

#### Beta

* [ ] Re-evaluate in production-like environments with scale-up/scale-down cycles.
* [ ] Re-evaluate full integration with required/preferred topology modes.
* [ ] Re-evaluate handling scale-ups and scale-downs during node repair.
* [ ] Correct handling of rank-based ordering for scale-downs.

## Implementation History

<!--
Major milestones in the lifecycle of a KEP should be tracked in this section.
Major milestones might include:
- the `Summary` and `Motivation` sections being merged, signaling SIG acceptance
- the `Proposal` section being merged, signaling agreement on a proposed design
- the date implementation started
- the first Kubernetes release where an initial version of the KEP was available
- the version of Kubernetes where the KEP graduated to general availability
- when the KEP was retired or superseded
-->

## Drawbacks

- Adds complexity to Kueue’s scheduling and preemption model.
- Users must understand new slice lifecycle semantics, which differ from traditional Workload behavior.
- Leveraging PodSchedulingGates together with ResourceQuota enforcement may result out-of-quota namespaces (see section below for more details).

### ResourceQuota

`ResourceQuota` in Kubernetes is a **namespace-scoped object** that limits how much compute, storage, and object-count-based resources a namespace can consume. It is used to enforce fair resource usage in multi-tenant environments and to prevent any single team or workload from consuming excessive cluster resources.

Quota enforcement occurs during the **pod admission phase**. When a pod is created, the API server checks whether the requested resources exceed the available quota in the namespace. If they do, the creation request is rejected.

`ResourceQuota` includes all pods by default, with a few specific exceptions:

| Pod Type / State         | Counted in Quota? | Notes                                |
| ------------------------ | ----------------- | ------------------------------------ |
| Running / Pending Pods   | ✅ Yes             | If they belong to the namespace      |
| Succeeded / Failed Pods  | ❌ No              | Considered completed and inactive    |
| Terminating Pods         | ❌ No              | Identified by a `deletionTimestamp`  |
| Mirror Pods              | ❌ No              | Created directly by the kubelet      |
| Pods outside quota scope | ❌ No              | If the `ResourceQuota` uses `scopes` |

---

### PodScheduling Readiness and ResourceQuota

**PodScheduling Readiness** is a mechanism that ensures a pod is only eligible for scheduling when it meets specific conditions. This is especially useful for multi-stage scheduling, resource reservation, or coordination with external systems.

An important observation: by default, pods that are gated using `PodSchedulingGates` are still counted toward the namespace's `ResourceQuota` if they do not match any of the exclusion conditions listed above.

As a result, when `PodSchedulingGates` are used in a namespace with quota enforcement, it is possible to exhaust the available quota with ScheduleGated pods that are not yet schedulable. This can block additional pods from being admitted, even though none are actively running.



## Alternatives

- Require users to manage resizing manually by recreating jobs.
- Defer support for elastic workloads to higher-level controllers (e.g., RayOperator), leaving Kueue unaware of scale operations.
- [WorkloadResize Request](https://github.com/kubernetes-sigs/kueue/issues/5897) an exploration of an alternative approach to elastic jobs.  