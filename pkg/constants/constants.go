/*
Copyright The Kubernetes Authors.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package constants

import (
	"time"
)

const (
	KueueName                    = "kueue"
	MultiKueueName               = "multikueue"
	JobControllerName            = KueueName + "-job-controller"
	WorkloadControllerName       = KueueName + "-workload-controller"
	PodTerminationControllerName = KueueName + "-pod-termination-controller"
	AdmissionName                = KueueName + "-admission"
	ReclaimablePodsMgr           = KueueName + "-reclaimable-pods"

	// UpdatesBatchPeriod is the duration used to delay the enqueueing of a reconcile request
	// after an event occurs. This facilitates "batching" multiple rapid updates into
	// a single reconciliation loop, reducing unnecessary API pressure and preventing
	// redundant processing during high-churn periods (e.g., mass Pod deletions or updates).
	UpdatesBatchPeriod = time.Second

	// DefaultPriority is used to set priority of workloads
	// that do not specify any priority class, and there is no priority class
	// marked as default.
	DefaultPriority int32 = 0

	DefaultPendingWorkloadsLimit = 1000

	// MaxPendingWorkloadsLimit is the maximum number of pending workloads the
	// visibility API will return in a single request. It bounds the size of a
	// single response (and its allocation) regardless of the user-provided limit,
	// so an authorized caller cannot request an arbitrarily large page.
	MaxPendingWorkloadsLimit = 100_000

	// ManagedByKueueLabelKey label that signalize that an object is managed by Kueue
	ManagedByKueueLabelKey   = "kueue.x-k8s.io/managed"
	ManagedByKueueLabelValue = "true"

	// PodSetLabel is a label set on the Job's PodTemplate to indicate the name
	// of the PodSet of the admitted Workload corresponding to the PodTemplate.
	// The label is set when starting the Job, and removed on stopping the Job.
	PodSetLabel = "kueue.x-k8s.io/podset"

	// ClusterQueueLabel is a pod label indicating the name of the ClusterQueue
	// which admitted the corresponding Kueue's workload.
	// It can be used, for example, to aggregate the actual resource usage (cpu/mem)
	// coming from workloads admitted to a given ClusterQueue.
	//
	// This label requires AssignQueueLabelsForPods feature, enabled by default.
	//
	// Warning: This label is added only if cluster queue name is a valid label value.
	// For limitations see Kubernetes [documentation](https://kubernetes.io/docs/concepts/overview/working-with-objects/labels/#syntax-and-character-set)
	ClusterQueueLabel = "kueue.x-k8s.io/cluster-queue-name"

	// LocalQueueLabel is a pod label indicating the name of the LocalQueue
	// which admitted the corresponding Kueue's workload.
	// It can be used, for example, to aggregate the actual resource usage (cpu/mem)
	// coming from workloads admitted to a given LocalQueue.
	//
	// This label requires AssignQueueLabelsForPods feature, enabled by default.
	LocalQueueLabel = "kueue.x-k8s.io/local-queue-name"

	// AdmissionGatedByAnnotation is the annotation key for gating admission.
	// When present on a Job, Kueue will not admit the corresponding Workload
	// until the annotation is removed. The value is a comma-separated list of
	// controller names (e.g., "example.com/controller1,example.com/controller2").
	//
	// This annotation is beta-level and requires the AdmissionGatedBy feature gate, enabled by default.
	AdmissionGatedByAnnotation = "kueue.x-k8s.io/admission-gated-by"

	// ElasticJobAnnotation is an annotation set on the Job to indicate that it is an elastic job.
	ElasticJobAnnotation = "kueue.x-k8s.io/elastic-job"

	// WorkloadSliceReplacementForAnnotation is set on a new workload slice to name the
	// workload slice it replaces (the "old" slice being superseded).
	//
	// It lives here, rather than in pkg/workloadslicing next to the rest of the slicing
	// helpers, because pkg/cache/scheduler needs it to avoid counting a replaced slice's
	// quota twice and cannot import pkg/workloadslicing (which imports the cache).
	// pkg/workloadslicing re-exports it as WorkloadSliceReplacementFor.
	WorkloadSliceReplacementForAnnotation = "kueue.x-k8s.io/workload-slice-replacement-for"
	// ElasticJobScaleUpAnnotationKey refers to the annotation key present on Jobs that support
	// partial scale up.
	// This annotation is alpha-level.
	// The default value is "atomic".
	ElasticJobScaleUpStrategyAnnotationKey = "kueue.x-k8s.io/elastic-job-scale-up-strategy"

	ElasticJobScaleUpStrategyAtomic  = "atomic"
	ElasticJobScaleUpStrategyPartial = "partial"

	// Partial preemption (docs/design/spark-aware-preemption.md). These live here, rather than
	// in the job framework, because the scheduler cache and the preemption planner read them.

	// PartialPreemptionPodSetAnnotation, on a Workload, names the PodSet that partial
	// preemption may reduce. A Workload without it is never partially preempted, which is
	// how the driver of a Spark application stays protected: it is never named.
	PartialPreemptionPodSetAnnotation = "kueue.x-k8s.io/partial-preemption-podset"
	// PartialPreemptionMinCountAnnotation, on a Workload, is the count partial preemption
	// must never take the reclaimable PodSet below.
	PartialPreemptionMinCountAnnotation = "kueue.x-k8s.io/partial-preemption-min-count"
	// PartialPreemptionMarkedPodsAnnotation, on a Workload, counts the reclaimable PodSet's
	// ungated Pods whose controller.kubernetes.io/pod-deletion-cost is below 0: Pods their
	// own job has already chosen to remove, and so cost nothing to reclaim.
	PartialPreemptionMarkedPodsAnnotation = "kueue.x-k8s.io/partial-preemption-marked-pods"
	// PartialPreemptionTargetAnnotation, on a victim Workload, is the granted count the
	// reclaimable PodSet must be reduced to. Set by the scheduler; cleared by the job
	// reconciler once the reclaim has completed.
	PartialPreemptionTargetAnnotation = "kueue.x-k8s.io/partial-preemption-target"
	// PartialPreemptionDeadlineAnnotation, on a victim Workload, is the RFC 3339 time after
	// which the grant is lowered even if the reclaimed Pods have not stopped.
	PartialPreemptionDeadlineAnnotation = "kueue.x-k8s.io/partial-preemption-deadline"
	// PartialPreemptionRestoreCountAnnotation, on a Workload, is the granted count of the
	// reclaimable PodSet before its first outstanding partial preemption. While the grant is
	// below it, the Workload's scale-up slices are grow-back and must not preempt. It is
	// carried onto replacement slices and removed once the grant reaches it again.
	PartialPreemptionRestoreCountAnnotation = "kueue.x-k8s.io/partial-preemption-restore-count"
	// PartialPreemptionFallbackAnnotation, on a ClusterQueue, chooses what happens when
	// partial preemption cannot fit the preemptor's minimum: "Evict" (default) falls back to
	// whole-workload preemption, "Wait" leaves the preemptor pending.
	PartialPreemptionFallbackAnnotation = "kueue.x-k8s.io/partial-preemption-fallback"
	PartialPreemptionFallbackEvict      = "Evict"
	PartialPreemptionFallbackWait       = "Wait"
	// PartialPreemptionTimeoutAnnotation, on a ClusterQueue, is a Go duration bounding how
	// long a partial preemption waits for the reclaimed Pods to stop. When it is not set, the
	// timeout is the reclaim's termination grace period plus PartialPreemptionTimeoutCushion.
	PartialPreemptionTimeoutAnnotation = "kueue.x-k8s.io/partial-preemption-timeout"
	// PartialPreemptionTerminationGracePeriodAnnotation is a Go duration: the termination grace
	// period Pods reclaimed by partial preemption are deleted with. On a ClusterQueue it is the
	// default for the Workloads admitted to it. On a Workload it is declared by the job, when the
	// job sets a grace period of its own for the reclaimable PodSet, and takes precedence.
	PartialPreemptionTerminationGracePeriodAnnotation = "kueue.x-k8s.io/partial-preemption-termination-grace-period"
	// PartialPreemptionReclaimGracePeriodAnnotation, on a victim Workload, is the termination
	// grace period the outstanding partial preemption deletes Pods with, as a Go duration. Set by
	// the scheduler with the target; cleared with it.
	PartialPreemptionReclaimGracePeriodAnnotation = "kueue.x-k8s.io/partial-preemption-reclaim-grace-period"

	// PartiallyPreemptedCondition is the Workload condition a partial preemption sets on its
	// victim: True while the reclaim is outstanding, False with reason
	// PartialPreemptionCompletedReason or PartialPreemptionTimedOutReason once it has ended.
	PartiallyPreemptedCondition      = "PartiallyPreempted"
	PartialPreemptionIssuedReason    = "PartialPreemptionIssued"
	PartialPreemptionCompletedReason = "PartialPreemptionCompleted"
	PartialPreemptionTimedOutReason  = "PartialPreemptionTimedOut"
	PartialPreemptionAbandonedReason = "PartialPreemptionAbandoned"

	// PodDeletionCostAnnotation is Kubernetes' ReplicaSet scale-down hint (KEP-2255). Lower
	// values are removed first; a Pod without it counts as 0.
	PodDeletionCostAnnotation = "controller.kubernetes.io/pod-deletion-cost"
)

const (
	// DefaultPartialPreemptionTerminationGracePeriod is the termination grace period of a reclaim
	// when neither the job nor its ClusterQueue sets one. It is Kubernetes' own Pod default.
	DefaultPartialPreemptionTerminationGracePeriod = 30 * time.Second
	// PartialPreemptionTimeoutCushion is added to a reclaim's grace period to derive its timeout
	// when the ClusterQueue sets none. It covers what the grace period does not: the time between
	// issuing the reclaim and deleting the Pods, the kubelet's teardown and status update, and the
	// job reconciler's poll interval.
	PartialPreemptionTimeoutCushion = 30 * time.Second
)
