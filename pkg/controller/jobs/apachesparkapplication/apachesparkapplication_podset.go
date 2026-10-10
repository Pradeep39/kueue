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

package apachesparkapplication

import (
	"context"
	"fmt"
	"slices"
	"strconv"
	"strings"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	"k8s.io/utils/ptr"
	"sigs.k8s.io/controller-runtime/pkg/client"

	kueue "sigs.k8s.io/kueue/apis/kueue/v1beta2"
	"sigs.k8s.io/kueue/pkg/controller/jobframework"
	sparkv1 "sigs.k8s.io/kueue/pkg/controller/jobs/apachesparkapplication/api/v1"
)

// Spark's own defaults, from org.apache.spark.deploy.k8s.Config and
// org.apache.spark.internal.config.
const (
	defaultDriverContainerName   = "spark-kubernetes-driver"
	defaultExecutorContainerName = "spark-kubernetes-executor"

	// defaultMemoryMiB is the Spark default for spark.{driver,executor}.memory (1g).
	defaultMemoryMiB int64 = 1024
	// defaultMinMemoryOverheadMiB is the default of spark.{driver,executor}.minMemoryOverhead,
	// the floor Spark applies to a factor-derived overhead.
	defaultMinMemoryOverheadMiB int64 = 384
	// jvmMemoryOverheadFactor is MEMORY_OVERHEAD_FACTOR, applied to JVM applications.
	jvmMemoryOverheadFactor = 0.1
	// nonJVMMemoryOverheadFactor is NON_JVM_MEMORY_OVERHEAD_FACTOR, applied to
	// PySpark and SparkR applications.
	nonJVMMemoryOverheadFactor = 0.4
	// defaultCores is the Spark default for spark.{driver,executor}.cores.
	defaultCores int64 = 1
	// defaultExecutorInstances is the Spark default for spark.executor.instances.
	defaultExecutorInstances int32 = 2

	mib int64 = 1024 * 1024
)

// sparkRole distinguishes the two pod roles whose resources are derived differently.
type sparkRole string

const (
	roleDriver   sparkRole = "driver"
	roleExecutor sparkRole = "executor"
)

// conf returns the value of key in .spec.sparkConf.
func (j *SparkApplication) conf(key string) (string, bool) {
	v, ok := j.Spec.SparkConf[key]
	if !ok || v == "" {
		return "", false
	}
	return v, true
}

// isNonJVMApplication reports whether Spark treats this application as non-JVM, which
// raises the default memory overhead factor from 0.1 to 0.4.
//
// spark.kubernetes.resource.type is authoritative when the submitter set it; otherwise
// the presence of Python or R files is what makes the submission non-JVM.
func (j *SparkApplication) isNonJVMApplication() bool {
	if rt, ok := j.conf("spark.kubernetes.resource.type"); ok {
		return !strings.EqualFold(rt, "java") && !strings.EqualFold(rt, "scala")
	}
	return j.Spec.PyFiles != "" || j.Spec.SparkRFiles != ""
}

// memoryOverheadFactor resolves the factor Spark multiplies base memory by when no
// explicit spark.{driver,executor}.memoryOverhead is configured.
func (j *SparkApplication) memoryOverheadFactor(role sparkRole) float64 {
	for _, key := range []string{
		fmt.Sprintf("spark.%s.memoryOverheadFactor", role),
		"spark.kubernetes.memoryOverheadFactor",
	} {
		if raw, ok := j.conf(key); ok {
			if f, err := strconv.ParseFloat(raw, 64); err == nil {
				return f
			}
		}
	}
	if j.isNonJVMApplication() {
		return nonJVMMemoryOverheadFactor
	}
	return jvmMemoryOverheadFactor
}

// minMemoryOverheadMiB resolves the floor Spark applies to a factor-derived overhead.
// spark.{driver,executor}.minMemoryOverhead is ignored when an explicit overhead is set,
// so callers only consult this on the factored path.
func (j *SparkApplication) minMemoryOverheadMiB(role sparkRole) (int64, error) {
	raw, ok := j.conf(fmt.Sprintf("spark.%s.minMemoryOverhead", role))
	if !ok {
		return defaultMinMemoryOverheadMiB, nil
	}
	parsed, err := parseSparkMemoryMiB(raw)
	if err != nil {
		return 0, fmt.Errorf("spark.%s.minMemoryOverhead: %w", role, err)
	}
	return parsed, nil
}

// totalMemoryBytes reproduces Spark's memory arithmetic for a pod of the given role:
// base memory, plus overhead (explicit or factor-derived with a floor), plus PySpark and
// off-heap allocations where configured.
//
// Getting this from spark.{driver,executor}.memory alone is what causes Kueue to
// under-charge Spark: the operator hands the pod a request of base+overhead, which for
// a 1g executor at the default 0.1 factor is 1408MiB, not 1024MiB.
func (j *SparkApplication) totalMemoryBytes(role sparkRole) (int64, error) {
	baseMiB := defaultMemoryMiB
	if raw, ok := j.conf(fmt.Sprintf("spark.%s.memory", role)); ok {
		parsed, err := parseSparkMemoryMiB(raw)
		if err != nil {
			return 0, fmt.Errorf("spark.%s.memory: %w", role, err)
		}
		baseMiB = parsed
	}

	var overheadMiB int64
	if raw, ok := j.conf(fmt.Sprintf("spark.%s.memoryOverhead", role)); ok {
		parsed, err := parseSparkMemoryMiB(raw)
		if err != nil {
			return 0, fmt.Errorf("spark.%s.memoryOverhead: %w", role, err)
		}
		overheadMiB = parsed
	} else {
		minOverheadMiB, err := j.minMemoryOverheadMiB(role)
		if err != nil {
			return 0, err
		}
		// Spark truncates rather than rounds: (factor * memoryMiB).toInt.
		factored := int64(j.memoryOverheadFactor(role) * float64(baseMiB))
		overheadMiB = max(factored, minOverheadMiB)
	}

	totalMiB := baseMiB + overheadMiB

	// PySpark worker memory is charged on executors only, matching
	// BasicExecutorFeatureStep.
	if role == roleExecutor {
		if raw, ok := j.conf("spark.executor.pyspark.memory"); ok {
			parsed, err := parseSparkMemoryMiB(raw)
			if err != nil {
				return 0, fmt.Errorf("spark.executor.pyspark.memory: %w", err)
			}
			totalMiB += parsed
		}
	}

	// Off-heap memory is only added when the allocator is actually enabled.
	if enabled, _ := strconv.ParseBool(j.Spec.SparkConf["spark.memory.offHeap.enabled"]); enabled {
		if raw, ok := j.conf("spark.memory.offHeap.size"); ok {
			parsed, err := parseSparkMemoryMiB(raw)
			if err != nil {
				return 0, fmt.Errorf("spark.memory.offHeap.size: %w", err)
			}
			totalMiB += parsed
		}
	}

	return totalMiB * mib, nil
}

// cpuRequest resolves the CPU request Spark puts on the pod: the Kubernetes-specific
// request override when set, otherwise the whole-core spark.{driver,executor}.cores.
func (j *SparkApplication) cpuRequest(role sparkRole) (resource.Quantity, error) {
	if raw, ok := j.conf(fmt.Sprintf("spark.kubernetes.%s.request.cores", role)); ok {
		q, err := resource.ParseQuantity(raw)
		if err != nil {
			return resource.Quantity{}, fmt.Errorf("spark.kubernetes.%s.request.cores: %w", role, err)
		}
		return q, nil
	}
	cores := defaultCores
	if raw, ok := j.conf(fmt.Sprintf("spark.%s.cores", role)); ok {
		parsed, err := strconv.ParseInt(raw, 10, 32)
		if err != nil {
			return resource.Quantity{}, fmt.Errorf("spark.%s.cores: %w", role, err)
		}
		cores = parsed
	}
	return *resource.NewQuantity(cores, resource.DecimalSI), nil
}

// cpuLimit resolves the optional spark.kubernetes.{driver,executor}.limit.cores.
func (j *SparkApplication) cpuLimit(role sparkRole) (*resource.Quantity, error) {
	raw, ok := j.conf(fmt.Sprintf("spark.kubernetes.%s.limit.cores", role))
	if !ok {
		return nil, nil
	}
	q, err := resource.ParseQuantity(raw)
	if err != nil {
		return nil, fmt.Errorf("spark.kubernetes.%s.limit.cores: %w", role, err)
	}
	return &q, nil
}

// parseSparkMemoryMiB parses a Spark/Java memory string into MiB.
//
// Spark accepts a bare number - interpreted as MiB in this position - or a number with a
// binary unit suffix (k, m, g, t, p, optionally followed by "b"). Kubernetes'
// resource.ParseQuantity cannot be used here: it reads "1g" as invalid and "1M" as a
// decimal megabyte, whereas Spark means 1 GiB and 1 MiB respectively.
func parseSparkMemoryMiB(s string) (int64, error) {
	trimmed := strings.ToLower(strings.TrimSpace(s))
	if trimmed == "" {
		return 0, fmt.Errorf("empty memory value")
	}
	digits := trimmed
	unit := byte('m')
	// Peel a trailing "b" only when it decorates a unit letter ("512mb"), so that a
	// bare byte count ("1b") still resolves to the byte unit rather than the default.
	if len(digits) >= 2 && digits[len(digits)-1] == 'b' && isMemoryUnit(digits[len(digits)-2]) {
		digits = digits[:len(digits)-1]
	}
	if last := digits[len(digits)-1]; last < '0' || last > '9' {
		unit = last
		digits = digits[:len(digits)-1]
	}
	value, err := strconv.ParseInt(digits, 10, 64)
	if err != nil {
		return 0, fmt.Errorf("invalid memory value %q", s)
	}
	if value < 0 {
		return 0, fmt.Errorf("negative memory value %q", s)
	}

	switch unit {
	case 'b':
		// Round a raw byte count up so a sub-MiB request is never charged as zero.
		return (value + mib - 1) / mib, nil
	case 'k':
		return (value + 1023) / 1024, nil
	case 'm':
		return value, nil
	case 'g':
		return value * 1024, nil
	case 't':
		return value * 1024 * 1024, nil
	case 'p':
		return value * 1024 * 1024 * 1024, nil
	default:
		return 0, fmt.Errorf("unknown memory unit %q in %q", string(unit), s)
	}
}

// isMemoryUnit reports whether c is one of Spark's binary unit letters.
func isMemoryUnit(c byte) bool {
	switch c {
	case 'k', 'm', 'g', 't', 'p':
		return true
	default:
		return false
	}
}

// dynamicAllocationEnabled reports whether Spark's Dynamic Allocation is on.
func (j *SparkApplication) dynamicAllocationEnabled() bool {
	enabled, _ := strconv.ParseBool(j.Spec.SparkConf["spark.dynamicAllocation.enabled"])
	return enabled
}

// staticExecutorCount returns the executor count declared in the spec, preferring the raw
// spark.executor.instances key in sparkConf over the structured
// spec.applicationTolerations.instanceConfig.initExecutors field, and falling back to Spark's
// own default.
//
// This is the one property where sparkConf outranks the structured field, because on this CRD
// the structured field never reaches Spark. The operator does not create executors itself -
// the driver does, from spark.executor.instances - and instanceConfig is consumed only by the
// operator's own readiness thresholds (AppRunningStep), never translated into a --conf at
// submission. Preferring instanceConfig would therefore reserve quota for an application
// declaring initExecutors: 2 while 15 pods from spark.executor.instances: 15 actually run.
// The operator's own Kueue integration (KueueWorkloadFactory) reads spark.executor.instances
// alone for the same reason.
func (j *SparkApplication) staticExecutorCount() (int32, error) {
	n, ok, err := j.explicitExecutorInstances()
	if err != nil {
		return 0, err
	}
	if ok {
		return n, nil
	}
	if ic := instanceConfig(j.SparkApplication); ic != nil && ic.InitExecutors > 0 {
		return ic.InitExecutors, nil
	}
	return defaultExecutorInstances, nil
}

// dynamicAllocationCount resolves one of the executor counts, preferring the
// spark.dynamicAllocation.* sparkConf key over its structured
// spec.applicationTolerations.instanceConfig equivalent - Dynamic Allocation runs inside the
// driver and obeys the sparkConf keys, while instanceConfig only feeds the operator's
// readiness thresholds. The instanceConfig fields are plain int32, so a zero is treated as
// unset - which matches the CRD, where a zero bound would be meaningless.
func (j *SparkApplication) dynamicAllocationCount(field string) (int32, bool, error) {
	if raw, ok := j.conf("spark.dynamicAllocation." + field); ok {
		n, err := strconv.ParseInt(raw, 10, 32)
		if err != nil {
			return 0, false, fmt.Errorf("spark.dynamicAllocation.%s: %w", field, err)
		}
		return int32(n), true, nil
	}
	if ic := instanceConfig(j.SparkApplication); ic != nil {
		var v int32
		switch field {
		case "initialExecutors":
			v = ic.InitExecutors
		case "minExecutors":
			v = ic.MinExecutors
		case "maxExecutors":
			v = ic.MaxExecutors
		}
		if v > 0 {
			return v, true, nil
		}
	}
	return 0, false, nil
}

// explicitExecutorInstances reports spark.executor.instances only when the application
// actually declares it, so callers can place it precisely in their precedence ladder without
// dragging in staticExecutorCount's fallbacks - instanceConfig, or Spark's static default of
// 2 - which belong at different rungs.
func (j *SparkApplication) explicitExecutorInstances() (int32, bool, error) {
	raw, ok := j.conf("spark.executor.instances")
	if !ok {
		return 0, false, nil
	}
	n, err := strconv.ParseInt(raw, 10, 32)
	if err != nil {
		return 0, false, fmt.Errorf("spark.executor.instances: %w", err)
	}
	return int32(n), true, nil
}

// clampToDynamicAllocationBounds constrains an executor count to the bounds Dynamic
// Allocation itself promises to respect: it never sustains fewer executors than
// spark.dynamicAllocation.minExecutors, and never requests more than maxExecutors.
//
// The lower bound is what keeps a freshly granted PodSet intact. computeLiveExecutorCount
// stops using initialExecutorCount() as soon as a single executor pod exists, so a reconcile
// landing while the driver is still creating its initial executors observes a strictly
// smaller prefix of them. Without a floor that is indistinguishable from a real scale-down:
// EnsureWorkloadSlices patches spec.podSets[].Count and the granted admission down in place,
// dismantling the gang that was just admitted, and the executors Spark is already asking for
// then need a replacement slice to come back. Reconciles triggered by the operator's own
// status updates are not covered by the executor pod watch's debounce, so this is reachable
// even though the debounce coalesces Dynamic Allocation's own bursts.
//
// The upper bound stops the requested count growing past anything Dynamic Allocation could
// legitimately want. It bounds by DA's own ceiling, not by what the ClusterQueue can grant,
// so it narrows rather than closes the gated-pod feedback loop.
//
// maxExecutors is applied last so a configuration with minExecutors > maxExecutors can never
// inflate the count above the declared maximum.
func (j *SparkApplication) clampToDynamicAllocationBounds(count int32) (int32, error) {
	n, ok, err := j.dynamicAllocationCount("minExecutors")
	if err != nil {
		return 0, err
	}
	if ok && count < n {
		count = n
	}
	n, ok, err = j.dynamicAllocationCount("maxExecutors")
	if err != nil {
		return 0, err
	}
	if ok && count > n {
		count = n
	}
	return count, nil
}

// initialExecutorCount returns the count to assume for a Dynamic-Allocation-enabled
// application before any executor pods have been observed, bounded by Dynamic Allocation's
// own limits. See declaredInitialExecutors for how the count is resolved.
func (j *SparkApplication) initialExecutorCount() (int32, error) {
	count, err := j.declaredInitialExecutors()
	if err != nil {
		return 0, err
	}
	return j.clampToDynamicAllocationBounds(count)
}

// declaredInitialExecutors resolves the initial executor count as Spark's
// Utils.getDynamicAllocationInitialExecutors does: the largest of minExecutors,
// initialExecutors and the resolved executor-instances count.
//
// Each individual term still resolves through its own precedence - the instances term
// through staticExecutorCount, the bounds through dynamicAllocationCount - so only the
// three-way combination is a maximum. Spark starts the largest of the three regardless of
// which the author considered authoritative, so resolving the combination by precedence
// would under-reserve.
func (j *SparkApplication) declaredInitialExecutors() (int32, error) {
	count, err := j.staticExecutorCount()
	if err != nil {
		return 0, err
	}
	for _, field := range []string{"initialExecutors", "minExecutors"} {
		n, ok, err := j.dynamicAllocationCount(field)
		if err != nil {
			return 0, err
		}
		if ok {
			count = max(count, n)
		}
	}
	return count, nil
}

// isVerifiedLiveExecutor reports whether pod should currently count against the executor
// PodSet: it exists and has not reached a terminal phase.
//
// A Pending pod still counts - quota must be reserved as soon as the pod is admitted to the
// cluster, not once it happens to reach Running, or there is a window in which Dynamic
// Allocation has consumed real capacity that Kueue does not know about. A pod with a
// DeletionTimestamp also still counts: Dynamic Allocation deletes executors it no longer
// wants, but the containers keep running and occupying node resources until the pod
// actually reaches Succeeded or Failed. Excluding it the instant the delete is issued would
// undercount live pods and manufacture spurious intermediate counts while Dynamic
// Allocation works through a batch of deletions.
func isVerifiedLiveExecutor(pod *corev1.Pod) bool {
	switch pod.Status.Phase {
	case corev1.PodSucceeded, corev1.PodFailed:
		return false
	default:
		return true
	}
}

// liveExecutorCount returns the executor count to size the executor PodSet with, caching
// the result for the lifetime of this *SparkApplication.
//
// PodSets() is called several times per reconcile (equivalence checks, then workload
// construction). Without caching, two calls could observe different live counts if Dynamic
// Allocation churns pods between them, producing a spurious "not equivalent" verdict and
// self-inflicted workload-slice churn. NewJob() allocates a fresh wrapper per reconcile, so
// the cache is scoped to one pass and never goes stale across reconciles.
func (j *SparkApplication) liveExecutorCount(ctx context.Context, c client.Client) (int32, error) {
	if j.cachedLiveExecutorCount != nil {
		return *j.cachedLiveExecutorCount, nil
	}
	count, err := j.computeLiveExecutorCount(ctx, c)
	if err != nil {
		return 0, err
	}
	j.cachedLiveExecutorCount = ptr.To(count)
	return count, nil
}

// computeLiveExecutorCount is the uncached implementation of liveExecutorCount.
//
// Spark's ExecutorAllocationManager runs in the driver and creates and deletes executor
// pods directly against the Kubernetes API without ever touching the SparkApplication, so
// spark.executor.instances goes stale the moment Dynamic Allocation scales. When Dynamic
// Allocation is on, this lists the live executor pods and trusts that count instead.
//
// This never writes back to the SparkApplication: the operator treats any spec change on a
// running application as a full update and tears the app down to resubmit it, so deriving
// the count read-only is the only way to keep accounting correct without disrupting the run.
//
// The derived count is clamped to Dynamic Allocation's own minExecutors/maxExecutors bounds;
// see clampToDynamicAllocationBounds for why the lower bound is load-bearing.
func (j *SparkApplication) computeLiveExecutorCount(ctx context.Context, c client.Client) (int32, error) {
	if !j.dynamicAllocationEnabled() {
		return j.staticExecutorCount()
	}

	if c == nil {
		// No client available, e.g. webhook validation building a PodSet template purely
		// to inspect its metadata. Fall back to the pre-startup estimate.
		return j.initialExecutorCount()
	}

	podList := &corev1.PodList{}
	if err := c.List(ctx, podList,
		client.InNamespace(j.Namespace),
		client.MatchingLabels{
			appNameLabel: j.Name,
			roleLabel:    executorRoleValue,
		},
	); err != nil {
		return 0, err
	}

	if len(podList.Items) == 0 {
		// No executor pods yet, e.g. the application was just admitted.
		return j.initialExecutorCount()
	}

	var live int32
	for i := range podList.Items {
		if isVerifiedLiveExecutor(&podList.Items[i]) {
			live++
		}
	}
	return j.clampToDynamicAllocationBounds(live)
}

// workloadSequenceNumber returns the number of Workloads ever created for this application,
// finished or not, caching it for the lifetime of this *SparkApplication.
//
// GetWorkloadNameExtraPart folds this into the generated slice name so a name is never
// reused across the application's lifetime. See that method for why a live executor count
// is not sufficient on its own.
func (j *SparkApplication) workloadSequenceNumber(ctx context.Context, c client.Client) (int32, error) {
	if j.cachedWorkloadSequenceNumber != nil {
		return *j.cachedWorkloadSequenceNumber, nil
	}
	if c == nil {
		// No client available (webhook validation): nothing to list against, so this is
		// left uncached and recomputes to 0. That only affects building a template to
		// inspect metadata, never the naming of a Workload that actually gets created.
		return 0, nil
	}

	wlList := &kueue.WorkloadList{}
	if err := c.List(ctx, wlList,
		client.InNamespace(j.Namespace),
		jobframework.OwnerReferenceIndexFieldMatcher(gvk, j.Name),
	); err != nil {
		return 0, err
	}

	count := int32(len(wlList.Items))
	j.cachedWorkloadSequenceNumber = ptr.To(count)
	return count, nil
}

// instanceConfig safely reaches .spec.applicationTolerations.instanceConfig.
func instanceConfig(app *sparkv1.SparkApplication) *sparkv1.ExecutorInstanceConfig {
	if app.Spec.ApplicationTolerations == nil {
		return nil
	}
	return app.Spec.ApplicationTolerations.InstanceConfig
}

// templateSpec safely reaches the pod template for the given role.
func templateSpec(app *sparkv1.SparkApplication, role sparkRole) *corev1.PodTemplateSpec {
	var holder *sparkv1.BaseApplicationTemplateSpec
	if role == roleDriver {
		holder = app.Spec.DriverSpec
	} else {
		holder = app.Spec.ExecutorSpec
	}
	if holder == nil {
		return nil
	}
	return holder.PodTemplateSpec
}

// containerName resolves which container in the pod template Spark treats as the Spark
// container, honouring spark.kubernetes.{driver,executor}.podTemplateContainerName.
func (j *SparkApplication) containerName(role sparkRole) string {
	if raw, ok := j.conf(fmt.Sprintf("spark.kubernetes.%s.podTemplateContainerName", role)); ok {
		return raw
	}
	if role == roleDriver {
		return defaultDriverContainerName
	}
	return defaultExecutorContainerName
}

// buildPodTemplateSpec produces the PodSet template for a role: the user's pod template
// with Spark's derived CPU and memory applied over the Spark container, mirroring how
// the operator and driver actually build the pod.
func (j *SparkApplication) buildPodTemplateSpec(role sparkRole) (*corev1.PodTemplateSpec, error) {
	var template *corev1.PodTemplateSpec
	if base := templateSpec(j.SparkApplication, role); base != nil {
		template = base.DeepCopy()
	} else {
		template = &corev1.PodTemplateSpec{}
	}

	name := j.containerName(role)
	idx := slices.IndexFunc(template.Spec.Containers, func(c corev1.Container) bool {
		return c.Name == name
	})
	if idx < 0 {
		// Spark overlays its container onto the first entry when the template has one
		// but does not name it; otherwise it appends the container itself.
		if len(template.Spec.Containers) > 0 {
			idx = 0
		} else {
			template.Spec.Containers = append(template.Spec.Containers, corev1.Container{Name: name})
			idx = len(template.Spec.Containers) - 1
		}
	}
	container := &template.Spec.Containers[idx]

	cpu, err := j.cpuRequest(role)
	if err != nil {
		return nil, err
	}
	limitCPU, err := j.cpuLimit(role)
	if err != nil {
		return nil, err
	}

	// Spark's derived cpu and memory overwrite whatever the pod template declares, because
	// that is what happens to the pod itself: the operator writes the template to a file and
	// passes it as spark.kubernetes.{driver,executor}.podTemplateFile, and Spark's
	// Basic{Driver,Executor}FeatureStep then replaces the Spark container's cpu and memory
	// with base+overhead before creating the pod. Honouring a template request verbatim would
	// charge a number the kubelet never sees. The operator's own Kueue integration
	// (KueueWorkloadFactory.decorateContainerResources) overwrites them for the same reason.
	if container.Resources.Requests == nil {
		container.Resources.Requests = corev1.ResourceList{}
	}
	if container.Resources.Limits == nil {
		container.Resources.Limits = corev1.ResourceList{}
	}
	container.Resources.Requests[corev1.ResourceCPU] = cpu

	memoryBytes, err := j.totalMemoryBytes(role)
	if err != nil {
		return nil, err
	}
	container.Resources.Requests[corev1.ResourceMemory] = *resource.NewQuantity(memoryBytes, resource.BinarySI)
	// Spark sets the memory limit equal to the request, since the JVM heap plus overhead is
	// the whole allocation it intends to use.
	container.Resources.Limits[corev1.ResourceMemory] = *resource.NewQuantity(memoryBytes, resource.BinarySI)

	if limitCPU != nil {
		container.Resources.Limits[corev1.ResourceCPU] = *limitCPU
	}

	return template, nil
}
