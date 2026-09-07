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
	"fmt"
	"math"
	"slices"
	"strconv"
	"strings"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"

	sparkv1 "sigs.k8s.io/kueue/pkg/controller/jobs/apachesparkapplication/api/v1"
)

// Spark's own defaults, from org.apache.spark.deploy.k8s.Config and
// org.apache.spark.internal.config.
const (
	defaultDriverContainerName   = "spark-kubernetes-driver"
	defaultExecutorContainerName = "spark-kubernetes-executor"

	// defaultMemoryMiB is the Spark default for spark.{driver,executor}.memory (1g).
	defaultMemoryMiB int64 = 1024
	// minMemoryOverheadMiB is Spark's MEMORY_OVERHEAD_MIN_MIB floor.
	minMemoryOverheadMiB int64 = 384
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

// totalMemoryBytes reproduces Spark's memory arithmetic for a pod of the given role:
// base memory, plus overhead (explicit or factor-derived with a 384MiB floor), plus
// PySpark and off-heap allocations where configured.
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
		factored := int64(math.Round(j.memoryOverheadFactor(role) * float64(baseMiB)))
		overheadMiB = max(factored, minMemoryOverheadMiB)
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

// executorCount returns the number of executor pods to reserve quota for.
//
// The operator does not create executors itself - the driver does, from spark.executor.
// instances (or Dynamic Allocation) - so sparkConf, not instanceConfig, is what decides
// how many pods actually appear. instanceConfig only drives the operator's own health
// thresholds, and is consulted here purely as a fallback for an application that leaves
// spark.executor.instances unset.
func (j *SparkApplication) executorCount() (int32, error) {
	if j.dynamicAllocationEnabled() {
		for _, key := range []string{
			"spark.dynamicAllocation.initialExecutors",
			"spark.dynamicAllocation.minExecutors",
		} {
			if raw, ok := j.conf(key); ok {
				n, err := strconv.ParseInt(raw, 10, 32)
				if err != nil {
					return 0, fmt.Errorf("%s: %w", key, err)
				}
				return int32(n), nil
			}
		}
	}
	if raw, ok := j.conf("spark.executor.instances"); ok {
		n, err := strconv.ParseInt(raw, 10, 32)
		if err != nil {
			return 0, fmt.Errorf("spark.executor.instances: %w", err)
		}
		return int32(n), nil
	}
	if ic := instanceConfig(j.SparkApplication); ic != nil && ic.InitExecutors > 0 {
		return ic.InitExecutors, nil
	}
	return defaultExecutorInstances, nil
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
	memoryBytes, err := j.totalMemoryBytes(role)
	if err != nil {
		return nil, err
	}
	limitCPU, err := j.cpuLimit(role)
	if err != nil {
		return nil, err
	}

	if container.Resources.Requests == nil {
		container.Resources.Requests = corev1.ResourceList{}
	}
	if container.Resources.Limits == nil {
		container.Resources.Limits = corev1.ResourceList{}
	}
	container.Resources.Requests[corev1.ResourceCPU] = cpu
	container.Resources.Requests[corev1.ResourceMemory] = *resource.NewQuantity(memoryBytes, resource.BinarySI)
	// Spark sets the memory limit equal to the request, since the JVM heap plus overhead
	// is the whole allocation it intends to use.
	container.Resources.Limits[corev1.ResourceMemory] = *resource.NewQuantity(memoryBytes, resource.BinarySI)
	if limitCPU != nil {
		container.Resources.Limits[corev1.ResourceCPU] = *limitCPU
	}

	return template, nil
}
