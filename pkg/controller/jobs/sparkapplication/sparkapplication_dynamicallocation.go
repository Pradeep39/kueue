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

package sparkapplication

import (
	"strconv"
	"strings"

	sparkv1beta2 "github.com/kubeflow/spark-operator/v2/api/v1beta2"
	sparkcommon "github.com/kubeflow/spark-operator/v2/pkg/common"
)

// dynamicAllocation is the Dynamic Allocation configuration Spark runs an application with.
//
// The Spark Operator passes spec.sparkConf to spark-submit as --conf, followed by
// spec.executor.instances when it is set, and by the spec.dynamicAllocation fields that are
// set, but only when spec.dynamicAllocation.enabled is true. The last value of a key wins, so a
// typed field takes precedence over the same key in spec.sparkConf, and spec.sparkConf alone
// configures Dynamic Allocation when the typed fields are not emitted, including when
// spec.dynamicAllocation.enabled is false.
type dynamicAllocation struct {
	enabled bool
	// The executor bounds, nil when unset.
	minExecutors, maxExecutors, initialExecutors *int32
}

// dynamicAllocation resolves Dynamic Allocation from spec.dynamicAllocation and spec.sparkConf.
func (j *SparkApplication) dynamicAllocation() dynamicAllocation {
	typed := j.Spec.DynamicAllocation
	emitted := typed != nil && typed.Enabled
	resolve := func(typedValue *int32, key string) *int32 {
		if emitted && typedValue != nil {
			return typedValue
		}
		return j.sparkConfInt32(key)
	}
	var da dynamicAllocation
	da.enabled = emitted || j.sparkConfBool(sparkcommon.SparkDynamicAllocationEnabled)
	if typed == nil {
		typed = &sparkv1beta2.DynamicAllocation{}
	}
	da.minExecutors = resolve(typed.MinExecutors, sparkcommon.SparkDynamicAllocationMinExecutors)
	da.maxExecutors = resolve(typed.MaxExecutors, sparkcommon.SparkDynamicAllocationMaxExecutors)
	da.initialExecutors = resolve(typed.InitialExecutors, sparkcommon.SparkDynamicAllocationInitialExecutors)
	return da
}

// executorInstances returns the executor count Spark starts with, spec.executor.instances or
// else spark.executor.instances in spec.sparkConf, or nil when neither is set.
func (j *SparkApplication) executorInstances() *int32 {
	if j.Spec.Executor.Instances != nil {
		return j.Spec.Executor.Instances
	}
	return j.sparkConfInt32(sparkcommon.SparkExecutorInstances)
}

// sparkConfBool reads a boolean property the way Spark does: "true" in any case. Anything else,
// including an unset property, is false.
func (j *SparkApplication) sparkConfBool(key string) bool {
	return strings.EqualFold(strings.TrimSpace(j.Spec.SparkConf[key]), "true")
}

// sparkConfInt32 reads an integer property, or nil when it is unset or not a non-negative
// integer. Spark rejects a malformed value when the application is submitted.
func (j *SparkApplication) sparkConfInt32(key string) *int32 {
	raw, ok := j.Spec.SparkConf[key]
	if !ok {
		return nil
	}
	n, err := strconv.ParseInt(strings.TrimSpace(raw), 10, 32)
	if err != nil || n < 0 {
		return nil
	}
	return new(int32(n))
}
