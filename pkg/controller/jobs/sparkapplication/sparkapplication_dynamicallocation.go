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
	"cmp"
	"strconv"
	"strings"

	sparkcommon "github.com/kubeflow/spark-operator/v2/pkg/common"
)

// dynamicAllocation is the Dynamic Allocation Spark runs with. The Spark Operator passes
// spec.sparkConf to spark-submit first, then spec.executor.instances and, only when
// spec.dynamicAllocation.enabled is true, the spec.dynamicAllocation fields that are set.
type dynamicAllocation struct {
	enabled                                      bool
	minExecutors, maxExecutors, initialExecutors *int32
}

func (j *SparkApplication) dynamicAllocation() dynamicAllocation {
	da := dynamicAllocation{
		enabled:          strings.EqualFold(strings.TrimSpace(j.Spec.SparkConf[sparkcommon.SparkDynamicAllocationEnabled]), "true"),
		minExecutors:     j.sparkConfInt32(sparkcommon.SparkDynamicAllocationMinExecutors),
		maxExecutors:     j.sparkConfInt32(sparkcommon.SparkDynamicAllocationMaxExecutors),
		initialExecutors: j.sparkConfInt32(sparkcommon.SparkDynamicAllocationInitialExecutors),
	}
	if t := j.Spec.DynamicAllocation; t != nil && t.Enabled {
		da.enabled = true
		da.minExecutors = cmp.Or(t.MinExecutors, da.minExecutors)
		da.maxExecutors = cmp.Or(t.MaxExecutors, da.maxExecutors)
		da.initialExecutors = cmp.Or(t.InitialExecutors, da.initialExecutors)
	}
	return da
}

func (j *SparkApplication) executorInstances() *int32 {
	return cmp.Or(j.Spec.Executor.Instances, j.sparkConfInt32(sparkcommon.SparkExecutorInstances))
}

// sparkConfInt32 returns a non-negative integer property, or nil if it is unset or malformed.
func (j *SparkApplication) sparkConfInt32(key string) *int32 {
	n, err := strconv.ParseInt(strings.TrimSpace(j.Spec.SparkConf[key]), 10, 32)
	if err != nil || n < 0 {
		return nil
	}
	return new(int32(n))
}
