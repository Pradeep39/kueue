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
	"testing"

	"github.com/google/go-cmp/cmp"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"

	sparkv1 "sigs.k8s.io/kueue/pkg/controller/jobs/apachesparkapplication/api/v1"
)

// wrap builds the job wrapper around a spec, which is all the podset helpers read.
func wrap(spec sparkv1.ApplicationSpec) *SparkApplication {
	return &SparkApplication{SparkApplication: &sparkv1.SparkApplication{Spec: spec}}
}

func TestParseSparkMemoryMiB(t *testing.T) {
	cases := map[string]struct {
		in      string
		want    int64
		wantErr bool
	}{
		"bare number is MiB":           {in: "2048", want: 2048},
		"explicit MiB":                 {in: "512m", want: 512},
		"MiB with b suffix":            {in: "512mb", want: 512},
		"GiB":                          {in: "1g", want: 1024},
		"GiB with b suffix":            {in: "4gb", want: 4096},
		"TiB":                          {in: "1t", want: 1024 * 1024},
		"PiB":                          {in: "1p", want: 1024 * 1024 * 1024},
		"KiB rounds down to whole MiB": {in: "1024k", want: 1},
		"KiB rounds up":                {in: "3072k", want: 3},
		"bare byte count rounds up":    {in: "1b", want: 1},
		"exact MiB in bytes":           {in: "1048576b", want: 1},
		"uppercase is accepted":        {in: "2G", want: 2048},
		"surrounding space is trimmed": {in: " 1g ", want: 1024},
		"zero":                         {in: "0", want: 0},
		"empty":                        {in: "", wantErr: true},
		"only a unit":                  {in: "b", wantErr: true},
		"unknown unit":                 {in: "5x", wantErr: true},
		"not a number":                 {in: "abc", wantErr: true},
		"negative":                     {in: "-1g", wantErr: true},
	}

	for name, tc := range cases {
		t.Run(name, func(t *testing.T) {
			got, err := parseSparkMemoryMiB(tc.in)
			if tc.wantErr {
				if err == nil {
					t.Fatalf("parseSparkMemoryMiB(%q) = %d, want error", tc.in, got)
				}
				return
			}
			if err != nil {
				t.Fatalf("parseSparkMemoryMiB(%q) returned unexpected error: %v", tc.in, err)
			}
			if got != tc.want {
				t.Errorf("parseSparkMemoryMiB(%q) = %d, want %d", tc.in, got, tc.want)
			}
		})
	}
}

func TestTotalMemoryBytes(t *testing.T) {
	cases := map[string]struct {
		spec    sparkv1.ApplicationSpec
		role    sparkRole
		wantMiB int64
		wantErr bool
	}{
		// 1g default base, and 0.1*1024=102 is below the 384MiB floor.
		"defaults apply the overhead floor": {
			role: roleExecutor, wantMiB: 1024 + 384,
		},
		// 0.1*4096=410 clears the floor, so the factor decides.
		"factor beats the floor above 3840MiB": {
			spec:    sparkv1.ApplicationSpec{SparkConf: map[string]string{"spark.executor.memory": "4g"}},
			role:    roleExecutor,
			wantMiB: 4096 + 410,
		},
		"explicit overhead wins over the factor": {
			spec: sparkv1.ApplicationSpec{SparkConf: map[string]string{
				"spark.executor.memory":         "4g",
				"spark.executor.memoryOverhead": "1g",
			}},
			role:    roleExecutor,
			wantMiB: 4096 + 1024,
		},
		"explicit factor is honoured": {
			spec: sparkv1.ApplicationSpec{SparkConf: map[string]string{
				"spark.executor.memory":               "4g",
				"spark.executor.memoryOverheadFactor": "0.25",
			}},
			role:    roleExecutor,
			wantMiB: 4096 + 1024,
		},
		"pyFiles raises the default factor to 0.4": {
			spec: sparkv1.ApplicationSpec{
				PyFiles:   "local:///opt/app.py",
				SparkConf: map[string]string{"spark.executor.memory": "4g"},
			},
			role:    roleExecutor,
			wantMiB: 4096 + 1638,
		},
		"resource type overrides the file based inference": {
			spec: sparkv1.ApplicationSpec{
				PyFiles: "local:///opt/app.py",
				SparkConf: map[string]string{
					"spark.executor.memory":          "4g",
					"spark.kubernetes.resource.type": "java",
				},
			},
			role:    roleExecutor,
			wantMiB: 4096 + 410,
		},
		"pyspark memory is added on executors": {
			spec: sparkv1.ApplicationSpec{SparkConf: map[string]string{
				"spark.executor.pyspark.memory": "512m",
			}},
			role:    roleExecutor,
			wantMiB: 1024 + 384 + 512,
		},
		"pyspark memory is not added on the driver": {
			spec: sparkv1.ApplicationSpec{SparkConf: map[string]string{
				"spark.executor.pyspark.memory": "512m",
			}},
			role:    roleDriver,
			wantMiB: 1024 + 384,
		},
		"off heap is added only when enabled": {
			spec: sparkv1.ApplicationSpec{SparkConf: map[string]string{
				"spark.memory.offHeap.enabled": "true",
				"spark.memory.offHeap.size":    "1g",
			}},
			role:    roleExecutor,
			wantMiB: 1024 + 384 + 1024,
		},
		"off heap size is ignored while disabled": {
			spec: sparkv1.ApplicationSpec{SparkConf: map[string]string{
				"spark.memory.offHeap.enabled": "false",
				"spark.memory.offHeap.size":    "1g",
			}},
			role:    roleExecutor,
			wantMiB: 1024 + 384,
		},
		"driver memory is read from the driver key": {
			spec:    sparkv1.ApplicationSpec{SparkConf: map[string]string{"spark.driver.memory": "2g"}},
			role:    roleDriver,
			wantMiB: 2048 + 384,
		},
		"unparseable memory is an error": {
			spec:    sparkv1.ApplicationSpec{SparkConf: map[string]string{"spark.executor.memory": "lots"}},
			role:    roleExecutor,
			wantErr: true,
		},
	}

	for name, tc := range cases {
		t.Run(name, func(t *testing.T) {
			got, err := wrap(tc.spec).totalMemoryBytes(tc.role)
			if tc.wantErr {
				if err == nil {
					t.Fatalf("totalMemoryBytes() = %d, want error", got)
				}
				return
			}
			if err != nil {
				t.Fatalf("totalMemoryBytes() returned unexpected error: %v", err)
			}
			if want := tc.wantMiB * mib; got != want {
				t.Errorf("totalMemoryBytes() = %d bytes (%dMiB), want %d bytes (%dMiB)",
					got, got/mib, want, tc.wantMiB)
			}
		})
	}
}

func TestCPURequestAndLimit(t *testing.T) {
	cases := map[string]struct {
		spec        sparkv1.ApplicationSpec
		role        sparkRole
		wantRequest string
		wantLimit   string // empty means no limit expected
		wantErr     bool
	}{
		"defaults to one core": {
			role: roleExecutor, wantRequest: "1",
		},
		"cores sets the request": {
			spec:        sparkv1.ApplicationSpec{SparkConf: map[string]string{"spark.executor.cores": "4"}},
			role:        roleExecutor,
			wantRequest: "4",
		},
		"kubernetes request override wins over cores": {
			spec: sparkv1.ApplicationSpec{SparkConf: map[string]string{
				"spark.executor.cores":                    "4",
				"spark.kubernetes.executor.request.cores": "3500m",
			}},
			role:        roleExecutor,
			wantRequest: "3500m",
		},
		"limit is set only when configured": {
			spec: sparkv1.ApplicationSpec{SparkConf: map[string]string{
				"spark.kubernetes.executor.limit.cores": "4",
			}},
			role:        roleExecutor,
			wantRequest: "1",
			wantLimit:   "4",
		},
		"driver reads the driver keys": {
			spec:        sparkv1.ApplicationSpec{SparkConf: map[string]string{"spark.driver.cores": "2"}},
			role:        roleDriver,
			wantRequest: "2",
		},
		"non integer cores is an error": {
			spec:    sparkv1.ApplicationSpec{SparkConf: map[string]string{"spark.executor.cores": "1.5"}},
			role:    roleExecutor,
			wantErr: true,
		},
	}

	for name, tc := range cases {
		t.Run(name, func(t *testing.T) {
			job := wrap(tc.spec)
			gotRequest, err := job.cpuRequest(tc.role)
			if tc.wantErr {
				if err == nil {
					t.Fatalf("cpuRequest() = %s, want error", gotRequest.String())
				}
				return
			}
			if err != nil {
				t.Fatalf("cpuRequest() returned unexpected error: %v", err)
			}
			if want := resource.MustParse(tc.wantRequest); gotRequest.Cmp(want) != 0 {
				t.Errorf("cpuRequest() = %s, want %s", gotRequest.String(), want.String())
			}

			gotLimit, err := job.cpuLimit(tc.role)
			if err != nil {
				t.Fatalf("cpuLimit() returned unexpected error: %v", err)
			}
			if tc.wantLimit == "" {
				if gotLimit != nil {
					t.Errorf("cpuLimit() = %s, want nil", gotLimit.String())
				}
				return
			}
			if gotLimit == nil {
				t.Fatalf("cpuLimit() = nil, want %s", tc.wantLimit)
			}
			if want := resource.MustParse(tc.wantLimit); gotLimit.Cmp(want) != 0 {
				t.Errorf("cpuLimit() = %s, want %s", gotLimit.String(), want.String())
			}
		})
	}
}

func TestExecutorCount(t *testing.T) {
	cases := map[string]struct {
		spec    sparkv1.ApplicationSpec
		want    int32
		wantErr bool
	}{
		"falls back to the Spark default": {
			want: defaultExecutorInstances,
		},
		"spark.executor.instances decides": {
			spec: sparkv1.ApplicationSpec{SparkConf: map[string]string{"spark.executor.instances": "5"}},
			want: 5,
		},
		"instanceConfig is used only when instances is unset": {
			spec: sparkv1.ApplicationSpec{ApplicationTolerations: &sparkv1.ApplicationTolerations{
				InstanceConfig: &sparkv1.ExecutorInstanceConfig{InitExecutors: 3},
			}},
			want: 3,
		},
		"spark.executor.instances beats instanceConfig": {
			spec: sparkv1.ApplicationSpec{
				SparkConf: map[string]string{"spark.executor.instances": "10"},
				ApplicationTolerations: &sparkv1.ApplicationTolerations{
					InstanceConfig: &sparkv1.ExecutorInstanceConfig{InitExecutors: 3},
				},
			},
			want: 10,
		},
		"zero initExecutors does not shadow the default": {
			spec: sparkv1.ApplicationSpec{ApplicationTolerations: &sparkv1.ApplicationTolerations{
				InstanceConfig: &sparkv1.ExecutorInstanceConfig{InitExecutors: 0},
			}},
			want: defaultExecutorInstances,
		},
		"dynamic allocation initialExecutors wins": {
			spec: sparkv1.ApplicationSpec{SparkConf: map[string]string{
				"spark.dynamicAllocation.enabled":          "true",
				"spark.dynamicAllocation.initialExecutors": "7",
				"spark.executor.instances":                 "2",
			}},
			want: 7,
		},
		"dynamic allocation falls back to minExecutors": {
			spec: sparkv1.ApplicationSpec{SparkConf: map[string]string{
				"spark.dynamicAllocation.enabled":      "true",
				"spark.dynamicAllocation.minExecutors": "4",
			}},
			want: 4,
		},
		"non numeric instances is an error": {
			spec:    sparkv1.ApplicationSpec{SparkConf: map[string]string{"spark.executor.instances": "many"}},
			wantErr: true,
		},
	}

	for name, tc := range cases {
		t.Run(name, func(t *testing.T) {
			got, err := wrap(tc.spec).executorCount()
			if tc.wantErr {
				if err == nil {
					t.Fatalf("executorCount() = %d, want error", got)
				}
				return
			}
			if err != nil {
				t.Fatalf("executorCount() returned unexpected error: %v", err)
			}
			if got != tc.want {
				t.Errorf("executorCount() = %d, want %d", got, tc.want)
			}
		})
	}
}

func TestBuildPodTemplateSpec(t *testing.T) {
	cases := map[string]struct {
		spec          sparkv1.ApplicationSpec
		role          sparkRole
		wantContainer string
		wantCPU       string
		wantMemMiB    int64
		wantOtherKept bool
	}{
		"appends the Spark container when the template has none": {
			role:          roleExecutor,
			wantContainer: defaultExecutorContainerName,
			wantCPU:       "1",
			wantMemMiB:    1024 + 384,
		},
		"overlays resources onto the named container": {
			spec: sparkv1.ApplicationSpec{
				SparkConf: map[string]string{"spark.executor.cores": "2"},
				ExecutorSpec: &sparkv1.BaseApplicationTemplateSpec{
					PodTemplateSpec: &corev1.PodTemplateSpec{Spec: corev1.PodSpec{
						Containers: []corev1.Container{{Name: defaultExecutorContainerName}},
					}},
				},
			},
			role:          roleExecutor,
			wantContainer: defaultExecutorContainerName,
			wantCPU:       "2",
			wantMemMiB:    1024 + 384,
		},
		"overlays onto the first container when unnamed": {
			spec: sparkv1.ApplicationSpec{
				ExecutorSpec: &sparkv1.BaseApplicationTemplateSpec{
					PodTemplateSpec: &corev1.PodTemplateSpec{Spec: corev1.PodSpec{
						Containers: []corev1.Container{{Name: "custom"}},
					}},
				},
			},
			role:          roleExecutor,
			wantContainer: "custom",
			wantCPU:       "1",
			wantMemMiB:    1024 + 384,
		},
		"honours podTemplateContainerName": {
			spec: sparkv1.ApplicationSpec{
				SparkConf: map[string]string{
					"spark.kubernetes.executor.podTemplateContainerName": "spark",
				},
				ExecutorSpec: &sparkv1.BaseApplicationTemplateSpec{
					PodTemplateSpec: &corev1.PodTemplateSpec{Spec: corev1.PodSpec{
						Containers: []corev1.Container{{Name: "sidecar"}, {Name: "spark"}},
					}},
				},
			},
			role:          roleExecutor,
			wantContainer: "spark",
			wantCPU:       "1",
			wantMemMiB:    1024 + 384,
			wantOtherKept: true,
		},
		"driver template is built from the driver spec": {
			spec: sparkv1.ApplicationSpec{
				SparkConf: map[string]string{"spark.driver.memory": "2g"},
			},
			role:          roleDriver,
			wantContainer: defaultDriverContainerName,
			wantCPU:       "1",
			wantMemMiB:    2048 + 384,
		},
	}

	for name, tc := range cases {
		t.Run(name, func(t *testing.T) {
			got, err := wrap(tc.spec).buildPodTemplateSpec(tc.role)
			if err != nil {
				t.Fatalf("buildPodTemplateSpec() returned unexpected error: %v", err)
			}

			idx := -1
			for i, c := range got.Spec.Containers {
				if c.Name == tc.wantContainer {
					idx = i
					break
				}
			}
			if idx < 0 {
				t.Fatalf("container %q not found in %v", tc.wantContainer, got.Spec.Containers)
			}
			if tc.wantOtherKept && len(got.Spec.Containers) < 2 {
				t.Errorf("expected sibling containers to be preserved, got %v", got.Spec.Containers)
			}

			res := got.Spec.Containers[idx].Resources
			wantCPU := resource.MustParse(tc.wantCPU)
			if cpu := res.Requests[corev1.ResourceCPU]; cpu.Cmp(wantCPU) != 0 {
				t.Errorf("cpu request = %s, want %s", cpu.String(), wantCPU.String())
			}
			wantMem := *resource.NewQuantity(tc.wantMemMiB*mib, resource.BinarySI)
			if mem := res.Requests[corev1.ResourceMemory]; mem.Cmp(wantMem) != 0 {
				t.Errorf("memory request = %s, want %s", mem.String(), wantMem.String())
			}
			// Spark sets the memory limit equal to the request.
			if mem := res.Limits[corev1.ResourceMemory]; mem.Cmp(wantMem) != 0 {
				t.Errorf("memory limit = %s, want %s", mem.String(), wantMem.String())
			}
		})
	}
}

func TestBuildPodTemplateSpecDoesNotMutateSpec(t *testing.T) {
	template := &corev1.PodTemplateSpec{Spec: corev1.PodSpec{
		Containers: []corev1.Container{{Name: defaultExecutorContainerName}},
	}}
	job := wrap(sparkv1.ApplicationSpec{
		ExecutorSpec: &sparkv1.BaseApplicationTemplateSpec{PodTemplateSpec: template},
	})
	before := template.DeepCopy()

	if _, err := job.buildPodTemplateSpec(roleExecutor); err != nil {
		t.Fatalf("buildPodTemplateSpec() returned unexpected error: %v", err)
	}

	if diff := cmp.Diff(before, template); diff != "" {
		t.Errorf("buildPodTemplateSpec() mutated the application spec (-want +got):\n%s", diff)
	}
}
