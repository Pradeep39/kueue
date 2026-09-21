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
	"testing"

	sparkv1beta2 "github.com/kubeflow/spark-operator/v2/api/v1beta2"
	sparkcommon "github.com/kubeflow/spark-operator/v2/pkg/common"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/utils/ptr"
	"sigs.k8s.io/controller-runtime/pkg/client"

	kueue "sigs.k8s.io/kueue/apis/kueue/v1beta2"
	utiltesting "sigs.k8s.io/kueue/pkg/util/testing"
	utiltestingapi "sigs.k8s.io/kueue/pkg/util/testing/v1beta2"
	sparkapplicationtesting "sigs.k8s.io/kueue/pkg/util/testingjobs/sparkapplication"
)

func executorPod(name string, phase corev1.PodPhase, deleting bool) *corev1.Pod {
	pod := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:      name,
			Namespace: "ns",
			Labels: map[string]string{
				sparkcommon.LabelSparkAppName: "app",
				sparkcommon.LabelSparkRole:    sparkcommon.SparkRoleExecutor,
			},
		},
		Status: corev1.PodStatus{Phase: phase},
	}
	if deleting {
		now := metav1.Now()
		pod.DeletionTimestamp = &now
		pod.Finalizers = []string{"kueue.x-k8s.io/keep-around"}
	}
	return pod
}

func driverPod(containerName string) *corev1.Pod {
	return &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Labels: map[string]string{
				sparkcommon.LabelSparkRole: sparkcommon.SparkRoleDriver,
			},
		},
		Spec: corev1.PodSpec{
			Containers: []corev1.Container{
				{Name: containerName},
			},
		},
	}
}

func TestAddVolumeMount(t *testing.T) {
	tests := map[string]struct {
		pod     *corev1.Pod
		wantErr bool
	}{
		"driver pod with matching container": {
			pod:     driverPod(sparkcommon.SparkDriverContainerName),
			wantErr: false,
		},
		"pod that is neither driver nor executor": {
			pod: &corev1.Pod{
				Spec: corev1.PodSpec{
					Containers: []corev1.Container{{Name: "not-spark"}},
				},
			},
			wantErr: true,
		},
	}

	for name, tc := range tests {
		t.Run(name, func(t *testing.T) {
			err := addVolumeMount(tc.pod, corev1.VolumeMount{Name: "data", MountPath: "/data"})
			if (err != nil) != tc.wantErr {
				t.Fatalf("addVolumeMount() error = %v, wantErr %v", err, tc.wantErr)
			}
		})
	}
}

func TestAddVolumes(t *testing.T) {
	tests := map[string]struct {
		pod             *corev1.Pod
		app             *sparkv1beta2.SparkApplication
		wantErr         bool
		wantVolumes     []string
		wantVolumeMount []string
	}{
		"adds a volume and mount that match by name": {
			pod: driverPod(sparkcommon.SparkDriverContainerName),
			app: &sparkv1beta2.SparkApplication{
				Spec: sparkv1beta2.SparkApplicationSpec{
					Volumes: []corev1.Volume{{Name: "data"}},
					Driver: sparkv1beta2.DriverSpec{
						SparkPodSpec: sparkv1beta2.SparkPodSpec{
							VolumeMounts: []corev1.VolumeMount{{Name: "data", MountPath: "/data"}},
						},
					},
				},
			},
			wantVolumes:     []string{"data"},
			wantVolumeMount: []string{"data"},
		},
		"skips a mount with no matching volume declared": {
			pod: driverPod(sparkcommon.SparkDriverContainerName),
			app: &sparkv1beta2.SparkApplication{
				Spec: sparkv1beta2.SparkApplicationSpec{
					Driver: sparkv1beta2.DriverSpec{
						SparkPodSpec: sparkv1beta2.SparkPodSpec{
							VolumeMounts: []corev1.VolumeMount{{Name: "unknown", MountPath: "/data"}},
						},
					},
				},
			},
			wantVolumes:     nil,
			wantVolumeMount: nil,
		},
		"skips localDir volume mounts": {
			pod: driverPod(sparkcommon.SparkDriverContainerName),
			app: &sparkv1beta2.SparkApplication{
				Spec: sparkv1beta2.SparkApplicationSpec{
					Volumes: []corev1.Volume{{Name: sparkcommon.SparkLocalDirVolumePrefix + "0"}},
					Driver: sparkv1beta2.DriverSpec{
						SparkPodSpec: sparkv1beta2.SparkPodSpec{
							VolumeMounts: []corev1.VolumeMount{{Name: sparkcommon.SparkLocalDirVolumePrefix + "0", MountPath: "/tmp"}},
						},
					},
				},
			},
			wantVolumes:     nil,
			wantVolumeMount: nil,
		},
		"adds the volume once for two mounts referencing the same volume": {
			pod: driverPod(sparkcommon.SparkDriverContainerName),
			app: &sparkv1beta2.SparkApplication{
				Spec: sparkv1beta2.SparkApplicationSpec{
					Volumes: []corev1.Volume{{Name: "data"}},
					Driver: sparkv1beta2.DriverSpec{
						SparkPodSpec: sparkv1beta2.SparkPodSpec{
							VolumeMounts: []corev1.VolumeMount{
								{Name: "data", MountPath: "/data-a"},
								{Name: "data", MountPath: "/data-b"},
							},
						},
					},
				},
			},
			wantVolumes:     []string{"data"},
			wantVolumeMount: []string{"data", "data"},
		},
	}

	for name, tc := range tests {
		t.Run(name, func(t *testing.T) {
			err := addVolumes(tc.pod, tc.app)
			if (err != nil) != tc.wantErr {
				t.Fatalf("addVolumes() error = %v, wantErr %v", err, tc.wantErr)
			}

			var gotVolumes []string
			for _, v := range tc.pod.Spec.Volumes {
				gotVolumes = append(gotVolumes, v.Name)
			}
			if len(gotVolumes) != len(tc.wantVolumes) {
				t.Fatalf("pod.Spec.Volumes = %v, want %v", gotVolumes, tc.wantVolumes)
			}
			for i, name := range tc.wantVolumes {
				if gotVolumes[i] != name {
					t.Errorf("pod.Spec.Volumes[%d] = %v, want %v", i, gotVolumes[i], name)
				}
			}

			var gotMounts []string
			for _, m := range tc.pod.Spec.Containers[0].VolumeMounts {
				gotMounts = append(gotMounts, m.Name)
			}
			if len(gotMounts) != len(tc.wantVolumeMount) {
				t.Fatalf("container.VolumeMounts = %v, want %v", gotMounts, tc.wantVolumeMount)
			}
			for i, name := range tc.wantVolumeMount {
				if gotMounts[i] != name {
					t.Errorf("container.VolumeMounts[%d] = %v, want %v", i, gotMounts[i], name)
				}
			}
		})
	}
}

func executorAppWithTemplateMemory(request, limit *string, memoryField *string) *sparkv1beta2.SparkApplication {
	tmpl := &corev1.PodTemplateSpec{
		Spec: corev1.PodSpec{
			Containers: []corev1.Container{{Name: sparkcommon.SparkExecutorContainerName}},
		},
	}
	if request != nil {
		tmpl.Spec.Containers[0].Resources.Requests = corev1.ResourceList{
			corev1.ResourceMemory: resource.MustParse(*request),
		}
	}
	if limit != nil {
		tmpl.Spec.Containers[0].Resources.Limits = corev1.ResourceList{
			corev1.ResourceMemory: resource.MustParse(*limit),
		}
	}
	return &sparkv1beta2.SparkApplication{
		Spec: sparkv1beta2.SparkApplicationSpec{
			Executor: sparkv1beta2.ExecutorSpec{
				SparkPodSpec: sparkv1beta2.SparkPodSpec{
					Memory:      memoryField,
					MemoryLimit: memoryField,
					Template:    tmpl,
				},
			},
		},
	}
}

// Spark overwrites the Spark container's memory with base+overhead when it builds the pod
// from spec.{driver,executor}.template, so a value declared there must not be charged.
func TestAddMemoryIgnoresThePodTemplate(t *testing.T) {
	tests := map[string]struct {
		app       *sparkv1beta2.SparkApplication
		wantReq   string
		wantLimit string
	}{
		// 512Mi of heap plus the 384MiB floor, regardless of the template's 2Gi.
		"template values are overwritten by Spark's arithmetic": {
			app:       executorAppWithTemplateMemory(ptr.To("2Gi"), ptr.To("2Gi"), ptr.To("512m")),
			wantReq:   "896Mi",
			wantLimit: "896Mi",
		},
		"no template values behaves the same": {
			app:       executorAppWithTemplateMemory(nil, nil, ptr.To("512m")),
			wantReq:   "896Mi",
			wantLimit: "896Mi",
		},
	}

	for name, tc := range tests {
		t.Run(name, func(t *testing.T) {
			pod := executorPod("e", corev1.PodRunning, false)
			pod.Spec.Containers = []corev1.Container{{Name: sparkcommon.Spark3DefaultExecutorContainerName}}

			if err := addMemoryRequests(pod, tc.app); err != nil {
				t.Fatalf("addMemoryRequests() returned an unexpected error: %v", err)
			}
			if err := addMemoryLimit(pod, tc.app); err != nil {
				t.Fatalf("addMemoryLimit() returned an unexpected error: %v", err)
			}

			gotReq := pod.Spec.Containers[0].Resources.Requests[corev1.ResourceMemory]
			if want := resource.MustParse(tc.wantReq); gotReq.Cmp(want) != 0 {
				t.Errorf("memory request = %s, want %s", gotReq.String(), want.String())
			}
			gotLimit := pod.Spec.Containers[0].Resources.Limits[corev1.ResourceMemory]
			if want := resource.MustParse(tc.wantLimit); gotLimit.Cmp(want) != 0 {
				t.Errorf("memory limit = %s, want %s", gotLimit.String(), want.String())
			}
		})
	}
}

// TestDynamicAllocationEnabled pins the OR semantics. The other Kubeflow properties here
// prefer their structured field over the sparkConf equivalent; this one cannot, because
// DynamicAllocation.Enabled is a non-pointer bool and an explicit false is indistinguishable
// from an omitted one. Honouring either surface over-reads enablement on purpose: see
// sparkapplication-sparkconf-executor-instances-design.md section 5 for why the alternative
// under-reserves.
func TestDynamicAllocationEnabled(t *testing.T) {
	cases := map[string]struct {
		app  *sparkv1beta2.SparkApplication
		want bool
	}{
		"neither surface configured": {
			app:  &sparkv1beta2.SparkApplication{},
			want: false,
		},
		"structured field enables it": {
			app: &sparkv1beta2.SparkApplication{Spec: sparkv1beta2.SparkApplicationSpec{
				DynamicAllocation: &sparkv1beta2.DynamicAllocation{Enabled: true},
			}},
			want: true,
		},
		"sparkConf enables it": {
			app: &sparkv1beta2.SparkApplication{Spec: sparkv1beta2.SparkApplicationSpec{
				SparkConf: map[string]string{"spark.dynamicAllocation.enabled": "true"},
			}},
			want: true,
		},
		// The load-bearing case: bounds declared structurally, enablement through sparkConf.
		// Letting the structured block win would read this as a static application.
		"bounds structured, enablement via sparkConf": {
			app: &sparkv1beta2.SparkApplication{Spec: sparkv1beta2.SparkApplicationSpec{
				DynamicAllocation: &sparkv1beta2.DynamicAllocation{MinExecutors: ptr.To[int32](3)},
				SparkConf:         map[string]string{"spark.dynamicAllocation.enabled": "true"},
			}},
			want: true,
		},
		"an unparseable sparkConf value is not an enablement": {
			app: &sparkv1beta2.SparkApplication{Spec: sparkv1beta2.SparkApplicationSpec{
				SparkConf: map[string]string{"spark.dynamicAllocation.enabled": "yes please"},
			}},
			want: false,
		},
	}

	for name, tc := range cases {
		t.Run(name, func(t *testing.T) {
			if got := fromObject(tc.app).dynamicAllocationEnabled(); got != tc.want {
				t.Errorf("dynamicAllocationEnabled() = %v, want %v", got, tc.want)
			}
		})
	}
}

func TestTotalMemoryBytes(t *testing.T) {
	mi := func(n int64) int64 { return n * 1024 * 1024 }

	cases := map[string]struct {
		spec sparkv1beta2.SparkApplicationSpec
		role string
		want int64
	}{
		// 0.1 x 512Mi is 51Mi, below Spark's floor, so the floor applies.
		"executor heap plus the 384MiB floor": {
			spec: sparkv1beta2.SparkApplicationSpec{Executor: sparkv1beta2.ExecutorSpec{
				SparkPodSpec: sparkv1beta2.SparkPodSpec{Memory: ptr.To("512m")}}},
			role: "executor",
			want: mi(896),
		},
		// 0.1 x 8192Mi is 819Mi, above the floor, so the factor applies.
		"executor heap plus the factored overhead": {
			spec: sparkv1beta2.SparkApplicationSpec{Executor: sparkv1beta2.ExecutorSpec{
				SparkPodSpec: sparkv1beta2.SparkPodSpec{Memory: ptr.To("8g")}}},
			role: "executor",
			want: mi(8192 + 819),
		},
		"an explicit memoryOverhead replaces the factor": {
			spec: sparkv1beta2.SparkApplicationSpec{Executor: sparkv1beta2.ExecutorSpec{
				SparkPodSpec: sparkv1beta2.SparkPodSpec{
					Memory:         ptr.To("512m"),
					MemoryOverhead: ptr.To("1g"),
				}}},
			role: "executor",
			want: mi(512 + 1024),
		},
		"a bare memoryOverhead is read as MiB": {
			spec: sparkv1beta2.SparkApplicationSpec{Executor: sparkv1beta2.ExecutorSpec{
				SparkPodSpec: sparkv1beta2.SparkPodSpec{
					Memory:         ptr.To("512m"),
					MemoryOverhead: ptr.To("512"),
				}}},
			role: "executor",
			want: mi(1024),
		},
		"memoryOverheadFactor overrides the default": {
			spec: sparkv1beta2.SparkApplicationSpec{
				MemoryOverheadFactor: ptr.To("0.5"),
				Executor: sparkv1beta2.ExecutorSpec{
					SparkPodSpec: sparkv1beta2.SparkPodSpec{Memory: ptr.To("8g")}},
			},
			role: "executor",
			want: mi(8192 + 4096),
		},
		// Python and R use Spark's non-JVM factor of 0.4 when none is set explicitly.
		// 0.4 x 8192 truncates to 3276, matching Spark's (factor * memoryMiB).toInt.
		"a Python application uses the non-JVM factor": {
			spec: sparkv1beta2.SparkApplicationSpec{
				Type: sparkv1beta2.SparkApplicationTypePython,
				Executor: sparkv1beta2.ExecutorSpec{
					SparkPodSpec: sparkv1beta2.SparkPodSpec{Memory: ptr.To("8g")}},
			},
			role: "executor",
			want: mi(8192 + 3276),
		},
		"sparkConf supplies the heap when the field is unset": {
			spec: sparkv1beta2.SparkApplicationSpec{
				SparkConf: map[string]string{"spark.executor.memory": "512m"},
			},
			role: "executor",
			want: mi(896),
		},
		"the structured field wins over sparkConf": {
			spec: sparkv1beta2.SparkApplicationSpec{
				SparkConf: map[string]string{"spark.executor.memory": "8g"},
				Executor: sparkv1beta2.ExecutorSpec{
					SparkPodSpec: sparkv1beta2.SparkPodSpec{Memory: ptr.To("512m")}},
			},
			role: "executor",
			want: mi(896),
		},
		// Spark 4 lets the floor itself be configured; the CRD has no field for it, so
		// sparkConf is the only surface.
		"spark.executor.minMemoryOverhead raises the floor": {
			spec: sparkv1beta2.SparkApplicationSpec{
				SparkConf: map[string]string{"spark.executor.minMemoryOverhead": "1g"},
				Executor: sparkv1beta2.ExecutorSpec{
					SparkPodSpec: sparkv1beta2.SparkPodSpec{Memory: ptr.To("512m")}},
			},
			role: "executor",
			want: mi(512 + 1024),
		},
		"minMemoryOverhead is ignored when the overhead is explicit": {
			spec: sparkv1beta2.SparkApplicationSpec{
				SparkConf: map[string]string{"spark.executor.minMemoryOverhead": "1g"},
				Executor: sparkv1beta2.ExecutorSpec{
					SparkPodSpec: sparkv1beta2.SparkPodSpec{
						Memory:         ptr.To("512m"),
						MemoryOverhead: ptr.To("128m"),
					}},
			},
			role: "executor",
			want: mi(512 + 128),
		},
		"pyspark memory is added on executors": {
			spec: sparkv1beta2.SparkApplicationSpec{
				SparkConf: map[string]string{"spark.executor.pyspark.memory": "256m"},
				Executor: sparkv1beta2.ExecutorSpec{
					SparkPodSpec: sparkv1beta2.SparkPodSpec{Memory: ptr.To("512m")}},
			},
			role: "executor",
			want: mi(896 + 256),
		},
		"pyspark memory is not added on the driver": {
			spec: sparkv1beta2.SparkApplicationSpec{
				SparkConf: map[string]string{"spark.executor.pyspark.memory": "256m"},
				Driver: sparkv1beta2.DriverSpec{
					SparkPodSpec: sparkv1beta2.SparkPodSpec{Memory: ptr.To("512m")}},
			},
			role: "driver",
			want: mi(896),
		},
		"off-heap counts only when the allocator is enabled": {
			spec: sparkv1beta2.SparkApplicationSpec{
				SparkConf: map[string]string{
					"spark.memory.offHeap.enabled": "true",
					"spark.memory.offHeap.size":    "1g",
				},
				Executor: sparkv1beta2.ExecutorSpec{
					SparkPodSpec: sparkv1beta2.SparkPodSpec{Memory: ptr.To("512m")}},
			},
			role: "executor",
			want: mi(896 + 1024),
		},
		"off-heap size is ignored while disabled": {
			spec: sparkv1beta2.SparkApplicationSpec{
				SparkConf: map[string]string{"spark.memory.offHeap.size": "1g"},
				Executor: sparkv1beta2.ExecutorSpec{
					SparkPodSpec: sparkv1beta2.SparkPodSpec{Memory: ptr.To("512m")}},
			},
			role: "executor",
			want: mi(896),
		},
		"nothing configured falls back to Spark's 1g default": {
			role: "executor",
			want: mi(1024 + 384),
		},
	}

	for name, tc := range cases {
		t.Run(name, func(t *testing.T) {
			app := fromObject(&sparkv1beta2.SparkApplication{Spec: tc.spec})
			got, err := app.totalMemoryBytes(tc.role)
			if err != nil {
				t.Fatalf("totalMemoryBytes() returned an unexpected error: %v", err)
			}
			if got != tc.want {
				t.Errorf("totalMemoryBytes() = %d (%dMi), want %d (%dMi)",
					got, got/1024/1024, tc.want, tc.want/1024/1024)
			}
		})
	}
}

func TestIsVerifiedLiveExecutor(t *testing.T) {
	tests := map[string]struct {
		phase    corev1.PodPhase
		deleting bool
		want     bool
	}{
		"running":                           {phase: corev1.PodRunning, want: true},
		"pending":                           {phase: corev1.PodPending, want: true},
		"succeeded":                         {phase: corev1.PodSucceeded, want: false},
		"failed":                            {phase: corev1.PodFailed, want: false},
		"terminating but not yet terminal":  {phase: corev1.PodRunning, deleting: true, want: true},
		"terminating and already succeeded": {phase: corev1.PodSucceeded, deleting: true, want: false},
	}

	for name, tc := range tests {
		t.Run(name, func(t *testing.T) {
			pod := executorPod("e", tc.phase, tc.deleting)
			if got := isVerifiedLiveExecutor(pod); got != tc.want {
				t.Errorf("isVerifiedLiveExecutor() = %v, want %v", got, tc.want)
			}
		})
	}
}

func TestLiveExecutorCount(t *testing.T) {
	tests := map[string]struct {
		app       *sparkv1beta2.SparkApplication
		pods      []client.Object
		nilClient bool
		want      int32
		wantErr   bool
	}{
		"dynamic allocation disabled uses the static instances field": {
			app: sparkapplicationtesting.MakeSparkApplication("app", "ns").
				ExecutorInstances(5).Obj(),
			pods: []client.Object{executorPod("e1", corev1.PodRunning, false)},
			want: 5,
		},
		// Spark Operator accepts the executor count through sparkConf as well as the
		// structured field, and maps the structured field onto the same key when submitting.
		// Reading only the structured field sized the executor PodSet at zero, so Kueue
		// charged for the driver alone while the driver created its executors outside quota
		// management. Observed on a real cluster with 15 executors running against a
		// ClusterQueue that had reserved one driver.
		"dynamic allocation disabled reads spark.executor.instances from sparkConf": {
			app: func() *sparkv1beta2.SparkApplication {
				app := sparkapplicationtesting.MakeSparkApplication("app", "ns").Obj()
				app.Spec.Executor.Instances = nil
				app.Spec.SparkConf = map[string]string{"spark.executor.instances": "15"}
				return app
			}(),
			pods: []client.Object{executorPod("e1", corev1.PodRunning, false)},
			want: 15,
		},
		// sparkConf is the fallback, so the structured field wins whenever it is set --
		// in either direction, not just when it is larger.
		"structured field takes precedence over sparkConf when smaller": {
			app: func() *sparkv1beta2.SparkApplication {
				app := sparkapplicationtesting.MakeSparkApplication("app", "ns").
					ExecutorInstances(3).Obj()
				app.Spec.SparkConf = map[string]string{"spark.executor.instances": "15"}
				return app
			}(),
			want: 3,
		},
		"structured field takes precedence over sparkConf when larger": {
			app: func() *sparkv1beta2.SparkApplication {
				app := sparkapplicationtesting.MakeSparkApplication("app", "ns").
					ExecutorInstances(9).Obj()
				app.Spec.SparkConf = map[string]string{"spark.executor.instances": "2"}
				return app
			}(),
			want: 9,
		},
		// Reserving zero would charge nothing while the driver went on to create Spark's
		// own default of two executors.
		"no executor count declared anywhere falls back to Spark's default": {
			app: func() *sparkv1beta2.SparkApplication {
				app := sparkapplicationtesting.MakeSparkApplication("app", "ns").Obj()
				app.Spec.Executor.Instances = nil
				return app
			}(),
			want: defaultExecutorInstances,
		},
		"malformed spark.executor.instances is an error, never a silent zero": {
			app: func() *sparkv1beta2.SparkApplication {
				app := sparkapplicationtesting.MakeSparkApplication("app", "ns").Obj()
				app.Spec.Executor.Instances = nil
				app.Spec.SparkConf = map[string]string{"spark.executor.instances": "fifteen"}
				return app
			}(),
			wantErr: true,
		},
		// The Dynamic Allocation path had the same blind spot: the structured field
		// contributed nothing to the max when the count lived only in sparkConf.
		"dynamic allocation falls back to sparkConf instances when the field is unset": {
			app: func() *sparkv1beta2.SparkApplication {
				app := sparkapplicationtesting.MakeSparkApplication("app", "ns").Obj()
				app.Spec.Executor.Instances = nil
				app.Spec.SparkConf = map[string]string{
					"spark.dynamicAllocation.enabled":      "true",
					"spark.executor.instances":             "15",
					"spark.dynamicAllocation.minExecutors": "3",
				}
				return app
			}(),
			want: 15,
		},
		"dynamic allocation enabled via structured spec, no pods yet falls back to minExecutors": {
			app: func() *sparkv1beta2.SparkApplication {
				app := sparkapplicationtesting.MakeSparkApplication("app", "ns").
					DynamicAllocation(&sparkv1beta2.DynamicAllocation{Enabled: true, MinExecutors: ptr.To[int32](2)}).
					Obj()
				app.Spec.Executor.Instances = nil
				return app
			}(),
			want: 2,
		},
		"dynamic allocation enabled via sparkConf, no pods yet falls back to initialExecutors": {
			app: func() *sparkv1beta2.SparkApplication {
				app := sparkapplicationtesting.MakeSparkApplication("app", "ns").Obj()
				app.Spec.Executor.Instances = nil
				app.Spec.SparkConf = map[string]string{
					"spark.dynamicAllocation.enabled":          "true",
					"spark.dynamicAllocation.initialExecutors": "4",
				}
				return app
			}(),
			want: 4,
		},
		"dynamic allocation enabled counts only non-terminal live pods": {
			app: sparkapplicationtesting.MakeSparkApplication("app", "ns").
				DynamicAllocation(&sparkv1beta2.DynamicAllocation{Enabled: true}).
				Obj(),
			pods: []client.Object{
				executorPod("e1", corev1.PodRunning, false),
				executorPod("e2", corev1.PodPending, false),
				executorPod("e3", corev1.PodSucceeded, false),
				executorPod("e4", corev1.PodFailed, false),
				executorPod("e5", corev1.PodRunning, true), // terminating, still live
			},
			want: 3,
		},
		"dynamic allocation enabled, nil client falls back to initial estimate": {
			app: sparkapplicationtesting.MakeSparkApplication("app", "ns").
				ExecutorInstances(3).
				DynamicAllocation(&sparkv1beta2.DynamicAllocation{Enabled: true}).
				Obj(),
			nilClient: true,
			want:      3,
		},
		// A reconcile landing while the driver is still creating its initial executors sees a
		// transient prefix of them. Without the minExecutors floor the derived count is 1, which
		// EnsureWorkloadSlices reads as a scale-down and patches the granted PodSet down to 1 --
		// dismantling the gang it was just admitted with. Observed on a real cluster as an
		// executor PodSet admitted at 3 and patched to 1 seven seconds later.
		"live count below minExecutors is raised to the floor": {
			app: func() *sparkv1beta2.SparkApplication {
				app := sparkapplicationtesting.MakeSparkApplication("app", "ns").Obj()
				app.Spec.Executor.Instances = nil
				app.Spec.SparkConf = map[string]string{
					"spark.dynamicAllocation.enabled":      "true",
					"spark.dynamicAllocation.minExecutors": "3",
					"spark.dynamicAllocation.maxExecutors": "30",
				}
				return app
			}(),
			pods: []client.Object{executorPod("e1", corev1.PodRunning, false)},
			want: 3,
		},
		"live count above maxExecutors is capped at the ceiling": {
			app: sparkapplicationtesting.MakeSparkApplication("app", "ns").
				DynamicAllocation(&sparkv1beta2.DynamicAllocation{
					Enabled:      true,
					MinExecutors: ptr.To[int32](1),
					MaxExecutors: ptr.To[int32](2),
				}).
				Obj(),
			pods: []client.Object{
				executorPod("e1", corev1.PodRunning, false),
				executorPod("e2", corev1.PodPending, false),
				executorPod("e3", corev1.PodPending, false),
				executorPod("e4", corev1.PodPending, false),
			},
			want: 2,
		},
		"live count within the bounds is reported as observed": {
			app: sparkapplicationtesting.MakeSparkApplication("app", "ns").
				DynamicAllocation(&sparkv1beta2.DynamicAllocation{
					Enabled:      true,
					MinExecutors: ptr.To[int32](1),
					MaxExecutors: ptr.To[int32](10),
				}).
				Obj(),
			pods: []client.Object{
				executorPod("e1", corev1.PodRunning, false),
				executorPod("e2", corev1.PodRunning, false),
			},
			want: 2,
		},
		// maxExecutors is applied last, so an inverted configuration can never inflate the
		// count above the declared maximum.
		"minExecutors above maxExecutors never exceeds the maximum": {
			app: sparkapplicationtesting.MakeSparkApplication("app", "ns").
				DynamicAllocation(&sparkv1beta2.DynamicAllocation{
					Enabled:      true,
					MinExecutors: ptr.To[int32](8),
					MaxExecutors: ptr.To[int32](2),
				}).
				Obj(),
			pods: []client.Object{executorPod("e1", corev1.PodRunning, false)},
			want: 2,
		},
		"instances below minExecutors reserves the Dynamic Allocation floor": {
			app: func() *sparkv1beta2.SparkApplication {
				app := sparkapplicationtesting.MakeSparkApplication("app", "ns").
					ExecutorInstances(1).
					DynamicAllocation(&sparkv1beta2.DynamicAllocation{
						Enabled:      true,
						MinExecutors: ptr.To[int32](3),
					}).
					Obj()
				return app
			}(),
			want: 3,
		},
		"instances above minExecutors is used as the initial count": {
			app: sparkapplicationtesting.MakeSparkApplication("app", "ns").
				ExecutorInstances(5).
				DynamicAllocation(&sparkv1beta2.DynamicAllocation{
					Enabled:      true,
					MinExecutors: ptr.To[int32](3),
				}).
				Obj(),
			want: 5,
		},
		// Spark's Utils.getDynamicAllocationInitialExecutors takes the largest of
		// minExecutors, initialExecutors and spark.executor.instances, so a smaller
		// instances value must not shrink the reservation below what Spark will start.
		"initial count is the largest of instances, initialExecutors and minExecutors": {
			app: sparkapplicationtesting.MakeSparkApplication("app", "ns").
				ExecutorInstances(2).
				DynamicAllocation(&sparkv1beta2.DynamicAllocation{
					Enabled:          true,
					InitialExecutors: ptr.To[int32](7),
				}).
				Obj(),
			want: 7,
		},
		"instances larger than the dynamic allocation counts still wins the max": {
			app: sparkapplicationtesting.MakeSparkApplication("app", "ns").
				ExecutorInstances(9).
				DynamicAllocation(&sparkv1beta2.DynamicAllocation{
					Enabled:          true,
					InitialExecutors: ptr.To[int32](3),
					MinExecutors:     ptr.To[int32](2),
				}).
				Obj(),
			want: 9,
		},
		"no Dynamic Allocation bounds set leaves the observed count untouched": {
			app: sparkapplicationtesting.MakeSparkApplication("app", "ns").
				DynamicAllocation(&sparkv1beta2.DynamicAllocation{Enabled: true}).
				Obj(),
			pods: []client.Object{
				executorPod("e1", corev1.PodRunning, false),
				executorPod("e2", corev1.PodRunning, false),
			},
			want: 2,
		},
	}

	for name, tc := range tests {
		t.Run(name, func(t *testing.T) {
			app := fromObject(tc.app)

			var c client.Client
			if !tc.nilClient {
				c = utiltesting.NewClientBuilder().WithObjects(tc.pods...).Build()
			}

			got, err := app.liveExecutorCount(t.Context(), c)
			if tc.wantErr {
				if err == nil {
					t.Fatalf("liveExecutorCount() = %d, want an error", got)
				}
				return
			}
			if err != nil {
				t.Fatalf("liveExecutorCount() returned an unexpected error: %v", err)
			}
			if got != tc.want {
				t.Errorf("liveExecutorCount() = %d, want %d", got, tc.want)
			}
		})
	}
}

func TestLiveExecutorCountCachedWithinReconcile(t *testing.T) {
	app := fromObject(sparkapplicationtesting.MakeSparkApplication("app", "ns").
		DynamicAllocation(&sparkv1beta2.DynamicAllocation{Enabled: true}).
		Obj())

	c := utiltesting.NewClientBuilder().WithObjects(
		executorPod("e1", corev1.PodRunning, false),
		executorPod("e2", corev1.PodRunning, false),
	).Build()

	first, err := app.liveExecutorCount(t.Context(), c)
	if err != nil {
		t.Fatalf("liveExecutorCount() returned an unexpected error: %v", err)
	}
	if first != 2 {
		t.Fatalf("liveExecutorCount() = %d, want 2", first)
	}

	// Simulate Dynamic Allocation adding another executor Pod mid-reconcile: a
	// second call against the same *SparkApplication instance must still return
	// the cached value, not a freshly-observed (and inconsistent) count.
	if err := c.Create(t.Context(), executorPod("e3", corev1.PodRunning, false)); err != nil {
		t.Fatalf("failed to create pod: %v", err)
	}

	second, err := app.liveExecutorCount(t.Context(), c)
	if err != nil {
		t.Fatalf("liveExecutorCount() returned an unexpected error: %v", err)
	}
	if second != first {
		t.Errorf("liveExecutorCount() second call = %d, want cached value %d", second, first)
	}
}

func TestWorkloadSequenceNumber(t *testing.T) {
	app := sparkapplicationtesting.MakeSparkApplication("app", "ns").Obj()
	app.UID = "app-uid"

	newWorkload := func(name string, finished bool) *kueue.Workload {
		w := utiltestingapi.MakeWorkload(name, "ns").
			ControllerReference(gvk, app.Name, string(app.UID))
		if finished {
			w = w.Condition(metav1.Condition{
				Type:   kueue.WorkloadFinished,
				Status: metav1.ConditionTrue,
				Reason: "Succeeded",
			})
		}
		return w.Obj()
	}

	tests := map[string]struct {
		workloads []client.Object
		want      int32
	}{
		"no workloads yet": {
			want: 0,
		},
		"one not-finished workload": {
			workloads: []client.Object{newWorkload("wl1", false)},
			want:      1,
		},
		"finished workloads still count, since their names are never reused": {
			workloads: []client.Object{
				newWorkload("wl1", true),
				newWorkload("wl2", true),
				newWorkload("wl3", false),
			},
			want: 3,
		},
	}

	for name, tc := range tests {
		t.Run(name, func(t *testing.T) {
			clientBuilder := utiltesting.NewClientBuilder().WithObjects(tc.workloads...)
			c := clientBuilder.Build()
			idx := utiltesting.AsIndexer(clientBuilder)
			if err := SetupIndexes(t.Context(), idx); err != nil {
				t.Fatalf("failed to setup indexes: %v", err)
			}

			j := fromObject(app)
			got, err := j.workloadSequenceNumber(t.Context(), c)
			if err != nil {
				t.Fatalf("workloadSequenceNumber() returned an unexpected error: %v", err)
			}
			if got != tc.want {
				t.Errorf("workloadSequenceNumber() = %d, want %d", got, tc.want)
			}
		})
	}
}

func TestWorkloadSequenceNumberCachedWithinReconcile(t *testing.T) {
	app := sparkapplicationtesting.MakeSparkApplication("app", "ns").Obj()
	app.UID = "app-uid"

	clientBuilder := utiltesting.NewClientBuilder().WithObjects(
		utiltestingapi.MakeWorkload("wl1", "ns").
			ControllerReference(gvk, app.Name, string(app.UID)).
			Obj(),
	)
	c := clientBuilder.Build()
	idx := utiltesting.AsIndexer(clientBuilder)
	if err := SetupIndexes(t.Context(), idx); err != nil {
		t.Fatalf("failed to setup indexes: %v", err)
	}

	j := fromObject(app)
	first, err := j.workloadSequenceNumber(t.Context(), c)
	if err != nil {
		t.Fatalf("workloadSequenceNumber() returned an unexpected error: %v", err)
	}
	if first != 1 {
		t.Fatalf("workloadSequenceNumber() = %d, want 1", first)
	}

	// Simulate a new slice being created mid-reconcile (e.g. by an earlier call within
	// the same reconcile pass): a second call against the same *SparkApplication
	// instance must still return the cached value.
	if err := c.Create(t.Context(), utiltestingapi.MakeWorkload("wl2", "ns").
		ControllerReference(gvk, app.Name, string(app.UID)).
		Obj()); err != nil {
		t.Fatalf("failed to create workload: %v", err)
	}

	second, err := j.workloadSequenceNumber(t.Context(), c)
	if err != nil {
		t.Fatalf("workloadSequenceNumber() returned an unexpected error: %v", err)
	}
	if second != first {
		t.Errorf("workloadSequenceNumber() second call = %d, want cached value %d", second, first)
	}
}
