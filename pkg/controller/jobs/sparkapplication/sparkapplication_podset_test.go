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
	}{
		"dynamic allocation disabled uses the static instances field": {
			app: sparkapplicationtesting.MakeSparkApplication("app", "ns").
				ExecutorInstances(5).Obj(),
			pods: []client.Object{executorPod("e1", corev1.PodRunning, false)},
			want: 5,
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
	}

	for name, tc := range tests {
		t.Run(name, func(t *testing.T) {
			app := fromObject(tc.app)

			var c client.Client
			if !tc.nilClient {
				c = utiltesting.NewClientBuilder().WithObjects(tc.pods...).Build()
			}

			got, err := app.liveExecutorCount(t.Context(), c)
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
