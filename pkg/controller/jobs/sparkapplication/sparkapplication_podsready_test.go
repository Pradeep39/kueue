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

	sparkappv1beta2 "github.com/kubeflow/spark-operator/v2/api/v1beta2"

	kueue "sigs.k8s.io/kueue/apis/kueue/v1beta2"
	sparkapplicationtesting "sigs.k8s.io/kueue/pkg/util/testingjobs/sparkapplication"
)

// grantedWorkload is an admitted Workload granting the given number of executors, or a Workload
// that is not admitted when granted is negative.
func grantedWorkload(granted int32) *kueue.Workload {
	wl := &kueue.Workload{Spec: kueue.WorkloadSpec{PodSets: []kueue.PodSet{
		{Name: driverPodSetName, Count: 1}, {Name: executorPodSetName, Count: 4},
	}}}
	if granted >= 0 {
		wl.Status.Admission = &kueue.Admission{PodSetAssignments: []kueue.PodSetAssignment{
			{Name: driverPodSetName, Count: new(int32(1))}, {Name: executorPodSetName, Count: new(granted)},
		}}
	}
	return wl
}

func TestPodsReadyForWorkload(t *testing.T) {
	withRunning := func(w *sparkapplicationtesting.SparkApplicationWrapper, n int) *sparkappv1beta2.SparkApplication {
		for i := range n {
			w = w.ExecutorState(string(rune('a'+i)), sparkappv1beta2.ExecutorStateRunning)
		}
		return w.Obj()
	}
	static := func() *sparkapplicationtesting.SparkApplicationWrapper {
		return sparkapplicationtesting.MakeSparkApplication("app", "ns").ExecutorInstances(4).
			AppState(sparkappv1beta2.ApplicationStateRunning)
	}
	cases := map[string]struct {
		app  *sparkappv1beta2.SparkApplication
		wl   *kueue.Workload
		want bool
	}{
		"grant lowered by partial preemption, every granted executor running": {
			app: withRunning(static(), 2), wl: grantedWorkload(2), want: true,
		},
		"full grant, half the executors running": {
			app: withRunning(static(), 2), wl: grantedWorkload(4), want: false,
		},
		"grant lowered, fewer executors running than granted": {
			app: withRunning(static(), 1), wl: grantedWorkload(2), want: false,
		},
		"not admitted: the executor instances": {
			app: withRunning(static(), 2), wl: grantedWorkload(-1), want: false,
		},
		"Dynamic Allocation without minExecutors, grant lowered below the instances": {
			app: withRunning(sparkapplicationtesting.MakeSparkApplication("app", "ns").ExecutorInstances(3).
				DynamicAllocation(&sparkappv1beta2.DynamicAllocation{Enabled: true, MaxExecutors: new(int32(4))}).
				AppState(sparkappv1beta2.ApplicationStateRunning), 1),
			wl: grantedWorkload(1), want: true,
		},
		"driver not running": {
			app: withRunning(static().AppState(sparkappv1beta2.ApplicationStateSubmitted), 2), wl: grantedWorkload(2), want: false,
		},
	}
	for name, tc := range cases {
		t.Run(name, func(t *testing.T) {
			if got := (*SparkApplication)(tc.app).PodsReadyForWorkload(t.Context(), nil, tc.wl); got != tc.want {
				t.Errorf("PodsReadyForWorkload() = %v, want %v", got, tc.want)
			}
		})
	}
}
