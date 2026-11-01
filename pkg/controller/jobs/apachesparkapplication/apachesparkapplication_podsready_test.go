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

	corev1 "k8s.io/api/core/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"

	kueue "sigs.k8s.io/kueue/apis/kueue/v1beta2"
	sparkv1 "sigs.k8s.io/kueue/pkg/controller/jobs/apachesparkapplication/api/v1"
	utiltesting "sigs.k8s.io/kueue/pkg/util/testing"
)

// grantedWorkload is an admitted Workload granting the given number of executors, or a Workload
// that is not admitted when granted is negative.
func grantedWorkload(granted int32) *kueue.Workload {
	wl := &kueue.Workload{Spec: kueue.WorkloadSpec{PodSets: []kueue.PodSet{
		{Name: driverPodSetName, Count: 1}, {Name: executorPodSetName, Count: 3},
	}}}
	if granted >= 0 {
		wl.Status.Admission = &kueue.Admission{PodSetAssignments: []kueue.PodSetAssignment{
			{Name: driverPodSetName, Count: new(int32(1))}, {Name: executorPodSetName, Count: new(granted)},
		}}
	}
	return wl
}

func TestPodsReadyForWorkload(t *testing.T) {
	pods := []client.Object{
		reclaimPod{name: "running", node: "n1"}.obj(),
		reclaimPod{name: "gated", gated: true, phase: corev1.PodPending}.obj(),
		reclaimPod{name: "deleting", node: "n1", deleting: true}.obj(),
	}
	cases := map[string]struct {
		state sparkv1.ApplicationStateSummary
		wl    *kueue.Workload
		want  bool
	}{
		"healthy, regardless of the grant": {
			state: sparkv1.RunningHealthy, wl: grantedWorkload(3), want: true,
		},
		"below the operator's threshold, every granted executor running": {
			state: sparkv1.RunningWithBelowThresholdExecutors, wl: grantedWorkload(1), want: true,
		},
		"below the threshold while starting, every granted executor running": {
			state: sparkv1.InitializedBelowThresholdExecutors, wl: grantedWorkload(1), want: true,
		},
		"below the threshold, a granted executor is still gated or stopping": {
			state: sparkv1.RunningWithBelowThresholdExecutors, wl: grantedWorkload(2), want: false,
		},
		"below the threshold, not admitted": {
			state: sparkv1.RunningWithBelowThresholdExecutors, wl: grantedWorkload(-1), want: false,
		},
		"driver not ready yet": {
			state: sparkv1.DriverStarted, wl: grantedWorkload(1), want: false,
		},
	}
	for name, tc := range cases {
		t.Run(name, func(t *testing.T) {
			c := utiltesting.NewClientBuilder().WithObjects(pods...).Build()
			job := elasticAppWithState(daSpec(nil), tc.state, nil)
			if got := job.PodsReadyForWorkload(t.Context(), c, tc.wl); got != tc.want {
				t.Errorf("PodsReadyForWorkload() = %v, want %v", got, tc.want)
			}
		})
	}
}
