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
	"testing"

	"github.com/google/go-cmp/cmp"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/utils/ptr"

	sparkv1 "sigs.k8s.io/kueue/pkg/controller/jobs/apachesparkapplication/api/v1"
	"sigs.k8s.io/kueue/pkg/podset"
)

func withStatus(status sparkv1.ApplicationStatus) *SparkApplication {
	return &SparkApplication{SparkApplication: &sparkv1.SparkApplication{Status: status}}
}

func stateOnly(summary sparkv1.ApplicationStateSummary) sparkv1.ApplicationStatus {
	return sparkv1.ApplicationStatus{
		CurrentState: sparkv1.ApplicationState{CurrentStateSummary: summary},
	}
}

func TestSuspendRoundTrip(t *testing.T) {
	job := wrap(sparkv1.ApplicationSpec{})
	if job.IsSuspended() {
		t.Error("IsSuspended() = true for an unset spec.suspend, want false")
	}

	job.Suspend()
	if !job.IsSuspended() {
		t.Error("IsSuspended() = false right after Suspend(), want true")
	}
	if got := ptr.Deref(job.Spec.Suspend, false); !got {
		t.Errorf("spec.suspend = %v, want true", got)
	}

	job.Spec.Suspend = ptr.To(false)
	if job.IsSuspended() {
		t.Error("IsSuspended() = true for spec.suspend=false, want false")
	}
}

func TestIsActive(t *testing.T) {
	cases := map[sparkv1.ApplicationStateSummary]bool{
		sparkv1.Submitted:                          false,
		sparkv1.Suspended:                          false,
		sparkv1.ScheduledToRestart:                 false,
		sparkv1.DriverRequested:                    true,
		sparkv1.DriverStarted:                      true,
		sparkv1.DriverReady:                        true,
		sparkv1.InitializedBelowThresholdExecutors: true,
		sparkv1.RunningHealthy:                     true,
		sparkv1.RunningWithPartialCapacity:         true,
		sparkv1.RunningWithBelowThresholdExecutors: true,
		sparkv1.StoppedByScheduler:                 false,
		sparkv1.Succeeded:                          false,
		sparkv1.Failed:                             false,
		sparkv1.ResourceReleased:                   false,
		sparkv1.TerminatedWithoutReleaseResources:  false,
	}

	for state, want := range cases {
		t.Run(string(state), func(t *testing.T) {
			if got := withStatus(stateOnly(state)).IsActive(); got != want {
				t.Errorf("IsActive() = %v for %s, want %v", got, state, want)
			}
		})
	}
}

func TestPodsReady(t *testing.T) {
	cases := map[sparkv1.ApplicationStateSummary]bool{
		sparkv1.Suspended:                          false,
		sparkv1.DriverRequested:                    false,
		sparkv1.DriverReady:                        false,
		sparkv1.InitializedBelowThresholdExecutors: false,
		sparkv1.RunningHealthy:                     true,
		sparkv1.RunningWithPartialCapacity:         true,
		sparkv1.RunningWithBelowThresholdExecutors: false,
		sparkv1.Succeeded:                          false,
	}

	for state, want := range cases {
		t.Run(string(state), func(t *testing.T) {
			if got := withStatus(stateOnly(state)).PodsReady(context.Background(), nil); got != want {
				t.Errorf("PodsReady() = %v for %s, want %v", got, state, want)
			}
		})
	}
}

func TestFinished(t *testing.T) {
	// history builds a state transition history ending in a release state, which is how
	// the operator leaves a terminated application.
	history := func(decisive sparkv1.ApplicationStateSummary, release sparkv1.ApplicationStateSummary) sparkv1.ApplicationStatus {
		return sparkv1.ApplicationStatus{
			CurrentState: sparkv1.ApplicationState{CurrentStateSummary: release},
			StateTransitionHistory: map[string]sparkv1.ApplicationState{
				"0": {CurrentStateSummary: sparkv1.Submitted},
				"1": {CurrentStateSummary: sparkv1.RunningHealthy},
				"2": {CurrentStateSummary: decisive},
				"3": {CurrentStateSummary: release},
			},
		}
	}

	cases := map[string]struct {
		status       sparkv1.ApplicationStatus
		wantFinished bool
		wantSuccess  bool
	}{
		"running is not finished":   {status: stateOnly(sparkv1.RunningHealthy)},
		"suspended is not finished": {status: stateOnly(sparkv1.Suspended)},
		// A scheduler requested stop is a preemption, so the workload must stay live
		// for Kueue to requeue it rather than being marked finished.
		"stopped by scheduler is not finished": {status: stateOnly(sparkv1.StoppedByScheduler)},

		"succeeded":                {status: stateOnly(sparkv1.Succeeded), wantFinished: true, wantSuccess: true},
		"failed":                   {status: stateOnly(sparkv1.Failed), wantFinished: true},
		"scheduling failure":       {status: stateOnly(sparkv1.SchedulingFailure), wantFinished: true},
		"driver evicted":           {status: stateOnly(sparkv1.DriverEvicted), wantFinished: true},
		"driver start timed out":   {status: stateOnly(sparkv1.DriverStartTimedOut), wantFinished: true},
		"driver ready timed out":   {status: stateOnly(sparkv1.DriverReadyTimedOut), wantFinished: true},
		"executor start timed out": {status: stateOnly(sparkv1.ExecutorsStartTimedOut), wantFinished: true},

		"released after success": {
			status: history(sparkv1.Succeeded, sparkv1.ResourceReleased), wantFinished: true, wantSuccess: true,
		},
		"released after failure": {
			status: history(sparkv1.Failed, sparkv1.ResourceReleased), wantFinished: true,
		},
		"retained after success": {
			status:       history(sparkv1.Succeeded, sparkv1.TerminatedWithoutReleaseResources),
			wantFinished: true, wantSuccess: true,
		},
		"retained after failure": {
			status:       history(sparkv1.Failed, sparkv1.TerminatedWithoutReleaseResources),
			wantFinished: true,
		},
		// Without a history there is nothing to attribute the outcome to, so the
		// conservative answer is "not successful" rather than a coin flip.
		"released with no history is finished but not successful": {
			status: stateOnly(sparkv1.ResourceReleased), wantFinished: true,
		},
	}

	for name, tc := range cases {
		t.Run(name, func(t *testing.T) {
			_, success, finished := withStatus(tc.status).Finished(context.Background())
			if finished != tc.wantFinished {
				t.Errorf("finished = %v, want %v", finished, tc.wantFinished)
			}
			if success != tc.wantSuccess {
				t.Errorf("success = %v, want %v", success, tc.wantSuccess)
			}
		})
	}
}

func TestFinishedReturnsMessage(t *testing.T) {
	job := withStatus(sparkv1.ApplicationStatus{
		CurrentState: sparkv1.ApplicationState{
			CurrentStateSummary: sparkv1.Failed,
			Message:             "driver exited with 1",
		},
	})
	message, success, finished := job.Finished(context.Background())
	if !finished || success {
		t.Fatalf("Finished() = (%v, %v), want (false, true)", success, finished)
	}
	if message != "driver exited with 1" {
		t.Errorf("message = %q, want %q", message, "driver exited with 1")
	}
}

func TestPodSets(t *testing.T) {
	job := wrap(sparkv1.ApplicationSpec{
		SparkConf: map[string]string{
			"spark.executor.instances": "3",
			"spark.executor.cores":     "2",
		},
	})

	podSets, err := job.PodSets(context.Background(), nil)
	if err != nil {
		t.Fatalf("PodSets() returned unexpected error: %v", err)
	}
	if len(podSets) != podSetCount {
		t.Fatalf("len(PodSets()) = %d, want %d", len(podSets), podSetCount)
	}
	if got := string(podSets[0].Name); got != driverPodSetName {
		t.Errorf("podSets[0].Name = %q, want %q", got, driverPodSetName)
	}
	if podSets[0].Count != 1 {
		t.Errorf("driver count = %d, want 1", podSets[0].Count)
	}
	if got := string(podSets[1].Name); got != executorPodSetName {
		t.Errorf("podSets[1].Name = %q, want %q", got, executorPodSetName)
	}
	if podSets[1].Count != 3 {
		t.Errorf("executor count = %d, want 3", podSets[1].Count)
	}
}

func TestPodSetsPropagatesResourceErrors(t *testing.T) {
	job := wrap(sparkv1.ApplicationSpec{
		SparkConf: map[string]string{"spark.executor.memory": "not-a-size"},
	})
	if _, err := job.PodSets(context.Background(), nil); err == nil {
		t.Error("PodSets() returned no error for an unparseable spark.executor.memory")
	}
}

func TestRunWithPodSetsInfo(t *testing.T) {
	driverInfo := podset.PodSetInfo{
		NodeSelector: map[string]string{"flavor": "on-demand"},
		Tolerations:  []corev1.Toleration{{Key: "driver", Operator: corev1.TolerationOpExists}},
	}
	executorInfo := podset.PodSetInfo{
		NodeSelector: map[string]string{"flavor": "spot"},
	}

	t.Run("unsuspends and injects into both templates", func(t *testing.T) {
		job := wrap(sparkv1.ApplicationSpec{Suspend: ptr.To(true)})

		if err := job.RunWithPodSetsInfo(context.Background(), nil,
			[]podset.PodSetInfo{driverInfo, executorInfo}); err != nil {
			t.Fatalf("RunWithPodSetsInfo() returned unexpected error: %v", err)
		}

		if job.IsSuspended() {
			t.Error("IsSuspended() = true after RunWithPodSetsInfo(), want false")
		}

		driver := templateSpec(job.SparkApplication, roleDriver)
		if driver == nil {
			t.Fatal("driver pod template was not created")
		}
		if diff := cmp.Diff(driverInfo.NodeSelector, driver.Spec.NodeSelector); diff != "" {
			t.Errorf("driver nodeSelector mismatch (-want +got):\n%s", diff)
		}
		if diff := cmp.Diff(driverInfo.Tolerations, driver.Spec.Tolerations); diff != "" {
			t.Errorf("driver tolerations mismatch (-want +got):\n%s", diff)
		}

		executor := templateSpec(job.SparkApplication, roleExecutor)
		if executor == nil {
			t.Fatal("executor pod template was not created")
		}
		if diff := cmp.Diff(executorInfo.NodeSelector, executor.Spec.NodeSelector); diff != "" {
			t.Errorf("executor nodeSelector mismatch (-want +got):\n%s", diff)
		}
	})

	t.Run("preserves an existing template", func(t *testing.T) {
		job := wrap(sparkv1.ApplicationSpec{
			Suspend: ptr.To(true),
			ExecutorSpec: &sparkv1.BaseApplicationTemplateSpec{
				PodTemplateSpec: &corev1.PodTemplateSpec{
					ObjectMeta: metav1.ObjectMeta{Labels: map[string]string{"team": "data"}},
					Spec: corev1.PodSpec{
						Containers: []corev1.Container{{Name: defaultExecutorContainerName}},
					},
				},
			},
		})

		if err := job.RunWithPodSetsInfo(context.Background(), nil,
			[]podset.PodSetInfo{driverInfo, executorInfo}); err != nil {
			t.Fatalf("RunWithPodSetsInfo() returned unexpected error: %v", err)
		}

		executor := templateSpec(job.SparkApplication, roleExecutor)
		if len(executor.Spec.Containers) != 1 {
			t.Errorf("containers = %v, want the original single container preserved", executor.Spec.Containers)
		}
		if executor.Labels["team"] != "data" {
			t.Errorf("labels = %v, want the original team label preserved", executor.Labels)
		}
	})

	t.Run("rejects a mismatched pod set count", func(t *testing.T) {
		job := wrap(sparkv1.ApplicationSpec{Suspend: ptr.To(true)})
		if err := job.RunWithPodSetsInfo(context.Background(), nil,
			[]podset.PodSetInfo{driverInfo}); err == nil {
			t.Error("RunWithPodSetsInfo() returned no error for 1 pod set, want an error")
		}
		if !job.IsSuspended() {
			t.Error("IsSuspended() = false after a rejected RunWithPodSetsInfo(), want true")
		}
	})
}

func TestRestorePodSetsInfo(t *testing.T) {
	admitted := []podset.PodSetInfo{
		{NodeSelector: map[string]string{"flavor": "on-demand"}},
		{NodeSelector: map[string]string{"flavor": "spot"}},
	}

	t.Run("clears injected scheduling directives", func(t *testing.T) {
		job := wrap(sparkv1.ApplicationSpec{Suspend: ptr.To(true)})
		if err := job.RunWithPodSetsInfo(context.Background(), nil, admitted); err != nil {
			t.Fatalf("RunWithPodSetsInfo() returned unexpected error: %v", err)
		}

		// Restoring to the empty pre-admission state must drop the node selectors.
		empty := []podset.PodSetInfo{{}, {}}
		if changed := job.RestorePodSetsInfo(context.Background(), empty); !changed {
			t.Error("RestorePodSetsInfo() = false, want true when node selectors are dropped")
		}
		for _, role := range []sparkRole{roleDriver, roleExecutor} {
			if ns := templateSpec(job.SparkApplication, role).Spec.NodeSelector; len(ns) != 0 {
				t.Errorf("%s nodeSelector = %v, want empty", role, ns)
			}
		}
	})

	t.Run("reports no change when already in the target state", func(t *testing.T) {
		job := wrap(sparkv1.ApplicationSpec{Suspend: ptr.To(true)})
		if err := job.RunWithPodSetsInfo(context.Background(), nil, admitted); err != nil {
			t.Fatalf("RunWithPodSetsInfo() returned unexpected error: %v", err)
		}
		if changed := job.RestorePodSetsInfo(context.Background(), admitted); changed {
			t.Error("RestorePodSetsInfo() = true for an unchanged target state, want false")
		}
	})

	t.Run("rejects a mismatched pod set count", func(t *testing.T) {
		job := wrap(sparkv1.ApplicationSpec{})
		if changed := job.RestorePodSetsInfo(context.Background(), admitted[:1]); changed {
			t.Error("RestorePodSetsInfo() = true for 1 pod set, want false")
		}
	})

	t.Run("is a no-op when no template exists", func(t *testing.T) {
		job := wrap(sparkv1.ApplicationSpec{})
		if changed := job.RestorePodSetsInfo(context.Background(), admitted); changed {
			t.Error("RestorePodSetsInfo() = true with no pod templates, want false")
		}
	})
}

// TestDeepCopyIsIndependent guards the hand-written deepcopy functions, which have no
// generator keeping them in step with the type definitions.
func TestDeepCopyIsIndependent(t *testing.T) {
	original := &sparkv1.SparkApplication{
		ObjectMeta: metav1.ObjectMeta{
			Name:   "pi",
			Labels: map[string]string{"a": "1"},
		},
		Spec: sparkv1.ApplicationSpec{
			Suspend:   ptr.To(true),
			SparkConf: map[string]string{"spark.executor.instances": "2"},
			DriverSpec: &sparkv1.BaseApplicationTemplateSpec{
				PodTemplateSpec: &corev1.PodTemplateSpec{Spec: corev1.PodSpec{
					NodeSelector: map[string]string{"flavor": "spot"},
					Containers:   []corev1.Container{{Name: defaultDriverContainerName}},
				}},
			},
			ExecutorSpec: &sparkv1.BaseApplicationTemplateSpec{
				PodTemplateSpec: &corev1.PodTemplateSpec{},
			},
			ApplicationTolerations: &sparkv1.ApplicationTolerations{
				InstanceConfig: &sparkv1.ExecutorInstanceConfig{InitExecutors: 2},
			},
			RuntimeVersions: &sparkv1.RuntimeVersions{SparkVersion: "4.2.0"},
		},
		Status: sparkv1.ApplicationStatus{
			CurrentState: sparkv1.ApplicationState{CurrentStateSummary: sparkv1.RunningHealthy},
			StateTransitionHistory: map[string]sparkv1.ApplicationState{
				"0": {CurrentStateSummary: sparkv1.Submitted},
			},
		},
	}

	copied := original.DeepCopy()
	if diff := cmp.Diff(original, copied); diff != "" {
		t.Fatalf("DeepCopy() is not equal to the original (-want +got):\n%s", diff)
	}

	snapshot := original.DeepCopy()

	// Mutate every reference-typed field reachable from the copy.
	*copied.Spec.Suspend = false
	copied.Spec.SparkConf["spark.executor.instances"] = "99"
	copied.Labels["a"] = "2"
	copied.Spec.DriverSpec.PodTemplateSpec.Spec.NodeSelector["flavor"] = "on-demand"
	copied.Spec.DriverSpec.PodTemplateSpec.Spec.Containers[0].Name = "other"
	copied.Spec.ApplicationTolerations.InstanceConfig.InitExecutors = 9
	copied.Spec.RuntimeVersions.SparkVersion = "3.5.0"
	copied.Status.StateTransitionHistory["0"] = sparkv1.ApplicationState{
		CurrentStateSummary: sparkv1.Failed,
	}

	if diff := cmp.Diff(snapshot, original); diff != "" {
		t.Errorf("mutating the copy changed the original (-want +got):\n%s", diff)
	}
}

// TestDeepCopyObjectHandlesNil documents that the runtime.Object contract is satisfied
// for a nil receiver, which the scheme relies on.
func TestDeepCopyObjectHandlesNil(t *testing.T) {
	var app *sparkv1.SparkApplication
	if got := app.DeepCopyObject(); got != nil {
		t.Errorf("DeepCopyObject() on a nil receiver = %v, want nil", got)
	}
	var list *sparkv1.SparkApplicationList
	if got := list.DeepCopyObject(); got != nil {
		t.Errorf("DeepCopyObject() on a nil list = %v, want nil", got)
	}
}
