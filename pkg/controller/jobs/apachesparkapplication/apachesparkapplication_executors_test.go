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

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/utils/ptr"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/event"

	kueue "sigs.k8s.io/kueue/apis/kueue/v1beta2"
	sparkv1 "sigs.k8s.io/kueue/pkg/controller/jobs/apachesparkapplication/api/v1"
	"sigs.k8s.io/kueue/pkg/features"
	utiltesting "sigs.k8s.io/kueue/pkg/util/testing"
)

type podOpt func(*corev1.Pod)

func readyPod(p *corev1.Pod) {
	p.Status.Conditions = append(p.Status.Conditions, corev1.PodCondition{Type: corev1.PodReady, Status: corev1.ConditionTrue})
}

func beingDeleted(p *corev1.Pod) {
	p.DeletionTimestamp = &metav1.Time{}
}

func withPhase(phase corev1.PodPhase) podOpt {
	return func(p *corev1.Pod) { p.Status.Phase = phase }
}

func withExecutorID(id string) podOpt {
	return func(p *corev1.Pod) { p.Labels[executorIDLabel] = id }
}

func execPod(name string, opts ...podOpt) *corev1.Pod {
	p := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: "ns", Labels: map[string]string{
			appNameLabel: "pi",
			roleLabel:    executorRoleValue,
		}},
		Status: corev1.PodStatus{Phase: corev1.PodRunning},
	}
	for _, opt := range opts {
		opt(p)
	}
	return p
}

// podClient serves pods. A pod marked as being deleted is created with a finalizer and then
// deleted, since the fake client sets the deletion timestamp only on Delete.
func podClient(t *testing.T, pods ...*corev1.Pod) client.Client {
	t.Helper()
	b := utiltesting.NewClientBuilder()
	var toDelete []*corev1.Pod
	for _, p := range pods {
		p = p.DeepCopy()
		if p.DeletionTimestamp != nil {
			p.DeletionTimestamp = nil
			p.Finalizers = []string{"test/keep"}
			toDelete = append(toDelete, p)
		}
		b = b.WithObjects(p)
	}
	c := b.Build()
	for _, p := range toDelete {
		if err := c.Delete(context.Background(), p); err != nil {
			t.Fatal(err)
		}
	}
	return c
}

func staticApp(instances string, state sparkv1.ApplicationStateSummary) *SparkApplication {
	job := elasticAppWithState(sparkv1.ApplicationSpec{SparkConf: map[string]string{"spark.executor.instances": instances}}, state, nil)
	job.Annotations = nil
	return job
}

func TestPodsReadyCountsReadyExecutors(t *testing.T) {
	pods := []*corev1.Pod{
		execPod("pi-exec-1", readyPod),
		execPod("pi-exec-2", readyPod),
		execPod("pi-exec-3"),                               // running, not ready
		execPod("pi-exec-4", readyPod, beingDeleted),       // being removed
		execPod("pi-exec-5", withPhase(corev1.PodPending)), // e.g. gated
		execPod("pi-exec-6", readyPod, withPhase(corev1.PodSucceeded)),
	}
	cases := map[string]struct {
		job     *SparkApplication
		declare *int32
		granted *int32
		want    bool
	}{
		"declared ready executors reach the instances": {
			job: staticApp("3", sparkv1.RunningWithBelowThresholdExecutors), declare: new(int32(3)), want: true,
		},
		"declared ready executors below the instances, though Pods would be enough": {
			job: staticApp("2", sparkv1.RunningHealthy), declare: new(int32(1)), want: false,
		},
		"not declared: two ready Pods reach two instances": {
			job: staticApp("2", sparkv1.RunningWithBelowThresholdExecutors), want: true,
		},
		"not declared: unready, terminating, pending and succeeded executors don't count": {
			job: staticApp("3", sparkv1.RunningHealthy), want: false,
		},
		"the grant caps the floor": {
			job: staticApp("3", sparkv1.RunningWithBelowThresholdExecutors), granted: new(int32(2)), want: true,
		},
		"dynamic allocation without minExecutors: the driver running is enough": {
			job: elasticAppWithState(daSpec(nil), sparkv1.DriverReady, nil), want: true,
		},
		"dynamic allocation: minExecutors is the floor": {
			job: elasticAppWithState(daSpec(map[string]string{"spark.dynamicAllocation.minExecutors": "3"}), sparkv1.RunningHealthy, nil), want: false,
		},
		"driver not running": {
			job: staticApp("1", sparkv1.DriverStarted), declare: new(int32(1)), want: false,
		},
	}
	for name, tc := range cases {
		t.Run(name, func(t *testing.T) {
			c := podClient(t, pods...)
			tc.job.Status.ReadyExecutors = tc.declare
			var got bool
			if tc.granted != nil {
				wl := &kueue.Workload{Status: kueue.WorkloadStatus{Admission: &kueue.Admission{PodSetAssignments: []kueue.PodSetAssignment{
					{Name: driverPodSetName, Count: new(int32(1))}, {Name: executorPodSetName, Count: tc.granted},
				}}}}
				got = tc.job.PodsReadyForWorkload(t.Context(), c, wl)
			} else {
				got = tc.job.PodsReady(t.Context(), c)
			}
			if got != tc.want {
				t.Errorf("PodsReady = %v, want %v", got, tc.want)
			}
		})
	}
}

func TestNumExecutorsFromDeclaredOrPods(t *testing.T) {
	features.SetFeatureGateDuringTest(t, features.ElasticJobsViaWorkloadSlicesForApacheSparkApplication, true)
	spec := daSpec(map[string]string{"spark.dynamicAllocation.maxExecutors": "8"})
	pods := []*corev1.Pod{
		execPod("pi-exec-1", readyPod),
		execPod("pi-exec-2", readyPod, beingDeleted),
		execPod("pi-exec-3", withPhase(corev1.PodPending)),
		execPod("pi-exec-4", withPhase(corev1.PodSucceeded)),
	}
	cases := map[string]struct {
		declare *int32
		pods    []*corev1.Pod
		want    int32
	}{
		"declared active executors":                                     {declare: new(int32(5)), pods: pods, want: 5},
		"not declared: pending and running Pods, including terminating": {pods: pods, want: 3},
		"not declared and no executor Pods yet: Spark's initial count":  {want: 2},
	}
	for name, tc := range cases {
		t.Run(name, func(t *testing.T) {
			job := elasticAppWithState(spec, sparkv1.RunningHealthy, nil)
			job.Status.ActiveExecutors = tc.declare
			got, err := job.numExecutors(t.Context(), podClient(t, tc.pods...))
			if err != nil {
				t.Fatal(err)
			}
			if got != tc.want {
				t.Errorf("numExecutors() = %d, want %d", got, tc.want)
			}
		})
	}
}

func TestMaxExecutorIDFromDeclaredOrRecorded(t *testing.T) {
	features.SetFeatureGateDuringTest(t, features.ElasticJobsViaWorkloadSlicesForApacheSparkApplication, true)
	newJob := func() *SparkApplication {
		job := elasticAppWithState(daSpec(nil), sparkv1.RunningHealthy, nil)
		job.Status.CurrentAttemptSummary = &sparkv1.ApplicationAttemptSummary{AttemptInfo: &sparkv1.AttemptInfo{ID: 2}}
		return job
	}
	c := podClient(t, execPod("pi-exec-3", withExecutorID("3")), execPod("pi-exec-7"))

	// Not declared: the highest ID among the Pods (label, else name) is recorded for the attempt
	job := newJob()
	annotations, err := job.GetCustomAnnotations(t.Context(), c)
	if err != nil {
		t.Fatal(err)
	}
	if annotations[maxExecutorIDAnnotation] != "7" || annotations[maxExecutorIDAttemptAnnotation] != "2" {
		t.Fatalf("GetCustomAnnotations() = %v, want ID 7 for attempt 2", annotations)
	}
	job.Annotations = map[string]string{maxExecutorIDAnnotation: "7", maxExecutorIDAttemptAnnotation: "2"}
	if got := job.executorSequence(); got != 7 {
		t.Errorf("executorSequence() = %d, want 7", got)
	}
	// A record from another attempt doesn't count
	job.Annotations[maxExecutorIDAttemptAnnotation] = "1"
	if got := job.executorSequence(); got != 0 {
		t.Errorf("executorSequence() with another attempt's record = %d, want 0", got)
	}

	// Declared: used, and nothing is recorded
	job = newJob()
	job.Status.MaxExecutorID = ptr.To[int64](1200)
	if annotations, _ := job.GetCustomAnnotations(t.Context(), c); annotations != nil {
		t.Errorf("GetCustomAnnotations() = %v with a declared maxExecutorId, want none", annotations)
	}
	if got := job.executorSequence(); got != 1200 {
		t.Errorf("executorSequence() = %d, want 1200", got)
	}
}

func TestExecutorPodChanged(t *testing.T) {
	base := execPod("pi-exec-1")
	ready := execPod("pi-exec-1", readyPod)
	deleting := execPod("pi-exec-1", beingDeleted)
	succeeded := execPod("pi-exec-1", withPhase(corev1.PodSucceeded))
	relabelled := execPod("pi-exec-1")
	relabelled.Labels["other"] = "x"
	cases := map[string]struct {
		old, updated *corev1.Pod
		want         bool
	}{
		"phase change":            {old: base, updated: succeeded, want: true},
		"becomes ready":           {old: base, updated: ready, want: true},
		"becomes unready":         {old: ready, updated: base, want: true},
		"starts being deleted":    {old: base, updated: deleting, want: true},
		"unrelated metadata only": {old: base, updated: relabelled, want: false},
	}
	for name, tc := range cases {
		t.Run(name, func(t *testing.T) {
			if got := executorPodChanged(event.UpdateEvent{ObjectOld: tc.old, ObjectNew: tc.updated}); got != tc.want {
				t.Errorf("executorPodChanged() = %v, want %v", got, tc.want)
			}
		})
	}
	if got := executorPodToSparkApplication(context.Background(), base); len(got) != 1 || got[0].Name != "pi" {
		t.Errorf("executorPodToSparkApplication() = %v", got)
	}
}
