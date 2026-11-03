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
	"bytes"
	"encoding/json"
	"fmt"
	"testing"

	"github.com/google/go-cmp/cmp"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	kueue "sigs.k8s.io/kueue/apis/kueue/v1beta2"
	sparkv1 "sigs.k8s.io/kueue/pkg/controller/jobs/apachesparkapplication/api/v1"
	"sigs.k8s.io/kueue/pkg/features"
	"sigs.k8s.io/kueue/pkg/podset"
	utiltesting "sigs.k8s.io/kueue/pkg/util/testing"
	"sigs.k8s.io/kueue/pkg/workloadslicing"
)

// daSpec builds a Dynamic-Allocation-enabled spec.
func daSpec(extra map[string]string) sparkv1.ApplicationSpec {
	conf := map[string]string{"spark.dynamicAllocation.enabled": "true"}
	for k, v := range extra {
		conf[k] = v
	}
	return sparkv1.ApplicationSpec{SparkConf: conf}
}

// elasticAppWithState builds an elastic application with spec, whose driver is in state and reports
// executors in status.executorState.
func elasticAppWithState(spec sparkv1.ApplicationSpec, state sparkv1.ApplicationStateSummary, executors map[string]sparkv1.ExecutorState) *SparkApplication {
	return &SparkApplication{SparkApplication: &sparkv1.SparkApplication{
		ObjectMeta: metav1.ObjectMeta{
			Name:        "pi",
			Namespace:   "ns",
			Annotations: map[string]string{workloadslicing.EnabledAnnotationKey: workloadslicing.EnabledAnnotationValue},
		},
		Spec: spec,
		Status: sparkv1.ApplicationStatus{
			CurrentState:  sparkv1.ApplicationState{CurrentStateSummary: state},
			ExecutorState: executors,
		},
	}}
}

// executors builds a status.executorState with the given states, one executor each.
func executors(states ...sparkv1.ExecutorState) map[string]sparkv1.ExecutorState {
	out := make(map[string]sparkv1.ExecutorState, len(states))
	for i, st := range states {
		out[fmt.Sprintf("pi-exec-%d", i+1)] = st
	}
	return out
}

func TestNumExecutors(t *testing.T) {
	const (
		pending   = sparkv1.ExecutorStatePending
		running   = sparkv1.ExecutorStateRunning
		completed = sparkv1.ExecutorStateCompleted
		failed    = sparkv1.ExecutorStateFailed
		unknown   = sparkv1.ExecutorStateUnknown
	)
	cases := map[string]struct {
		spec       sparkv1.ApplicationSpec
		state      sparkv1.ApplicationStateSummary
		executors  map[string]sparkv1.ExecutorState
		active     *int32
		notElastic bool
		gateOff    bool
		want       int32
	}{
		// The operator stops tracking executors beyond its cap, while it counts activeExecutors
		// from the pods.
		"activeExecutors wins over the tracked executors": {
			spec:      daSpec(nil),
			state:     sparkv1.RunningHealthy,
			executors: executors(running),
			active:    new(int32(5)),
			want:      5,
		},
		"activeExecutors with the executor states off": {
			spec:   daSpec(nil),
			state:  sparkv1.RunningHealthy,
			active: new(int32(4)),
			want:   4,
		},
		"activeExecutors above maxExecutors is capped": {
			spec:   daSpec(map[string]string{"spark.dynamicAllocation.maxExecutors": "2"}),
			state:  sparkv1.RunningHealthy,
			active: new(int32(6)),
			want:   2,
		},
		"activeExecutors of a driver not started is ignored": {
			spec:   daSpec(map[string]string{"spark.dynamicAllocation.initialExecutors": "5"}),
			state:  sparkv1.Submitted,
			active: new(int32(6)),
			want:   5,
		},
		"dynamic allocation off ignores executorState entirely": {
			spec:      sparkv1.ApplicationSpec{SparkConf: map[string]string{"spark.executor.instances": "3"}},
			state:     sparkv1.RunningHealthy,
			executors: executors(running, running),
			want:      3,
		},
		"a non-elastic application uses the static count": {
			spec:       daSpec(map[string]string{"spark.executor.instances": "3"}),
			state:      sparkv1.RunningHealthy,
			executors:  executors(running),
			notElastic: true,
			want:       3,
		},
		"with the Apache elastic gate off the application is not elastic": {
			spec:      daSpec(map[string]string{"spark.executor.instances": "3"}),
			state:     sparkv1.RunningHealthy,
			executors: executors(running),
			gateOff:   true,
			want:      3,
		},
		"no executors reported yet falls back to initialExecutors": {
			spec:  daSpec(map[string]string{"spark.dynamicAllocation.initialExecutors": "4"}),
			state: sparkv1.DriverReady,
			want:  4,
		},
		"no executors reported yet falls back to minExecutors": {
			spec:  daSpec(map[string]string{"spark.dynamicAllocation.minExecutors": "2"}),
			state: sparkv1.DriverReady,
			want:  2,
		},
		"initialExecutors beats minExecutors": {
			spec: daSpec(map[string]string{
				"spark.dynamicAllocation.initialExecutors": "5",
				"spark.dynamicAllocation.minExecutors":     "2",
			}),
			state: sparkv1.Submitted,
			want:  5,
		},
		"no dynamic allocation counts at all falls back to the static count": {
			spec:  daSpec(map[string]string{"spark.executor.instances": "6"}),
			state: sparkv1.Submitted,
			want:  6,
		},
		"reported executors override the configured count": {
			spec:      daSpec(map[string]string{"spark.dynamicAllocation.initialExecutors": "1"}),
			state:     sparkv1.RunningHealthy,
			executors: executors(running, running, running),
			want:      3,
		},
		// Quota must be reserved as soon as the driver asks for an executor, not once it
		// reaches Running. That includes executors still held by the elastic gate.
		"pending executors count": {
			spec:      daSpec(nil),
			state:     sparkv1.RunningHealthy,
			executors: executors(running, pending),
			want:      2,
		},
		"terminated and unknown executors do not count": {
			spec:      daSpec(nil),
			state:     sparkv1.RunningHealthy,
			executors: executors(running, completed, failed, unknown),
			want:      1,
		},
		// The operator's executorState belongs to the previous attempt until a new driver
		// reports, so a restart reserves the initial count again.
		"a restarting application falls back to the initial count": {
			spec:      daSpec(map[string]string{"spark.dynamicAllocation.initialExecutors": "2"}),
			state:     sparkv1.ScheduledToRestart,
			executors: executors(running, running, running, running),
			want:      2,
		},
		"a requested driver has not reported executors yet": {
			spec:      daSpec(map[string]string{"spark.dynamicAllocation.initialExecutors": "2"}),
			state:     sparkv1.DriverRequested,
			executors: executors(running, running, running, running),
			want:      2,
		},
		// Falling back to the initial count once the application ends would look like a
		// scale-up, and the job reconciler would not find the active slice to finish.
		"a finished application keeps following executorState": {
			spec: daSpec(map[string]string{
				"spark.dynamicAllocation.initialExecutors": "5",
				"spark.dynamicAllocation.minExecutors":     "1",
			}),
			state:     sparkv1.Succeeded,
			executors: executors(running, completed, completed),
			want:      1,
		},
		// Executors the driver is still starting are reported one by one. Without the
		// minExecutors floor the derived count would be 1, which EnsureWorkloadSlices reads
		// as a scale-down of the gang it was just admitted with.
		"a count below minExecutors is raised to the floor": {
			spec: daSpec(map[string]string{
				"spark.dynamicAllocation.minExecutors": "3",
				"spark.dynamicAllocation.maxExecutors": "30",
			}),
			state:     sparkv1.DriverReady,
			executors: executors(running),
			want:      3,
		},
		"a count above maxExecutors is capped at the ceiling": {
			spec: daSpec(map[string]string{
				"spark.dynamicAllocation.minExecutors": "1",
				"spark.dynamicAllocation.maxExecutors": "2",
			}),
			state:     sparkv1.RunningHealthy,
			executors: executors(running, pending, pending, pending),
			want:      2,
		},
		// maxExecutors is applied last, so an inverted configuration can never inflate the
		// count above the declared maximum.
		"minExecutors above maxExecutors never exceeds the maximum": {
			spec: daSpec(map[string]string{
				"spark.dynamicAllocation.minExecutors": "8",
				"spark.dynamicAllocation.maxExecutors": "2",
			}),
			state:     sparkv1.RunningHealthy,
			executors: executors(running),
			want:      2,
		},
		"declared instances below minExecutors reserves the floor before any executors exist": {
			spec: daSpec(map[string]string{
				"spark.executor.instances":             "1",
				"spark.dynamicAllocation.minExecutors": "3",
			}),
			state: sparkv1.Submitted,
			want:  3,
		},
		// The conf keys Spark itself acts on outrank instanceConfig on the Dynamic
		// Allocation path too, and drive the clamp. instanceConfig remains the fallback.
		"sparkConf instances outranks instanceConfig initExecutors": {
			spec: func() sparkv1.ApplicationSpec {
				spec := daSpec(map[string]string{"spark.executor.instances": "9"})
				spec.ApplicationTolerations = &sparkv1.ApplicationTolerations{
					InstanceConfig: &sparkv1.ExecutorInstanceConfig{InitExecutors: 4},
				}
				return spec
			}(),
			state: sparkv1.Submitted,
			want:  9,
		},
		"instanceConfig minExecutors raises a count below the floor": {
			spec: func() sparkv1.ApplicationSpec {
				spec := daSpec(nil)
				spec.ApplicationTolerations = &sparkv1.ApplicationTolerations{
					InstanceConfig: &sparkv1.ExecutorInstanceConfig{MinExecutors: 3},
				}
				return spec
			}(),
			state:     sparkv1.RunningWithBelowThresholdExecutors,
			executors: executors(running),
			want:      3,
		},
		"instanceConfig maxExecutors caps a count above the ceiling": {
			spec: func() sparkv1.ApplicationSpec {
				spec := daSpec(nil)
				spec.ApplicationTolerations = &sparkv1.ApplicationTolerations{
					InstanceConfig: &sparkv1.ExecutorInstanceConfig{MaxExecutors: 2},
				}
				return spec
			}(),
			state:     sparkv1.RunningHealthy,
			executors: executors(running, running, running),
			want:      2,
		},
		// Dynamic Allocation obeys the sparkConf bounds, so a count of 3 is raised to the
		// sparkConf floor of 8 rather than capped at instanceConfig's 2.
		"sparkConf bounds take precedence over the instanceConfig equivalents": {
			spec: func() sparkv1.ApplicationSpec {
				spec := daSpec(map[string]string{
					"spark.dynamicAllocation.minExecutors": "8",
					"spark.dynamicAllocation.maxExecutors": "9",
				})
				spec.ApplicationTolerations = &sparkv1.ApplicationTolerations{
					InstanceConfig: &sparkv1.ExecutorInstanceConfig{MinExecutors: 1, MaxExecutors: 2},
				}
				return spec
			}(),
			state:     sparkv1.RunningWithPartialCapacity,
			executors: executors(running, running, running),
			want:      8,
		},
		"declared instances above the dynamic allocation counts wins": {
			spec: daSpec(map[string]string{
				"spark.executor.instances":                 "6",
				"spark.dynamicAllocation.initialExecutors": "2",
				"spark.dynamicAllocation.minExecutors":     "1",
			}),
			state: sparkv1.Submitted,
			want:  6,
		},
	}

	for name, tc := range cases {
		t.Run(name, func(t *testing.T) {
			features.SetFeatureGateDuringTest(t, features.ElasticJobsViaWorkloadSlicesForApacheSparkApplication, !tc.gateOff)
			job := elasticAppWithState(tc.spec, tc.state, tc.executors)
			job.Status.ActiveExecutors = tc.active
			if tc.notElastic {
				job.Annotations = nil
			}
			got, err := job.numExecutors()
			if err != nil {
				t.Fatalf("numExecutors() returned unexpected error: %v", err)
			}
			if got != tc.want {
				t.Errorf("numExecutors() = %d, want %d", got, tc.want)
			}
		})
	}
}

func TestGetWorkloadNameExtraPart(t *testing.T) {
	job := elasticAppWithState(daSpec(nil), sparkv1.Submitted, nil)
	job.Generation = 2
	if got, want := job.GetWorkloadNameExtraPart(), "2"; got != want {
		t.Errorf("before the first attempt: GetWorkloadNameExtraPart() = %q, want %q", got, want)
	}
	job.Status.CurrentAttemptSummary = &sparkv1.ApplicationAttemptSummary{AttemptInfo: &sparkv1.AttemptInfo{ID: 0}}
	if got, want := job.GetWorkloadNameExtraPart(), "2_0_0"; got != want {
		t.Errorf("first attempt, no executors: GetWorkloadNameExtraPart() = %q, want %q", got, want)
	}
	// Every scale-up adds an entry, so the name never repeats within an attempt, even when
	// the driver later shrinks back to an earlier count.
	job.Status.ExecutorState = executors(sparkv1.ExecutorStateRunning, sparkv1.ExecutorStateCompleted, sparkv1.ExecutorStateRunning)
	if got, want := job.GetWorkloadNameExtraPart(), "2_0_3"; got != want {
		t.Errorf("three tracked executors: GetWorkloadNameExtraPart() = %q, want %q", got, want)
	}
	// maxExecutorId keeps growing past the operator's tracking cap, and with the executor states
	// off, so it wins over the tracked executors.
	job.Status.MaxExecutorID = new(int64(1200))
	if got, want := job.GetWorkloadNameExtraPart(), "2_0_1200"; got != want {
		t.Errorf("maxExecutorId set: GetWorkloadNameExtraPart() = %q, want %q", got, want)
	}
	job.Status.CurrentAttemptSummary.AttemptInfo.ID = 1
	job.Status.ExecutorState = nil
	job.Status.MaxExecutorID = nil
	if got, want := job.GetWorkloadNameExtraPart(), "2_1_0"; got != want {
		t.Errorf("second attempt: GetWorkloadNameExtraPart() = %q, want %q", got, want)
	}
}

// TestEnsureTemplateSpecNeverMarshalsNullContainers is a regression guard.
//
// corev1.PodSpec.Containers carries no omitempty, so a bare &corev1.PodTemplateSpec{}
// marshals to `"containers": null`. The CRD schema declares containers as `type: array`,
// so the API server rejects the object outright with:
//
//	spec.executorSpec.podTemplateSpec.spec.containers: Invalid value: "null":
//	... in body must be of type array: "null"
//
// This bites whenever an application omits driverSpec/executorSpec, which is the common
// case, both when the webhook gates an elastic job and when RunWithPodSetsInfo injects
// pod set info on admission.
func TestEnsureTemplateSpecNeverMarshalsNullContainers(t *testing.T) {
	cases := map[string]struct {
		spec sparkv1.ApplicationSpec
		role sparkRole
	}{
		"executor template absent entirely": {role: roleExecutor},
		"driver template absent entirely":   {role: roleDriver},
		"executor wrapper present, template nil": {
			spec: sparkv1.ApplicationSpec{ExecutorSpec: &sparkv1.BaseApplicationTemplateSpec{}},
			role: roleExecutor,
		},
		"template present but containers nil": {
			spec: sparkv1.ApplicationSpec{
				ExecutorSpec: &sparkv1.BaseApplicationTemplateSpec{
					PodTemplateSpec: &corev1.PodTemplateSpec{},
				},
			},
			role: roleExecutor,
		},
	}

	for name, tc := range cases {
		t.Run(name, func(t *testing.T) {
			job := wrap(tc.spec)
			template := job.ensureTemplateSpec(tc.role)

			if template.Spec.Containers == nil {
				t.Fatal("Containers is nil, which marshals to null and fails CRD validation")
			}

			// Assert on the wire form, since that is what the API server validates.
			encoded, err := json.Marshal(job.SparkApplication)
			if err != nil {
				t.Fatalf("failed to marshal the application: %v", err)
			}
			if bytes.Contains(encoded, []byte(`"containers":null`)) {
				t.Errorf("marshalled application contains a null containers list:\n%s", encoded)
			}
		})
	}
}

// TestRestorePodSetsInfoPreservesElasticSchedulingGate guards the elastic gate across a stop.
//
// stopJob applies Suspend() and RestorePodSetsInfo() in one patch, and validateElasticJob
// requires an elastic job to keep the ElasticJobSchedulingGate, so a restore that wiped it
// would have the webhook reject the suspend: preemption could never stop the job, the evicted
// slice would keep its quota reservation and the preemptor would starve. The gates are
// restored from the Workload's PodSets, whose templates carry the executor template's gates,
// so the elastic gate survives and a gate added on admission is removed.
//
// Negative control: restore from PodSetInfos with no gates and the gate is wiped.
func TestRestorePodSetsInfoPreservesElasticSchedulingGate(t *testing.T) {
	elasticGate := corev1.PodSchedulingGate{Name: kueue.ElasticJobSchedulingGate}
	admissionGate := corev1.PodSchedulingGate{Name: "example.com/admission"}

	job := wrap(daSpec(nil))
	// Mirror what the mutating webhook does at CREATE.
	job.ensureTemplateSpec(roleExecutor).Spec.SchedulingGates = []corev1.PodSchedulingGate{elasticGate}
	job.ensureTemplateSpec(roleDriver)

	// What stopJob passes: derived from the Workload's PodSets, built before admission.
	podSets, err := job.PodSets(t.Context(), nil)
	if err != nil {
		t.Fatalf("PodSets() = %v", err)
	}
	info := []podset.PodSetInfo{podset.FromPodSet(&podSets[0]), podset.FromPodSet(&podSets[1])}

	// Admission adds a gate of its own.
	executor := templateSpec(job.SparkApplication, roleExecutor)
	executor.Spec.SchedulingGates = append(executor.Spec.SchedulingGates, admissionGate)

	job.RestorePodSetsInfo(t.Context(), info)

	if diff := cmp.Diff([]corev1.PodSchedulingGate{elasticGate}, templateSpec(job.SparkApplication, roleExecutor).Spec.SchedulingGates); diff != "" {
		t.Errorf("executor SchedulingGates after restore (-want,+got):\n%s\n"+
			"the elastic gate must survive, or the webhook rejects the suspend patch and preemption deadlocks", diff)
	}
}

// TestStopJobAndReadmitPatchesPassElasticValidation closes the loop that the existing
// gate test leaves open.
//
// TestRestorePodSetsInfoPreservesElasticSchedulingGate asserts the gate survives in
// memory. That is necessary but not what actually failed in production: the failure was
// the *API server* rejecting the patch stopJob produces, because validateElasticJob
// requires the ElasticJobSchedulingGate on the executor template. So assert the real
// invariant — the object each mutation leaves behind must still pass that validator.
//
// This matters most for this integration. The Kubeflow path has end-to-end cluster
// evidence; the Apache integration is not deployed anywhere, so these unit assertions are
// the only evidence its equivalent fix is correct.
//
// The PodSetInfos are derived from the job's own PodSets() rather than written by hand,
// because that is where the production staleness comes from: PodSets() reads the CR, so
// once an admission has written the Workload name onto the CR, every later slice's
// recorded template carries it, and the merge sees that stale value against the current
// slice's. A fixture that restores from empty maps wipes the CR and cannot reproduce it.
//
// Negative control: have RestorePodSetsInfo restore gates from empty PodSetInfos and the
// suspend subtest fails with the real validator error.
//
// The re-admission subtest has NO verified negative control. Bypassing podset.Merge in
// RunWithPodSetsInfo — the mutation that broke the Kubeflow integration the same way —
// does not make it fail, so on this path it is a barrier against future drift rather than
// a demonstrated regression guard. Do not read it as proof that this integration is
// susceptible to that bug; the evidence is that it is not.
func TestStopJobAndReadmitPatchesPassElasticValidation(t *testing.T) {
	elasticGate := corev1.PodSchedulingGate{Name: kueue.ElasticJobSchedulingGate}
	clnt := utiltesting.NewClientBuilder().Build()

	// newGatedApp mirrors what the mutating webhook leaves on the object at CREATE.
	newGatedApp := func() *SparkApplication {
		job := wrap(daSpec(nil))
		job.ensureTemplateSpec(roleDriver)
		job.ensureTemplateSpec(roleExecutor).Spec.SchedulingGates = []corev1.PodSchedulingGate{elasticGate}
		if errs := validateElasticJob(job.SparkApplication); len(errs) > 0 {
			t.Fatalf("fixture is already invalid before any mutation: %v", errs)
		}
		return job
	}

	// fromPodSets reproduces getPodSetsInfoFromStatus: the Workload's recorded PodSet
	// templates, plus the annotations Kueue stamps for the admission in progress.
	fromPodSets := func(job *SparkApplication, sliceName string) []podset.PodSetInfo {
		podSets, err := job.PodSets(t.Context(), clnt)
		if err != nil {
			t.Fatalf("PodSets() = %v", err)
		}
		info := make([]podset.PodSetInfo, 0, len(podSets))
		for i := range podSets {
			psi := podset.FromPodSet(&podSets[i])
			if psi.Annotations == nil {
				psi.Annotations = map[string]string{}
			}
			if sliceName != "" {
				psi.Annotations[kueue.WorkloadAnnotation] = sliceName
				psi.Annotations[kueue.WorkloadSliceNameAnnotation] = "app-root-slice"
			}
			info = append(info, psi)
		}
		return info
	}

	t.Run("the patch stopJob produces still validates", func(t *testing.T) {
		job := newGatedApp()

		// stopJob: RestorePodSetsInfo from the Workload's PodSets, then Suspend, in one patch.
		job.RestorePodSetsInfo(t.Context(), fromPodSets(job, ""))
		job.Suspend()

		if errs := validateElasticJob(job.SparkApplication); len(errs) > 0 {
			t.Errorf("validateElasticJob() after stopJob = %v, want no errors\n"+
				"the API server would reject the suspend patch and preemption would never stop the job", errs)
		}
	})

	t.Run("the patch re-admission produces still validates", func(t *testing.T) {
		job := newGatedApp()

		// First admission bakes the current Workload name onto the CR.
		if err := job.RunWithPodSetsInfo(t.Context(), clnt, fromPodSets(job, "app-slice-1")); err != nil {
			t.Fatalf("first admission: RunWithPodSetsInfo() = %v, want nil", err)
		}

		// Preemption. The restore carries the CR's own annotations back, stale name included.
		job.RestorePodSetsInfo(t.Context(), fromPodSets(job, ""))
		job.Suspend()

		// Resume under a NEW slice. The stale name must not conflict with the current one.
		if err := job.RunWithPodSetsInfo(t.Context(), clnt, fromPodSets(job, "app-slice-2")); err != nil {
			t.Fatalf("re-admission under a new slice: RunWithPodSetsInfo() = %v, want nil\n"+
				"a permanent error here leaves the job suspended for ever", err)
		}

		if errs := validateElasticJob(job.SparkApplication); len(errs) > 0 {
			t.Errorf("validateElasticJob() after re-admission = %v, want no errors", errs)
		}
	})
}
