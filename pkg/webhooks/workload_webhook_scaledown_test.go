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

package webhooks

import (
	"testing"

	corev1 "k8s.io/api/core/v1"

	kueue "sigs.k8s.io/kueue/apis/kueue/v1beta2"
	"sigs.k8s.io/kueue/pkg/features"
	utiltestingapi "sigs.k8s.io/kueue/pkg/util/testing/v1beta2"
	"sigs.k8s.io/kueue/pkg/workloadslicing"
)

// TestValidateAdmissionUpdateElasticScaleDown covers the admission-immutability exception
// that lets an elastic workload's granted counts fall in step with a scale-down.
//
// Without it, the scale-down patch that releases quota would be rejected by Kueue's own
// validating webhook, status.admission would stay frozen at the high-water mark, and the
// ClusterQueue would keep charging pods that no longer exist.
//
// Plain batch/v1 Jobs (and anything else without the elastic-job annotation) must keep a
// fully immutable admission, so the exception is gated on the workload being elastic.
func TestValidateAdmissionUpdateElasticScaleDown(t *testing.T) {
	features.SetFeatureGateDuringTest(t, features.ElasticJobsViaWorkloadSlices, true)

	admissionWith := func(count int32, usage string) *kueue.Admission {
		return utiltestingapi.MakeAdmission("cq").
			PodSets(utiltestingapi.MakePodSetAssignment("main").
				Assignment(corev1.ResourceMemory, "default", usage).
				Count(count).
				Obj()).
			Obj()
	}

	cases := map[string]struct {
		elastic bool
		old     *kueue.Admission
		new     *kueue.Admission
		wantErr bool
	}{
		"elastic scale-down of granted count is allowed": {
			elastic: true,
			old:     admissionWith(7, "3584Mi"),
			new:     admissionWith(3, "1536Mi"),
		},
		"elastic scale-down to zero is allowed": {
			elastic: true,
			old:     admissionWith(4, "2Gi"),
			new:     admissionWith(0, "0"),
		},
		"elastic scale-UP of granted count is still rejected": {
			// Growing the grant must go through the scheduler, which checks quota.
			elastic: true,
			old:     admissionWith(3, "1536Mi"),
			new:     admissionWith(7, "3584Mi"),
			wantErr: true,
		},
		"elastic change of flavor is still rejected": {
			elastic: true,
			old:     admissionWith(3, "1536Mi"),
			new: utiltestingapi.MakeAdmission("cq").
				PodSets(utiltestingapi.MakePodSetAssignment("main").
					Assignment(corev1.ResourceMemory, "other", "1536Mi").
					Count(3).
					Obj()).
				Obj(),
			wantErr: true,
		},
		"non-elastic scale-down is rejected: admission stays immutable for plain Jobs": {
			elastic: false,
			old:     admissionWith(7, "3584Mi"),
			new:     admissionWith(3, "1536Mi"),
			wantErr: true,
		},
		"unchanged admission is allowed": {
			elastic: true,
			old:     admissionWith(3, "1536Mi"),
			new:     admissionWith(3, "1536Mi"),
		},
	}

	for name, tc := range cases {
		t.Run(name, func(t *testing.T) {
			errs := validateAdmissionUpdate(tc.new, tc.old, nil, tc.elastic)
			if gotErr := len(errs) > 0; gotErr != tc.wantErr {
				t.Errorf("validateAdmissionUpdate() error = %v, wantErr %v", errs.ToAggregate(), tc.wantErr)
			}
		})
	}
}

// TestValidateAdmissionUpdateElasticGateOff confirms the exception is inert when the
// ElasticJobsViaWorkloadSlices feature gate is off, so clusters without elastic jobs keep
// a strictly immutable admission.
func TestValidateAdmissionUpdateElasticGateOff(t *testing.T) {
	features.SetFeatureGateDuringTest(t, features.ElasticJobsViaWorkloadSlices, false)

	old := utiltestingapi.MakeAdmission("cq").
		PodSets(utiltestingapi.MakePodSetAssignment("main").
			Assignment(corev1.ResourceMemory, "default", "3584Mi").Count(7).Obj()).
		Obj()
	newAdmission := utiltestingapi.MakeAdmission("cq").
		PodSets(utiltestingapi.MakePodSetAssignment("main").
			Assignment(corev1.ResourceMemory, "default", "1536Mi").Count(3).Obj()).
		Obj()

	if errs := validateAdmissionUpdate(newAdmission, old, nil, true); len(errs) == 0 {
		t.Error("validateAdmissionUpdate() allowed a granted-count change with the elastic feature gate off")
	}
	// Guard against the elastic path being reachable via the annotation alone.
	wl := utiltestingapi.MakeWorkload("wl", "ns").
		Annotation(workloadslicing.EnabledAnnotationKey, workloadslicing.EnabledAnnotationValue).
		Obj()
	if workloadslicing.Enabled(wl) {
		t.Error("workloadslicing.Enabled() = true with the feature gate off")
	}
}
