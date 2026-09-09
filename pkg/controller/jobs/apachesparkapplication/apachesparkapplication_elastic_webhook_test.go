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
	"slices"
	"testing"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime/schema"

	kueue "sigs.k8s.io/kueue/apis/kueue/v1beta2"
	"sigs.k8s.io/kueue/pkg/controller/jobframework"
	sparkv1 "sigs.k8s.io/kueue/pkg/controller/jobs/apachesparkapplication/api/v1"
	"sigs.k8s.io/kueue/pkg/features"
	utilpod "sigs.k8s.io/kueue/pkg/util/pod"
	"sigs.k8s.io/kueue/pkg/workloadslicing"
)

func elasticApp(spec sparkv1.ApplicationSpec) *sparkv1.SparkApplication {
	return &sparkv1.SparkApplication{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "pi",
			Namespace: "ns",
			Annotations: map[string]string{
				workloadslicing.EnabledAnnotationKey: workloadslicing.EnabledAnnotationValue,
			},
		},
		Spec: spec,
	}
}

func hasElasticGate(app *sparkv1.SparkApplication) bool {
	template := templateSpec(app, roleExecutor)
	if template == nil {
		return false
	}
	return slices.Contains(template.Spec.SchedulingGates,
		corev1.PodSchedulingGate{Name: kueue.ElasticJobSchedulingGate})
}

func TestIsAnElasticJob(t *testing.T) {
	// ElasticJobsViaWorkloadSlices has been Beta and enabled by default since 0.18, so
	// this has to be turned off explicitly rather than relying on the default.
	t.Run("annotation without the feature gate is not elastic", func(t *testing.T) {
		features.SetFeatureGateDuringTest(t, features.ElasticJobsViaWorkloadSlices, false)
		if isAnElasticJob(elasticApp(sparkv1.ApplicationSpec{})) {
			t.Error("isAnElasticJob() = true with the feature gate off, want false")
		}
	})

	t.Run("annotation with the feature gate is elastic", func(t *testing.T) {
		features.SetFeatureGateDuringTest(t, features.ElasticJobsViaWorkloadSlices, true)
		if !isAnElasticJob(elasticApp(sparkv1.ApplicationSpec{})) {
			t.Error("isAnElasticJob() = false with annotation and gate set, want true")
		}
	})

	t.Run("no annotation is not elastic", func(t *testing.T) {
		features.SetFeatureGateDuringTest(t, features.ElasticJobsViaWorkloadSlices, true)
		if isAnElasticJob(&sparkv1.SparkApplication{}) {
			t.Error("isAnElasticJob() = true without the annotation, want false")
		}
	})
}

// TestGateInjection covers what Default() does for an elastic job. The gate has to land on
// the executor pod template because the operator materialises that template into the file it
// passes as spark.kubernetes.executor.podTemplateFile, which is the only way a
// Dynamic-Allocation-created executor pod inherits it.
func TestGateInjection(t *testing.T) {
	cases := map[string]struct {
		spec sparkv1.ApplicationSpec
	}{
		"creates the template when absent": {},
		"preserves an existing template": {
			spec: sparkv1.ApplicationSpec{
				ExecutorSpec: &sparkv1.BaseApplicationTemplateSpec{
					PodTemplateSpec: &corev1.PodTemplateSpec{Spec: corev1.PodSpec{
						Containers: []corev1.Container{{Name: defaultExecutorContainerName}},
					}},
				},
			},
		},
	}

	for name, tc := range cases {
		t.Run(name, func(t *testing.T) {
			app := elasticApp(tc.spec)
			job := fromObject(app)

			utilpod.GateTemplate(job.ensureTemplateSpec(roleExecutor), kueue.ElasticJobSchedulingGate)

			if !hasElasticGate(app) {
				t.Errorf("executor template is not gated: %v",
					templateSpec(app, roleExecutor).Spec.SchedulingGates)
			}
			if tc.spec.ExecutorSpec != nil {
				if got := templateSpec(app, roleExecutor).Spec.Containers; len(got) != 1 {
					t.Errorf("containers = %v, want the original container preserved", got)
				}
			}
		})
	}

	t.Run("gating twice does not duplicate the gate", func(t *testing.T) {
		app := elasticApp(sparkv1.ApplicationSpec{})
		job := fromObject(app)
		utilpod.GateTemplate(job.ensureTemplateSpec(roleExecutor), kueue.ElasticJobSchedulingGate)
		utilpod.GateTemplate(job.ensureTemplateSpec(roleExecutor), kueue.ElasticJobSchedulingGate)

		if got := templateSpec(app, roleExecutor).Spec.SchedulingGates; len(got) != 1 {
			t.Errorf("schedulingGates = %v, want exactly one", got)
		}
	})
}

func TestValidateElasticJob(t *testing.T) {
	cases := map[string]struct {
		spec    sparkv1.ApplicationSpec
		wantErr bool
	}{
		"no executor template at all is rejected": {wantErr: true},
		"executor template without the gate is rejected": {
			spec: sparkv1.ApplicationSpec{
				ExecutorSpec: &sparkv1.BaseApplicationTemplateSpec{
					PodTemplateSpec: &corev1.PodTemplateSpec{},
				},
			},
			wantErr: true,
		},
		"an unrelated gate does not satisfy the requirement": {
			spec: sparkv1.ApplicationSpec{
				ExecutorSpec: &sparkv1.BaseApplicationTemplateSpec{
					PodTemplateSpec: &corev1.PodTemplateSpec{Spec: corev1.PodSpec{
						SchedulingGates: []corev1.PodSchedulingGate{{Name: "example.com/other"}},
					}},
				},
			},
			wantErr: true,
		},
		"the elastic gate is accepted": {
			spec: sparkv1.ApplicationSpec{
				ExecutorSpec: &sparkv1.BaseApplicationTemplateSpec{
					PodTemplateSpec: &corev1.PodTemplateSpec{Spec: corev1.PodSpec{
						SchedulingGates: []corev1.PodSchedulingGate{
							{Name: kueue.ElasticJobSchedulingGate},
						},
					}},
				},
			},
		},
	}

	for name, tc := range cases {
		t.Run(name, func(t *testing.T) {
			errs := validateElasticJob(elasticApp(tc.spec))
			if gotErr := len(errs) > 0; gotErr != tc.wantErr {
				t.Errorf("validateElasticJob() errors = %v, wantErr %v", errs, tc.wantErr)
			}
		})
	}
}

// TestDefaultGatesOnlyElasticJobs makes sure a plain application is not gated, since an
// un-ungated gate on a non-elastic job would leave its executors unschedulable forever.
func TestDefaultGatesOnlyElasticJobs(t *testing.T) {
	features.SetFeatureGateDuringTest(t, features.ElasticJobsViaWorkloadSlices, true)

	plain := &sparkv1.SparkApplication{
		ObjectMeta: metav1.ObjectMeta{Name: "pi", Namespace: "ns"},
	}
	if isAnElasticJob(plain) {
		t.Fatal("a plain application should not be elastic")
	}
	if hasElasticGate(plain) {
		t.Error("a plain application must not carry the elastic scheduling gate")
	}
}

// TestGVKIsAllowedForElasticJobs guards against the framework-level allowlist in
// jobframework.ValidateElasticJobAnnotation, which is keyed by GVK and independent of
// anything this package registers. Omitting the GVK there rejects every elastic
// SparkApplication at admission with "elastic job is not supported", no matter how complete
// the integration is.
func TestGVKIsAllowedForElasticJobs(t *testing.T) {
	features.SetFeatureGateDuringTest(t, features.ElasticJobsViaWorkloadSlices, true)

	app := elasticApp(sparkv1.ApplicationSpec{})

	if errs := jobframework.ValidateElasticJobAnnotation(app, gvk); len(errs) > 0 {
		t.Errorf("ValidateElasticJobAnnotation() rejected %s: %v", gvk, errs)
	}

	// Negative control: an unrelated GVK must still be rejected, so the assertion above
	// is not passing because the check is inert.
	other := schema.GroupVersionKind{Group: "example.com", Version: "v1", Kind: "Widget"}
	if errs := jobframework.ValidateElasticJobAnnotation(app, other); len(errs) == 0 {
		t.Errorf("ValidateElasticJobAnnotation() accepted %s, want it rejected", other)
	}
}
