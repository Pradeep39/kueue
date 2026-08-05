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
	"fmt"
	"maps"
	"testing"
	"time"

	"github.com/google/go-cmp/cmp"
	"github.com/google/go-cmp/cmp/cmpopts"
	sparkappv1beta2 "github.com/kubeflow/spark-operator/v2/api/v1beta2"
	sparkcommon "github.com/kubeflow/spark-operator/v2/pkg/common"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/component-base/featuregate"
	testingclock "k8s.io/utils/clock/testing"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/interceptor"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"

	configapi "sigs.k8s.io/kueue/apis/config/v1beta2"
	kueue "sigs.k8s.io/kueue/apis/kueue/v1beta2"
	schdcache "sigs.k8s.io/kueue/pkg/cache/scheduler"
	"sigs.k8s.io/kueue/pkg/controller/jobframework"
	"sigs.k8s.io/kueue/pkg/features"
	"sigs.k8s.io/kueue/pkg/podset"
	utiltesting "sigs.k8s.io/kueue/pkg/util/testing"
	utiltestingapi "sigs.k8s.io/kueue/pkg/util/testing/v1beta2"
	sparkapplicationtesting "sigs.k8s.io/kueue/pkg/util/testingjobs/sparkapplication"
	"sigs.k8s.io/kueue/pkg/workloadslicing"
)

var (
	sparkAppCmpOpts = cmp.Options{
		cmpopts.EquateEmpty(),
		cmpopts.IgnoreFields(metav1.ObjectMeta{}, "ResourceVersion"),
	}
	workloadCmpOpts = cmp.Options{
		cmpopts.EquateEmpty(),
		cmpopts.SortSlices(func(a, b metav1.Condition) bool { return a.Type < b.Type }),
		cmpopts.IgnoreFields(kueue.Workload{}, "TypeMeta"),
		cmpopts.IgnoreFields(metav1.ObjectMeta{}, "Name", "Labels", "ResourceVersion", "OwnerReferences", "Finalizers"),
		cmpopts.IgnoreFields(kueue.WorkloadSpec{}, "Priority"),
		cmpopts.IgnoreFields(metav1.Condition{}, "LastTransitionTime"),
		cmpopts.IgnoreFields(kueue.PodSet{}, "Template"),
	}
)

func TestPodSets(t *testing.T) {
	toleration := corev1.Toleration{
		Key:      "t1k",
		Operator: corev1.TolerationOpEqual,
		Value:    "t1v",
		Effect:   corev1.TaintEffectNoExecute,
	}
	nodeSelector := map[string]string{"test": "test"}
	testSparkApp := sparkapplicationtesting.MakeSparkApplication("sparkapp", "ns")

	testCases := map[string]struct {
		sparkApp     *sparkappv1beta2.SparkApplication
		featureGates map[featuregate.Feature]bool
		want         []kueue.PodSet
	}{
		"base": {
			sparkApp: testSparkApp.Clone().
				DriverNodeSelector(maps.Clone(nodeSelector)).
				DriverTolerations([]corev1.Toleration{*toleration.DeepCopy()}).
				ExecutorNodeSelector(maps.Clone(nodeSelector)).
				ExecutorTolerations([]corev1.Toleration{*toleration.DeepCopy()}).
				ExecutorInstances(3).Obj(),
			want: []kueue.PodSet{
				*utiltestingapi.MakePodSet("driver", 1).PodSpec(corev1.PodSpec{
					NodeSelector:   maps.Clone(nodeSelector),
					Tolerations:    []corev1.Toleration{*toleration.DeepCopy()},
					InitContainers: []corev1.Container{},
					Containers: []corev1.Container{
						{
							Name: sparkcommon.SparkDriverContainerName,
							Resources: corev1.ResourceRequirements{
								Requests: corev1.ResourceList{
									corev1.ResourceCPU:    resource.MustParse("100m"),
									corev1.ResourceMemory: resource.MustParse("512Mi"),
								},
							},
						},
					},
				}).Obj(),
				*utiltestingapi.MakePodSet("executor", 3).PodSpec(corev1.PodSpec{
					NodeSelector:   maps.Clone(nodeSelector),
					Tolerations:    []corev1.Toleration{*toleration.DeepCopy()},
					InitContainers: []corev1.Container{},
					Containers: []corev1.Container{
						{
							Name: sparkcommon.Spark3DefaultExecutorContainerName,
							Resources: corev1.ResourceRequirements{
								Requests: corev1.ResourceList{
									corev1.ResourceCPU:    resource.MustParse("100m"),
									corev1.ResourceMemory: resource.MustParse("512Mi"),
								},
							},
						},
					},
				}).Obj(),
			},
		},
		"with TopologyAwareScheduling": {
			featureGates: map[featuregate.Feature]bool{features.TopologyAwareScheduling: true},
			sparkApp: testSparkApp.Clone().Queue("local-queue").
				DriverAnnotation(
					kueue.PodSetRequiredTopologyAnnotation, "cloud.com/block",
				).
				ExecutorAnnotation(
					kueue.PodSetRequiredTopologyAnnotation, "cloud.com/block",
				).Obj(),
			want: []kueue.PodSet{
				*utiltestingapi.MakePodSet("driver", 1).
					RequiredTopologyRequest("cloud.com/block").
					Annotations(map[string]string{
						kueue.PodSetRequiredTopologyAnnotation: "cloud.com/block",
					}).
					PodSpec(corev1.PodSpec{
						NodeSelector:   map[string]string{},
						Tolerations:    []corev1.Toleration{},
						InitContainers: []corev1.Container{},
						Containers: []corev1.Container{
							{
								Name: sparkcommon.SparkDriverContainerName,
								Resources: corev1.ResourceRequirements{
									Requests: corev1.ResourceList{
										corev1.ResourceCPU:    resource.MustParse("100m"),
										corev1.ResourceMemory: resource.MustParse("512Mi"),
									},
								},
							},
						},
					}).
					Obj(),
				*utiltestingapi.MakePodSet("executor", 1).
					RequiredTopologyRequest("cloud.com/block").
					Annotations(map[string]string{
						kueue.PodSetRequiredTopologyAnnotation: "cloud.com/block",
					}).
					PodSpec(corev1.PodSpec{
						NodeSelector:   map[string]string{},
						Tolerations:    []corev1.Toleration{},
						InitContainers: []corev1.Container{},
						Containers: []corev1.Container{
							{
								Name: sparkcommon.Spark3DefaultExecutorContainerName,
								Resources: corev1.ResourceRequirements{
									Requests: corev1.ResourceList{
										corev1.ResourceCPU:    resource.MustParse("100m"),
										corev1.ResourceMemory: resource.MustParse("512Mi"),
									},
								},
							},
						},
					}).Obj(),
			},
		},
		"with TopologyAwareScheduling annotation but TopologyAwareScheduling feature gate disabled": {
			featureGates: map[featuregate.Feature]bool{features.TopologyAwareScheduling: false},
			sparkApp: testSparkApp.Clone().Queue("local-queue").
				DriverAnnotation(
					kueue.PodSetRequiredTopologyAnnotation, "cloud.com/block",
				).
				ExecutorAnnotation(
					kueue.PodSetRequiredTopologyAnnotation, "cloud.com/block",
				).Obj(),
			want: []kueue.PodSet{
				*utiltestingapi.MakePodSet("driver", 1).
					Annotations(map[string]string{
						kueue.PodSetRequiredTopologyAnnotation: "cloud.com/block",
					}).
					PodSpec(corev1.PodSpec{
						NodeSelector:   map[string]string{},
						Tolerations:    []corev1.Toleration{},
						InitContainers: []corev1.Container{},
						Containers: []corev1.Container{
							{
								Name: sparkcommon.SparkDriverContainerName,
								Resources: corev1.ResourceRequirements{
									Requests: corev1.ResourceList{
										corev1.ResourceCPU:    resource.MustParse("100m"),
										corev1.ResourceMemory: resource.MustParse("512Mi"),
									},
								},
							},
						},
					}).
					Obj(),
				*utiltestingapi.MakePodSet("executor", 1).
					Annotations(map[string]string{
						kueue.PodSetRequiredTopologyAnnotation: "cloud.com/block",
					}).
					PodSpec(corev1.PodSpec{
						NodeSelector:   map[string]string{},
						Tolerations:    []corev1.Toleration{},
						InitContainers: []corev1.Container{},
						Containers: []corev1.Container{
							{
								Name: sparkcommon.Spark3DefaultExecutorContainerName,
								Resources: corev1.ResourceRequirements{
									Requests: corev1.ResourceList{
										corev1.ResourceCPU:    resource.MustParse("100m"),
										corev1.ResourceMemory: resource.MustParse("512Mi"),
									},
								},
							},
						},
					}).Obj(),
			},
		},
	}

	for name, tc := range testCases {
		t.Run(name, func(t *testing.T) {
			features.SetFeatureGatesDuringTest(t, tc.featureGates)

			ctx, _ := utiltesting.ContextWithLog(t)

			kSparkApp := fromObject(tc.sparkApp)
			got, err := kSparkApp.PodSets(ctx, nil)

			if err != nil {
				t.Fatalf("PodSets() returned error: %v", err)
			}

			if diff := cmp.Diff(tc.want, got); diff != "" {
				t.Errorf("PodSets() mismatch (-want +got):\n%s", diff)
			}
		})
	}
}

func TestGetWorkloadNameExtraPart(t *testing.T) {
	sparkApp := sparkapplicationtesting.MakeSparkApplication("sparkapp", "ns").Obj()
	sparkApp.Generation = 1

	j := fromObject(sparkApp)

	// Before workloadSequenceNumber() has ever run (cachedWorkloadSequenceNumber is nil,
	// e.g. a job type that hasn't implemented PodSets()-driven caching yet), the extra
	// part falls back to the plain generation, matching newWorkloadName()'s default for
	// job types that don't implement ElasticWorkloadNameProvider at all.
	if got, want := j.GetWorkloadNameExtraPart(), "1"; got != want {
		t.Errorf("GetWorkloadNameExtraPart() with no cached sequence number = %q, want %q", got, want)
	}

	// Two scale-up events on the same generation (Dynamic Allocation never bumps
	// generation, since it never writes to Spec) must still produce distinct extra
	// parts, or newWorkloadName() will hash the same name for both slices and the
	// second slice's Create will collide with the first, already-admitted one. A raw
	// live executor count isn't sufficient for this (a later scale-up can revisit a
	// previously-used count), so the extra part is keyed off a monotonically
	// increasing sequence number instead — see GetWorkloadNameExtraPart's doc comment.
	j.cachedWorkloadSequenceNumber = new(int32(3))
	firstSliceExtra := j.GetWorkloadNameExtraPart()

	j2 := fromObject(sparkApp)
	j2.cachedWorkloadSequenceNumber = new(int32(4))
	secondSliceExtra := j2.GetWorkloadNameExtraPart()

	if firstSliceExtra == secondSliceExtra {
		t.Errorf("GetWorkloadNameExtraPart() = %q for two different sequence numbers on the same generation, want distinct values", firstSliceExtra)
	}

	// Revisiting a previously-used executor count (e.g. Dynamic Allocation scales
	// 3 -> 5 -> 3) must NOT reproduce a prior extra part, since the sequence number
	// only ever grows within a SparkApplication's lifetime — unlike a raw live
	// executor count, which would collide here.
	j3 := fromObject(sparkApp)
	j3.cachedWorkloadSequenceNumber = new(int32(5))
	thirdSliceExtra := j3.GetWorkloadNameExtraPart()
	if thirdSliceExtra == firstSliceExtra {
		t.Errorf("GetWorkloadNameExtraPart() = %q reused a prior sequence number's extra part, want distinct values", thirdSliceExtra)
	}
}

func TestRunWithPodsetsInfo(t *testing.T) {
	toleration := corev1.Toleration{
		Key:      "t1k",
		Operator: corev1.TolerationOpEqual,
		Value:    "t1v",
		Effect:   corev1.TaintEffectNoExecute,
	}
	toleration2 := corev1.Toleration{
		Key:      "t2k",
		Operator: corev1.TolerationOpExists,
		Effect:   corev1.TaintEffectNoSchedule,
	}
	nodeSelector := map[string]string{"disktype": "ssd"}
	nodeSelector2 := map[string]string{"disktype": "hdd"}
	schedulingGate := corev1.PodSchedulingGate{Name: "test-scheduling-gate-1"}
	schedulingGate2 := corev1.PodSchedulingGate{Name: "test-scheduling-gate-2"}

	testSparkApp := sparkapplicationtesting.MakeSparkApplication("test-sparkapp", "ns")

	cases := map[string]struct {
		sparkApp     *sparkappv1beta2.SparkApplication
		podsetsInfo  []podset.PodSetInfo
		wantSparkApp *sparkappv1beta2.SparkApplication
		wantErr      bool
	}{
		"should add to the SparkApplication specified in the PodSet info": {
			sparkApp: testSparkApp.DeepCopy(),
			podsetsInfo: []podset.PodSetInfo{
				{
					Name:            "driver",
					NodeSelector:    maps.Clone(nodeSelector),
					Tolerations:     []corev1.Toleration{*toleration.DeepCopy()},
					SchedulingGates: []corev1.PodSchedulingGate{*schedulingGate.DeepCopy()},
				},
				{
					Name:            "executor",
					NodeSelector:    maps.Clone(nodeSelector),
					Tolerations:     []corev1.Toleration{*toleration.DeepCopy()},
					SchedulingGates: []corev1.PodSchedulingGate{*schedulingGate.DeepCopy()},
				},
			},
			wantSparkApp: testSparkApp.Clone().
				Suspend(false).
				DriverNodeSelector(maps.Clone(nodeSelector)).
				DriverTolerations([]corev1.Toleration{*toleration.DeepCopy()}).
				DriverTemplate(&corev1.PodTemplateSpec{
					Spec: corev1.PodSpec{
						SchedulingGates: []corev1.PodSchedulingGate{*schedulingGate.DeepCopy()},
						Containers: []corev1.Container{
							{Name: sparkcommon.SparkDriverContainerName},
						},
					},
				}).
				ExecutorNodeSelector(maps.Clone(nodeSelector)).
				ExecutorTolerations([]corev1.Toleration{*toleration.DeepCopy()}).
				ExecutorTemplate(&corev1.PodTemplateSpec{
					Spec: corev1.PodSpec{
						SchedulingGates: []corev1.PodSchedulingGate{*schedulingGate.DeepCopy()},
						Containers: []corev1.Container{
							{Name: sparkcommon.Spark3DefaultExecutorContainerName},
						},
					},
				}).
				Obj(),
			wantErr: false,
		},
		"should raise error when PodSet info config conflicts to the SparkApplication": {
			sparkApp: testSparkApp.Clone().
				DriverNodeSelector(maps.Clone(nodeSelector2)).
				DriverTolerations([]corev1.Toleration{*toleration2.DeepCopy()}).
				DriverTemplate(&corev1.PodTemplateSpec{
					Spec: corev1.PodSpec{
						SchedulingGates: []corev1.PodSchedulingGate{*schedulingGate2.DeepCopy()},
						Containers: []corev1.Container{
							{Name: sparkcommon.SparkDriverContainerName},
						},
					},
				}).
				ExecutorTolerations([]corev1.Toleration{*toleration2.DeepCopy()}).
				ExecutorNodeSelector(maps.Clone(nodeSelector2)).
				ExecutorTemplate(&corev1.PodTemplateSpec{
					Spec: corev1.PodSpec{
						SchedulingGates: []corev1.PodSchedulingGate{*schedulingGate2.DeepCopy()},
						Containers: []corev1.Container{
							{Name: sparkcommon.Spark3DefaultExecutorContainerName},
						},
					},
				}).
				Obj(),
			podsetsInfo: []podset.PodSetInfo{
				{
					Name:            "driver",
					NodeSelector:    maps.Clone(nodeSelector),
					Tolerations:     []corev1.Toleration{*toleration.DeepCopy()},
					SchedulingGates: []corev1.PodSchedulingGate{*schedulingGate.DeepCopy()},
				},
				{
					Name:            "executor",
					NodeSelector:    maps.Clone(nodeSelector),
					Tolerations:     []corev1.Toleration{*toleration.DeepCopy()},
					SchedulingGates: []corev1.PodSchedulingGate{*schedulingGate.DeepCopy()},
				},
			},
			wantErr: true,
		},
		"should apply different node selectors per role when global nodeSelector is set": {
			sparkApp: testSparkApp.Clone().
				NodeSelector(map[string]string{"zone": "us-east"}).
				Obj(),
			podsetsInfo: []podset.PodSetInfo{
				{
					Name:         "driver",
					NodeSelector: map[string]string{"zone": "us-east", "node-type": "cpu"},
				},
				{
					Name:         "executor",
					NodeSelector: map[string]string{"zone": "us-east", "node-type": "gpu"},
				},
			},
			wantSparkApp: testSparkApp.Clone().
				Suspend(false).
				DriverNodeSelector(map[string]string{"zone": "us-east", "node-type": "cpu"}).
				DriverTemplate(&corev1.PodTemplateSpec{
					Spec: corev1.PodSpec{
						Containers: []corev1.Container{
							{Name: sparkcommon.SparkDriverContainerName},
						},
					},
				}).
				ExecutorNodeSelector(map[string]string{"zone": "us-east", "node-type": "gpu"}).
				ExecutorTemplate(&corev1.PodTemplateSpec{
					Spec: corev1.PodSpec{
						Containers: []corev1.Container{
							{Name: sparkcommon.Spark3DefaultExecutorContainerName},
						},
					},
				}).
				Obj(),
			wantErr: false,
		},
		"should raise error if the wrong number of PodSet infos is provided": {
			sparkApp: testSparkApp.DeepCopy(),
			podsetsInfo: []podset.PodSetInfo{
				{
					Name:            "driver",
					NodeSelector:    maps.Clone(nodeSelector),
					Tolerations:     []corev1.Toleration{*toleration.DeepCopy()},
					SchedulingGates: []corev1.PodSchedulingGate{*schedulingGate.DeepCopy()},
				},
			},
			wantErr: true,
		},
	}

	for name, tc := range cases {
		t.Run(name, func(t *testing.T) {
			ctx, _ := utiltesting.ContextWithLog(t)

			kSparkApp := fromObject(tc.sparkApp)
			err := kSparkApp.RunWithPodSetsInfo(ctx, nil, tc.podsetsInfo)
			if tc.wantErr {
				if err == nil {
					t.Errorf("expected RunWithPodSetsInfo() to fail")
					return
				}
				return
			}

			if err != nil {
				t.Errorf("unexpected RunWithPodSetsInfo() error: %v", err)
				return
			}
			if diff := cmp.Diff(tc.wantSparkApp, tc.sparkApp, sparkAppCmpOpts); diff != "" {
				t.Errorf("RunWithPodSetsInfo() mismatch (-want,+got):\n%s", diff)
				return
			}
		})
	}
}

func TestRestorePodSetsInfo(t *testing.T) {
	toleration := corev1.Toleration{
		Key:      "t1k",
		Operator: corev1.TolerationOpEqual,
		Value:    "t1v",
		Effect:   corev1.TaintEffectNoExecute,
	}
	nodeSelector := map[string]string{"disktype": "ssd"}
	schedulingGate := corev1.PodSchedulingGate{Name: "test-scheduling-gate-1"}
	testSparkApp := sparkapplicationtesting.MakeSparkApplication("test-sparkapp", "ns")

	cases := map[string]struct {
		sparkApp     *sparkappv1beta2.SparkApplication
		podsetsInfo  []podset.PodSetInfo
		wantSparkApp *sparkappv1beta2.SparkApplication
		wantChanged  bool
	}{
		"should restore PodSet info to the SparkApplication": {
			sparkApp: testSparkApp.DeepCopy(),
			podsetsInfo: []podset.PodSetInfo{
				{
					Name:            "driver",
					NodeSelector:    maps.Clone(nodeSelector),
					Tolerations:     []corev1.Toleration{*toleration.DeepCopy()},
					SchedulingGates: []corev1.PodSchedulingGate{*schedulingGate.DeepCopy()},
				},
				{
					Name:            "executor",
					NodeSelector:    maps.Clone(nodeSelector),
					Tolerations:     []corev1.Toleration{*toleration.DeepCopy()},
					SchedulingGates: []corev1.PodSchedulingGate{*schedulingGate.DeepCopy()},
					Count:           3,
				},
			},
			wantSparkApp: testSparkApp.Clone().
				DriverNodeSelector(maps.Clone(nodeSelector)).
				DriverTolerations([]corev1.Toleration{*toleration.DeepCopy()}).
				DriverTemplate(&corev1.PodTemplateSpec{
					Spec: corev1.PodSpec{
						SchedulingGates: []corev1.PodSchedulingGate{*schedulingGate.DeepCopy()},
						Containers: []corev1.Container{
							{Name: sparkcommon.SparkDriverContainerName},
						},
					},
				}).
				ExecutorNodeSelector(maps.Clone(nodeSelector)).
				ExecutorTolerations([]corev1.Toleration{*toleration.DeepCopy()}).
				ExecutorTemplate(&corev1.PodTemplateSpec{
					Spec: corev1.PodSpec{
						SchedulingGates: []corev1.PodSchedulingGate{*schedulingGate.DeepCopy()},
						Containers: []corev1.Container{
							{Name: sparkcommon.Spark3DefaultExecutorContainerName},
						},
					},
				}).
				Obj(),
			wantChanged: true,
		},
		"should not modify the SparkApplication when already restored": {
			sparkApp: testSparkApp.Clone().
				DriverNodeSelector(maps.Clone(nodeSelector)).
				DriverTolerations([]corev1.Toleration{*toleration.DeepCopy()}).
				DriverTemplate(&corev1.PodTemplateSpec{
					Spec: corev1.PodSpec{
						SchedulingGates: []corev1.PodSchedulingGate{*schedulingGate.DeepCopy()},
						Containers: []corev1.Container{
							{Name: sparkcommon.SparkDriverContainerName},
						},
					},
				}).
				ExecutorTolerations([]corev1.Toleration{*toleration.DeepCopy()}).
				ExecutorNodeSelector(maps.Clone(nodeSelector)).
				ExecutorTemplate(&corev1.PodTemplateSpec{
					Spec: corev1.PodSpec{
						SchedulingGates: []corev1.PodSchedulingGate{*schedulingGate.DeepCopy()},
						Containers: []corev1.Container{
							{Name: sparkcommon.Spark3DefaultExecutorContainerName},
						},
					},
				}).
				ExecutorInstances(3).
				Obj(),
			podsetsInfo: []podset.PodSetInfo{
				{
					Name:            "driver",
					NodeSelector:    maps.Clone(nodeSelector),
					Tolerations:     []corev1.Toleration{*toleration.DeepCopy()},
					SchedulingGates: []corev1.PodSchedulingGate{*schedulingGate.DeepCopy()},
				},
				{
					Name:            "executor",
					NodeSelector:    maps.Clone(nodeSelector),
					Tolerations:     []corev1.Toleration{*toleration.DeepCopy()},
					SchedulingGates: []corev1.PodSchedulingGate{*schedulingGate.DeepCopy()},
					Count:           3,
				},
			},
			wantSparkApp: testSparkApp.Clone().
				DriverNodeSelector(maps.Clone(nodeSelector)).
				DriverTolerations([]corev1.Toleration{*toleration.DeepCopy()}).
				DriverTemplate(&corev1.PodTemplateSpec{
					Spec: corev1.PodSpec{
						SchedulingGates: []corev1.PodSchedulingGate{*schedulingGate.DeepCopy()},
						Containers: []corev1.Container{
							{Name: sparkcommon.SparkDriverContainerName},
						},
					},
				}).
				ExecutorNodeSelector(maps.Clone(nodeSelector)).
				ExecutorTolerations([]corev1.Toleration{*toleration.DeepCopy()}).
				ExecutorTemplate(&corev1.PodTemplateSpec{
					Spec: corev1.PodSpec{
						SchedulingGates: []corev1.PodSchedulingGate{*schedulingGate.DeepCopy()},
						Containers: []corev1.Container{
							{Name: sparkcommon.Spark3DefaultExecutorContainerName},
						},
					},
				}).
				ExecutorInstances(3).
				Obj(),
			wantChanged: false,
		},
		"should restore per-role node selectors when global nodeSelector is set": {
			sparkApp: testSparkApp.Clone().
				NodeSelector(map[string]string{"zone": "us-east"}).
				Obj(),
			podsetsInfo: []podset.PodSetInfo{
				{
					Name:         "driver",
					NodeSelector: map[string]string{"zone": "us-east", "node-type": "cpu"},
				},
				{
					Name:         "executor",
					NodeSelector: map[string]string{"zone": "us-east", "node-type": "gpu"},
					Count:        3,
				},
			},
			wantSparkApp: testSparkApp.Clone().
				DriverNodeSelector(map[string]string{"zone": "us-east", "node-type": "cpu"}).
				DriverTemplate(&corev1.PodTemplateSpec{
					Spec: corev1.PodSpec{
						Containers: []corev1.Container{
							{Name: sparkcommon.SparkDriverContainerName},
						},
					},
				}).
				ExecutorNodeSelector(map[string]string{"zone": "us-east", "node-type": "gpu"}).
				ExecutorTemplate(&corev1.PodTemplateSpec{
					Spec: corev1.PodSpec{
						Containers: []corev1.Container{
							{Name: sparkcommon.Spark3DefaultExecutorContainerName},
						},
					},
				}).
				Obj(),
			wantChanged: true,
		},
		"should not modify the SparkApplication  if the wrong number of PodSet infos is provided": {
			sparkApp: testSparkApp.DeepCopy(),
			podsetsInfo: []podset.PodSetInfo{
				{
					Name:            "driver",
					NodeSelector:    maps.Clone(nodeSelector),
					Tolerations:     []corev1.Toleration{*toleration.DeepCopy()},
					SchedulingGates: []corev1.PodSchedulingGate{*schedulingGate.DeepCopy()},
				},
			},
			wantSparkApp: testSparkApp.DeepCopy(),
			wantChanged:  false,
		},
		"should never write spec.executor.instances, even when restoring a zero count from a scaled-down Dynamic Allocation slice": {
			sparkApp: testSparkApp.DeepCopy(),
			podsetsInfo: []podset.PodSetInfo{
				{Name: "driver"},
				{Name: "executor", Count: 0},
			},
			// spec.executor.instances is CRD-validated Minimum=1; a live count of 0 must
			// never be written back onto the SparkApplication, only ever consumed by
			// liveExecutorCount() via the Workload's PodSet. testSparkApp's default
			// Instances(1) must survive untouched.
			wantSparkApp: testSparkApp.Clone().
				DriverTemplate(&corev1.PodTemplateSpec{
					Spec: corev1.PodSpec{
						Containers: []corev1.Container{
							{Name: sparkcommon.SparkDriverContainerName},
						},
					},
				}).
				ExecutorTemplate(&corev1.PodTemplateSpec{
					Spec: corev1.PodSpec{
						Containers: []corev1.Container{
							{Name: sparkcommon.Spark3DefaultExecutorContainerName},
						},
					},
				}).
				Obj(),
			wantChanged: false,
		},
	}

	for name, tc := range cases {
		t.Run(name, func(t *testing.T) {
			kSparkApp := fromObject(tc.sparkApp)
			changed := kSparkApp.RestorePodSetsInfo(t.Context(), tc.podsetsInfo)
			if diff := cmp.Diff(tc.wantChanged, changed); diff != "" {
				t.Errorf("changed mismatch (-want,+got):\n%s", diff)
				return
			}
			if diff := cmp.Diff(tc.wantSparkApp, tc.sparkApp, sparkAppCmpOpts); diff != "" {
				t.Errorf("RunWithPodSetsInfo() mismatch (-want,+got):\n%s", diff)
				return
			}
		})
	}
}

func TestReconciler(t *testing.T) {
	now := time.Now().Truncate(time.Second)

	testNamespace := utiltesting.MakeNamespaceWrapper("ns").Label(corev1.LabelMetadataName, "ns").Obj()
	const testSparkAppUID types.UID = "test-sparkapp-uid"
	testSparkApp := sparkapplicationtesting.MakeSparkApplication("test-sparkapp", testNamespace.Name)

	// Helpers for the PodsReady cases: build a SparkApplication whose status
	// simulates "driver Running and N executors in the given state". The
	// framework calls PodsReady() during Reconcile and we assert the resulting
	// Workload condition.
	withUID := func(s *sparkappv1beta2.SparkApplication) *sparkappv1beta2.SparkApplication {
		s.UID = testSparkAppUID
		return s
	}
	withExecutorsRunning := func(s *sparkappv1beta2.SparkApplication, n int32) *sparkappv1beta2.SparkApplication {
		s.Spec.Executor.Instances = new(n)
		s.Status.AppState.State = sparkappv1beta2.ApplicationStateRunning
		states := make(map[string]sparkappv1beta2.ExecutorState, n)
		for i := int32(1); i <= n; i++ {
			states[fmt.Sprintf("%s-exec-%d", s.Name, i)] = sparkappv1beta2.ExecutorStateRunning
		}
		s.Status.ExecutorState = states
		return s
	}
	withDriverRunningOnly := func(s *sparkappv1beta2.SparkApplication, expected int32) *sparkappv1beta2.SparkApplication {
		s.Spec.Executor.Instances = new(expected)
		s.Status.AppState.State = sparkappv1beta2.ApplicationStateRunning
		// No ExecutorState entries — simulates the Spark Operator state where
		// AppState flips to Running as soon as the driver starts even though
		// executors haven't reported ready yet (the bug this fix addresses).
		return s
	}

	// Build a workload admitted with the given pod sets for the given SparkApplication.
	makeAdmittedWorkloadFromPodSets := func(s *sparkappv1beta2.SparkApplication, podSets []kueue.PodSet) *utiltestingapi.WorkloadWrapper {
		psas := make([]kueue.PodSetAssignment, 0, len(podSets))
		for i := range podSets {
			psas = append(psas, utiltestingapi.MakePodSetAssignment(podSets[i].Name).Count(podSets[i].Count).Obj())
		}
		return utiltestingapi.MakeWorkload("wl", testNamespace.Name).
			Finalizers(kueue.ResourceInUseFinalizerName).
			ControllerReference(gvk, s.Name, string(s.UID)).
			PodSets(podSets...).
			ReserveQuotaAt(
				utiltestingapi.MakeAdmission("cq").PodSets(psas...).Obj(),
				now,
			).
			AdmittedAt(true, now)
	}

	// Build a workload whose PodSets are derived from the same SparkApplication
	// the framework will Reconcile, so EquivalentToWorkload returns true and
	// the workload is treated as "matching" rather than recreated.
	makeAdmittedWorkload := func(s *sparkappv1beta2.SparkApplication) *utiltestingapi.WorkloadWrapper {
		t.Helper()
		podSets, err := fromObject(s).PodSets(t.Context(), nil)
		if err != nil {
			t.Fatalf("PodSets returned error during test setup: %v", err)
		}
		return makeAdmittedWorkloadFromPodSets(s, podSets)
	}

	baseWaitForPodsReadyConf := &configapi.WaitForPodsReady{}

	// SparkApp variants used by the PodsReady cases.
	sparkAppDriverRunningOnly := withDriverRunningOnly(withUID(testSparkApp.DeepCopy()), 2)
	sparkAppAllExecutorsReady := withExecutorsRunning(withUID(testSparkApp.DeepCopy()), 2)

	// sparkAppDALiveExecutors has Dynamic Allocation enabled and a stale
	// spec.executor.instances (5) left over from before Dynamic Allocation
	// scaled the pool down to the 2 live executor Pods below — PodSets() and
	// PodsReady() must both size themselves off the live Pod count, not the
	// stale spec field.
	sparkAppDALiveExecutors := withUID(testSparkApp.DeepCopy())
	sparkAppDALiveExecutors.Spec.Executor.Instances = new(int32(5))
	sparkAppDALiveExecutors.Spec.DynamicAllocation = &sparkappv1beta2.DynamicAllocation{Enabled: true}
	sparkAppDALiveExecutors.Status.AppState.State = sparkappv1beta2.ApplicationStateRunning
	sparkAppDALiveExecutors.Status.ExecutorState = map[string]sparkappv1beta2.ExecutorState{
		fmt.Sprintf("%s-exec-1", sparkAppDALiveExecutors.Name): sparkappv1beta2.ExecutorStateRunning,
		fmt.Sprintf("%s-exec-2", sparkAppDALiveExecutors.Name): sparkappv1beta2.ExecutorStateRunning,
	}

	newLiveExecutorPod := func(name string) *corev1.Pod {
		return &corev1.Pod{
			ObjectMeta: metav1.ObjectMeta{
				Name:      name,
				Namespace: testNamespace.Name,
				Labels: map[string]string{
					sparkcommon.LabelSparkAppName: sparkAppDALiveExecutors.Name,
					sparkcommon.LabelSparkRole:    sparkcommon.SparkRoleExecutor,
				},
			},
			Status: corev1.PodStatus{Phase: corev1.PodRunning},
		}
	}
	sparkAppDALiveExecutorPods := []*corev1.Pod{
		newLiveExecutorPod(sparkAppDALiveExecutors.Name + "-exec-1"),
		newLiveExecutorPod(sparkAppDALiveExecutors.Name + "-exec-2"),
	}

	// The admitted workload reflects the live Pod count (2), not the stale
	// spec.executor.instances (5): built directly from PodSet templates rather
	// than via PodSets(ctx, nil), since a nil client can't see the live Pods.
	daDriverTemplate, err := fromObject(sparkAppDALiveExecutors).buildDriverPodTemplateSpec()
	if err != nil {
		t.Fatalf("failed building driver template for test setup: %v", err)
	}
	daExecutorTemplate, err := fromObject(sparkAppDALiveExecutors).buildExecutorPodTemplateSpec()
	if err != nil {
		t.Fatalf("failed building executor template for test setup: %v", err)
	}
	sparkAppDALivePodSets := []kueue.PodSet{
		{Name: driverPodSetName, Template: *daDriverTemplate, Count: 1},
		{Name: executorPodSetName, Template: *daExecutorTemplate, Count: 2},
	}

	cases := map[string]struct {
		reconcilerOptions []jobframework.Option
		sparkApp          *sparkappv1beta2.SparkApplication
		executorPods      []*corev1.Pod
		workloads         []kueue.Workload
		wantWorkloads     []kueue.Workload
	}{
		"workload is created with the corresponding podsets": {
			reconcilerOptions: []jobframework.Option{
				jobframework.WithManageJobsWithoutQueueName(true),
				jobframework.WithManagedJobsNamespaceSelector(labels.Everything()),
			},
			sparkApp: testSparkApp.DeepCopy(),
			wantWorkloads: []kueue.Workload{
				*utiltestingapi.MakeWorkload(testSparkApp.Name, testSparkApp.Namespace).
					PodSets(
						*utiltestingapi.MakePodSet("driver", 1).Obj(),
						*utiltestingapi.MakePodSet("executor", 1).Obj(),
					).
					Obj(),
			},
		},
		"PodsReady stays False/WaitForStart while AppState=Running but no executors reported ready": {
			reconcilerOptions: []jobframework.Option{
				jobframework.WithManageJobsWithoutQueueName(true),
				jobframework.WithManagedJobsNamespaceSelector(labels.Everything()),
				jobframework.WithWaitForPodsReady(baseWaitForPodsReadyConf),
			},
			sparkApp: sparkAppDriverRunningOnly,
			workloads: []kueue.Workload{
				*makeAdmittedWorkload(sparkAppDriverRunningOnly).Obj(),
			},
			wantWorkloads: []kueue.Workload{
				*makeAdmittedWorkload(sparkAppDriverRunningOnly).
					Condition(metav1.Condition{
						Type:    kueue.WorkloadPodsReady,
						Status:  metav1.ConditionFalse,
						Reason:  kueue.WorkloadWaitForStart,
						Message: "Not all pods are ready or succeeded",
					}).
					Obj(),
			},
		},
		"PodsReady becomes True/Started after AppState=Running and all executors reach Running": {
			reconcilerOptions: []jobframework.Option{
				jobframework.WithManageJobsWithoutQueueName(true),
				jobframework.WithManagedJobsNamespaceSelector(labels.Everything()),
				jobframework.WithWaitForPodsReady(baseWaitForPodsReadyConf),
			},
			sparkApp: sparkAppAllExecutorsReady,
			workloads: []kueue.Workload{
				*makeAdmittedWorkload(sparkAppAllExecutorsReady).Obj(),
			},
			wantWorkloads: []kueue.Workload{
				*makeAdmittedWorkload(sparkAppAllExecutorsReady).
					Condition(metav1.Condition{
						Type:    kueue.WorkloadPodsReady,
						Status:  metav1.ConditionTrue,
						Reason:  kueue.WorkloadStarted,
						Message: "All pods reached readiness and the workload is running",
					}).
					Obj(),
			},
		},
		"PodsReady under Dynamic Allocation sizes off the live executor Pod count, not stale spec.executor.instances": {
			reconcilerOptions: []jobframework.Option{
				jobframework.WithManageJobsWithoutQueueName(true),
				jobframework.WithManagedJobsNamespaceSelector(labels.Everything()),
				jobframework.WithWaitForPodsReady(baseWaitForPodsReadyConf),
			},
			sparkApp:     sparkAppDALiveExecutors,
			executorPods: sparkAppDALiveExecutorPods,
			workloads: []kueue.Workload{
				*makeAdmittedWorkloadFromPodSets(sparkAppDALiveExecutors, sparkAppDALivePodSets).Obj(),
			},
			wantWorkloads: []kueue.Workload{
				*makeAdmittedWorkloadFromPodSets(sparkAppDALiveExecutors, sparkAppDALivePodSets).
					Condition(metav1.Condition{
						Type:    kueue.WorkloadPodsReady,
						Status:  metav1.ConditionTrue,
						Reason:  kueue.WorkloadStarted,
						Message: "All pods reached readiness and the workload is running",
					}).
					Obj(),
			},
		},
	}

	for name, tc := range cases {
		t.Run(name, func(t *testing.T) {
			ctx, _ := utiltesting.ContextWithLog(t)

			clientBuilder := utiltesting.NewClientBuilder(sparkappv1beta2.AddToScheme).
				WithInterceptorFuncs(interceptor.Funcs{SubResourcePatch: utiltesting.TreatSSAAsStrategicMerge})
			objs := []client.Object{tc.sparkApp, testNamespace}
			for _, pod := range tc.executorPods {
				objs = append(objs, pod)
			}
			kClient := clientBuilder.
				WithObjects(objs...).
				WithStatusSubresource(&kueue.Workload{}).
				Build()
			// Pre-existing workloads must be created via the client (not WithObjects)
			// so that the status subresource is initialized correctly.
			for i := range tc.workloads {
				if err := kClient.Create(ctx, &tc.workloads[i]); err != nil {
					t.Fatalf("Could not create pre-existing workload: %v", err)
				}
			}
			indexer := utiltesting.AsIndexer(clientBuilder)
			if err := SetupIndexes(ctx, indexer); err != nil {
				t.Fatalf("Could not setup indexes: %v", err)
			}
			recorder := &utiltesting.EventRecorder{}
			reconciler, err := NewReconciler(ctx, kClient, indexer, recorder,
				append(tc.reconcilerOptions,
					jobframework.WithCache(schdcache.New(kClient)),
					jobframework.WithClock(testingclock.NewFakeClock(now)),
				)...)
			if err != nil {
				t.Errorf("Error creating the reconciler: %v", err)
			}

			sparkAppKey := client.ObjectKeyFromObject(tc.sparkApp)
			_, err = reconciler.Reconcile(ctx, reconcile.Request{
				NamespacedName: sparkAppKey,
			})
			if err != nil {
				t.Errorf("Reconcile returned error: %v", err)
			}

			var gotWorkloads kueue.WorkloadList
			if err := kClient.List(ctx, &gotWorkloads); err != nil {
				t.Fatalf("Could not get Workloads after reconcile: %v", err)
			}
			if diff := cmp.Diff(tc.wantWorkloads, gotWorkloads.Items, workloadCmpOpts...); diff != "" {
				t.Errorf("Workloads after reconcile (-want,+got):\n%s", diff)
			}
		})
	}
}

// TestReconcilerElasticScaleUpAvoidsStaleSliceNameCollision reproduces, end-to-end
// through a real Reconcile() call, the bug workloadSequenceNumber() fixes: Dynamic
// Allocation scaling to a live executor count that a superseded-but-never-deleted
// Finished slice already claimed the deterministic name for, under the old naming
// scheme that folded in the raw live executor count instead of a monotonic sequence
// number. Before the fix, this scenario's Create call returned AlreadyExists and no
// new slice was ever created for that SparkApplication again.
func TestReconcilerElasticScaleUpAvoidsStaleSliceNameCollision(t *testing.T) {
	features.SetFeatureGatesDuringTest(t, map[featuregate.Feature]bool{features.ElasticJobsViaWorkloadSlices: true})

	ctx, _ := utiltesting.ContextWithLog(t)

	testNamespace := utiltesting.MakeNamespaceWrapper("ns").Label(corev1.LabelMetadataName, "ns").Obj()
	const testSparkAppUID types.UID = "elastic-sparkapp-uid"

	sparkApp := sparkapplicationtesting.MakeSparkApplication("elastic-app", testNamespace.Name).
		Annotation(workloadslicing.EnabledAnnotationKey, workloadslicing.EnabledAnnotationValue).
		DynamicAllocation(&sparkappv1beta2.DynamicAllocation{Enabled: true}).
		Obj()
	sparkApp.UID = testSparkAppUID
	sparkApp.Status.AppState.State = sparkappv1beta2.ApplicationStateRunning

	newLiveExecutorPod := func(name string) *corev1.Pod {
		return &corev1.Pod{
			ObjectMeta: metav1.ObjectMeta{
				Name:      name,
				Namespace: testNamespace.Name,
				Labels: map[string]string{
					sparkcommon.LabelSparkAppName: sparkApp.Name,
					sparkcommon.LabelSparkRole:    sparkcommon.SparkRoleExecutor,
				},
			},
			Status: corev1.PodStatus{Phase: corev1.PodRunning},
		}
	}
	executorPods := []*corev1.Pod{
		newLiveExecutorPod(sparkApp.Name + "-exec-1"),
		newLiveExecutorPod(sparkApp.Name + "-exec-2"),
		newLiveExecutorPod(sparkApp.Name + "-exec-3"),
	}

	// staleFinishedSlice simulates a slice created by an earlier scale-up to the same
	// live executor count (3), using the pre-fix deterministic name — extra part
	// "<generation>_3", with no sequence number folded in. It was later superseded
	// and Finished, but never deleted (no retention policy configured), leaving its
	// name permanently claimed in etcd.
	staleFinishedSliceName := jobframework.GenerateWorkloadNameWithExtra(
		sparkApp.Name, sparkApp.UID, gvk, fmt.Sprintf("%d_3", sparkApp.Generation),
	)
	staleFinishedSlice := utiltestingapi.MakeWorkload(staleFinishedSliceName, testNamespace.Name).
		ControllerReference(gvk, sparkApp.Name, string(sparkApp.UID)).
		Condition(metav1.Condition{
			Type:   kueue.WorkloadFinished,
			Status: metav1.ConditionTrue,
			Reason: "OutOfSync",
		}).
		Obj()

	clientBuilder := utiltesting.NewClientBuilder(sparkappv1beta2.AddToScheme).
		WithInterceptorFuncs(interceptor.Funcs{SubResourcePatch: utiltesting.TreatSSAAsStrategicMerge})
	objs := []client.Object{sparkApp, testNamespace}
	for _, pod := range executorPods {
		objs = append(objs, pod)
	}
	kClient := clientBuilder.
		WithObjects(objs...).
		WithStatusSubresource(&kueue.Workload{}).
		Build()
	if err := kClient.Create(ctx, staleFinishedSlice); err != nil {
		t.Fatalf("Could not create pre-existing stale Finished slice: %v", err)
	}

	indexer := utiltesting.AsIndexer(clientBuilder)
	if err := SetupIndexes(ctx, indexer); err != nil {
		t.Fatalf("Could not setup indexes: %v", err)
	}
	recorder := &utiltesting.EventRecorder{}
	reconciler, err := NewReconciler(ctx, kClient, indexer, recorder,
		jobframework.WithManageJobsWithoutQueueName(true),
		jobframework.WithManagedJobsNamespaceSelector(labels.Everything()),
		jobframework.WithCache(schdcache.New(kClient)),
		jobframework.WithClock(testingclock.NewFakeClock(time.Now())),
	)
	if err != nil {
		t.Fatalf("Error creating the reconciler: %v", err)
	}

	if _, err := reconciler.Reconcile(ctx, reconcile.Request{NamespacedName: client.ObjectKeyFromObject(sparkApp)}); err != nil {
		t.Fatalf("Reconcile returned an unexpected error (likely the AlreadyExists name collision this test guards against): %v", err)
	}

	var gotWorkloads kueue.WorkloadList
	if err := kClient.List(ctx, &gotWorkloads); err != nil {
		t.Fatalf("Could not list Workloads after reconcile: %v", err)
	}

	var newSlices []kueue.Workload
	for _, wl := range gotWorkloads.Items {
		if wl.Name != staleFinishedSlice.Name {
			newSlices = append(newSlices, wl)
		}
	}
	if len(newSlices) != 1 {
		t.Fatalf("got %d new workload slices besides the stale Finished one, want exactly 1: %v", len(newSlices), gotWorkloads.Items)
	}
	if newSlices[0].Name == staleFinishedSlice.Name {
		t.Errorf("new slice reused the stale Finished slice's name %q", staleFinishedSlice.Name)
	}
}
