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
	"time"

	"github.com/google/go-cmp/cmp"
	"github.com/google/go-cmp/cmp/cmpopts"
	sparkappv1beta2 "github.com/kubeflow/spark-operator/v2/api/v1beta2"
	sparkcommon "github.com/kubeflow/spark-operator/v2/pkg/common"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	clientgoscheme "k8s.io/client-go/kubernetes/scheme"
	"k8s.io/client-go/rest"
	"k8s.io/component-base/featuregate"
	"k8s.io/utils/ptr"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	metricsserver "sigs.k8s.io/controller-runtime/pkg/metrics/server"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"

	"sigs.k8s.io/kueue/pkg/features"
	utiltesting "sigs.k8s.io/kueue/pkg/util/testing"
	testingjobspod "sigs.k8s.io/kueue/pkg/util/testingjobs/pod"
	sparkapplicationtesting "sigs.k8s.io/kueue/pkg/util/testingjobs/sparkapplication"
	"sigs.k8s.io/kueue/pkg/workloadslicing"
)

func makeExecutorPod(name, ns, appName string) *testingjobspod.PodWrapper {
	return testingjobspod.MakePod(name, ns).
		Label(sparkcommon.LabelSparkAppName, appName).
		Label(sparkcommon.LabelSparkRole, sparkcommon.SparkRoleExecutor)
}

func makeDriverPod(name, ns, appName string) *testingjobspod.PodWrapper {
	return testingjobspod.MakePod(name, ns).
		Label(sparkcommon.LabelSparkAppName, appName).
		Label(sparkcommon.LabelSparkRole, sparkcommon.SparkRoleDriver)
}

func elasticDynamicAllocationSparkApp(name, ns string) *sparkapplicationtesting.SparkApplicationWrapper {
	return sparkapplicationtesting.MakeSparkApplication(name, ns).
		Annotation(workloadslicing.EnabledAnnotationKey, workloadslicing.EnabledAnnotationValue).
		DynamicAllocation(&sparkappv1beta2.DynamicAllocation{Enabled: true})
}

func TestExecutorInstancesReconciler(t *testing.T) {
	now := time.Now()

	cases := map[string]struct {
		sparkApp         *sparkappv1beta2.SparkApplication
		pods             []*corev1.Pod
		featureGates     map[featuregate.Feature]bool
		reconcileMissing bool
		wantInstances    *int32
		wantErr          error
	}{
		"scale up: live executor pods exceed spec.executor.instances": {
			sparkApp: elasticDynamicAllocationSparkApp("app", "ns").ExecutorInstances(1).Obj(),
			pods: []*corev1.Pod{
				makeExecutorPod("exec-1", "ns", "app").StatusPhase(corev1.PodRunning).Obj(),
				makeExecutorPod("exec-2", "ns", "app").StatusPhase(corev1.PodRunning).Obj(),
				makeExecutorPod("exec-3", "ns", "app").StatusPhase(corev1.PodRunning).Obj(),
			},
			wantInstances: ptr.To[int32](3),
		},
		"scale down: an executor pod was deleted by dynamic allocation": {
			sparkApp: elasticDynamicAllocationSparkApp("app", "ns").ExecutorInstances(3).Obj(),
			pods: []*corev1.Pod{
				makeExecutorPod("exec-1", "ns", "app").StatusPhase(corev1.PodRunning).Obj(),
				makeExecutorPod("exec-2", "ns", "app").StatusPhase(corev1.PodRunning).Obj(),
			},
			wantInstances: ptr.To[int32](2),
		},
		"scale down to zero live executors: clamped to 1 since spec.executor.instances requires >= 1": {
			sparkApp: elasticDynamicAllocationSparkApp("app", "ns").ExecutorInstances(3).Obj(),
			pods: []*corev1.Pod{
				makeExecutorPod("exec-1", "ns", "app").StatusPhase(corev1.PodSucceeded).Obj(),
				makeExecutorPod("exec-2", "ns", "app").StatusPhase(corev1.PodSucceeded).Obj(),
			},
			wantInstances: ptr.To[int32](1),
		},
		"no-op: live count already matches spec.executor.instances": {
			sparkApp: elasticDynamicAllocationSparkApp("app", "ns").ExecutorInstances(2).Obj(),
			pods: []*corev1.Pod{
				makeExecutorPod("exec-1", "ns", "app").StatusPhase(corev1.PodRunning).Obj(),
				makeExecutorPod("exec-2", "ns", "app").StatusPhase(corev1.PodRunning).Obj(),
			},
			wantInstances: ptr.To[int32](2),
		},
		"pending/unscheduled executor pods still count towards the live total": {
			sparkApp: elasticDynamicAllocationSparkApp("app", "ns").ExecutorInstances(1).Obj(),
			pods: []*corev1.Pod{
				makeExecutorPod("exec-1", "ns", "app").StatusPhase(corev1.PodRunning).Obj(),
				makeExecutorPod("exec-2", "ns", "app").StatusPhase(corev1.PodPending).Obj(),
			},
			wantInstances: ptr.To[int32](2),
		},
		"terminal (succeeded/failed) executor pods are excluded from the live total": {
			sparkApp: elasticDynamicAllocationSparkApp("app", "ns").ExecutorInstances(3).Obj(),
			pods: []*corev1.Pod{
				makeExecutorPod("exec-1", "ns", "app").StatusPhase(corev1.PodRunning).Obj(),
				makeExecutorPod("exec-2", "ns", "app").StatusPhase(corev1.PodSucceeded).Obj(),
				makeExecutorPod("exec-3", "ns", "app").StatusPhase(corev1.PodFailed).Obj(),
			},
			wantInstances: ptr.To[int32](1),
		},
		"executor pods already terminating (DeletionTimestamp set) are excluded from the live total": {
			sparkApp: elasticDynamicAllocationSparkApp("app", "ns").ExecutorInstances(2).Obj(),
			pods: []*corev1.Pod{
				makeExecutorPod("exec-1", "ns", "app").StatusPhase(corev1.PodRunning).Obj(),
				makeExecutorPod("exec-2", "ns", "app").StatusPhase(corev1.PodRunning).
					Finalizer("kueue.x-k8s.io/test").DeletionTimestamp(now).Obj(),
			},
			wantInstances: ptr.To[int32](1),
		},
		"driver pod for the same application is not counted as an executor": {
			sparkApp: elasticDynamicAllocationSparkApp("app", "ns").ExecutorInstances(1).Obj(),
			pods: []*corev1.Pod{
				makeDriverPod("driver", "ns", "app").StatusPhase(corev1.PodRunning).Obj(),
				makeExecutorPod("exec-1", "ns", "app").StatusPhase(corev1.PodRunning).Obj(),
			},
			wantInstances: ptr.To[int32](1),
		},
		"executor pods belonging to a different SparkApplication are ignored": {
			sparkApp: elasticDynamicAllocationSparkApp("app", "ns").ExecutorInstances(1).Obj(),
			pods: []*corev1.Pod{
				makeExecutorPod("exec-1", "ns", "app").StatusPhase(corev1.PodRunning).Obj(),
				makeExecutorPod("other-exec-1", "ns", "other-app").StatusPhase(corev1.PodRunning).Obj(),
				makeExecutorPod("other-exec-2", "ns", "other-app").StatusPhase(corev1.PodRunning).Obj(),
			},
			wantInstances: ptr.To[int32](1),
		},
		"not an elastic job: spec.executor.instances is left untouched even if pods scaled": {
			sparkApp: sparkapplicationtesting.MakeSparkApplication("app", "ns").
				DynamicAllocation(&sparkappv1beta2.DynamicAllocation{Enabled: true}).
				ExecutorInstances(1).Obj(),
			pods: []*corev1.Pod{
				makeExecutorPod("exec-1", "ns", "app").StatusPhase(corev1.PodRunning).Obj(),
				makeExecutorPod("exec-2", "ns", "app").StatusPhase(corev1.PodRunning).Obj(),
			},
			wantInstances: ptr.To[int32](1),
		},
		"dynamic allocation disabled: spec.executor.instances is left untouched even if pods scaled": {
			sparkApp: sparkapplicationtesting.MakeSparkApplication("app", "ns").
				Annotation(workloadslicing.EnabledAnnotationKey, workloadslicing.EnabledAnnotationValue).
				DynamicAllocation(&sparkappv1beta2.DynamicAllocation{Enabled: false}).
				ExecutorInstances(1).Obj(),
			pods: []*corev1.Pod{
				makeExecutorPod("exec-1", "ns", "app").StatusPhase(corev1.PodRunning).Obj(),
				makeExecutorPod("exec-2", "ns", "app").StatusPhase(corev1.PodRunning).Obj(),
			},
			wantInstances: ptr.To[int32](1),
		},
		"dynamic allocation configured via sparkConf only: spec.executor.instances is still synced": {
			sparkApp: sparkapplicationtesting.MakeSparkApplication("app", "ns").
				Annotation(workloadslicing.EnabledAnnotationKey, workloadslicing.EnabledAnnotationValue).
				SparkConf("spark.dynamicAllocation.enabled", "true").
				ExecutorInstances(1).Obj(),
			pods: []*corev1.Pod{
				makeExecutorPod("exec-1", "ns", "app").StatusPhase(corev1.PodRunning).Obj(),
				makeExecutorPod("exec-2", "ns", "app").StatusPhase(corev1.PodRunning).Obj(),
				makeExecutorPod("exec-3", "ns", "app").StatusPhase(corev1.PodRunning).Obj(),
			},
			wantInstances: ptr.To[int32](3),
		},
		"feature gate disabled: spec.executor.instances is left untouched even if pods scaled": {
			featureGates: map[featuregate.Feature]bool{features.ElasticJobsViaWorkloadSlices: false},
			sparkApp:     elasticDynamicAllocationSparkApp("app", "ns").ExecutorInstances(1).Obj(),
			pods: []*corev1.Pod{
				makeExecutorPod("exec-1", "ns", "app").StatusPhase(corev1.PodRunning).Obj(),
				makeExecutorPod("exec-2", "ns", "app").StatusPhase(corev1.PodRunning).Obj(),
			},
			wantInstances: ptr.To[int32](1),
		},
		"SparkApplication no longer exists: reconcile is a no-op, not an error": {
			sparkApp:         elasticDynamicAllocationSparkApp("app", "ns").ExecutorInstances(1).Obj(),
			reconcileMissing: true,
		},
	}

	for name, tc := range cases {
		t.Run(name, func(t *testing.T) {
			featureGates := tc.featureGates
			if featureGates == nil {
				featureGates = map[featuregate.Feature]bool{features.ElasticJobsViaWorkloadSlices: true}
			} else if _, ok := featureGates[features.ElasticJobsViaWorkloadSlices]; !ok {
				featureGates[features.ElasticJobsViaWorkloadSlices] = true
			}
			features.SetFeatureGatesDuringTest(t, featureGates)

			ctx, _ := utiltesting.ContextWithLog(t)

			objs := []client.Object{tc.sparkApp}
			for _, p := range tc.pods {
				objs = append(objs, p)
			}
			kClient := utiltesting.NewClientBuilder(sparkappv1beta2.AddToScheme).WithObjects(objs...).Build()

			reconciler, err := NewExecutorInstancesReconciler(ctx, kClient, nil, nil)
			if err != nil {
				t.Fatalf("Error creating the reconciler: %v", err)
			}

			req := reconcile.Request{NamespacedName: types.NamespacedName{Namespace: tc.sparkApp.Namespace, Name: tc.sparkApp.Name}}
			if tc.reconcileMissing {
				if err := kClient.Delete(ctx, tc.sparkApp); err != nil {
					t.Fatalf("Failed deleting SparkApplication ahead of reconcile: %v", err)
				}
			}

			_, err = reconciler.Reconcile(ctx, req)
			if diff := cmp.Diff(tc.wantErr, err, cmpopts.EquateErrors()); diff != "" {
				t.Errorf("Reconcile returned error (-want,+got):\n%s", diff)
			}
			if tc.reconcileMissing {
				return
			}

			gotSparkApp := &sparkappv1beta2.SparkApplication{}
			if err := kClient.Get(ctx, req.NamespacedName, gotSparkApp); err != nil {
				t.Fatalf("Could not get SparkApplication after reconcile: %v", err)
			}

			if diff := cmp.Diff(tc.wantInstances, gotSparkApp.Spec.Executor.Instances); diff != "" {
				t.Errorf("spec.executor.instances after reconcile (-want,+got):\n%s", diff)
			}
		})
	}
}

func TestIsTrackedExecutorPod(t *testing.T) {
	cases := map[string]struct {
		obj  client.Object
		want bool
	}{
		"executor pod with app-name label": {
			obj:  makeExecutorPod("exec-1", "ns", "app").Obj(),
			want: true,
		},
		"driver pod is not tracked": {
			obj:  makeDriverPod("driver", "ns", "app").Obj(),
			want: false,
		},
		"executor pod without app-name label is not tracked": {
			obj: testingjobspod.MakePod("exec-1", "ns").
				Label(sparkcommon.LabelSparkRole, sparkcommon.SparkRoleExecutor).Obj(),
			want: false,
		},
		"non-pod object is not tracked": {
			obj:  &sparkappv1beta2.SparkApplication{},
			want: false,
		},
	}

	for name, tc := range cases {
		t.Run(name, func(t *testing.T) {
			if got := isTrackedExecutorPod(tc.obj); got != tc.want {
				t.Errorf("isTrackedExecutorPod() = %v, want %v", got, tc.want)
			}
		})
	}
}

func TestMapExecutorPodToSparkApplication(t *testing.T) {
	ctx, _ := utiltesting.ContextWithLog(t)

	cases := map[string]struct {
		obj  client.Object
		want []reconcile.Request
	}{
		"executor pod maps to its owning SparkApplication": {
			obj:  makeExecutorPod("exec-1", "ns", "app").Obj(),
			want: []reconcile.Request{{NamespacedName: types.NamespacedName{Namespace: "ns", Name: "app"}}},
		},
		"pod without app-name label maps to nothing": {
			obj:  testingjobspod.MakePod("exec-1", "ns").Obj(),
			want: nil,
		},
		"non-pod object maps to nothing": {
			obj:  &sparkappv1beta2.SparkApplication{},
			want: nil,
		},
	}

	for name, tc := range cases {
		t.Run(name, func(t *testing.T) {
			got := mapExecutorPodToSparkApplication(ctx, tc.obj)
			if diff := cmp.Diff(tc.want, got); diff != "" {
				t.Errorf("mapExecutorPodToSparkApplication() (-want,+got):\n%s", diff)
			}
		})
	}
}

// TestSetupWithManager verifies that the reconciler's watch/predicate/handler wiring
// (For, Watches, the predicate, and the mapping function) is accepted by a real
// controller-runtime manager. It intentionally never calls mgr.Start(): the informer/cache
// machinery that would need a live API server only runs on Start(), while
// controller registration itself (what this test exercises) does not, so this catches
// wiring mistakes (bad GVKs, incompatible predicate/handler types, duplicate controller
// names) without requiring envtest or a live cluster.
func TestSetupWithManager(t *testing.T) {
	scheme := runtime.NewScheme()
	if err := clientgoscheme.AddToScheme(scheme); err != nil {
		t.Fatalf("Failed adding client-go scheme: %v", err)
	}
	if err := sparkappv1beta2.AddToScheme(scheme); err != nil {
		t.Fatalf("Failed adding SparkApplication scheme: %v", err)
	}

	newManager := func(t *testing.T) ctrl.Manager {
		mgr, err := ctrl.NewManager(&rest.Config{Host: "http://127.0.0.1:0"}, ctrl.Options{
			Scheme:  scheme,
			Metrics: metricsserver.Options{BindAddress: "0"},
		})
		if err != nil {
			t.Fatalf("ctrl.NewManager failed: %v", err)
		}
		return mgr
	}

	t.Run("feature enabled: registers a controller with a Pod watch", func(t *testing.T) {
		features.SetFeatureGatesDuringTest(t, map[featuregate.Feature]bool{features.ElasticJobsViaWorkloadSlices: true})
		ctx, _ := utiltesting.ContextWithLog(t)
		mgr := newManager(t)

		reconciler, err := NewExecutorInstancesReconciler(ctx, mgr.GetClient(), mgr.GetFieldIndexer(), nil)
		if err != nil {
			t.Fatalf("NewExecutorInstancesReconciler failed: %v", err)
		}
		if err := reconciler.SetupWithManager(mgr); err != nil {
			t.Fatalf("SetupWithManager failed: %v", err)
		}
	})

	t.Run("feature disabled: no-op, does not register a controller", func(t *testing.T) {
		features.SetFeatureGatesDuringTest(t, map[featuregate.Feature]bool{features.ElasticJobsViaWorkloadSlices: false})
		ctx, _ := utiltesting.ContextWithLog(t)
		mgr := newManager(t)

		reconciler, err := NewExecutorInstancesReconciler(ctx, mgr.GetClient(), mgr.GetFieldIndexer(), nil)
		if err != nil {
			t.Fatalf("NewExecutorInstancesReconciler failed: %v", err)
		}
		if err := reconciler.SetupWithManager(mgr); err != nil {
			t.Fatalf("SetupWithManager returned an error instead of a clean no-op: %v", err)
		}
	})
}

func TestIsVerifiedLiveExecutor(t *testing.T) {
	now := time.Now()

	cases := map[string]struct {
		pod  *corev1.Pod
		want bool
	}{
		"running pod is live":       {pod: makeExecutorPod("e", "ns", "app").StatusPhase(corev1.PodRunning).Obj(), want: true},
		"pending pod is live":       {pod: makeExecutorPod("e", "ns", "app").StatusPhase(corev1.PodPending).Obj(), want: true},
		"succeeded pod is not live": {pod: makeExecutorPod("e", "ns", "app").StatusPhase(corev1.PodSucceeded).Obj(), want: false},
		"failed pod is not live":    {pod: makeExecutorPod("e", "ns", "app").StatusPhase(corev1.PodFailed).Obj(), want: false},
		"terminating pod is not live": {
			pod:  makeExecutorPod("e", "ns", "app").StatusPhase(corev1.PodRunning).Finalizer("f").DeletionTimestamp(now).Obj(),
			want: false,
		},
	}

	for name, tc := range cases {
		t.Run(name, func(t *testing.T) {
			if got := isVerifiedLiveExecutor(tc.pod); got != tc.want {
				t.Errorf("isVerifiedLiveExecutor() = %v, want %v", got, tc.want)
			}
		})
	}
}
