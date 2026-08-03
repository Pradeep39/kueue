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
	"context"

	"github.com/go-logr/logr"
	sparkv1beta2 "github.com/kubeflow/spark-operator/v2/api/v1beta2"
	sparkcommon "github.com/kubeflow/spark-operator/v2/pkg/common"
	sparkutil "github.com/kubeflow/spark-operator/v2/pkg/util"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/tools/events"
	"k8s.io/utils/ptr"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/builder"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/event"
	"sigs.k8s.io/controller-runtime/pkg/handler"
	"sigs.k8s.io/controller-runtime/pkg/predicate"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"

	"sigs.k8s.io/kueue/pkg/controller/jobframework"
	"sigs.k8s.io/kueue/pkg/features"
	clientutil "sigs.k8s.io/kueue/pkg/util/client"
	"sigs.k8s.io/kueue/pkg/workloadslicing"
)

const executorInstancesControllerName = "sparkapplication_executor_instances"

// ExecutorInstancesReconciler keeps SparkApplication.Spec.Executor.Instances in sync with
// the number of executor Pods that are actually live on the cluster.
//
// Spark's own Dynamic Allocation (ExecutorAllocationManager, driven by the driver's
// KubernetesClusterSchedulerBackend) creates and deletes executor Pods directly against
// the Kubernetes API, without ever going through the SparkApplication CR. Because Kueue's
// PodSets() for this integration derives the executor PodSet count solely from
// spec.executor.instances (see numInitialExecutors), that field silently goes stale the
// moment Dynamic Allocation scales up or down, and Kueue's WorkloadSlice
// scale-down/quota-release path (workloadslicing.EnsureWorkloadSlices, wired to fire on
// workload_controller.go's ScaledDown check) never gets a signal to run.
//
// This reconciler is the bridge: it watches executor Pod add/delete events for elastic,
// dynamic-allocation-enabled SparkApplications and patches spec.executor.instances to
// match the live, verified executor Pod count. That in turn is picked up by the
// SparkApplication integration's normal reconcile loop, which re-derives PodSets() and lets
// the existing WorkloadSlice plumbing update the Workload's PodSet count and release/claim
// quota accordingly — the missing link the user asked for, expressed as "increase on add /
// decrease on delete" but implemented as a self-healing reconciliation to the observed
// count rather than a per-event counter (see reconcileExecutorInstances for why).
type ExecutorInstancesReconciler struct {
	client client.Client
}

func NewExecutorInstancesReconciler(_ context.Context, c client.Client, _ client.FieldIndexer, _ events.EventRecorder, _ ...jobframework.Option) (jobframework.JobReconcilerInterface, error) {
	return &ExecutorInstancesReconciler{client: c}, nil
}

var _ jobframework.JobReconcilerInterface = (*ExecutorInstancesReconciler)(nil)

// +kubebuilder:rbac:groups="",resources=pods,verbs=get;list;watch

func (r *ExecutorInstancesReconciler) SetupWithManager(mgr ctrl.Manager) error {
	if !features.Enabled(features.ElasticJobsViaWorkloadSlices) {
		// Avoid registering a cluster-wide Pod watch when the feature this
		// reconciler exists to support is off.
		return nil
	}
	return ctrl.NewControllerManagedBy(mgr).
		For(&sparkv1beta2.SparkApplication{}).
		Named(executorInstancesControllerName).
		Watches(
			&corev1.Pod{},
			handler.EnqueueRequestsFromMapFunc(mapExecutorPodToSparkApplication),
			builder.WithPredicates(executorPodPredicate{}),
		).
		Complete(r)
}

// executorPodPredicate filters Pod events down to Spark executor Pods that carry the
// Spark Operator's admission-webhook-assigned owning-application label. Driver Pods and
// any non-Spark Pods are ignored before they ever reach the map function/queue.
type executorPodPredicate struct{}

var _ predicate.Predicate = executorPodPredicate{}

func (executorPodPredicate) Create(e event.CreateEvent) bool { return isTrackedExecutorPod(e.Object) }
func (executorPodPredicate) Update(e event.UpdateEvent) bool {
	return isTrackedExecutorPod(e.ObjectOld) || isTrackedExecutorPod(e.ObjectNew)
}
func (executorPodPredicate) Delete(e event.DeleteEvent) bool { return isTrackedExecutorPod(e.Object) }
func (executorPodPredicate) Generic(event.GenericEvent) bool { return false }

func isTrackedExecutorPod(obj client.Object) bool {
	pod, ok := obj.(*corev1.Pod)
	if !ok {
		return false
	}
	return sparkutil.IsExecutorPod(pod) && pod.Labels[sparkcommon.LabelSparkAppName] != ""
}

// mapExecutorPodToSparkApplication turns an executor Pod event into a reconcile.Request
// for its owning SparkApplication.
//
// Executor Pods are not linked to the SparkApplication via OwnerReference (they're owned
// by the driver Pod, which Spark itself creates), so the standard Owns()-based watch
// can't be used here. Instead we rely on the sparkoperator.k8s.io/app-name label, which
// the Spark Operator's own admission webhook stamps onto every driver/executor Pod
// (including ones Dynamic Allocation creates well after the SparkApplication was
// admitted) — see PodLabelSelector() in sparkapplication_controller.go, which already
// depends on this same label for the analogous problem of listing an application's Pods.
//
// Crucially, for Delete events this reads labels off the informer's last-known copy of
// the Pod (passed in via the event object), not a live Get — a live Get against a
// just-deleted Pod would 404 before we ever learn which SparkApplication it belonged to.
func mapExecutorPodToSparkApplication(_ context.Context, obj client.Object) []reconcile.Request {
	pod, ok := obj.(*corev1.Pod)
	if !ok {
		return nil
	}
	appName := pod.Labels[sparkcommon.LabelSparkAppName]
	if appName == "" {
		return nil
	}
	return []reconcile.Request{{
		NamespacedName: types.NamespacedName{Namespace: pod.Namespace, Name: appName},
	}}
}

func (r *ExecutorInstancesReconciler) Reconcile(ctx context.Context, req reconcile.Request) (reconcile.Result, error) {
	log := ctrl.LoggerFrom(ctx).WithValues("sparkApplication", req.NamespacedName)

	sparkApp := &sparkv1beta2.SparkApplication{}
	if err := r.client.Get(ctx, req.NamespacedName, sparkApp); err != nil {
		return ctrl.Result{}, client.IgnoreNotFound(err)
	}

	// Only elastic jobs opted into workload-slice scaling, and only while Dynamic
	// Allocation is actually turned on for them, are in scope. Everything else keeps
	// relying solely on the static spec.executor.instances value it already had.
	//
	// dynamicAllocationEnabled also checks the raw spark.dynamicAllocation.enabled key in
	// spec.sparkConf: the structured spec.dynamicAllocation.enabled field alone misses any
	// SparkApplication that only sets Dynamic Allocation via sparkConf (see numInitialExecutors).
	if !workloadslicing.Enabled(sparkApp) || !(*SparkApplication)(sparkApp).dynamicAllocationEnabled() {
		return ctrl.Result{}, nil
	}

	return ctrl.Result{}, r.reconcileExecutorInstances(ctx, log, sparkApp)
}

// reconcileExecutorInstances lists the live executor Pods for sparkApp and, if that count
// differs from spec.executor.instances, patches the field to match.
//
// The user's original ask was framed as a per-event counter: +1 when Dynamic Allocation
// adds an executor Pod, -1 when it removes one. That framing doesn't hold up as a literal
// implementation: Spark's ExecutorAllocationManager does not distinguish "originally
// admitted" executor Pods from ones it added later when it picks scale-down victims, so a
// scheme that only decrements for Pods it previously incremented for will drift out of
// sync the first time Dynamic Allocation kills one of the initial executors instead of a
// dynamically-added one. It's also exposed to double-counting on informer resyncs and to
// permanently losing counts if a single Delete/Create event is ever missed.
//
// Reconciling to the currently-observed, verified-live Pod count sidesteps all of that: it
// produces the same practical outcome the user wants (more live executor Pods -> higher
// instances; fewer -> lower) while being idempotent and self-healing on every event,
// including ones that arrive out of order or get redelivered.
func (r *ExecutorInstancesReconciler) reconcileExecutorInstances(ctx context.Context, log logr.Logger, sparkApp *sparkv1beta2.SparkApplication) error {
	podList := &corev1.PodList{}
	if err := r.client.List(ctx, podList,
		client.InNamespace(sparkApp.Namespace),
		client.MatchingLabels{
			sparkcommon.LabelSparkAppName: sparkApp.Name,
			sparkcommon.LabelSparkRole:    sparkcommon.SparkRoleExecutor,
		},
	); err != nil {
		return err
	}

	var liveCount int32
	for i := range podList.Items {
		if isVerifiedLiveExecutor(&podList.Items[i]) {
			liveCount++
		}
	}

	current := ptr.Deref(sparkApp.Spec.Executor.Instances, 0)
	if liveCount == current {
		return nil
	}

	log.V(2).Info("Dynamic allocation changed the live executor pod count; syncing spec.executor.instances so Kueue's elastic workload accounting stays correct",
		"specInstances", current, "liveExecutorPods", liveCount)

	return clientutil.Patch(ctx, r.client, sparkApp, func() (bool, error) {
		sparkApp.Spec.Executor.Instances = ptr.To(liveCount)
		return true, nil
	}, clientutil.WithRetryOnConflict())
}

// isVerifiedLiveExecutor reports whether pod represents a Dynamic-Allocation-managed
// executor that should currently count against spec.executor.instances: it exists, isn't
// already on its way out (no DeletionTimestamp), and hasn't reached a terminal phase.
// A Pod that's merely Pending/ContainerCreating still counts — quota needs to be reserved
// as soon as the Pod is admitted to the cluster, not once it happens to reach Running,
// otherwise there's a window where Dynamic Allocation has already consumed real cluster
// capacity that Kueue's accounting doesn't yet know about.
func isVerifiedLiveExecutor(pod *corev1.Pod) bool {
	if pod.DeletionTimestamp != nil {
		return false
	}
	switch pod.Status.Phase {
	case corev1.PodSucceeded, corev1.PodFailed:
		return false
	default:
		return true
	}
}
