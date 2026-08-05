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
	"fmt"
	"maps"
	"slices"
	"strconv"

	sparkv1beta2 "github.com/kubeflow/spark-operator/v2/api/v1beta2"
	sparkcommon "github.com/kubeflow/spark-operator/v2/pkg/common"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"
	utilruntime "k8s.io/apimachinery/pkg/util/runtime"
	"k8s.io/utils/ptr"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/builder"
	"sigs.k8s.io/controller-runtime/pkg/client"

	kueue "sigs.k8s.io/kueue/apis/kueue/v1beta2"
	"sigs.k8s.io/kueue/pkg/controller/jobframework"
	"sigs.k8s.io/kueue/pkg/features"
	"sigs.k8s.io/kueue/pkg/podset"
)

var (
	gvk = sparkv1beta2.GroupVersion.WithKind("SparkApplication")
)

const (
	FrameworkName      = "sparkoperator.k8s.io/sparkapplication"
	driverPodSetName   = "driver"
	executorPodSetName = "executor"
)

func init() {
	utilruntime.Must(jobframework.RegisterIntegration(FrameworkName, jobframework.IntegrationCallbacks{
		SetupIndexes:          SetupIndexes,
		NewJob:                NewJob,
		NewReconciler:         NewReconciler,
		SetupWebhook:          SetupWebhook,
		JobType:               &sparkv1beta2.SparkApplication{},
		AddToScheme:           sparkv1beta2.AddToScheme,
		CanSupportIntegration: CanSupportIntegration,
	}))
}

// +kubebuilder:rbac:groups=sparkoperator.k8s.io,resources=sparkapplications,verbs=get;list;watch;update;patch;delete

func NewJob() jobframework.GenericJob {
	return &SparkApplication{SparkApplication: &sparkv1beta2.SparkApplication{}}
}

var NewReconciler = jobframework.NewGenericReconcilerFactory(NewJob,
	func(b *builder.Builder, c client.Client) *builder.Builder {
		if !features.Enabled(features.ElasticJobsViaWorkloadSlices) {
			// Avoid registering a cluster-wide Pod watch when the feature this
			// exists to support is off; liveExecutorCount() still runs on every
			// normal reconcile, it just won't be prompted by Pod events alone.
			return b
		}
		return b.Watches(&corev1.Pod{}, newExecutorPodHandler(), builder.WithPredicates(executorPodPredicate{}))
	})

// SparkApplication wraps the CRD type rather than aliasing it so reconcile-scoped cache
// fields (cachedLiveExecutorCount, cachedWorkloadSequenceNumber) can live alongside it.
// NewJob() allocates a fresh *SparkApplication per Reconcile() call, so the cache is
// automatically scoped to a single reconcile pass and never leaks or goes stale across
// reconciles.
type SparkApplication struct {
	*sparkv1beta2.SparkApplication

	// cachedLiveExecutorCount memoizes liveExecutorCount() for the lifetime of this
	// wrapper. PodSets() is called multiple times per Reconcile() (equivalence checks,
	// workload construction, ...); without caching, two calls could observe different
	// live executor Pod counts if Dynamic Allocation churns pods between them, causing
	// spurious "not equivalent" verdicts and self-inflicted workload-slice churn.
	cachedLiveExecutorCount *int32

	// cachedWorkloadSequenceNumber memoizes workloadSequenceNumber() for the lifetime of
	// this wrapper. See GetWorkloadNameExtraPart for why this exists.
	cachedWorkloadSequenceNumber *int32
}

var _ jobframework.GenericJob = (*SparkApplication)(nil)
var _ jobframework.ElasticWorkloadNameProvider = (*SparkApplication)(nil)

func (j *SparkApplication) Object() client.Object {
	return j.SparkApplication
}

func (j *SparkApplication) IsSuspended() bool {
	return ptr.Deref(j.Spec.Suspend, false)
}

func (j *SparkApplication) IsActive() bool {
	return j.Status.AppState.State == sparkv1beta2.ApplicationStateRunning
}

func (j *SparkApplication) Suspend() {
	j.Spec.Suspend = new(true)
}

func (j *SparkApplication) GVK() schema.GroupVersionKind {
	return gvk
}

func (j *SparkApplication) PodLabelSelector() string {
	return fmt.Sprintf("%s=%s", sparkcommon.LabelSparkAppName, j.Name)
}

// GetWorkloadNameExtraPart implements jobframework.ElasticWorkloadNameProvider.
//
// The default extra part newWorkloadName() would otherwise fall back to is
// object.GetGeneration(), which only changes when SparkApplication.Spec changes.
// Dynamic Allocation scales executors by creating/deleting live Pods directly
// against the API server without ever touching Spec (the whole point of
// liveExecutorCount() is to avoid that, since any Spec write makes the Spark
// Operator kill and resubmit the running app) — so generation alone stays frozen
// across every scale-up after the first.
//
// Folding in the live executor count (as this used to do) isn't enough either:
// once a superseded slice is Finished it's never deleted (absent a configured
// retention policy), so its deterministic name persists in etcd forever. Since
// real Dynamic Allocation workloads oscillate within a narrow band of executor
// counts, a later scale-up that revisits a previously-used count recomputes the
// exact same hash and its Create collides with the old, dead object — a
// permanent, self-reinforcing failure once every count in the band has been
// "used up" once. workloadSequenceNumber(), the count of every Workload ever
// owned by this job (Finished or not), only grows, so a name is never reused for
// the lifetime of the SparkApplication.
func (j *SparkApplication) GetWorkloadNameExtraPart() string {
	extra := strconv.FormatInt(j.GetGeneration(), 10)
	if j.cachedWorkloadSequenceNumber != nil {
		extra += "_" + strconv.FormatInt(int64(*j.cachedWorkloadSequenceNumber), 10)
	}
	return extra
}

func (j *SparkApplication) PodSets(ctx context.Context, c client.Client) ([]kueue.PodSet, error) {
	// driver and executor
	podSets := make([]kueue.PodSet, 2)

	setTopologyRequestToPodSetIfEnabled := func(podSet *kueue.PodSet, podTemplateSpec *corev1.PodTemplateSpec) error {
		if features.Enabled(features.TopologyAwareScheduling) {
			topologyRequest, err := jobframework.NewPodSetTopologyRequest(
				&podTemplateSpec.ObjectMeta).Build()
			if err != nil {
				return err
			}
			podSet.TopologyRequest = topologyRequest
		}
		return nil
	}

	// driver
	driverPodTemplateSpec, err := j.buildDriverPodTemplateSpec()
	if err != nil {
		return nil, err
	}
	podSets[0] = kueue.PodSet{
		Name:     driverPodSetName,
		Template: *driverPodTemplateSpec,
		Count:    1,
	}

	if err := setTopologyRequestToPodSetIfEnabled(
		&podSets[0], driverPodTemplateSpec,
	); err != nil {
		return nil, err
	}

	// executors
	executorPodTemplateSpec, err := j.buildExecutorPodTemplateSpec()
	if err != nil {
		return nil, err
	}
	executorCount, err := j.liveExecutorCount(ctx, c)
	if err != nil {
		return nil, err
	}
	podSets[1] = kueue.PodSet{
		Name:     executorPodSetName,
		Template: *executorPodTemplateSpec,
		Count:    executorCount,
	}

	if err := setTopologyRequestToPodSetIfEnabled(
		&podSets[1], executorPodTemplateSpec,
	); err != nil {
		return nil, err
	}

	// Pre-compute and cache the sequence number GetWorkloadNameExtraPart() needs, since
	// that method has no client of its own to List() with. Only needed for elastic jobs,
	// where the generated workload name must never be reused (see GetWorkloadNameExtraPart).
	if jobframework.WorkloadSliceEnabled(j) {
		if _, err := j.workloadSequenceNumber(ctx, c); err != nil {
			return nil, err
		}
	}

	return podSets, nil
}

func (j *SparkApplication) RunWithPodSetsInfo(ctx context.Context, _ client.Client, podSetsInfo []podset.PodSetInfo) error {
	expectedLen := 2 // driver + executor
	if len(podSetsInfo) != expectedLen {
		return podset.BadPodSetsInfoLenError(expectedLen, len(podSetsInfo))
	}

	j.Spec.Suspend = new(false)

	origGlobalNodeSelector := maps.Clone(j.Spec.NodeSelector)

	mutatePodSetInfoFor := func(role string) error {
		var podSetInfo podset.PodSetInfo
		var nodeSelector map[string]string
		var sparkPodSpec *sparkv1beta2.SparkPodSpec

		switch role {
		case sparkcommon.SparkRoleDriver:
			podSetInfo = podSetsInfo[0]
			if j.Spec.Driver.Template == nil {
				j.Spec.Driver.Template = emptyDriverPodTemplateSpec.DeepCopy()
			}
			// spec.NodeSelector and spec.driver.nodeSelector is mutually exclusive
			nodeSelector = j.Spec.Driver.NodeSelector
			if origGlobalNodeSelector != nil {
				nodeSelector = maps.Clone(origGlobalNodeSelector)
			}
			sparkPodSpec = &j.Spec.Driver.SparkPodSpec
		case sparkcommon.SparkRoleExecutor:
			podSetInfo = podSetsInfo[1]
			sparkPodSpec = &j.Spec.Executor.SparkPodSpec
			if j.Spec.Executor.Template == nil {
				j.Spec.Executor.Template = emptyExecutorPodTemplateSpec.DeepCopy()
			}
			// spec.NodeSelector and spec.executor.nodeSelector is mutually exclusive
			nodeSelector = j.Spec.Executor.NodeSelector
			if origGlobalNodeSelector != nil {
				nodeSelector = maps.Clone(origGlobalNodeSelector)
			}
		default:
			return fmt.Errorf("unknown Spark role: %s", role)
		}

		sparkPodSetInfo := &podset.PodSetInfo{
			Annotations:     sparkPodSpec.Annotations,
			Labels:          sparkPodSpec.Labels,
			NodeSelector:    nodeSelector,
			Tolerations:     sparkPodSpec.Tolerations,
			SchedulingGates: sparkPodSpec.Template.Spec.SchedulingGates,
		}
		if err := sparkPodSetInfo.Merge(podSetInfo); err != nil {
			return err
		}
		sparkPodSpec.Annotations = sparkPodSetInfo.Annotations
		sparkPodSpec.Labels = sparkPodSetInfo.Labels
		sparkPodSpec.NodeSelector = sparkPodSetInfo.NodeSelector
		sparkPodSpec.Tolerations = sparkPodSetInfo.Tolerations
		sparkPodSpec.Template.Spec.SchedulingGates = sparkPodSetInfo.SchedulingGates
		return nil
	}

	if err := mutatePodSetInfoFor(sparkcommon.SparkRoleDriver); err != nil {
		return err
	}
	if err := mutatePodSetInfoFor(sparkcommon.SparkRoleExecutor); err != nil {
		return err
	}

	if origGlobalNodeSelector != nil {
		j.Spec.NodeSelector = nil
	}

	return nil
}

func (j *SparkApplication) RestorePodSetsInfo(ctx context.Context, podSetsInfo []podset.PodSetInfo) bool {
	expectedLength := 2 // driver + executor
	if len(podSetsInfo) != expectedLength {
		ctrl.LoggerFrom(ctx).V(2).Info(
			"Skipping pod set info restore because the pod set count does not match the admitted workload",
			"expectedCount", expectedLength,
			"gotCount", len(podSetsInfo),
		)
		return false
	}

	hadGlobalNodeSelector := j.Spec.NodeSelector != nil

	restorePodSetsInfoFrom := func(role string) bool {
		var podSetInfo podset.PodSetInfo
		var sparkPodSpec *sparkv1beta2.SparkPodSpec
		var emptyPodTemplate *corev1.PodTemplateSpec
		var changed bool

		switch role {
		case sparkcommon.SparkRoleDriver:
			podSetInfo = podSetsInfo[0]
			sparkPodSpec = &j.Spec.Driver.SparkPodSpec
			emptyPodTemplate = emptyDriverPodTemplateSpec
		case sparkcommon.SparkRoleExecutor:
			podSetInfo = podSetsInfo[1]
			sparkPodSpec = &j.Spec.Executor.SparkPodSpec
			emptyPodTemplate = emptyExecutorPodTemplateSpec
		default:
			return false
		}

		if !maps.Equal(sparkPodSpec.Annotations, podSetInfo.Annotations) {
			sparkPodSpec.Annotations = maps.Clone(podSetInfo.Annotations)
			changed = true
		}
		if !maps.Equal(sparkPodSpec.Labels, podSetInfo.Labels) {
			sparkPodSpec.Labels = maps.Clone(podSetInfo.Labels)
			changed = true
		}
		if !maps.Equal(sparkPodSpec.NodeSelector, podSetInfo.NodeSelector) {
			sparkPodSpec.NodeSelector = maps.Clone(podSetInfo.NodeSelector)
			changed = true
		}
		if !slices.Equal(sparkPodSpec.Tolerations, podSetInfo.Tolerations) {
			sparkPodSpec.Tolerations = slices.Clone(podSetInfo.Tolerations)
			changed = true
		}
		if sparkPodSpec.Template == nil {
			sparkPodSpec.Template = emptyPodTemplate.DeepCopy()
		}
		if !slices.Equal(sparkPodSpec.Template.Spec.SchedulingGates, podSetInfo.SchedulingGates) {
			sparkPodSpec.Template.Spec.SchedulingGates = slices.Clone(podSetInfo.SchedulingGates)
			changed = true
		}

		return changed
	}

	driverChanged := restorePodSetsInfoFrom(sparkcommon.SparkRoleDriver)
	executorChanged := restorePodSetsInfoFrom(sparkcommon.SparkRoleExecutor)

	// Defensive: RunWithPodSetsInfo clears spec.nodeSelector, so this only
	// fires if the object was never admitted or was re-populated externally.
	if hadGlobalNodeSelector && j.Spec.NodeSelector != nil {
		j.Spec.NodeSelector = nil
		driverChanged = true
	}

	return driverChanged || executorChanged
}

func (j *SparkApplication) Finished(ctx context.Context) (message string, success, finished bool) {
	return j.Status.AppState.ErrorMessage,
		j.Status.AppState.State == sparkv1beta2.ApplicationStateCompleted,
		j.Status.AppState.State == sparkv1beta2.ApplicationStateCompleted ||
			j.Status.AppState.State == sparkv1beta2.ApplicationStateFailed ||
			j.Status.AppState.State == sparkv1beta2.ApplicationStateFailedSubmission
}

func (j *SparkApplication) PodsReady(ctx context.Context, c client.Client) bool {
	// Driver must be running.
	if j.Status.AppState.State != sparkv1beta2.ApplicationStateRunning {
		return false
	}
	// And every requested executor must have reported a Running/Completed state.
	// AppState.State alone goes to Running as soon as the driver starts even if
	// executors are stuck (e.g. unschedulable), which would let the
	// waitForPodsReady timeout never fire on heterogeneous resource shortages.
	//
	// The expected count must agree with what PodSets() requested, or a
	// Dynamic-Allocation-scaled-down application could report not-ready forever
	// against a stale, higher expectation derived from spec.executor.instances.
	executorCount, err := j.liveExecutorCount(ctx, c)
	if err != nil {
		return false
	}
	expected := int(executorCount)
	if expected == 0 {
		return true
	}
	ready := 0
	for _, st := range j.Status.ExecutorState {
		switch st {
		case sparkv1beta2.ExecutorStateRunning, sparkv1beta2.ExecutorStateCompleted:
			ready++
		}
	}
	return ready >= expected
}

func SetupIndexes(ctx context.Context, indexer client.FieldIndexer) error {
	return jobframework.SetupWorkloadOwnerIndex(ctx, indexer, gvk)
}

func GetWorkloadNameForSparkApplication(sparkAppName string, sparkAppUID types.UID) string {
	return jobframework.GetWorkloadNameForOwnerWithGVK(sparkAppName, sparkAppUID, gvk)
}

func CanSupportIntegration(opts ...jobframework.Option) (bool, error) {
	if !features.Enabled(features.SparkApplicationIntegration) {
		return false, fmt.Errorf("%s integration is alpha feature. please enable %s featuregate", FrameworkName, features.SparkApplicationIntegration)
	}
	return true, nil
}

func fromObject(o runtime.Object) *SparkApplication {
	return &SparkApplication{SparkApplication: o.(*sparkv1beta2.SparkApplication)}
}
