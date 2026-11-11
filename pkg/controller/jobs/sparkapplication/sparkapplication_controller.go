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
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/utils/ptr"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"

	kueue "sigs.k8s.io/kueue/apis/kueue/v1beta2"
	"sigs.k8s.io/kueue/pkg/controller/jobframework"
	"sigs.k8s.io/kueue/pkg/features"
	"sigs.k8s.io/kueue/pkg/podset"
	"sigs.k8s.io/kueue/pkg/workload"
	"sigs.k8s.io/kueue/pkg/workloadslicing"
)

var (
	gvk = sparkv1beta2.GroupVersion.WithKind("SparkApplication")
)

const (
	FrameworkName      = "sparkoperator.k8s.io/sparkapplication"
	driverPodSetName   = "driver"
	executorPodSetName = "executor"
)

func RegisterIntegration(m *jobframework.IntegrationManager) error {
	return m.RegisterIntegration(FrameworkName, jobframework.IntegrationCallbacks{
		SetupIndexes:          SetupIndexes,
		NewJob:                NewJob,
		NewReconciler:         NewReconciler,
		SetupWebhook:          SetupWebhook,
		JobType:               &sparkv1beta2.SparkApplication{},
		AddToScheme:           sparkv1beta2.AddToScheme,
		CanSupportIntegration: CanSupportIntegration,
	})
}

// +kubebuilder:rbac:groups=sparkoperator.k8s.io,resources=sparkapplications,verbs=get;list;watch;update;patch;delete

func NewJob() jobframework.GenericJob {
	return &SparkApplication{}
}

type SparkApplication sparkv1beta2.SparkApplication

var _ jobframework.GenericJob = (*SparkApplication)(nil)
var _ jobframework.ElasticWorkloadNameProvider = (*SparkApplication)(nil)

func (j *SparkApplication) Object() client.Object {
	return (*sparkv1beta2.SparkApplication)(j)
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

func (j *SparkApplication) PodSets(ctx context.Context, _ client.Client) ([]kueue.PodSet, error) {
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
	podSets[1] = kueue.PodSet{
		Name:     executorPodSetName,
		Template: *executorPodTemplateSpec,
		Count:    j.numExecutors(ctx),
	}

	if err := setTopologyRequestToPodSetIfEnabled(
		&podSets[1], executorPodTemplateSpec,
	); err != nil {
		return nil, err
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

		// Merge through podset.Merge, as the other integrations do, so that a
		// Kueue-managed annotation left on the spec by a previous admission, such as
		// the workload or workload-slice name of an elastic job admitted again under a
		// new slice, is replaced rather than reported as a permanent conflict.
		meta := metav1.ObjectMeta{
			Annotations: sparkPodSpec.Annotations,
			Labels:      sparkPodSpec.Labels,
		}
		spec := corev1.PodSpec{
			NodeSelector:    nodeSelector,
			Tolerations:     sparkPodSpec.Tolerations,
			SchedulingGates: sparkPodSpec.Template.Spec.SchedulingGates,
		}
		if err := podset.Merge(ctrl.LoggerFrom(ctx), &meta, &spec, podSetInfo); err != nil {
			return err
		}
		sparkPodSpec.Annotations = meta.Annotations
		sparkPodSpec.Labels = meta.Labels
		sparkPodSpec.NodeSelector = spec.NodeSelector
		sparkPodSpec.Tolerations = spec.Tolerations
		sparkPodSpec.Template.Spec.SchedulingGates = spec.SchedulingGates
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

		// An elastic job's executor count follows the running executors, not
		// spec.executor.instances, and Kueue never changes instances when it
		// starts the job, so there is nothing to restore.
		// The count may come from spec.sparkConf (see executorInstances); it is
		// restored to spec.executor.instances only if that changes it, so that an
		// unchanged count set in spec.sparkConf stays there.
		if role == sparkcommon.SparkRoleExecutor && !isAnElasticJob(j) && j.numInitialExecutors() != podSetInfo.Count {
			// An unset spec.executor.instances is counted as 0 executors, while the
			// CRD requires the field to be at least 1 when set. Restore 0 as unset,
			// otherwise the API server rejects the patch that suspends the job.
			if podSetInfo.Count == 0 {
				j.Spec.Executor.Instances = nil
			} else {
				j.Spec.Executor.Instances = new(podSetInfo.Count)
			}
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
	// SUBMISSION_FAILED is not terminal: depending on the restartPolicy, the
	// operator resubmits the application, and it moves the application to
	// FAILED once no retries are left.
	return j.Status.AppState.ErrorMessage,
		j.Status.AppState.State == sparkv1beta2.ApplicationStateCompleted,
		j.Status.AppState.State == sparkv1beta2.ApplicationStateCompleted ||
			j.Status.AppState.State == sparkv1beta2.ApplicationStateFailed
}

func (j *SparkApplication) PodsReady(ctx context.Context, _ client.Client) bool {
	return j.podsReady(ctx, nil)
}

// PodsReadyForWorkload implements jobframework.JobWithGrantAwarePodsReady: the application is
// ready once the executors Kueue grants it are, even if that is fewer than it would otherwise
// expect, e.g. after a partial preemption lowered the grant below its executor instances.
func (j *SparkApplication) PodsReadyForWorkload(ctx context.Context, _ client.Client, wl *kueue.Workload) bool {
	return j.podsReady(ctx, wl)
}

// podsReady reports whether the driver runs and enough executors are ready: running and ready,
// and not being deleted (see executors), in the workload wl when known. Its grant caps the
// expected count at the executors Kueue grants.
func (j *SparkApplication) podsReady(ctx context.Context, wl *kueue.Workload) bool {
	// Driver must be running.
	if j.Status.AppState.State != sparkv1beta2.ApplicationStateRunning {
		return false
	}
	// And every requested executor must have reported a Running/Completed state.
	// AppState.State alone goes to Running as soon as the driver starts even if
	// executors are stuck (e.g. unschedulable), which would let the
	// waitForPodsReady timeout never fire on heterogeneous resource shortages.
	expected := j.minReadyExecutors(wl)
	if granted, ok := grantedExecutors(wl); ok {
		expected = min(expected, granted)
	}
	if expected == 0 {
		return true
	}
	return j.executors(ctx).ready >= expected
}

// minReadyExecutors returns the executors that must be ready for PodsReady in
// the workload wl. Without dynamic allocation, it's the executor instances.
// With dynamic allocation, the driver starts with its initial executors, and
// they must be ready. Once they have been, the executors can fluctuate between
// minExecutors and maxExecutors, and minExecutors, Spark's default of 0 if it is
// set neither in spec.dynamicAllocation nor in spec.sparkConf, is how far the
// application tolerates scaling down.
func (j *SparkApplication) minReadyExecutors(wl *kueue.Workload) int32 {
	da := j.dynamicAllocation()
	if !da.enabled {
		return j.numInitialExecutors()
	}
	if startedExecutors(wl) {
		return ptr.Deref(da.minExecutors, 0)
	}
	return j.initialExecutors(da)
}

// startedExecutors reports whether the application's executors have started in
// the workload wl: they have been ready in it, or it replaces a workload slice,
// which Kueue creates only once the driver stops ramping up to its initial
// executors (see numExecutors).
func startedExecutors(wl *kueue.Workload) bool {
	return wl != nil && (workload.HasBeenReady(wl) || workloadslicing.ReplacementForKey(wl) != nil)
}

// grantedExecutors returns the executors Kueue grants the workload wl, if known.
func grantedExecutors(wl *kueue.Workload) (int32, bool) {
	if wl == nil {
		return 0, false
	}
	return workload.GrantedCount(wl, executorPodSetName)
}

// initialExecutors returns the executors the driver starts with under dynamic
// allocation da: the largest of the executor instances, minExecutors and
// initialExecutors.
func (j *SparkApplication) initialExecutors(da dynamicAllocation) int32 {
	return max(j.numInitialExecutors(), ptr.Deref(da.minExecutors, 0), ptr.Deref(da.initialExecutors, 0))
}

// numExecutors returns the executor count to request quota for. With dynamic
// allocation, an elastic job follows the executors the driver keeps (pending or
// running), so terminating executors hold quota until they stop.
// See executors for where that count comes from.
func (j *SparkApplication) numExecutors(ctx context.Context) int32 {
	da := j.dynamicAllocation()
	if !isAnElasticJob(j) || !da.enabled {
		return j.numInitialExecutors()
	}
	executors := j.executors(ctx)
	if !j.driverStarted() || !executors.exist {
		return j.initialExecutors(da)
	}
	count := executors.active
	// cap the upper and lower bounds
	if da.minExecutors != nil {
		count = max(count, *da.minExecutors)
	}
	if da.maxExecutors != nil {
		count = min(count, *da.maxExecutors)
	}
	return count
}

// driverStarted reports whether status.executorState belongs to the current
// submission. It includes the states after RUNNING: falling back to the initial
// executors there would look like a scale-up, and the job reconciler would then
// not find the active workload slice to finish it.
func (j *SparkApplication) driverStarted() bool {
	switch j.Status.AppState.State {
	case sparkv1beta2.ApplicationStateRunning,
		sparkv1beta2.ApplicationStateSucceeding, sparkv1beta2.ApplicationStateCompleted,
		sparkv1beta2.ApplicationStateFailing, sparkv1beta2.ApplicationStateFailed:
		return true
	}
	return false
}

// GetWorkloadNameExtraPart folds the submission attempt and the highest
// executor ID into the elastic workload-slice name. The Spark driver scales
// executors without bumping metadata.generation; without this the slice name
// would key on that generation alone, so a second scale-up would recompute an
// already-existing slice name and fail to mint its replacement. Spark never
// reuses an executor ID within a submission, so status.maxExecutorId grows with
// every scale-up and not on a scale-down, and unlike the size of
// status.executorState it keeps growing beyond the operator's tracking cap. An
// operator which does not record it falls back to that size, which also only
// grows within a submission.
func (j *SparkApplication) GetWorkloadNameExtraPart() string {
	extra := strconv.FormatInt(j.Generation, 10)
	if j.Status.SubmissionAttempts > 0 {
		extra += fmt.Sprintf("_%d_%d", j.Status.SubmissionAttempts, j.executorSequence())
	}
	return extra
}

// executorSequence returns the higher of status.maxExecutorId, read from the untyped copy (see
// declaredExecutors), and the number of tracked executors, which an operator that doesn't declare
// the former falls back to. Taking the higher one keeps the sequence from going back when the two
// copies of the application briefly disagree, which would rename the workload slice.
func (j *SparkApplication) executorSequence() int64 {
	return max(int64(len(j.Status.ExecutorState)), j.declaredExecutors(context.TODO()).highestID())
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
	return (*SparkApplication)(o.(*sparkv1beta2.SparkApplication))
}
