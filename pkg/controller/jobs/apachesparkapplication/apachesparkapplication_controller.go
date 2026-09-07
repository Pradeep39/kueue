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
	"fmt"
	"maps"
	"slices"
	"strconv"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/utils/ptr"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"

	kueue "sigs.k8s.io/kueue/apis/kueue/v1beta2"
	"sigs.k8s.io/kueue/pkg/controller/jobframework"
	sparkv1 "sigs.k8s.io/kueue/pkg/controller/jobs/apachesparkapplication/api/v1"
	"sigs.k8s.io/kueue/pkg/features"
	"sigs.k8s.io/kueue/pkg/podset"
)

var (
	gvk = sparkv1.GroupVersion.WithKind("SparkApplication")

	// terminalStates are the states from which the application will not run again
	// without a new attempt being opened by the operator.
	terminalStates = []sparkv1.ApplicationStateSummary{
		sparkv1.Succeeded,
		sparkv1.Failed,
		sparkv1.SchedulingFailure,
		sparkv1.DriverEvicted,
		sparkv1.DriverStartTimedOut,
		sparkv1.DriverReadyTimedOut,
		sparkv1.ExecutorsStartTimedOut,
		sparkv1.ResourceReleased,
		sparkv1.TerminatedWithoutReleaseResources,
	}

	// runningStates are the states in which a driver exists and is consuming quota.
	runningStates = []sparkv1.ApplicationStateSummary{
		sparkv1.DriverRequested,
		sparkv1.DriverStarted,
		sparkv1.DriverReady,
		sparkv1.InitializedBelowThresholdExecutors,
		sparkv1.RunningHealthy,
		sparkv1.RunningWithPartialCapacity,
		sparkv1.RunningWithBelowThresholdExecutors,
	}

	// healthyStates are the states in which the application holds at least the
	// minimum executors it asked for.
	healthyStates = []sparkv1.ApplicationStateSummary{
		sparkv1.RunningHealthy,
		sparkv1.RunningWithPartialCapacity,
	}

	// releaseStates are terminal states the operator enters from both success and
	// failure, so they carry no outcome of their own.
	releaseStates = []sparkv1.ApplicationStateSummary{
		sparkv1.ResourceReleased,
		sparkv1.TerminatedWithoutReleaseResources,
	}
)

const (
	// FrameworkName is the name used to enable this integration in the Kueue
	// configuration's .integrations.frameworks.
	FrameworkName      = "spark.apache.org/sparkapplication"
	driverPodSetName   = "driver"
	executorPodSetName = "executor"

	// podSetCount is driver + executor.
	podSetCount = 2

	// appNameLabel is the label the operator stamps on every resource it owns, and
	// which it also injects into the executor pod template so the driver-created
	// executor pods carry it too. See org.apache.spark.k8s.operator.Constants
	// LABEL_SPARK_APPLICATION_NAME.
	appNameLabel = "spark.operator/spark-app-name"
)

func RegisterIntegration(m *jobframework.IntegrationManager) error {
	return m.RegisterIntegration(FrameworkName, jobframework.IntegrationCallbacks{
		SetupIndexes:          SetupIndexes,
		NewJob:                NewJob,
		NewReconciler:         NewReconciler,
		SetupWebhook:          SetupWebhook,
		JobType:               &sparkv1.SparkApplication{},
		AddToScheme:           sparkv1.AddToScheme,
		CanSupportIntegration: CanSupportIntegration,
	})
}

// +kubebuilder:rbac:groups=spark.apache.org,resources=sparkapplications,verbs=get;list;watch;update;patch;delete

func NewJob() jobframework.GenericJob {
	return &SparkApplication{SparkApplication: &sparkv1.SparkApplication{}}
}

var NewReconciler = jobframework.NewGenericReconcilerFactory(NewJob)

// SparkApplication wraps the CRD type so the GenericJob methods can hang off it without
// taking a dependency on the API package from jobframework.
type SparkApplication struct {
	*sparkv1.SparkApplication
}

var _ jobframework.GenericJob = (*SparkApplication)(nil)

func (j *SparkApplication) Object() client.Object {
	return j.SparkApplication
}

func (j *SparkApplication) GVK() schema.GroupVersionKind {
	return gvk
}

func (j *SparkApplication) IsSuspended() bool {
	return ptr.Deref(j.Spec.Suspend, false)
}

func (j *SparkApplication) Suspend() {
	j.Spec.Suspend = ptr.To(true)
}

// IsActive reports whether a driver currently exists for this application.
func (j *SparkApplication) IsActive() bool {
	return slices.Contains(runningStates, j.Status.CurrentState.CurrentStateSummary)
}

// Finished reports terminal completion.
//
// StoppedByScheduler is deliberately absent from terminalStates: the operator releases
// the driver on a scheduler requested stop but reopens the application in Suspended, so
// the workload has been preempted, not finished.
func (j *SparkApplication) Finished(context.Context) (message string, success, finished bool) {
	state := j.Status.CurrentState
	if !slices.Contains(terminalStates, state.CurrentStateSummary) {
		return state.Message, false, false
	}
	return state.Message, j.terminalOutcome() == sparkv1.Succeeded, true
}

// terminalOutcome resolves the state that decided the application's fate.
//
// ResourceReleased and TerminatedWithoutReleaseResources are both reached from success
// and from failure, so when the application is sitting in one of them the outcome is the
// last state of the attempt that was not itself a release state. An application whose
// history is unavailable is reported as unsuccessful rather than guessed at.
func (j *SparkApplication) terminalOutcome() sparkv1.ApplicationStateSummary {
	current := j.Status.CurrentState.CurrentStateSummary
	if !slices.Contains(releaseStates, current) {
		return current
	}
	var (
		bestID  int64 = -1
		outcome sparkv1.ApplicationStateSummary
	)
	for rawID, state := range j.Status.StateTransitionHistory {
		if slices.Contains(releaseStates, state.CurrentStateSummary) {
			continue
		}
		id, err := strconv.ParseInt(rawID, 10, 64)
		if err != nil {
			continue
		}
		if id > bestID {
			bestID, outcome = id, state.CurrentStateSummary
		}
	}
	return outcome
}

// PodsReady reports whether the application acquired the executors it asked for. The
// operator only advances to RunningHealthy or RunningWithPartialCapacity once at least
// minExecutors are ready, so the state machine is a sufficient signal here.
func (j *SparkApplication) PodsReady(context.Context, client.Client) bool {
	return slices.Contains(healthyStates, j.Status.CurrentState.CurrentStateSummary)
}

func (j *SparkApplication) PodLabelSelector() string {
	return fmt.Sprintf("%s=%s", appNameLabel, j.Name)
}

func (j *SparkApplication) PodSets(ctx context.Context, _ client.Client) ([]kueue.PodSet, error) {
	executorCount, err := j.executorCount()
	if err != nil {
		return nil, err
	}

	podSets := make([]kueue.PodSet, podSetCount)
	for i, def := range []struct {
		name  string
		role  sparkRole
		count int32
	}{
		{name: driverPodSetName, role: roleDriver, count: 1},
		{name: executorPodSetName, role: roleExecutor, count: executorCount},
	} {
		template, err := j.buildPodTemplateSpec(def.role)
		if err != nil {
			return nil, err
		}
		podSets[i] = kueue.PodSet{
			Name:     kueue.NewPodSetReference(def.name),
			Template: *template,
			Count:    def.count,
		}
		if features.Enabled(features.TopologyAwareScheduling) {
			topologyRequest, err := jobframework.NewPodSetTopologyRequest(&template.ObjectMeta).Build()
			if err != nil {
				return nil, err
			}
			podSets[i].TopologyRequest = topologyRequest
		}
	}

	return podSets, nil
}

// RunWithPodSetsInfo clears .spec.suspend and pushes the admitted flavor's scheduling
// directives into the driver and executor pod templates.
//
// Executor counts are intentionally not written back. The driver, not the operator,
// creates executors from spark.executor.instances, and the operator restarts an
// application whose spec changes - so narrowing the count here would both fail to take
// effect and disrupt the run. Partial admission is therefore not supported.
func (j *SparkApplication) RunWithPodSetsInfo(ctx context.Context, _ client.Client, podSetsInfo []podset.PodSetInfo) error {
	if len(podSetsInfo) != podSetCount {
		return podset.BadPodSetsInfoLenError(podSetCount, len(podSetsInfo))
	}

	j.Spec.Suspend = ptr.To(false)

	log := ctrl.LoggerFrom(ctx)
	for i, role := range []sparkRole{roleDriver, roleExecutor} {
		template := j.ensureTemplateSpec(role)
		if err := podset.Merge(log, &template.ObjectMeta, &template.Spec, podSetsInfo[i]); err != nil {
			return err
		}
	}

	return nil
}

// RestorePodSetsInfo puts the driver and executor pod templates back to the state they
// held before admission, so a re-admitted application is not left carrying a previous
// flavor's node selectors.
func (j *SparkApplication) RestorePodSetsInfo(ctx context.Context, podSetsInfo []podset.PodSetInfo) bool {
	if len(podSetsInfo) != podSetCount {
		ctrl.LoggerFrom(ctx).V(2).Info(
			"Skipping pod set info restore because the pod set count does not match the admitted workload",
			"expectedCount", podSetCount,
			"gotCount", len(podSetsInfo),
		)
		return false
	}

	var changed bool
	for i, role := range []sparkRole{roleDriver, roleExecutor} {
		template := templateSpec(j.SparkApplication, role)
		if template == nil {
			// Nothing was ever injected into a template that does not exist.
			continue
		}
		info := podSetsInfo[i]
		if !maps.Equal(template.Spec.NodeSelector, info.NodeSelector) {
			template.Spec.NodeSelector = maps.Clone(info.NodeSelector)
			changed = true
		}
		if !slices.Equal(template.Spec.Tolerations, info.Tolerations) {
			template.Spec.Tolerations = slices.Clone(info.Tolerations)
			changed = true
		}
		if !slices.Equal(template.Spec.SchedulingGates, info.SchedulingGates) {
			template.Spec.SchedulingGates = slices.Clone(info.SchedulingGates)
			changed = true
		}
		if !maps.Equal(template.Labels, info.Labels) {
			template.Labels = maps.Clone(info.Labels)
			changed = true
		}
		if !maps.Equal(template.Annotations, info.Annotations) {
			template.Annotations = maps.Clone(info.Annotations)
			changed = true
		}
	}
	return changed
}

// ensureTemplateSpec returns the pod template for the given role, allocating the
// intermediate spec wrapper when the application did not declare one.
func (j *SparkApplication) ensureTemplateSpec(role sparkRole) *corev1.PodTemplateSpec {
	holder := &j.Spec.DriverSpec
	if role == roleExecutor {
		holder = &j.Spec.ExecutorSpec
	}
	if *holder == nil {
		*holder = &sparkv1.BaseApplicationTemplateSpec{}
	}
	if (*holder).PodTemplateSpec == nil {
		(*holder).PodTemplateSpec = &corev1.PodTemplateSpec{}
	}
	return (*holder).PodTemplateSpec
}

func SetupIndexes(ctx context.Context, indexer client.FieldIndexer) error {
	return jobframework.SetupWorkloadOwnerIndex(ctx, indexer, gvk)
}

func CanSupportIntegration(...jobframework.Option) (bool, error) {
	if !features.Enabled(features.ApacheSparkApplicationIntegration) {
		return false, fmt.Errorf("%s integration is an alpha feature. please enable the %s feature gate",
			FrameworkName, features.ApacheSparkApplicationIntegration)
	}
	return true, nil
}

func fromObject(o runtime.Object) *SparkApplication {
	return &SparkApplication{SparkApplication: o.(*sparkv1.SparkApplication)}
}
