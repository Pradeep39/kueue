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
	"sigs.k8s.io/controller-runtime/pkg/builder"
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

	// controllerName disambiguates this controller from the Kubeflow SparkApplication
	// integration. controller-runtime derives a controller's name from the Kind of the
	// object passed to For(), and both CRDs are Kind "SparkApplication", so without an
	// explicit name the second integration to be set up fails the manager's uniqueness
	// check and the whole controller manager refuses to start.
	controllerName = "apachesparkapplication"

	// appNameLabel is the label the operator stamps on every resource it owns, and
	// which it also injects into the executor pod template so the driver-created
	// executor pods carry it too. See org.apache.spark.k8s.operator.Constants
	// LABEL_SPARK_APPLICATION_NAME.
	appNameLabel = "spark.operator/spark-app-name"

	// roleLabel and its values are Spark's own pod role labels, which the operator sets
	// on the driver and injects into the executor pod template.
	roleLabel         = "spark-role"
	driverRoleValue   = "driver"
	executorRoleValue = "executor"
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

var NewReconciler = jobframework.NewGenericReconcilerFactory(NewJob,
	func(b *builder.Builder, _ client.Client) *builder.Builder {
		b = b.Named(controllerName)
		if !features.Enabled(features.ElasticJobsViaWorkloadSlices) {
			// Avoid registering a cluster-wide Pod watch when the feature it exists to
			// serve is off. liveExecutorCount() still runs on every normal reconcile; it
			// just won't be prompted by Pod events alone.
			return b
		}
		return b.Watches(&corev1.Pod{}, newExecutorPodHandler(), builder.WithPredicates(executorPodPredicate{}))
	})

// SparkApplication wraps the CRD type rather than aliasing it, so that reconcile-scoped
// cache fields can live alongside it. NewJob() allocates a fresh wrapper per Reconcile(),
// which scopes those caches to a single reconcile pass automatically.
type SparkApplication struct {
	*sparkv1.SparkApplication

	// cachedLiveExecutorCount memoizes liveExecutorCount() for the lifetime of this
	// wrapper. PodSets() is called several times per reconcile; without caching, two
	// calls could observe different live counts if Dynamic Allocation churns pods in
	// between, causing spurious "not equivalent" verdicts and workload-slice churn.
	cachedLiveExecutorCount *int32

	// cachedWorkloadSequenceNumber memoizes workloadSequenceNumber(). See
	// GetWorkloadNameExtraPart for why it exists.
	cachedWorkloadSequenceNumber *int32
}

var (
	_ jobframework.GenericJob                  = (*SparkApplication)(nil)
	_ jobframework.ElasticWorkloadNameProvider = (*SparkApplication)(nil)
)

// GetWorkloadNameExtraPart implements jobframework.ElasticWorkloadNameProvider.
//
// The default extra part is object.GetGeneration(), which only advances when the spec
// changes. Dynamic Allocation scales executors by creating and deleting pods directly
// against the API server without ever touching the spec - that is the whole point of
// liveExecutorCount() - so generation alone stays frozen across every scale event.
//
// Folding in the live executor count is not sufficient either: a superseded slice is
// Finished but not deleted absent a retention policy, so its deterministic name persists.
// Real Dynamic Allocation workloads oscillate within a narrow band of counts, so a later
// scale-up that revisits a previously used count would recompute the same name and collide
// with the old, dead object - a permanent failure once every count in the band has been used
// once. workloadSequenceNumber() counts every Workload ever owned by this application, so it
// only grows and a name is never reused.
func (j *SparkApplication) GetWorkloadNameExtraPart() string {
	extra := strconv.FormatInt(j.GetGeneration(), 10)
	if j.cachedWorkloadSequenceNumber != nil {
		extra += "_" + strconv.FormatInt(int64(*j.cachedWorkloadSequenceNumber), 10)
	}
	return extra
}

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

func (j *SparkApplication) PodSets(ctx context.Context, c client.Client) ([]kueue.PodSet, error) {
	executorCount, err := j.liveExecutorCount(ctx, c)
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

	// Pre-compute and cache the sequence number GetWorkloadNameExtraPart() needs, since
	// that method has no client of its own to List() with. Only relevant for elastic jobs,
	// where a generated slice name must never be reused.
	if jobframework.WorkloadSliceEnabled(j) {
		if _, err := j.workloadSequenceNumber(ctx, c); err != nil {
			return nil, err
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
//
// The freshly allocated template is seeded with the Spark container rather than left bare.
// corev1.PodSpec.Containers has no omitempty, so an empty PodSpec marshals to
// `"containers": null`, which the CRD's `type: array` schema rejects with
// "must be of type array". Seeding also keeps the template consistent with what
// buildPodTemplateSpec looks for when it overlays resources.
func (j *SparkApplication) ensureTemplateSpec(role sparkRole) *corev1.PodTemplateSpec {
	holder := &j.Spec.DriverSpec
	if role == roleExecutor {
		holder = &j.Spec.ExecutorSpec
	}
	if *holder == nil {
		*holder = &sparkv1.BaseApplicationTemplateSpec{}
	}
	if (*holder).PodTemplateSpec == nil {
		(*holder).PodTemplateSpec = &corev1.PodTemplateSpec{
			Spec: corev1.PodSpec{
				Containers: []corev1.Container{{Name: j.containerName(role)}},
			},
		}
	}
	if (*holder).PodTemplateSpec.Spec.Containers == nil {
		// A template supplied without any containers would marshal back as null too.
		(*holder).PodTemplateSpec.Spec.Containers = []corev1.Container{{Name: j.containerName(role)}}
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
