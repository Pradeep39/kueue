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
	"slices"
	"strconv"
	"strings"

	sparkv1beta2 "github.com/kubeflow/spark-operator/v2/api/v1beta2"
	admissionv1 "k8s.io/api/admission/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/apimachinery/pkg/util/validation/field"
	"k8s.io/utils/ptr"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/webhook/admission"

	kueue "sigs.k8s.io/kueue/apis/kueue/v1beta2"
	qcache "sigs.k8s.io/kueue/pkg/cache/queue"
	schdcache "sigs.k8s.io/kueue/pkg/cache/scheduler"
	"sigs.k8s.io/kueue/pkg/controller/jobframework"
	"sigs.k8s.io/kueue/pkg/features"
	utilpod "sigs.k8s.io/kueue/pkg/util/pod"
	"sigs.k8s.io/kueue/pkg/util/podset"
	"sigs.k8s.io/kueue/pkg/util/webhook"
	"sigs.k8s.io/kueue/pkg/workloadslicing"
)

var (
	specPath                     = field.NewPath("spec")
	sparkConfPath                = specPath.Child("sparkConf")
	dynamicAllocationEnabledPath = specPath.Child("dynamicAllocation").Child("enabled")
	driverSpecPath               = specPath.Child("driver")
	executorSpecPath             = specPath.Child("executor")
)

type SparkApplicationWebhook struct {
	integrationManager           *jobframework.IntegrationManager
	client                       client.Client
	queues                       *qcache.Manager
	manageJobsWithoutQueueName   bool
	managedJobsNamespaceSelector labels.Selector
	cache                        *schdcache.Cache
	maxTimeoutOnWorkload         *metav1.Duration
}

func SetupWebhook(mgr ctrl.Manager, opts ...jobframework.Option) error {
	options := jobframework.ProcessOptions(opts...)
	wh := &SparkApplicationWebhook{
		integrationManager:           options.IntegrationManager,
		client:                       mgr.GetClient(),
		queues:                       options.Queues,
		manageJobsWithoutQueueName:   options.ManageJobsWithoutQueueName,
		managedJobsNamespaceSelector: options.ManagedJobsNamespaceSelector,
		cache:                        options.Cache,
		maxTimeoutOnWorkload:         options.MaxTimeoutOnWorkload,
	}
	obj := &sparkv1beta2.SparkApplication{}
	if options.NoopWebhook {
		return webhook.SetupNoopWebhook(mgr, obj)
	}
	return ctrl.NewWebhookManagedBy(mgr, obj).
		WithValidator(wh).
		WithDefaulter(wh).
		WithLogConstructor(jobframework.WebhookLogConstructor(fromObject(obj).GVK(), options.RoleTracker)).
		Complete()
}

// +kubebuilder:webhook:path=/mutate-sparkoperator-k8s-io-v1beta2-sparkapplication,mutating=true,failurePolicy=fail,sideEffects=None,groups=sparkoperator.k8s.io,resources=sparkapplications,verbs=create,versions=v1beta2,name=msparkapplication.kb.io,admissionReviewVersions=v1

var _ admission.Defaulter[*sparkv1beta2.SparkApplication] = &SparkApplicationWebhook{}

// Default implements webhook.CustomDefaulter so a webhook will be registered for the type
func (w *SparkApplicationWebhook) Default(ctx context.Context, obj *sparkv1beta2.SparkApplication) error {
	job := fromObject(obj)
	log := ctrl.LoggerFrom(ctx).WithName("sparkapplication-webhook")
	log.V(5).Info("Applying defaults")

	if err := w.integrationManager.ApplyDefaultLocalQueue(ctx, w.client, job.Object(), w.queues.DefaultLocalQueueExist, w.managedJobsNamespaceSelector); err != nil {
		return err
	}
	if err := w.integrationManager.ApplyDefaultWorkloadPriorityClass(ctx, w.client, job.Object(), w.managedJobsNamespaceSelector); err != nil {
		return err
	}
	if err := w.integrationManager.ApplyDefaultForSuspend(ctx, job, w.client, w.manageJobsWithoutQueueName, w.managedJobsNamespaceSelector); err != nil {
		return err
	}
	jobframework.ApplyDefaultForManagedBy(job, w.queues, w.cache, log)

	if isAnElasticJob(obj) {
		// The Spark driver creates executors on its own, so they must wait for
		// the workload slice that accounts for them to be admitted.
		if obj.Spec.Executor.Template == nil {
			obj.Spec.Executor.Template = emptyExecutorPodTemplateSpec.DeepCopy()
		}
		utilpod.GateTemplate(obj.Spec.Executor.Template, kueue.ElasticJobSchedulingGate)
	}

	if isCreateRequest(ctx) && partialPreemptionEnabled(job) && job.reclaimable() {
		applyDecommissionDefaults(job)
	}

	return nil
}

// isCreateRequest reports whether the admission request being defaulted is a CREATE. The
// mutating webhook is registered for CREATE only (see the marker above), but the
// partial-preemption defaults must never touch an existing application's spec: the Spark
// Operator re-runs an application on any spec change outside its exemption list. A context
// without an admission request (unit tests) is treated as a create.
func isCreateRequest(ctx context.Context) bool {
	req, err := admission.RequestFromContext(ctx)
	if err != nil {
		return true
	}
	return req.Operation == admissionv1.Create
}

// reclaimable reports whether partial preemption could ever take an executor from the
// application: its resolved floor is below the most executors it can run. For Dynamic
// Allocation that is maxExecutors (unbounded when unset, as in Spark); for a static
// application it is its executor count.
func (j *SparkApplication) reclaimable() bool {
	floor := j.resolvedPartialPreemptionFloor()
	if j.dynamicAllocationEnabled() {
		maxExecutors := j.dynamicAllocation().maxExecutors
		return maxExecutors == nil || floor < *maxExecutors
	}
	return floor < j.numInitialExecutors()
}

// applyDecommissionDefaults turns on Spark's graceful decommissioning for a reclaimable
// application, so deleting an executor Pod migrates its blocks rather than losing them
// (docs/design/spark-aware-preemption.md §5.4). Values the application sets explicitly,
// including "false", are left alone.
func applyDecommissionDefaults(job *SparkApplication) {
	for _, key := range decommissionSparkConfDefaults {
		if _, ok := job.Spec.SparkConf[key]; ok {
			continue
		}
		if job.Spec.SparkConf == nil {
			job.Spec.SparkConf = map[string]string{}
		}
		job.Spec.SparkConf[key] = "true"
	}
	mirrorExecutorTerminationGracePeriod(job)
}

// mirrorExecutorTerminationGracePeriod copies an executor grace period the application sets in
// one place to the other: the Pod template (Spark before 4.1) and
// spark.kubernetes.executor.terminationGracePeriodSeconds (Spark 4.1+, which overrides the
// template). An application that sets neither is left alone: Kubernetes and Spark both default
// to 30s, and partial preemption applies its own grace period when it deletes the Pods.
func mirrorExecutorTerminationGracePeriod(job *SparkApplication) {
	var templateGrace *int64
	if t := job.Spec.Executor.Template; t != nil {
		templateGrace = t.Spec.TerminationGracePeriodSeconds
	}
	confGrace, confSet := job.Spec.SparkConf[sparkConfExecutorTerminationGracePeriod]
	switch {
	case templateGrace != nil && !confSet:
		job.Spec.SparkConf[sparkConfExecutorTerminationGracePeriod] = strconv.FormatInt(*templateGrace, 10) + "s"
	case templateGrace == nil && confSet:
		seconds, ok := sparkTimeSeconds(confGrace)
		if !ok {
			return
		}
		if job.Spec.Executor.Template == nil {
			job.Spec.Executor.Template = emptyExecutorPodTemplateSpec.DeepCopy()
		}
		job.Spec.Executor.Template.Spec.TerminationGracePeriodSeconds = ptr.To(seconds)
	}
}

// sparkTimeSeconds parses a Spark time property whose default unit is seconds, the way Spark's
// timeConf(TimeUnit.SECONDS) does: a bare number is seconds, and the suffixes ms, s, m/min, h
// and d are accepted. Sub-second values are truncated.
func sparkTimeSeconds(v string) (int64, bool) {
	v = strings.ToLower(strings.TrimSpace(v))
	units := []struct {
		suffix string
		ms     int64
	}{{"ms", 1}, {"min", 60_000}, {"s", 1_000}, {"m", 60_000}, {"h", 3_600_000}, {"d", 86_400_000}}
	mult := int64(1_000)
	for _, u := range units {
		if strings.HasSuffix(v, u.suffix) {
			v, mult = strings.TrimSuffix(v, u.suffix), u.ms
			break
		}
	}
	n, err := strconv.ParseInt(v, 10, 64)
	if err != nil || n < 0 {
		return 0, false
	}
	return n * mult / 1_000, true
}

// +kubebuilder:webhook:path=/validate-sparkoperator-k8s-io-v1beta2-sparkapplication,mutating=false,failurePolicy=fail,sideEffects=None,groups=sparkoperator.k8s.io,resources=sparkapplications,verbs=create;update,versions=v1beta2,name=vsparkapplication.kb.io,admissionReviewVersions=v1

var _ admission.Validator[*sparkv1beta2.SparkApplication] = &SparkApplicationWebhook{}

// ValidateCreate implements webhook.CustomValidator so a webhook will be registered for the type
func (w *SparkApplicationWebhook) ValidateCreate(ctx context.Context, obj *sparkv1beta2.SparkApplication) (admission.Warnings, error) {
	log := ctrl.LoggerFrom(ctx).WithName("sparkapplication-webhook")
	log.Info("Validating create")
	validationErrs, err := w.validateCreate(ctx, nil, obj, w.maxTimeoutOnWorkload)
	if err != nil {
		return nil, err
	}
	return w.partialPreemptionWarnings(nil, obj), validationErrs.ToAggregate()
}

// partialPreemptionWarnings warns, without rejecting, about a partial-preemption floor that
// will be ignored: spark.kueue.preemption.minExecutors only takes effect on the elastic path,
// so on an application without the elastic-job annotation it silently does nothing, and the
// application is preempted whole. Found in testing: the property alone looks like an opt-in.
// On update it warns only when the property is added or changed.
func (w *SparkApplicationWebhook) partialPreemptionWarnings(oldJob, job *sparkv1beta2.SparkApplication) admission.Warnings {
	if !features.Enabled(features.SparkPartialPreemption) {
		return nil
	}
	if !w.manageJobsWithoutQueueName && jobframework.QueueName(fromObject(job)) == "" {
		return nil
	}
	if _, set := job.Spec.SparkConf[sparkConfPreemptionMinExecutors]; !set || isAnElasticJob(job) {
		return nil
	}
	if oldJob != nil && !preemptionMinExecutorsChanged(oldJob, job) {
		return nil
	}
	return admission.Warnings{floorIgnoredWarning()}
}

func floorIgnoredWarning() string {
	return fmt.Sprintf(
		"sparkConf %q has no effect without the %s: %q annotation; this application is not partially preempted and will be preempted whole",
		sparkConfPreemptionMinExecutors, workloadslicing.EnabledAnnotationKey, workloadslicing.EnabledAnnotationValue)
}

// returns whether the SparkApplication is an elastic job or not
func isAnElasticJob(sparkApp metav1.Object) bool {
	return features.Enabled(features.ElasticJobsViaWorkloadSlicesForSparkApplication) && workloadslicing.Enabled(sparkApp)
}

// validateCreate validates job. oldJob is nil on create, and the previous object on update.
func (w *SparkApplicationWebhook) validateCreate(ctx context.Context, oldJob, job *sparkv1beta2.SparkApplication, maxTimeoutOnWorkload *metav1.Duration) (field.ErrorList, error) {
	var allErrors field.ErrorList
	kueueJob := (*SparkApplication)(job)

	if w.manageJobsWithoutQueueName || jobframework.QueueName(kueueJob) != "" {
		spec := &job.Spec

		if spec.Mode != sparkv1beta2.DeployModeCluster {
			allErrors = append(allErrors, field.Invalid(specPath.Child("mode"), spec.Mode, "only Cluster mode is supported for a kueue managed job"))
		}

		if isAnElasticJob(job) {
			allErrors = append(allErrors, validateElasticJob(job)...)
		}

		if !isAnElasticJob(job) && ptr.Deref(spec.DynamicAllocation, sparkv1beta2.DynamicAllocation{}).Enabled {
			allErrors = append(allErrors,
				field.Invalid(dynamicAllocationEnabledPath,
					ptr.Deref(spec.DynamicAllocation, sparkv1beta2.DynamicAllocation{}).Enabled,
					"a kueue managed job can use dynamicAllocation only when the ElasticJobsViaWorkloadSlicesForSparkApplication feature gate is on and the job is an elastic job",
				),
			)
		}

		// The partial-preemption floor. Only a new or changed value is rejected: an
		// application created before the gate was turned on must still accept updates,
		// including Kueue's own suspend, and is simply treated as having no property.
		if features.Enabled(features.SparkPartialPreemption) && preemptionMinExecutorsChanged(oldJob, job) {
			if _, _, err := kueueJob.sparkConfPreemptionMinExecutorsValue(); err != nil {
				allErrors = append(allErrors,
					field.Invalid(sparkConfPath.Key(sparkConfPreemptionMinExecutors),
						spec.SparkConf[sparkConfPreemptionMinExecutors],
						"must be a non-negative integer",
					),
				)
			}
		}
	}

	allErrors = append(allErrors, jobframework.ValidateJobOnCreate(kueueJob, maxTimeoutOnWorkload)...)
	if features.Enabled(features.TopologyAwareScheduling) {
		validationErrs, err := w.validateTopologyRequest(ctx, kueueJob)
		if err != nil {
			return nil, err
		}
		allErrors = append(allErrors, validationErrs...)
	}

	return allErrors, nil
}

// preemptionMinExecutorsChanged reports whether job's spark.kueue.preemption.minExecutors is
// new (oldJob is nil, i.e. a create) or differs from oldJob's.
func preemptionMinExecutorsChanged(oldJob, job *sparkv1beta2.SparkApplication) bool {
	if oldJob == nil {
		return true
	}
	oldValue, oldOk := oldJob.Spec.SparkConf[sparkConfPreemptionMinExecutors]
	newValue, newOk := job.Spec.SparkConf[sparkConfPreemptionMinExecutors]
	return oldOk != newOk || oldValue != newValue
}

func validateElasticJob(job *sparkv1beta2.SparkApplication) field.ErrorList {
	allErrors := field.ErrorList{}

	workloadSliceSchedulingGate := corev1.PodSchedulingGate{
		Name: kueue.ElasticJobSchedulingGate,
	}

	var schedulingGates []corev1.PodSchedulingGate
	if job.Spec.Executor.Template != nil {
		schedulingGates = job.Spec.Executor.Template.Spec.SchedulingGates
	}
	if !slices.Contains(schedulingGates, workloadSliceSchedulingGate) {
		allErrors = append(
			allErrors,
			field.Invalid(
				executorSpecPath.Child("template").Child("spec").Child("schedulingGates"),
				schedulingGates,
				"an elastic job must have the ElasticJobSchedulingGate",
			),
		)
	}

	return allErrors
}

func (w *SparkApplicationWebhook) validateTopologyRequest(ctx context.Context, sparkApp *SparkApplication) (field.ErrorList, error) {
	var allErrs field.ErrorList

	podSets, podSetsErr := sparkApp.PodSets(ctx, nil)

	if podSetsErr == nil {
		driverPodSet := podset.FindPodSetByName(podSets, driverPodSetName)
		allErrs = append(allErrs, jobframework.ValidateTASPodSetRequest(driverSpecPath, &driverPodSet.Template.ObjectMeta)...)
		allErrs = append(allErrs, jobframework.ValidateSliceSizeAnnotationUpperBound(driverSpecPath, &driverPodSet.Template.ObjectMeta, driverPodSet)...)

		executorPodSet := podset.FindPodSetByName(podSets, executorPodSetName)
		allErrs = append(allErrs, jobframework.ValidateTASPodSetRequest(executorSpecPath, &executorPodSet.Template.ObjectMeta)...)
		allErrs = append(allErrs, jobframework.ValidateSliceSizeAnnotationUpperBound(executorSpecPath, &executorPodSet.Template.ObjectMeta, executorPodSet)...)
	}

	if len(allErrs) > 0 {
		return allErrs, nil
	}

	return nil, podSetsErr
}

// ValidateUpdate implements webhook.CustomValidator so a webhook will be registered for the type
func (w *SparkApplicationWebhook) ValidateUpdate(ctx context.Context, oldSparkApp, newSparkApp *sparkv1beta2.SparkApplication) (admission.Warnings, error) {
	if !jobframework.ShouldValidateRayOrSparkJobOnUpdate(fromObject(oldSparkApp), fromObject(newSparkApp), w.manageJobsWithoutQueueName) {
		return nil, nil
	}
	log := ctrl.LoggerFrom(ctx).WithName("sparkapplication-webhook")
	log.V(5).Info("Validating update")
	allErrors := jobframework.ValidateJobOnUpdate(fromObject(oldSparkApp), fromObject(newSparkApp), w.queues.DefaultLocalQueueExist, w.maxTimeoutOnWorkload)
	validationErrs, err := w.validateCreate(ctx, oldSparkApp, newSparkApp, nil)
	if err != nil {
		return nil, err
	}
	allErrors = append(allErrors, validationErrs...)
	return w.partialPreemptionWarnings(oldSparkApp, newSparkApp), allErrors.ToAggregate()
}

// ValidateDelete implements webhook.CustomValidator so a webhook will be registered for the type
func (w *SparkApplicationWebhook) ValidateDelete(_ context.Context, _ *sparkv1beta2.SparkApplication) (admission.Warnings, error) {
	return nil, nil
}
