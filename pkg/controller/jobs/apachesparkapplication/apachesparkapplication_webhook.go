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
	"slices"
	"strconv"

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
	sparkv1 "sigs.k8s.io/kueue/pkg/controller/jobs/apachesparkapplication/api/v1"
	"sigs.k8s.io/kueue/pkg/features"
	utilpod "sigs.k8s.io/kueue/pkg/util/pod"
	"sigs.k8s.io/kueue/pkg/util/podset"
	"sigs.k8s.io/kueue/pkg/util/webhook"
	"sigs.k8s.io/kueue/pkg/workloadslicing"
)

var (
	specPath           = field.NewPath("spec")
	deploymentModePath = specPath.Child("deploymentMode")
	driverSpecPath     = specPath.Child("driverSpec")
	executorSpecPath   = specPath.Child("executorSpec")
	sparkConfPath      = specPath.Child("sparkConf")
)

const webhookName = "apachesparkapplication-webhook"

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
	obj := &sparkv1.SparkApplication{}
	if options.NoopWebhook {
		return webhook.SetupNoopWebhook(mgr, obj)
	}
	return ctrl.NewWebhookManagedBy(mgr, obj).
		WithValidator(wh).
		WithDefaulter(wh).
		WithLogConstructor(jobframework.WebhookLogConstructor(fromObject(obj).GVK(), options.RoleTracker)).
		Complete()
}

// +kubebuilder:webhook:path=/mutate-spark-apache-org-v1-sparkapplication,mutating=true,failurePolicy=fail,sideEffects=None,groups=spark.apache.org,resources=sparkapplications,verbs=create,versions=v1,name=mapachesparkapplication.kb.io,admissionReviewVersions=v1

var _ admission.Defaulter[*sparkv1.SparkApplication] = &SparkApplicationWebhook{}

// Default suspends a newly created SparkApplication that Kueue manages, so the operator
// never requests a driver before the workload has been admitted.
func (w *SparkApplicationWebhook) Default(ctx context.Context, obj *sparkv1.SparkApplication) error {
	job := fromObject(obj)
	log := ctrl.LoggerFrom(ctx).WithName(webhookName)
	log.V(5).Info("Applying defaults")

	if err := w.integrationManager.ApplyDefaultLocalQueue(ctx, w.client, job.Object(), w.queues.DefaultLocalQueueExist, w.managedJobsNamespaceSelector); err != nil {
		return err
	}
	w.integrationManager.ApplyDefaultWorkloadPriorityClass(ctx, w.client, job.Object(), w.managedJobsNamespaceSelector)
	if err := w.integrationManager.ApplyDefaultForSuspend(ctx, job, w.client, w.manageJobsWithoutQueueName, w.managedJobsNamespaceSelector); err != nil {
		return err
	}
	jobframework.ApplyDefaultForManagedBy(job, w.queues, w.cache, log)

	if isAnElasticJob(obj) {
		// Ensure the scheduling gate is present in the executor pod template. The operator
		// materialises that template into a file and hands it to the driver as
		// spark.kubernetes.executor.podTemplateFile, so Dynamic-Allocation-created executor
		// pods inherit the gate and stay unschedulable until the ElasticJobUngater confirms
		// the owning workload slice has been granted quota for them.
		utilpod.GateTemplate(job.ensureTemplateSpec(roleExecutor), kueue.ElasticJobSchedulingGate)
	}

	if isCreateRequest(ctx) && partialPreemptionEnabled(job) && job.reclaimable() {
		applyDecommissionDefaults(job)
	}

	return nil
}

// isCreateRequest reports whether the admission request being defaulted is a CREATE. The
// mutating webhook is registered for CREATE only (see the marker above), but the
// partial-preemption defaults must never touch an existing application's spec. A context
// without an admission request (unit tests) is treated as a create.
func isCreateRequest(ctx context.Context) bool {
	req, err := admission.RequestFromContext(ctx)
	if err != nil {
		return true
	}
	return req.Operation == admissionv1.Create
}

// applyDecommissionDefaults turns on Spark's graceful decommissioning for a reclaimable
// application, so deleting an executor Pod migrates its blocks rather than losing them
// (docs/design/spark-aware-preemption.md §5.4). Values the application sets explicitly,
// including "false", are left alone. Executors inherit the settings from sparkConf: the operator
// passes sparkConf to spark-submit and the driver builds each executor from it.
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
// one place to the other: the executor Pod template (Spark before 4.1) and
// spark.kubernetes.executor.terminationGracePeriodSeconds (Spark 4.1+, which overrides the
// template). An application that sets neither is left alone: Kubernetes and Spark both default
// to 30s, and partial preemption applies its own grace period when it deletes the Pods.
func mirrorExecutorTerminationGracePeriod(job *SparkApplication) {
	var templateGrace *int64
	if t := templateSpec(job.SparkApplication, roleExecutor); t != nil {
		templateGrace = t.Spec.TerminationGracePeriodSeconds
	}
	confGrace, confSet := job.conf(sparkConfExecutorTerminationGracePeriod)
	switch {
	case templateGrace != nil && !confSet:
		job.Spec.SparkConf[sparkConfExecutorTerminationGracePeriod] = strconv.FormatInt(*templateGrace, 10) + "s"
	case templateGrace == nil && confSet:
		seconds, ok := sparkTimeSeconds(confGrace)
		if !ok {
			return
		}
		job.ensureTemplateSpec(roleExecutor).Spec.TerminationGracePeriodSeconds = ptr.To(seconds)
	}
}

// isAnElasticJob reports whether the application opted into workload slices.
func isAnElasticJob(app *sparkv1.SparkApplication) bool {
	return workloadslicing.Enabled(app)
}

// +kubebuilder:webhook:path=/validate-spark-apache-org-v1-sparkapplication,mutating=false,failurePolicy=fail,sideEffects=None,groups=spark.apache.org,resources=sparkapplications,verbs=create;update,versions=v1,name=vapachesparkapplication.kb.io,admissionReviewVersions=v1

var _ admission.Validator[*sparkv1.SparkApplication] = &SparkApplicationWebhook{}

func (w *SparkApplicationWebhook) ValidateCreate(ctx context.Context, obj *sparkv1.SparkApplication) (admission.Warnings, error) {
	log := ctrl.LoggerFrom(ctx).WithName(webhookName)
	log.V(5).Info("Validating create")
	validationErrs, err := w.validateCreate(ctx, nil, obj)
	if err != nil {
		return nil, err
	}
	return w.partialPreemptionWarnings(nil, obj), validationErrs.ToAggregate()
}

// partialPreemptionWarnings warns, without rejecting, about a partial-preemption floor that will
// be ignored: spark.kueue.preemption.minExecutors only takes effect on the elastic path, so on an
// application without the elastic-job annotation it silently does nothing, and the application
// is preempted whole. On update it warns only when the property is added or changed.
func (w *SparkApplicationWebhook) partialPreemptionWarnings(oldObj, obj *sparkv1.SparkApplication) admission.Warnings {
	if !features.Enabled(features.SparkPartialPreemption) {
		return nil
	}
	if !w.manageJobsWithoutQueueName && jobframework.QueueName(fromObject(obj)) == "" {
		return nil
	}
	if _, set := obj.Spec.SparkConf[sparkConfPreemptionMinExecutors]; !set || isAnElasticJob(obj) {
		return nil
	}
	if oldObj != nil && !preemptionMinExecutorsChanged(oldObj, obj) {
		return nil
	}
	return admission.Warnings{floorIgnoredWarning()}
}

func floorIgnoredWarning() string {
	return fmt.Sprintf(
		"sparkConf %q has no effect without the %s: %q annotation; this application is not partially preempted and will be preempted whole",
		sparkConfPreemptionMinExecutors, workloadslicing.EnabledAnnotationKey, workloadslicing.EnabledAnnotationValue)
}

// preemptionMinExecutorsChanged reports whether obj's spark.kueue.preemption.minExecutors is new
// (oldObj is nil, i.e. a create) or differs from oldObj's.
func preemptionMinExecutorsChanged(oldObj, obj *sparkv1.SparkApplication) bool {
	if oldObj == nil {
		return true
	}
	oldValue, oldOk := oldObj.Spec.SparkConf[sparkConfPreemptionMinExecutors]
	newValue, newOk := obj.Spec.SparkConf[sparkConfPreemptionMinExecutors]
	return oldOk != newOk || oldValue != newValue
}

// validateCreate validates obj. oldObj is nil on create, and the previous object on update.
func (w *SparkApplicationWebhook) validateCreate(ctx context.Context, oldObj, obj *sparkv1.SparkApplication) (field.ErrorList, error) {
	var allErrors field.ErrorList
	job := fromObject(obj)

	if w.manageJobsWithoutQueueName || jobframework.QueueName(job) != "" {
		// ClientMode runs the driver outside the cluster, so Kueue would be reserving
		// quota for a driver PodSet that never becomes a pod.
		if obj.Spec.DeploymentMode == sparkv1.ClientMode {
			allErrors = append(allErrors, field.Invalid(deploymentModePath, obj.Spec.DeploymentMode,
				"only ClusterMode is supported for a Kueue managed job"))
		}

		// Dynamic Allocation lets the driver create and delete executors directly against
		// the API server without touching the spec. That is only safe to admit when
		// workload slices are in play, because then the executor PodSet count is re-derived
		// from live pods and each scale-up is gated on a new slice being granted quota.
		// Without slices the count would go stale and quota accounting would silently rot.
		if isAnElasticJob(obj) {
			allErrors = append(allErrors, validateElasticJob(obj)...)
		} else if job.dynamicAllocationEnabled() {
			allErrors = append(allErrors, field.Invalid(
				sparkConfPath.Key("spark.dynamicAllocation.enabled"), true,
				fmt.Sprintf("a Kueue managed job can use dynamicAllocation only when the %s feature gate is on "+
					"and the application is annotated %s=%s",
					features.ElasticJobsViaWorkloadSlices,
					workloadslicing.EnabledAnnotationKey, workloadslicing.EnabledAnnotationValue)))
		}

		// The partial-preemption floor. Only a new or changed value is rejected: an application
		// created before the gate was turned on must still accept updates, including Kueue's
		// own suspend, and is simply treated as having no property.
		if features.Enabled(features.SparkPartialPreemption) && preemptionMinExecutorsChanged(oldObj, obj) {
			if _, _, err := job.preemptionMinExecutors(); err != nil {
				allErrors = append(allErrors, field.Invalid(
					sparkConfPath.Key(sparkConfPreemptionMinExecutors),
					obj.Spec.SparkConf[sparkConfPreemptionMinExecutors],
					"must be a non-negative integer"))
			}
		}

		// Surface an unparseable resource value here rather than letting every
		// reconcile fail while building PodSets.
		if _, err := job.PodSets(ctx, nil); err != nil {
			allErrors = append(allErrors, field.Invalid(sparkConfPath, obj.Spec.SparkConf, err.Error()))
		}
	}

	allErrors = append(allErrors, jobframework.ValidateJobOnCreate(job, w.maxTimeoutOnWorkload)...)
	if features.Enabled(features.TopologyAwareScheduling) {
		validationErrs, err := w.validateTopologyRequest(ctx, job)
		if err != nil {
			return nil, err
		}
		allErrors = append(allErrors, validationErrs...)
	}

	return allErrors, nil
}

// validateElasticJob checks the invariant the workload-slice scheme depends on: executor
// pods must come up gated, or Dynamic Allocation would schedule them before the slice
// covering them had been granted quota.
func validateElasticJob(app *sparkv1.SparkApplication) field.ErrorList {
	var allErrors field.ErrorList

	var gates []corev1.PodSchedulingGate
	if template := templateSpec(app, roleExecutor); template != nil {
		gates = template.Spec.SchedulingGates
	}

	if !slices.Contains(gates, corev1.PodSchedulingGate{Name: kueue.ElasticJobSchedulingGate}) {
		allErrors = append(allErrors, field.Invalid(
			executorSpecPath.Child("podTemplateSpec").Child("spec").Child("schedulingGates"),
			gates,
			fmt.Sprintf("an elastic job must carry the %s scheduling gate on its executor pod template",
				kueue.ElasticJobSchedulingGate)))
	}

	return allErrors
}

func (w *SparkApplicationWebhook) validateTopologyRequest(ctx context.Context, job *SparkApplication) (field.ErrorList, error) {
	var allErrs field.ErrorList

	podSets, podSetsErr := job.PodSets(ctx, nil)
	if podSetsErr == nil {
		for path, name := range map[*field.Path]kueue.PodSetReference{
			driverSpecPath:   kueue.NewPodSetReference(driverPodSetName),
			executorSpecPath: kueue.NewPodSetReference(executorPodSetName),
		} {
			ps := podset.FindPodSetByName(podSets, name)
			allErrs = append(allErrs, jobframework.ValidateTASPodSetRequest(path, &ps.Template.ObjectMeta)...)
			allErrs = append(allErrs, jobframework.ValidateSliceSizeAnnotationUpperBound(path, &ps.Template.ObjectMeta, ps)...)
		}
	}

	if len(allErrs) > 0 {
		return allErrs, nil
	}
	return nil, podSetsErr
}

func (w *SparkApplicationWebhook) ValidateUpdate(ctx context.Context, oldObj, newObj *sparkv1.SparkApplication) (admission.Warnings, error) {
	log := ctrl.LoggerFrom(ctx).WithName(webhookName)
	if w.manageJobsWithoutQueueName || jobframework.QueueName(fromObject(newObj)) != "" {
		log.V(5).Info("Validating update")
		allErrors := jobframework.ValidateJobOnUpdate(fromObject(oldObj), fromObject(newObj), w.queues.DefaultLocalQueueExist, w.maxTimeoutOnWorkload)
		validationErrs, err := w.validateCreate(ctx, oldObj, newObj)
		if err != nil {
			return nil, err
		}
		allErrors = append(allErrors, validationErrs...)
		return w.partialPreemptionWarnings(oldObj, newObj), allErrors.ToAggregate()
	}
	return nil, nil
}

func (w *SparkApplicationWebhook) ValidateDelete(context.Context, *sparkv1.SparkApplication) (admission.Warnings, error) {
	return nil, nil
}
