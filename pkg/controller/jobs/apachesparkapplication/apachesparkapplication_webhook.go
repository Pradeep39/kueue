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

	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/apimachinery/pkg/util/validation/field"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/webhook/admission"

	kueue "sigs.k8s.io/kueue/apis/kueue/v1beta2"
	qcache "sigs.k8s.io/kueue/pkg/cache/queue"
	schdcache "sigs.k8s.io/kueue/pkg/cache/scheduler"
	"sigs.k8s.io/kueue/pkg/controller/jobframework"
	sparkv1 "sigs.k8s.io/kueue/pkg/controller/jobs/apachesparkapplication/api/v1"
	"sigs.k8s.io/kueue/pkg/features"
	"sigs.k8s.io/kueue/pkg/util/podset"
	"sigs.k8s.io/kueue/pkg/util/webhook"
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
	w.integrationManager.ApplyDefaultWorkloadPriorityClass(ctx, w.client, job.Object())
	if err := w.integrationManager.ApplyDefaultForSuspend(ctx, job, w.client, w.manageJobsWithoutQueueName, w.managedJobsNamespaceSelector); err != nil {
		return err
	}
	jobframework.ApplyDefaultForManagedBy(job, w.queues, w.cache, log)

	return nil
}

// +kubebuilder:webhook:path=/validate-spark-apache-org-v1-sparkapplication,mutating=false,failurePolicy=fail,sideEffects=None,groups=spark.apache.org,resources=sparkapplications,verbs=create;update,versions=v1,name=vapachesparkapplication.kb.io,admissionReviewVersions=v1

var _ admission.Validator[*sparkv1.SparkApplication] = &SparkApplicationWebhook{}

func (w *SparkApplicationWebhook) ValidateCreate(ctx context.Context, obj *sparkv1.SparkApplication) (admission.Warnings, error) {
	log := ctrl.LoggerFrom(ctx).WithName(webhookName)
	log.V(5).Info("Validating create")
	validationErrs, err := w.validateCreate(ctx, obj)
	if err != nil {
		return nil, err
	}
	return nil, validationErrs.ToAggregate()
}

func (w *SparkApplicationWebhook) validateCreate(ctx context.Context, obj *sparkv1.SparkApplication) (field.ErrorList, error) {
	var allErrors field.ErrorList
	job := fromObject(obj)

	if w.manageJobsWithoutQueueName || jobframework.QueueName(job) != "" {
		// ClientMode runs the driver outside the cluster, so Kueue would be reserving
		// quota for a driver PodSet that never becomes a pod.
		if obj.Spec.DeploymentMode == sparkv1.ClientMode {
			allErrors = append(allErrors, field.Invalid(deploymentModePath, obj.Spec.DeploymentMode,
				"only ClusterMode is supported for a Kueue managed job"))
		}

		// Dynamic Allocation lets the driver create and delete executors directly
		// against the API server without touching the spec, so the executor PodSet
		// count would drift from reality and quota accounting would silently rot.
		if job.dynamicAllocationEnabled() {
			allErrors = append(allErrors, field.Invalid(
				sparkConfPath.Key("spark.dynamicAllocation.enabled"), true,
				"a Kueue managed job cannot use dynamicAllocation, because executor counts would not be reflected in the reserved quota"))
		}

		// Surface an unparseable resource value here rather than letting every
		// reconcile fail while building PodSets.
		if _, err := job.PodSets(ctx, nil); err != nil {
			allErrors = append(allErrors, field.Invalid(sparkConfPath, obj.Spec.SparkConf, err.Error()))
		}
	}

	allErrors = append(allErrors, jobframework.ValidateJobOnCreate(job)...)
	if features.Enabled(features.TopologyAwareScheduling) {
		validationErrs, err := w.validateTopologyRequest(ctx, job)
		if err != nil {
			return nil, err
		}
		allErrors = append(allErrors, validationErrs...)
	}

	return allErrors, nil
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
		allErrors := jobframework.ValidateJobOnUpdate(fromObject(oldObj), fromObject(newObj), w.queues.DefaultLocalQueueExist)
		validationErrs, err := w.validateCreate(ctx, newObj)
		if err != nil {
			return nil, err
		}
		allErrors = append(allErrors, validationErrs...)
		return nil, allErrors.ToAggregate()
	}
	return nil, nil
}

func (w *SparkApplicationWebhook) ValidateDelete(context.Context, *sparkv1.SparkApplication) (admission.Warnings, error) {
	return nil, nil
}
