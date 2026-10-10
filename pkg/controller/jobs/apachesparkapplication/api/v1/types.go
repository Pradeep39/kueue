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

// Package v1 contains a partial Go projection of the spark.apache.org/v1 SparkApplication
// CRD served by the Apache Spark Kubernetes Operator.
//
// Unlike every other job integration, this one cannot import generated types from the
// operator: the Apache Spark Kubernetes Operator is written in Java and publishes no Go
// module. These types are therefore maintained by hand, and deliberately cover only the
// fields Kueue reads or writes rather than the full CRD schema.
//
// A partial type is safe here because jobframework never writes the job object with a
// full Update. Every mutation goes through clientutil.Patch, which computes a merge
// patch by diffing two instances of this same type - fields this projection does not
// declare are identical on both sides, so they never appear in the patch and cannot be
// clobbered. Do not introduce a plain client.Update on a SparkApplication; it would
// erase every field omitted below.
package v1

import (
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"sigs.k8s.io/controller-runtime/pkg/scheme"
)

var (
	// GroupVersion is the group version served by the Apache Spark Kubernetes Operator.
	GroupVersion = schema.GroupVersion{Group: "spark.apache.org", Version: "v1"}

	// SchemeBuilder registers these types with a runtime.Scheme.
	SchemeBuilder = &scheme.Builder{GroupVersion: GroupVersion}

	// AddToScheme adds these types to a runtime.Scheme.
	AddToScheme = SchemeBuilder.AddToScheme
)

func init() {
	SchemeBuilder.Register(&SparkApplication{}, &SparkApplicationList{})
}

// ApplicationStateSummary mirrors org.apache.spark.k8s.operator.status.ApplicationStateSummary.
type ApplicationStateSummary string

const (
	// Submitted means the application was created but no driver has been requested yet.
	Submitted ApplicationStateSummary = "Submitted"
	// Suspended means .spec.suspend is set and the operator is withholding the driver.
	Suspended ApplicationStateSummary = "Suspended"
	// ScheduledToRestart means the application will be retried with the same configuration.
	ScheduledToRestart ApplicationStateSummary = "ScheduledToRestart"
	// DriverRequested means the driver pod has been created but is not running yet.
	DriverRequested ApplicationStateSummary = "DriverRequested"
	// DriverStarted means the driver pod reached Running.
	DriverStarted ApplicationStateSummary = "DriverStarted"
	// DriverReady means the driver is ready to serve executor connections.
	DriverReady ApplicationStateSummary = "DriverReady"
	// InitializedBelowThresholdExecutors means fewer than initExecutors are ready during start up.
	InitializedBelowThresholdExecutors ApplicationStateSummary = "InitializedBelowThresholdExecutors"
	// RunningHealthy means the application holds at least minExecutors.
	RunningHealthy ApplicationStateSummary = "RunningHealthy"
	// RunningWithPartialCapacity means the executor count sits between min and max, exclusive.
	RunningWithPartialCapacity ApplicationStateSummary = "RunningWithPartialCapacity"
	// RunningWithBelowThresholdExecutors means executors were lost after a healthy start up.
	RunningWithBelowThresholdExecutors ApplicationStateSummary = "RunningWithBelowThresholdExecutors"
	// StoppedByScheduler means an external scheduler set .spec.suspend on a running
	// application and the operator is releasing the driver. This is not a failure.
	StoppedByScheduler ApplicationStateSummary = "StoppedByScheduler"
	// DriverStartTimedOut means the driver did not start within the configured threshold.
	DriverStartTimedOut ApplicationStateSummary = "DriverStartTimedOut"
	// ExecutorsStartTimedOut means the minimum executors were not acquired in time.
	ExecutorsStartTimedOut ApplicationStateSummary = "ExecutorsStartTimedOut"
	// DriverReadyTimedOut means the driver never reached its ready state in time.
	DriverReadyTimedOut ApplicationStateSummary = "DriverReadyTimedOut"
	// Succeeded means the application completed successfully.
	Succeeded ApplicationStateSummary = "Succeeded"
	// Failed means the application failed.
	Failed ApplicationStateSummary = "Failed"
	// SchedulingFailure means the operator could not orchestrate the application.
	SchedulingFailure ApplicationStateSummary = "SchedulingFailure"
	// DriverEvicted means the driver pod was evicted.
	DriverEvicted ApplicationStateSummary = "DriverEvicted"
	// ResourceReleased means every secondary resource has been cleaned up.
	ResourceReleased ApplicationStateSummary = "ResourceReleased"
	// TerminatedWithoutReleaseResources means the application ended with resources retained.
	TerminatedWithoutReleaseResources ApplicationStateSummary = "TerminatedWithoutReleaseResources"
)

// DeploymentMode mirrors org.apache.spark.k8s.operator.spec.DeploymentMode.
type DeploymentMode string

const (
	// ClusterMode runs the driver inside the cluster.
	ClusterMode DeploymentMode = "ClusterMode"
	// ClientMode runs the driver outside the cluster.
	ClientMode DeploymentMode = "ClientMode"
)

// SparkApplication is the Schema for the spark.apache.org SparkApplication API.
type SparkApplication struct {
	metav1.TypeMeta   `json:",inline"`
	metav1.ObjectMeta `json:"metadata,omitempty"`

	Spec   ApplicationSpec   `json:"spec,omitempty"`
	Status ApplicationStatus `json:"status,omitempty"`
}

// SparkApplicationList contains a list of SparkApplication.
type SparkApplicationList struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:"metadata,omitempty"`

	Items []SparkApplication `json:"items"`
}

// ApplicationSpec is the partial spec of a SparkApplication.
type ApplicationSpec struct {
	// Suspend tells the operator to withhold the driver, or to release an already
	// running one. This is the field Kueue drives to gate admission and to preempt.
	Suspend *bool `json:"suspend,omitempty"`

	// DeploymentMode selects whether the driver runs in the cluster or on the client.
	DeploymentMode DeploymentMode `json:"deploymentMode,omitempty"`

	// PyFiles and SparkRFiles select a non-JVM application, which changes the default
	// memory overhead factor Spark applies. They are read only for that purpose.
	PyFiles     string `json:"pyFiles,omitempty"`
	SparkRFiles string `json:"sparkRFiles,omitempty"`

	// SparkConf carries the Spark configuration. The operator derives driver and
	// executor resource requests from it, so it has to be read to size PodSets.
	SparkConf map[string]string `json:"sparkConf,omitempty"`

	// DriverSpec holds the driver pod template.
	DriverSpec *BaseApplicationTemplateSpec `json:"driverSpec,omitempty"`

	// ExecutorSpec holds the executor pod template.
	ExecutorSpec *BaseApplicationTemplateSpec `json:"executorSpec,omitempty"`

	// ApplicationTolerations carries the executor instance configuration.
	ApplicationTolerations *ApplicationTolerations `json:"applicationTolerations,omitempty"`

	// RuntimeVersions is required by the CRD, so it is projected to keep objects
	// built from these types round-trippable through the API server.
	RuntimeVersions *RuntimeVersions `json:"runtimeVersions,omitempty"`
}

// BaseApplicationTemplateSpec mirrors the driver and executor template wrapper.
type BaseApplicationTemplateSpec struct {
	PodTemplateSpec *corev1.PodTemplateSpec `json:"podTemplateSpec,omitempty"`
}

// ApplicationTolerations is the partial projection of the tolerations block.
type ApplicationTolerations struct {
	InstanceConfig *ExecutorInstanceConfig `json:"instanceConfig,omitempty"`
}

// ExecutorInstanceConfig describes how many executors the application expects.
type ExecutorInstanceConfig struct {
	InitExecutors int32 `json:"initExecutors,omitempty"`
	MinExecutors  int32 `json:"minExecutors,omitempty"`
	MaxExecutors  int32 `json:"maxExecutors,omitempty"`
}

// RuntimeVersions selects the Spark version the operator builds the submission with.
type RuntimeVersions struct {
	SparkVersion string `json:"sparkVersion,omitempty"`
}

// ApplicationStatus is the partial status of a SparkApplication.
type ApplicationStatus struct {
	CurrentState ApplicationState `json:"currentState,omitempty"`

	// StateTransitionHistory records every state of the current attempt, keyed by an
	// ascending numeric id. ResourceReleased and TerminatedWithoutReleaseResources are
	// reached from both success and failure, so the history is the only way to recover
	// which outcome a released application actually had.
	StateTransitionHistory map[string]ApplicationState `json:"stateTransitionHistory,omitempty"`
}

// ApplicationState is a single entry of the operator's state machine.
type ApplicationState struct {
	CurrentStateSummary ApplicationStateSummary `json:"currentStateSummary,omitempty"`
	LastTransitionTime  string                  `json:"lastTransitionTime,omitempty"`
	Message             string                  `json:"message,omitempty"`
}

// DeepCopyInto copies the receiver into out.
func (in *SparkApplication) DeepCopyInto(out *SparkApplication) {
	*out = *in
	out.TypeMeta = in.TypeMeta
	in.ObjectMeta.DeepCopyInto(&out.ObjectMeta)
	in.Spec.DeepCopyInto(&out.Spec)
	in.Status.DeepCopyInto(&out.Status)
}

// DeepCopyInto copies the receiver into out.
func (in *ApplicationStatus) DeepCopyInto(out *ApplicationStatus) {
	*out = *in
	out.CurrentState = in.CurrentState
	if in.StateTransitionHistory != nil {
		out.StateTransitionHistory = make(map[string]ApplicationState, len(in.StateTransitionHistory))
		for k, v := range in.StateTransitionHistory {
			out.StateTransitionHistory[k] = v
		}
	}
}

// DeepCopy returns a deep copy of the receiver.
func (in *ApplicationStatus) DeepCopy() *ApplicationStatus {
	if in == nil {
		return nil
	}
	out := new(ApplicationStatus)
	in.DeepCopyInto(out)
	return out
}

// DeepCopy returns a deep copy of the receiver.
func (in *SparkApplication) DeepCopy() *SparkApplication {
	if in == nil {
		return nil
	}
	out := new(SparkApplication)
	in.DeepCopyInto(out)
	return out
}

// DeepCopyObject returns a deep copy of the receiver as a runtime.Object.
func (in *SparkApplication) DeepCopyObject() runtime.Object {
	if c := in.DeepCopy(); c != nil {
		return c
	}
	return nil
}

// DeepCopyInto copies the receiver into out.
func (in *SparkApplicationList) DeepCopyInto(out *SparkApplicationList) {
	*out = *in
	out.TypeMeta = in.TypeMeta
	in.ListMeta.DeepCopyInto(&out.ListMeta)
	if in.Items != nil {
		out.Items = make([]SparkApplication, len(in.Items))
		for i := range in.Items {
			in.Items[i].DeepCopyInto(&out.Items[i])
		}
	}
}

// DeepCopy returns a deep copy of the receiver.
func (in *SparkApplicationList) DeepCopy() *SparkApplicationList {
	if in == nil {
		return nil
	}
	out := new(SparkApplicationList)
	in.DeepCopyInto(out)
	return out
}

// DeepCopyObject returns a deep copy of the receiver as a runtime.Object.
func (in *SparkApplicationList) DeepCopyObject() runtime.Object {
	if c := in.DeepCopy(); c != nil {
		return c
	}
	return nil
}

// DeepCopyInto copies the receiver into out.
func (in *ApplicationSpec) DeepCopyInto(out *ApplicationSpec) {
	*out = *in
	if in.Suspend != nil {
		out.Suspend = new(bool)
		*out.Suspend = *in.Suspend
	}
	if in.SparkConf != nil {
		out.SparkConf = make(map[string]string, len(in.SparkConf))
		for k, v := range in.SparkConf {
			out.SparkConf[k] = v
		}
	}
	out.DriverSpec = in.DriverSpec.DeepCopy()
	out.ExecutorSpec = in.ExecutorSpec.DeepCopy()
	out.ApplicationTolerations = in.ApplicationTolerations.DeepCopy()
	out.RuntimeVersions = in.RuntimeVersions.DeepCopy()
}

// DeepCopy returns a deep copy of the receiver.
func (in *ApplicationSpec) DeepCopy() *ApplicationSpec {
	if in == nil {
		return nil
	}
	out := new(ApplicationSpec)
	in.DeepCopyInto(out)
	return out
}

// DeepCopy returns a deep copy of the receiver.
func (in *BaseApplicationTemplateSpec) DeepCopy() *BaseApplicationTemplateSpec {
	if in == nil {
		return nil
	}
	out := new(BaseApplicationTemplateSpec)
	out.PodTemplateSpec = in.PodTemplateSpec.DeepCopy()
	return out
}

// DeepCopy returns a deep copy of the receiver.
func (in *ApplicationTolerations) DeepCopy() *ApplicationTolerations {
	if in == nil {
		return nil
	}
	out := new(ApplicationTolerations)
	out.InstanceConfig = in.InstanceConfig.DeepCopy()
	return out
}

// DeepCopy returns a deep copy of the receiver.
func (in *ExecutorInstanceConfig) DeepCopy() *ExecutorInstanceConfig {
	if in == nil {
		return nil
	}
	out := new(ExecutorInstanceConfig)
	*out = *in
	return out
}

// DeepCopy returns a deep copy of the receiver.
func (in *RuntimeVersions) DeepCopy() *RuntimeVersions {
	if in == nil {
		return nil
	}
	out := new(RuntimeVersions)
	*out = *in
	return out
}
