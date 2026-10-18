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
	"slices"
	"testing"
	"time"

	"github.com/google/go-cmp/cmp"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/component-base/featuregate"
	"k8s.io/utils/ptr"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/interceptor"

	kueue "sigs.k8s.io/kueue/apis/kueue/v1beta2"
	"sigs.k8s.io/kueue/pkg/constants"
	controllerconstants "sigs.k8s.io/kueue/pkg/controller/constants"
	coreindexer "sigs.k8s.io/kueue/pkg/controller/core/indexer"
	"sigs.k8s.io/kueue/pkg/controller/jobframework"
	sparkv1 "sigs.k8s.io/kueue/pkg/controller/jobs/apachesparkapplication/api/v1"
	"sigs.k8s.io/kueue/pkg/features"
	utiltesting "sigs.k8s.io/kueue/pkg/util/testing"
)

var partialPreemptionGates = map[featuregate.Feature]bool{
	features.ElasticJobsViaWorkloadSlices: true,
	features.SparkPartialPreemption:       true,
}

// confSpec builds a spec from sparkConf entries.
func confSpec(conf map[string]string) sparkv1.ApplicationSpec {
	return sparkv1.ApplicationSpec{SparkConf: conf}
}

func TestPartialPreemptionFloor(t *testing.T) {
	cases := map[string]struct {
		app       *sparkv1.SparkApplication
		gates     map[featuregate.Feature]bool
		wantFloor int32
		wantOK    bool
	}{
		"gate off: not partially preemptible": {
			app:   elasticApp(confSpec(map[string]string{"spark.executor.instances": "6", sparkConfPreemptionMinExecutors: "2"})),
			gates: map[featuregate.Feature]bool{features.ElasticJobsViaWorkloadSlices: true},
		},
		"not elastic: not partially preemptible": {
			app: &sparkv1.SparkApplication{
				ObjectMeta: metav1.ObjectMeta{Name: "pi", Namespace: "ns"},
				Spec:       confSpec(map[string]string{"spark.executor.instances": "6", sparkConfPreemptionMinExecutors: "2"}),
			},
		},
		"static without the property: the floor is the executor count, so nothing is reclaimable": {
			app:       elasticApp(confSpec(map[string]string{"spark.executor.instances": "6"})),
			wantFloor: 6, wantOK: true,
		},
		"static with the property": {
			app:       elasticApp(confSpec(map[string]string{"spark.executor.instances": "6", sparkConfPreemptionMinExecutors: "2"})),
			wantFloor: 2, wantOK: true,
		},
		"static property above the count is capped at the count": {
			app:       elasticApp(confSpec(map[string]string{"spark.executor.instances": "6", sparkConfPreemptionMinExecutors: "9"})),
			wantFloor: 6, wantOK: true,
		},
		"Dynamic Allocation minExecutors from sparkConf": {
			app:       elasticApp(daSpec(map[string]string{"spark.dynamicAllocation.minExecutors": "3", "spark.dynamicAllocation.maxExecutors": "10"})),
			wantFloor: 3, wantOK: true,
		},
		"Dynamic Allocation minExecutors from instanceConfig": {
			app: elasticApp(func() sparkv1.ApplicationSpec {
				s := daSpec(nil)
				s.ApplicationTolerations = &sparkv1.ApplicationTolerations{InstanceConfig: &sparkv1.ExecutorInstanceConfig{MinExecutors: 2, MaxExecutors: 10}}
				return s
			}()),
			wantFloor: 2, wantOK: true,
		},
		"Dynamic Allocation without minExecutors: at least 1": {
			app:       elasticApp(daSpec(nil)),
			wantFloor: 1, wantOK: true,
		},
		"the property wins over Dynamic Allocation's minExecutors": {
			app:       elasticApp(daSpec(map[string]string{"spark.dynamicAllocation.minExecutors": "3", sparkConfPreemptionMinExecutors: "1"})),
			wantFloor: 1, wantOK: true,
		},
		"the floor never goes below the operator's instanceConfig.minExecutors": {
			app: elasticApp(func() sparkv1.ApplicationSpec {
				s := confSpec(map[string]string{"spark.executor.instances": "6", sparkConfPreemptionMinExecutors: "1"})
				s.ApplicationTolerations = &sparkv1.ApplicationTolerations{InstanceConfig: &sparkv1.ExecutorInstanceConfig{MinExecutors: 4, InitExecutors: 6}}
				return s
			}()),
			wantFloor: 4, wantOK: true,
		},
		"Dynamic Allocation: sparkConf minExecutors below instanceConfig.minExecutors is raised": {
			app: elasticApp(func() sparkv1.ApplicationSpec {
				s := daSpec(map[string]string{"spark.dynamicAllocation.minExecutors": "1", "spark.dynamicAllocation.initialExecutors": "4"})
				s.ApplicationTolerations = &sparkv1.ApplicationTolerations{InstanceConfig: &sparkv1.ExecutorInstanceConfig{MinExecutors: 3}}
				return s
			}()),
			wantFloor: 3, wantOK: true,
		},
		"a malformed property is treated as unset": {
			app:       elasticApp(daSpec(map[string]string{"spark.dynamicAllocation.minExecutors": "3", sparkConfPreemptionMinExecutors: "x"})),
			wantFloor: 3, wantOK: true,
		},
	}
	for name, tc := range cases {
		t.Run(name, func(t *testing.T) {
			gates := tc.gates
			if gates == nil {
				gates = partialPreemptionGates
			}
			features.SetFeatureGatesDuringTest(t, gates)
			podSet, floor, ok := fromObject(tc.app).PartialPreemptionFloor()
			if ok != tc.wantOK || floor != tc.wantFloor {
				t.Errorf("PartialPreemptionFloor() = (%q, %d, %v), want (%d, %v)", podSet, floor, ok, tc.wantFloor, tc.wantOK)
			}
			if ok && podSet != executorPodSetName {
				t.Errorf("PartialPreemptionFloor() PodSet = %q, want %q", podSet, executorPodSetName)
			}
		})
	}
}

func TestReclaimable(t *testing.T) {
	cases := map[string]struct {
		spec sparkv1.ApplicationSpec
		want bool
	}{
		"static, floor below the count":          {spec: confSpec(map[string]string{"spark.executor.instances": "6", sparkConfPreemptionMinExecutors: "2"}), want: true},
		"static, no property (floor = count)":    {spec: confSpec(map[string]string{"spark.executor.instances": "6"})},
		"Dynamic Allocation, maxExecutors unset": {spec: daSpec(map[string]string{"spark.dynamicAllocation.minExecutors": "2"}), want: true},
		"Dynamic Allocation, floor below maxExecutors": {
			spec: daSpec(map[string]string{"spark.dynamicAllocation.minExecutors": "2", "spark.dynamicAllocation.maxExecutors": "4"}), want: true,
		},
		"Dynamic Allocation, floor equals maxExecutors": {
			spec: daSpec(map[string]string{"spark.dynamicAllocation.minExecutors": "2", "spark.dynamicAllocation.maxExecutors": "2"}),
		},
	}
	for name, tc := range cases {
		t.Run(name, func(t *testing.T) {
			if got := fromObject(elasticApp(tc.spec)).reclaimable(); got != tc.want {
				t.Errorf("reclaimable() = %v, want %v", got, tc.want)
			}
		})
	}
}

// reclaimPod is an executor Pod of the application "pi" for the reclaim tests.
type reclaimPod struct {
	name     string
	role     string
	cost     string
	node     string
	ageSec   int
	gated    bool
	deleting bool
	phase    corev1.PodPhase
}

var reclaimBaseTime = time.Date(2026, 10, 1, 0, 0, 0, 0, time.UTC)

func (p reclaimPod) obj() *corev1.Pod {
	role, phase := p.role, p.phase
	if role == "" {
		role = executorRoleValue
	}
	if phase == "" {
		phase = corev1.PodRunning
	}
	pod := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:              p.name,
			Namespace:         "ns",
			Labels:            map[string]string{appNameLabel: "pi", roleLabel: role},
			CreationTimestamp: metav1.NewTime(reclaimBaseTime.Add(-time.Duration(p.ageSec) * time.Second)),
			// Every Pod carries a finalizer so a delete leaves it in place with a
			// deletionTimestamp, as a terminating Pod does.
			Finalizers: []string{"test/keep"},
		},
		Spec:   corev1.PodSpec{NodeName: p.node},
		Status: corev1.PodStatus{Phase: phase},
	}
	if p.cost != "" {
		pod.Annotations = map[string]string{constants.PodDeletionCostAnnotation: p.cost}
	}
	if p.gated {
		pod.Spec.SchedulingGates = []corev1.PodSchedulingGate{{Name: kueue.ElasticJobSchedulingGate}}
	}
	if p.deleting {
		pod.DeletionTimestamp = ptr.To(metav1.NewTime(reclaimBaseTime))
	}
	return pod
}

func deletedPods(t *testing.T, c client.Client) []string {
	t.Helper()
	pods := &corev1.PodList{}
	if err := c.List(t.Context(), pods); err != nil {
		t.Fatalf("List() failed: %v", err)
	}
	var got []string
	for i := range pods.Items {
		if pods.Items[i].DeletionTimestamp != nil {
			got = append(got, pods.Items[i].Name)
		}
	}
	slices.Sort(got)
	return got
}

func TestReclaimPods(t *testing.T) {
	static := func(n, floor string) *sparkv1.SparkApplication {
		return elasticApp(confSpec(map[string]string{"spark.executor.instances": n, sparkConfPreemptionMinExecutors: floor}))
	}
	cases := map[string]struct {
		app         *sparkv1.SparkApplication
		gates       map[featuregate.Feature]bool
		pods        []reclaimPod
		podSet      kueue.PodSetReference
		keep        int32
		wantDone    bool
		wantErr     bool
		wantDeleted []string // Pods with a deletionTimestamp afterwards, including pre-existing ones
	}{
		"orders by cost, then unbound first, then newest first": {
			app: static("6", "1"),
			pods: []reclaimPod{
				{name: "a-cost5", cost: "5", ageSec: 100},
				{name: "b-old-bound", node: "n1", ageSec: 50},
				{name: "c-new-bound", node: "n1", ageSec: 1},
				{name: "d-unbound", ageSec: 0},
				{name: "e-neg", cost: "-10", node: "n1", ageSec: 0},
				{name: "f-unparsable", cost: "x", node: "n1", ageSec: 10},
			},
			keep:        2,
			wantDeleted: []string{"c-new-bound", "d-unbound", "e-neg", "f-unparsable"},
		},
		"never deletes the driver or gated Pods": {
			app: static("4", "0"),
			pods: []reclaimPod{
				{name: "driver", role: driverRoleValue, cost: "-100"},
				{name: "gated1", cost: "-100", gated: true, phase: corev1.PodPending},
				{name: "e1", node: "n1"},
				{name: "e2", node: "n1"},
			},
			keep:        0,
			wantDeleted: []string{"e1", "e2"},
		},
		"never below the floor": {
			app:         static("4", "3"),
			pods:        []reclaimPod{{name: "e1", node: "n1"}, {name: "e2", node: "n1"}, {name: "e3", node: "n1"}, {name: "e4", node: "n1"}},
			keep:        1,
			wantDeleted: []string{"e1"},
		},
		"Dynamic Allocation floor is at least 1": {
			app:         elasticApp(daSpec(nil)),
			pods:        []reclaimPod{{name: "e1", node: "n1"}, {name: "e2", node: "n1"}},
			keep:        0,
			wantDeleted: []string{"e1"},
		},
		"deletions in flight count as holding: nothing more is deleted": {
			app:         static("4", "1"),
			pods:        []reclaimPod{{name: "e1", node: "n1", deleting: true}, {name: "e2", node: "n1", deleting: true}, {name: "e3", node: "n1"}, {name: "e4", node: "n1"}},
			keep:        2,
			wantDeleted: []string{"e1", "e2"},
		},
		"done when holding <= keep, ignoring gated and terminal Pods": {
			app: static("4", "1"),
			pods: []reclaimPod{
				{name: "e1", node: "n1"}, {name: "e2", node: "n1"},
				{name: "gated", gated: true, phase: corev1.PodPending},
				{name: "done", node: "n1", phase: corev1.PodSucceeded},
			},
			keep:     2,
			wantDone: true,
		},
		"other PodSet is rejected": {
			app:     static("4", "1"),
			pods:    []reclaimPod{{name: "e1"}, {name: "e2"}},
			podSet:  driverPodSetName,
			wantErr: true,
		},
		"gate off is rejected": {
			app:     static("4", "1"),
			gates:   map[featuregate.Feature]bool{features.ElasticJobsViaWorkloadSlices: true},
			pods:    []reclaimPod{{name: "e1"}, {name: "e2"}},
			wantErr: true,
		},
	}
	for name, tc := range cases {
		t.Run(name, func(t *testing.T) {
			gates := tc.gates
			if gates == nil {
				gates = partialPreemptionGates
			}
			features.SetFeatureGatesDuringTest(t, gates)
			objs := make([]client.Object, 0, len(tc.pods))
			for _, p := range tc.pods {
				objs = append(objs, p.obj())
			}
			c := utiltesting.NewClientBuilder().WithObjects(objs...).Build()
			podSet := tc.podSet
			if podSet == "" {
				podSet = executorPodSetName
			}
			done, err := fromObject(tc.app).ReclaimPods(t.Context(), c, podSet, tc.keep, nil)
			if (err != nil) != tc.wantErr {
				t.Fatalf("ReclaimPods() error = %v, wantErr %v", err, tc.wantErr)
			}
			if done != tc.wantDone {
				t.Errorf("ReclaimPods() done = %v, want %v", done, tc.wantDone)
			}
			if diff := cmp.Diff(tc.wantDeleted, deletedPods(t, c)); diff != "" {
				t.Errorf("deleted Pods (-want +got):\n%s", diff)
			}
			if tc.wantErr {
				return
			}
			// Level-based: a second call, with the first call's deletions still in flight,
			// deletes nothing more.
			if _, err := fromObject(tc.app).ReclaimPods(t.Context(), c, podSet, tc.keep, nil); err != nil {
				t.Fatalf("second ReclaimPods() returned an unexpected error: %v", err)
			}
			if diff := cmp.Diff(tc.wantDeleted, deletedPods(t, c)); diff != "" {
				t.Errorf("second ReclaimPods() deleted more Pods (-want +got):\n%s", diff)
			}
		})
	}
}

// TestReclaimPodsGracePeriod checks that Pods are deleted with the reclaim's grace period,
// rounded up to whole seconds, and with none when the reclaim has none.
func TestReclaimPodsGracePeriod(t *testing.T) {
	cases := map[string]struct {
		grace *time.Duration
		want  *int64
	}{
		"no grace period: the Pod's own applies": {},
		"whole seconds":                          {grace: ptr.To(45 * time.Second), want: ptr.To[int64](45)},
		"sub-second remainder rounds up":         {grace: ptr.To(1500 * time.Millisecond), want: ptr.To[int64](2)},
	}
	for name, tc := range cases {
		t.Run(name, func(t *testing.T) {
			features.SetFeatureGatesDuringTest(t, partialPreemptionGates)
			var got []*int64
			c := utiltesting.NewClientBuilder().
				WithObjects(reclaimPod{name: "e1", node: "n1"}.obj(), reclaimPod{name: "e2", node: "n1"}.obj()).
				WithInterceptorFuncs(interceptor.Funcs{Delete: func(ctx context.Context, c client.WithWatch, obj client.Object, opts ...client.DeleteOption) error {
					o := &client.DeleteOptions{}
					o.ApplyOptions(opts)
					got = append(got, o.GracePeriodSeconds)
					return c.Delete(ctx, obj, opts...)
				}}).Build()
			app := elasticApp(confSpec(map[string]string{"spark.executor.instances": "2", sparkConfPreemptionMinExecutors: "1"}))
			if _, err := fromObject(app).ReclaimPods(t.Context(), c, executorPodSetName, 1, tc.grace); err != nil {
				t.Fatalf("ReclaimPods() error = %v", err)
			}
			if diff := cmp.Diff([]*int64{tc.want}, got); diff != "" {
				t.Errorf("Delete grace period (-want +got):\n%s", diff)
			}
		})
	}
}

// TestPartialPreemptionMarkedPods checks that PodSets() observes the executors Spark has marked
// for removal, for both a static and a Dynamic Allocation application.
func TestPartialPreemptionMarkedPods(t *testing.T) {
	cases := map[string]struct {
		app  *sparkv1.SparkApplication
		want int32
	}{
		"static": {
			app:  elasticApp(confSpec(map[string]string{"spark.executor.instances": "3", sparkConfPreemptionMinExecutors: "1"})),
			want: 1,
		},
		"Dynamic Allocation": {
			app:  elasticApp(daSpec(map[string]string{"spark.dynamicAllocation.minExecutors": "1"})),
			want: 1,
		},
	}
	for name, tc := range cases {
		t.Run(name, func(t *testing.T) {
			features.SetFeatureGatesDuringTest(t, partialPreemptionGates)
			c := utiltesting.NewClientBuilder().
				WithIndex(&kueue.Workload{}, jobframework.OwnerReferenceIndexKey(gvk), coreindexer.WorkloadOwnerIndexFunc(gvk)).
				WithObjects(
					reclaimPod{name: "marked", cost: "-5", node: "n1"}.obj(),
					reclaimPod{name: "plain", node: "n1"}.obj(),
					reclaimPod{name: "marked-gated", cost: "-5", gated: true, phase: corev1.PodPending}.obj(),
					reclaimPod{name: "marked-deleting", cost: "-5", node: "n1", deleting: true}.obj(),
				).Build()
			job := fromObject(tc.app)
			if _, ok := job.PartialPreemptionMarkedPods(); ok {
				t.Fatal("PartialPreemptionMarkedPods() reported a count before PodSets() ran")
			}
			if _, err := job.PodSets(t.Context(), c); err != nil {
				t.Fatalf("PodSets() error = %v", err)
			}
			if got, ok := job.PartialPreemptionMarkedPods(); !ok || got != tc.want {
				t.Errorf("PartialPreemptionMarkedPods() = (%d, %v), want (%d, true)", got, ok, tc.want)
			}
		})
	}
}

func TestApplyDecommissionDefaults(t *testing.T) {
	withGrace := func(spec sparkv1.ApplicationSpec, seconds int64) sparkv1.ApplicationSpec {
		spec.ExecutorSpec = &sparkv1.BaseApplicationTemplateSpec{PodTemplateSpec: &corev1.PodTemplateSpec{
			Spec: corev1.PodSpec{TerminationGracePeriodSeconds: ptr.To(seconds)},
		}}
		return spec
	}
	defaults := map[string]string{
		"spark.decommission.enabled":                       "true",
		"spark.storage.decommission.enabled":               "true",
		"spark.storage.decommission.shuffleBlocks.enabled": "true",
		"spark.storage.decommission.rddBlocks.enabled":     "true",
	}
	with := func(extra map[string]string) map[string]string {
		out := map[string]string{}
		for k, v := range defaults {
			out[k] = v
		}
		for k, v := range extra {
			out[k] = v
		}
		return out
	}
	cases := map[string]struct {
		spec          sparkv1.ApplicationSpec
		wantConf      map[string]string
		wantGraceTmpl *int64
	}{
		"defaults the decommission settings and leaves the grace period unset": {
			spec:     confSpec(map[string]string{"spark.executor.instances": "4"}),
			wantConf: with(map[string]string{"spark.executor.instances": "4"}),
		},
		"keeps explicit values, including false": {
			spec:     confSpec(map[string]string{"spark.decommission.enabled": "false"}),
			wantConf: with(map[string]string{"spark.decommission.enabled": "false"}),
		},
		"copies the template's grace period to sparkConf": {
			spec:          withGrace(confSpec(nil), 90),
			wantConf:      with(map[string]string{sparkConfExecutorTerminationGracePeriod: "90s"}),
			wantGraceTmpl: ptr.To[int64](90),
		},
		"copies sparkConf's grace period to the template": {
			spec:          confSpec(map[string]string{sparkConfExecutorTerminationGracePeriod: "2m"}),
			wantConf:      with(map[string]string{sparkConfExecutorTerminationGracePeriod: "2m"}),
			wantGraceTmpl: ptr.To[int64](120),
		},
	}
	for name, tc := range cases {
		t.Run(name, func(t *testing.T) {
			job := fromObject(elasticApp(tc.spec))
			applyDecommissionDefaults(job)
			if diff := cmp.Diff(tc.wantConf, job.Spec.SparkConf); diff != "" {
				t.Errorf("sparkConf (-want +got):\n%s", diff)
			}
			var gotGrace *int64
			if tmpl := templateSpec(job.SparkApplication, roleExecutor); tmpl != nil {
				gotGrace = tmpl.Spec.TerminationGracePeriodSeconds
			}
			if diff := cmp.Diff(tc.wantGraceTmpl, gotGrace); diff != "" {
				t.Errorf("executor template terminationGracePeriodSeconds (-want +got):\n%s", diff)
			}
		})
	}
}

func TestPartialPreemptionTerminationGracePeriod(t *testing.T) {
	cases := map[string]struct {
		spec      sparkv1.ApplicationSpec
		want      time.Duration
		wantFound bool
	}{
		"neither set": {},
		"template": {
			spec: sparkv1.ApplicationSpec{ExecutorSpec: &sparkv1.BaseApplicationTemplateSpec{PodTemplateSpec: &corev1.PodTemplateSpec{
				Spec: corev1.PodSpec{TerminationGracePeriodSeconds: ptr.To[int64](90)},
			}}},
			want: 90 * time.Second, wantFound: true,
		},
		"Spark property": {
			spec: confSpec(map[string]string{sparkConfExecutorTerminationGracePeriod: "2m"}),
			want: 2 * time.Minute, wantFound: true,
		},
		"malformed Spark property is ignored": {
			spec: confSpec(map[string]string{sparkConfExecutorTerminationGracePeriod: "soon"}),
		},
	}
	for name, tc := range cases {
		t.Run(name, func(t *testing.T) {
			got, found := fromObject(elasticApp(tc.spec)).PartialPreemptionTerminationGracePeriod()
			if got != tc.want || found != tc.wantFound {
				t.Errorf("PartialPreemptionTerminationGracePeriod() = (%v, %v), want (%v, %v)", got, found, tc.want, tc.wantFound)
			}
		})
	}
}

func TestSparkTimeSeconds(t *testing.T) {
	cases := map[string]struct {
		in     string
		want   int64
		wantOK bool
	}{
		"bare number is seconds": {in: "30", want: 30, wantOK: true},
		"seconds":                {in: "45s", want: 45, wantOK: true},
		"minutes":                {in: "2m", want: 120, wantOK: true},
		"min":                    {in: "3min", want: 180, wantOK: true},
		"milliseconds truncate":  {in: "1500ms", want: 1, wantOK: true},
		"hours":                  {in: "1h", want: 3600, wantOK: true},
		"negative":               {in: "-5s"},
		"garbage":                {in: "soon"},
	}
	for name, tc := range cases {
		t.Run(name, func(t *testing.T) {
			got, ok := sparkTimeSeconds(tc.in)
			if got != tc.want || ok != tc.wantOK {
				t.Errorf("sparkTimeSeconds(%q) = (%d, %v), want (%d, %v)", tc.in, got, ok, tc.want, tc.wantOK)
			}
		})
	}
}

func TestPartialPreemptionFloorValidationAndWarnings(t *testing.T) {
	queued := func(app *sparkv1.SparkApplication) *sparkv1.SparkApplication {
		if app.Labels == nil {
			app.Labels = map[string]string{}
		}
		app.Labels[controllerconstants.QueueLabel] = "lq"
		return app
	}
	notElastic := func(conf map[string]string) *sparkv1.SparkApplication {
		return queued(&sparkv1.SparkApplication{ObjectMeta: metav1.ObjectMeta{Name: "pi", Namespace: "ns"}, Spec: confSpec(conf)})
	}
	elastic := func(conf map[string]string) *sparkv1.SparkApplication {
		app := queued(elasticApp(confSpec(conf)))
		utilpodGateExecutor(app)
		return app
	}
	cases := map[string]struct {
		gates     map[featuregate.Feature]bool
		old       *sparkv1.SparkApplication
		app       *sparkv1.SparkApplication
		wantErr   bool
		wantWarns int
	}{
		"malformed floor on create is rejected": {
			app:     elastic(map[string]string{"spark.executor.instances": "4", sparkConfPreemptionMinExecutors: "x"}),
			wantErr: true,
		},
		"negative floor on create is rejected": {
			app:     elastic(map[string]string{"spark.executor.instances": "4", sparkConfPreemptionMinExecutors: "-1"}),
			wantErr: true,
		},
		"valid floor on an elastic app: no error, no warning": {
			app: elastic(map[string]string{"spark.executor.instances": "4", sparkConfPreemptionMinExecutors: "1"}),
		},
		"unchanged malformed floor on update is accepted": {
			old: elastic(map[string]string{"spark.executor.instances": "4", sparkConfPreemptionMinExecutors: "x"}),
			app: elastic(map[string]string{"spark.executor.instances": "4", sparkConfPreemptionMinExecutors: "x"}),
		},
		"changed malformed floor on update is rejected": {
			old:     elastic(map[string]string{"spark.executor.instances": "4", sparkConfPreemptionMinExecutors: "1"}),
			app:     elastic(map[string]string{"spark.executor.instances": "4", sparkConfPreemptionMinExecutors: "x"}),
			wantErr: true,
		},
		"floor without the elastic annotation warns": {
			app:       notElastic(map[string]string{"spark.executor.instances": "4", sparkConfPreemptionMinExecutors: "1"}),
			wantWarns: 1,
		},
		"unchanged floor without the elastic annotation on update: no warning": {
			old: notElastic(map[string]string{"spark.executor.instances": "4", sparkConfPreemptionMinExecutors: "1"}),
			app: notElastic(map[string]string{"spark.executor.instances": "4", sparkConfPreemptionMinExecutors: "1"}),
		},
		"gate off: neither rejected nor warned": {
			gates: map[featuregate.Feature]bool{features.ElasticJobsViaWorkloadSlices: true},
			app:   notElastic(map[string]string{"spark.executor.instances": "4", sparkConfPreemptionMinExecutors: "x"}),
		},
	}
	for name, tc := range cases {
		t.Run(name, func(t *testing.T) {
			gates := tc.gates
			if gates == nil {
				gates = partialPreemptionGates
			}
			features.SetFeatureGatesDuringTest(t, gates)
			w := &SparkApplicationWebhook{}
			errs, err := w.validateCreate(t.Context(), tc.old, tc.app)
			if err != nil {
				t.Fatalf("validateCreate() error = %v", err)
			}
			if gotErr := len(errs) > 0; gotErr != tc.wantErr {
				t.Errorf("validateCreate() errors = %v, wantErr %v", errs, tc.wantErr)
			}
			if got := w.partialPreemptionWarnings(tc.old, tc.app); len(got) != tc.wantWarns {
				t.Errorf("partialPreemptionWarnings() = %v, want %d warning(s)", got, tc.wantWarns)
			}
		})
	}
}

// utilpodGateExecutor adds the elastic scheduling gate to the executor template, as the defaulting
// webhook does, so the elastic validation passes.
func utilpodGateExecutor(app *sparkv1.SparkApplication) {
	tmpl := fromObject(app).ensureTemplateSpec(roleExecutor)
	tmpl.Spec.SchedulingGates = append(tmpl.Spec.SchedulingGates, corev1.PodSchedulingGate{Name: kueue.ElasticJobSchedulingGate})
}
