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

package preemption

import (
	"testing"

	"github.com/google/go-cmp/cmp"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	kueuealpha "sigs.k8s.io/kueue/apis/kueue/v1alpha1"
	kueue "sigs.k8s.io/kueue/apis/kueue/v1beta2"
	utiltestingalpha "sigs.k8s.io/kueue/pkg/util/testing/v1alpha1"
	utiltestingapi "sigs.k8s.io/kueue/pkg/util/testing/v1beta2"
)

// TestPartialPreemptionConfigurableCandidates covers a ClusterQueue that preempts only through
// a PreemptionConfig (spec.preemption Never): partial preemption must draw on the candidates the
// config selects, whatever their priority, rather than finding none and leaving the Evict
// fallback to evict them whole.
func TestPartialPreemptionConfigurableCandidates(t *testing.T) {
	const configName = "pool"
	cq := func(within kueue.PreemptionPolicy) []*kueue.ClusterQueue {
		return []*kueue.ClusterQueue{utiltestingapi.MakeClusterQueue("cq").
			ResourceGroup(*utiltestingapi.MakeFlavorQuotas("default").Resource(corev1.ResourceCPU, "12").Obj()).
			Preemption(kueue.ClusterQueuePreemption{WithinClusterQueue: within, ReclaimWithinCohort: kueue.PreemptionPolicyNever}).
			Annotation(kueuealpha.PreemptionConfigNameAnnotation, configName).
			Obj()}
	}
	urgent := &metav1.LabelSelector{MatchLabels: map[string]string{"role": "urgent"}}
	pool := func(name string) kueuealpha.PreemptionConfigPreemptionCandidateSelector {
		return utiltestingalpha.MakeCandidateSelector(kueuealpha.WithinClusterQueue).
			LabelSelector(&metav1.LabelSelector{MatchLabels: map[string]string{"pool": name}}).Obj()
	}
	batchOnly := utiltestingalpha.MakePreemptionConfig(configName).
		RuleWithPreemptorSelector("batch", kueuealpha.Always, urgent, pool("batch")).Obj()
	batchThenSpare := utiltestingalpha.MakePreemptionConfig(configName).
		RuleWithPreemptorSelector("batch", kueuealpha.Always, urgent, pool("batch")).
		RuleWithPreemptorSelector("spare", kueuealpha.InsufficientQuota, urgent, pool("spare")).Obj()

	victim := func(name, poolName string, prio int32, pods, floor int32, age int) partialVictimSpec {
		v := partialVictimSpec{name: name, cq: "cq", pods: pods, floor: floor, priority: prio, age: age}
		if poolName != "" {
			v.labels = map[string]string{"pool": poolName}
		}
		return v
	}
	preemptor := func(pods int32, role string) partialVictimSpec {
		return partialVictimSpec{name: "in", cq: "cq", pods: pods, priority: 100, labels: map[string]string{"role": role}}
	}

	cases := map[string]struct {
		tc   partialCase
		want map[string]int32
	}{
		"classical preemption off: the config's pool is reclaimed partially, an unlabeled lower-priority workload is spared": {
			tc: partialCase{
				cqs:    cq(kueue.PreemptionPolicyNever),
				config: batchOnly,
				victims: []partialVictimSpec{
					victim("a", "batch", 50, 4, 1, 2), victim("b", "batch", 50, 4, 1, 1), victim("low", "", 0, 4, 1, 3),
				},
				preempt: preemptor(4, "urgent"),
			},
			want: map[string]int32{"a": 2, "b": 2},
		},
		"InsufficientQuota candidates only once every baseline victim is at its floor": {
			tc: partialCase{
				cqs:    cq(kueue.PreemptionPolicyNever),
				config: batchThenSpare,
				victims: []partialVictimSpec{
					victim("a", "batch", 50, 4, 3, 2), victim("b", "batch", 50, 4, 3, 1), victim("s", "spare", 0, 4, 1, 3),
				},
				preempt: preemptor(4, "urgent"),
			},
			want: map[string]int32{"a": 1, "b": 1, "s": 2},
		},
		"InsufficientQuota candidates untouched while the baseline suffices": {
			tc: partialCase{
				cqs:    cq(kueue.PreemptionPolicyNever),
				config: batchThenSpare,
				victims: []partialVictimSpec{
					victim("a", "batch", 50, 4, 1, 2), victim("b", "batch", 50, 4, 1, 1), victim("s", "spare", 0, 4, 1, 3),
				},
				preempt: preemptor(4, "urgent"),
			},
			want: map[string]int32{"a": 2, "b": 2},
		},
		"a candidate both the classical policy and the config select is planned once": {
			tc: partialCase{
				cqs:    cq(kueue.PreemptionPolicyLowerPriority),
				config: batchOnly,
				victims: []partialVictimSpec{
					victim("a", "batch", 50, 4, 1, 2), victim("b", "batch", 50, 4, 1, 1), victim("c", "batch", 50, 4, 1, 3),
				},
				preempt: preemptor(3, "urgent"),
			},
			want: map[string]int32{"a": 1, "b": 1, "c": 1},
		},
		"preemptor the rule does not select: nothing": {
			tc: partialCase{
				cqs:     cq(kueue.PreemptionPolicyNever),
				config:  batchOnly,
				victims: []partialVictimSpec{victim("a", "batch", 50, 6, 1, 2), victim("b", "batch", 50, 6, 1, 1)},
				preempt: preemptor(4, "routine"),
			},
			want: map[string]int32{},
		},
		"ConfigurablePreemptions gate off: nothing": {
			tc: partialCase{
				cqs:                  cq(kueue.PreemptionPolicyNever),
				config:               batchOnly,
				configurableDisabled: true,
				victims:              []partialVictimSpec{victim("a", "batch", 50, 6, 1, 2), victim("b", "batch", 50, 6, 1, 1)},
				preempt:              preemptor(4, "urgent"),
			},
			want: map[string]int32{},
		},
	}
	for name, tc := range cases {
		t.Run(name, func(t *testing.T) {
			got, _ := runPartial(t, tc.tc)
			if diff := cmp.Diff(tc.want, got); diff != "" {
				t.Errorf("Pods taken per victim (-want,+got):\n%s", diff)
			}
		})
	}
}
