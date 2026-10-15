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
	"time"

	schdcache "sigs.k8s.io/kueue/pkg/cache/scheduler"
	"sigs.k8s.io/kueue/pkg/constants"
	utiltestingapi "sigs.k8s.io/kueue/pkg/util/testing/v1beta2"
	"sigs.k8s.io/kueue/pkg/workload"
)

func TestPartialPreemptionGracePeriodAndTimeout(t *testing.T) {
	cases := map[string]struct {
		jobGrace    string
		cqGrace     *time.Duration
		cqTimeout   time.Duration
		noCQ        bool
		wantGrace   time.Duration
		wantTimeout time.Duration
	}{
		"nothing set: Kubernetes' default grace period plus the cushion": {
			wantGrace: 30 * time.Second, wantTimeout: 60 * time.Second,
		},
		"no ClusterQueue snapshot: the defaults": {
			noCQ: true, wantGrace: 30 * time.Second, wantTimeout: 60 * time.Second,
		},
		"ClusterQueue grace period": {
			cqGrace: new(2 * time.Minute), wantGrace: 2 * time.Minute, wantTimeout: 150 * time.Second,
		},
		"job's grace period wins over the ClusterQueue's": {
			jobGrace: "90s", cqGrace: new(2 * time.Minute), wantGrace: 90 * time.Second, wantTimeout: 120 * time.Second,
		},
		"a zero grace period is honoured": {
			jobGrace: "0s", wantGrace: 0, wantTimeout: 30 * time.Second,
		},
		"malformed job grace period falls through to the ClusterQueue's": {
			jobGrace: "soon", cqGrace: new(45 * time.Second), wantGrace: 45 * time.Second, wantTimeout: 75 * time.Second,
		},
		"explicit ClusterQueue timeout wins over the derived one": {
			jobGrace: "90s", cqTimeout: 40 * time.Second, wantGrace: 90 * time.Second, wantTimeout: 40 * time.Second,
		},
	}
	for name, tc := range cases {
		t.Run(name, func(t *testing.T) {
			w := utiltestingapi.MakeWorkload("victim", "ns")
			if tc.jobGrace != "" {
				w = w.Annotation(constants.PartialPreemptionTerminationGracePeriodAnnotation, tc.jobGrace)
			}
			target := &Target{WorkloadInfo: &workload.Info{Obj: w.Obj()}}
			if !tc.noCQ {
				target.WorkloadCq = &schdcache.ClusterQueueSnapshot{PartialPreemption: schdcache.PartialPreemptionSettings{
					Enabled: true, Timeout: tc.cqTimeout, TerminationGracePeriod: tc.cqGrace,
				}}
			}
			grace := partialPreemptionGracePeriod(target)
			if grace != tc.wantGrace {
				t.Errorf("grace period = %v, want %v", grace, tc.wantGrace)
			}
			if got := partialPreemptionTimeout(target, grace); got != tc.wantTimeout {
				t.Errorf("timeout = %v, want %v", got, tc.wantTimeout)
			}
		})
	}
}
