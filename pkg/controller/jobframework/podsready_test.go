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

package jobframework

import (
	"context"
	"testing"

	"sigs.k8s.io/controller-runtime/pkg/client"

	kueue "sigs.k8s.io/kueue/apis/kueue/v1beta2"
)

// podsReadyJob answers PodsReady with ready; only that method is used.
type podsReadyJob struct {
	GenericJob
	ready bool
}

func (j podsReadyJob) PodsReady(context.Context, client.Client) bool { return j.ready }

// grantAwareJob also answers PodsReadyForWorkload, with grantReady.
type grantAwareJob struct {
	podsReadyJob
	grantReady bool
	seen       *kueue.Workload
}

func (j *grantAwareJob) PodsReadyForWorkload(_ context.Context, _ client.Client, wl *kueue.Workload) bool {
	j.seen = wl
	return j.grantReady
}

func TestPodsReadyForWorkload(t *testing.T) {
	wl := &kueue.Workload{}
	if got := podsReadyForWorkload(t.Context(), nil, podsReadyJob{ready: true}, wl); !got {
		t.Errorf("a job without the grant-aware method: podsReadyForWorkload() = %v, want its PodsReady (true)", got)
	}
	job := &grantAwareJob{podsReadyJob: podsReadyJob{ready: false}, grantReady: true}
	if got := podsReadyForWorkload(t.Context(), nil, job, wl); !got {
		t.Errorf("a grant-aware job: podsReadyForWorkload() = %v, want its PodsReadyForWorkload (true)", got)
	}
	if job.seen != wl {
		t.Errorf("PodsReadyForWorkload received %p, want the Workload %p", job.seen, wl)
	}
}
