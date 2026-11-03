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

package workload

import (
	"context"
	"testing"
	"time"

	"github.com/google/go-cmp/cmp"
	"k8s.io/utils/ptr"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/interceptor"

	kueue "sigs.k8s.io/kueue/apis/kueue/v1beta2"
	"sigs.k8s.io/kueue/pkg/features"
	utiltesting "sigs.k8s.io/kueue/pkg/util/testing"
	utiltestingapi "sigs.k8s.io/kueue/pkg/util/testing/v1beta2"
)

// reclaimTargets returns each PodSet's reclaimTargetCount on wl's admission, nil when unset.
func reclaimTargets(wl *kueue.Workload) map[kueue.PodSetReference]*int32 {
	if wl.Status.Admission == nil {
		return nil
	}
	out := make(map[kueue.PodSetReference]*int32)
	for _, psa := range wl.Status.Admission.PodSetAssignments {
		out[psa.Name] = psa.ReclaimTargetCount
	}
	return out
}

func reclaimTargetTestWorkload(driver, executor *int32) *kueue.Workload {
	return utiltestingapi.MakeWorkload("wl", "ns").
		PodSets(*utiltestingapi.MakePodSet("driver", 1).Obj(), *utiltestingapi.MakePodSet("executor", 4).Obj()).
		ReserveQuotaAt(utiltestingapi.MakeAdmission("cq").PodSets(
			kueue.PodSetAssignment{Name: "driver", Count: ptr.To[int32](1), ReclaimTargetCount: driver},
			kueue.PodSetAssignment{Name: "executor", Count: ptr.To[int32](4), ReclaimTargetCount: executor},
		).Obj(), time.Now()).
		Obj()
}

func TestSetReclaimTargetCount(t *testing.T) {
	cases := map[string]struct {
		wl          *kueue.Workload
		podSet      kueue.PodSetReference
		target      *int32
		wantChanged bool
		want        map[kueue.PodSetReference]*int32
	}{
		"sets the named PodSet only": {
			wl: reclaimTargetTestWorkload(nil, nil), podSet: "executor", target: ptr.To[int32](2), wantChanged: true,
			want: map[kueue.PodSetReference]*int32{"driver": nil, "executor": ptr.To[int32](2)},
		},
		"the same value is no change": {
			wl: reclaimTargetTestWorkload(nil, ptr.To[int32](2)), podSet: "executor", target: ptr.To[int32](2),
			want: map[kueue.PodSetReference]*int32{"driver": nil, "executor": ptr.To[int32](2)},
		},
		"a new value replaces the old one": {
			wl: reclaimTargetTestWorkload(nil, ptr.To[int32](3)), podSet: "executor", target: ptr.To[int32](2), wantChanged: true,
			want: map[kueue.PodSetReference]*int32{"driver": nil, "executor": ptr.To[int32](2)},
		},
		"nil clears the named PodSet": {
			wl: reclaimTargetTestWorkload(ptr.To[int32](1), ptr.To[int32](2)), podSet: "executor", wantChanged: true,
			want: map[kueue.PodSetReference]*int32{"driver": ptr.To[int32](1), "executor": nil},
		},
		"an empty PodSet clears every PodSet, and never sets": {
			wl: reclaimTargetTestWorkload(ptr.To[int32](1), ptr.To[int32](2)), target: ptr.To[int32](5), wantChanged: true,
			want: map[kueue.PodSetReference]*int32{"driver": nil, "executor": nil},
		},
		"an unknown PodSet is no change": {
			wl: reclaimTargetTestWorkload(nil, nil), podSet: "other", target: ptr.To[int32](2),
			want: map[kueue.PodSetReference]*int32{"driver": nil, "executor": nil},
		},
		"no admission is no change": {
			wl: utiltestingapi.MakeWorkload("wl", "ns").Obj(), podSet: "executor", target: ptr.To[int32](2),
		},
	}
	for name, tc := range cases {
		t.Run(name, func(t *testing.T) {
			if got := SetReclaimTargetCount(tc.wl, tc.podSet, tc.target); got != tc.wantChanged {
				t.Errorf("SetReclaimTargetCount() changed = %v, want %v", got, tc.wantChanged)
			}
			if diff := cmp.Diff(tc.want, reclaimTargets(tc.wl)); diff != "" {
				t.Errorf("reclaimTargetCount per PodSet (-want,+got):\n%s", diff)
			}
			for ps, want := range tc.want {
				if n, ok := ReclaimTargetCount(tc.wl, ps); ok != (want != nil) || (ok && n != *want) {
					t.Errorf("ReclaimTargetCount(%s) = (%d, %v), want %v", ps, n, ok, want)
				}
			}
		})
	}
}

func TestUpdateReclaimTargetCount(t *testing.T) {
	cases := map[string]struct {
		gateOff     bool
		wl          *kueue.Workload
		target      *int32
		wantWrites  int
		wantStored  *int32
		wantStoredK bool
	}{
		"gate on: written and stored": {
			wl: reclaimTargetTestWorkload(nil, nil), target: ptr.To[int32](2), wantWrites: 1, wantStored: ptr.To[int32](2), wantStoredK: true,
		},
		"gate on: already set, nothing written": {
			wl: reclaimTargetTestWorkload(nil, ptr.To[int32](2)), target: ptr.To[int32](2), wantStored: ptr.To[int32](2), wantStoredK: true,
		},
		"gate on: cleared": {
			wl: reclaimTargetTestWorkload(nil, ptr.To[int32](2)), wantWrites: 1, wantStoredK: true,
		},
		"gate off: nothing written": {
			gateOff: true, wl: reclaimTargetTestWorkload(nil, nil), target: ptr.To[int32](2), wantStoredK: true,
		},
		"no quota reservation: nothing written": {
			wl:     utiltestingapi.MakeWorkload("wl", "ns").PodSets(*utiltestingapi.MakePodSet("executor", 4).Obj()).Obj(),
			target: ptr.To[int32](2),
		},
	}
	for name, tc := range cases {
		t.Run(name, func(t *testing.T) {
			features.SetFeatureGateDuringTest(t, features.SparkPartialPreemption, !tc.gateOff)
			ctx, _ := utiltesting.ContextWithLog(t)
			writes := 0
			c := utiltesting.NewClientBuilder().
				WithObjects(tc.wl).
				WithStatusSubresource(&kueue.Workload{}).
				WithInterceptorFuncs(interceptor.Funcs{
					SubResourceUpdate: func(ctx context.Context, c client.Client, sub string, obj client.Object, opts ...client.SubResourceUpdateOption) error {
						writes++
						return c.SubResource(sub).Update(ctx, obj, opts...)
					},
				}).Build()
			wl := tc.wl.DeepCopy()
			if err := UpdateReclaimTargetCount(ctx, c, wl, "executor", tc.target); err != nil {
				t.Fatalf("UpdateReclaimTargetCount() error = %v", err)
			}
			if writes != tc.wantWrites {
				t.Errorf("status writes = %d, want %d", writes, tc.wantWrites)
			}
			stored := &kueue.Workload{}
			if err := c.Get(ctx, client.ObjectKeyFromObject(tc.wl), stored); err != nil {
				t.Fatal(err)
			}
			got, ok := reclaimTargets(stored)["executor"]
			if ok != tc.wantStoredK || !ptr.Equal(got, tc.wantStored) {
				t.Errorf("stored executor reclaimTargetCount = (%v, %v), want (%v, %v)", got, ok, tc.wantStored, tc.wantStoredK)
			}
		})
	}
}
