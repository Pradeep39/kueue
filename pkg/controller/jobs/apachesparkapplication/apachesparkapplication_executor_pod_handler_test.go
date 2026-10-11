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
	"testing"
	"time"

	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/util/workqueue"
	testingclock "k8s.io/utils/clock/testing"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"
)

// TestExecutorPodHandlerStaleTimerDoesNotOrphanNewer: a callback from a superseded timer must
// not delete the bookkeeping of the timer that replaced it.
func TestExecutorPodHandlerStaleTimerDoesNotOrphanNewer(t *testing.T) {
	fakeClock := testingclock.NewFakeClock(time.Now())
	h := newExecutorPodHandler()
	h.clock = fakeClock
	q := workqueue.NewTypedRateLimitingQueue(workqueue.DefaultTypedControllerRateLimiter[reconcile.Request]())
	defer q.ShutDown()
	key := types.NamespacedName{Namespace: "ns", Name: "pi"}
	req := reconcile.Request{NamespacedName: key}

	stale, current := fakeClock.NewTimer(time.Hour), fakeClock.NewTimer(time.Hour)
	h.timers[key], h.burstStarts[key] = current, fakeClock.Now()

	h.fire(key, stale, req, q)
	if h.timers[key] != current {
		t.Fatalf("a stale callback removed the current timer")
	}
	if q.Len() != 1 {
		t.Errorf("a stale callback must still enqueue (queue length %d)", q.Len())
	}
	h.fire(key, current, req, q)
	if _, ok := h.timers[key]; ok {
		t.Errorf("the current timer's own callback left its entry behind")
	}
}
