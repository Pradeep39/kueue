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
	"net/http"
	"testing"

	kubeflowsparkv1beta2 "github.com/kubeflow/spark-operator/v2/api/v1beta2"
	apimeta "k8s.io/apimachinery/pkg/api/meta"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/client-go/rest"
	"sigs.k8s.io/controller-runtime/pkg/client"
	ctrlmgr "sigs.k8s.io/controller-runtime/pkg/manager"

	sparkv1 "sigs.k8s.io/kueue/pkg/controller/jobs/apachesparkapplication/api/v1"
	kueuesparkapplication "sigs.k8s.io/kueue/pkg/controller/jobs/sparkapplication"
	utiltesting "sigs.k8s.io/kueue/pkg/util/testing"
)

// TestControllerNameDoesNotCollideWithKubeflow is a regression guard.
//
// controller-runtime derives a controller's name from the Kind of the object passed to
// For(), and this CRD and Kubeflow's sparkoperator.k8s.io one are both Kind
// "SparkApplication". Before this integration named its controller explicitly, enabling
// both frameworks made the second one to be set up fail with "controller with name
// sparkapplication already exists", which aborts the whole controller manager rather
// than degrading one integration.
func TestControllerNameDoesNotCollideWithKubeflow(t *testing.T) {
	ctx, _ := utiltesting.ContextWithLog(t)

	k8sClient := utiltesting.NewClientBuilder(
		sparkv1.AddToScheme,
		kubeflowsparkv1beta2.AddToScheme,
	).Build()

	mgr, err := ctrlmgr.New(&rest.Config{}, ctrlmgr.Options{
		Scheme: k8sClient.Scheme(),
		NewClient: func(*rest.Config, client.Options) (client.Client, error) {
			return k8sClient, nil
		},
		MapperProvider: func(*rest.Config, *http.Client) (apimeta.RESTMapper, error) {
			return apimeta.NewDefaultRESTMapper([]schema.GroupVersion{
				gvk.GroupVersion(),
				kubeflowsparkv1beta2.GroupVersion,
			}), nil
		},
	})
	if err != nil {
		t.Fatalf("Failed to set up manager: %v", err)
	}

	setup := func(t *testing.T, name string, factory func() error) {
		t.Helper()
		if err := factory(); err != nil {
			t.Fatalf("Failed to set up the %s controller: %v", name, err)
		}
	}

	// Order matters only in which integration reports the collision, so set up both.
	setup(t, "apache", func() error {
		r, err := NewReconciler(ctx, mgr.GetClient(), mgr.GetFieldIndexer(),
			mgr.GetEventRecorder("apache-test"))
		if err != nil {
			return err
		}
		return r.SetupWithManager(mgr)
	})

	setup(t, "kubeflow", func() error {
		r, err := kueuesparkapplication.NewReconciler(ctx, mgr.GetClient(), mgr.GetFieldIndexer(),
			mgr.GetEventRecorder("kubeflow-test"))
		if err != nil {
			return err
		}
		return r.SetupWithManager(mgr)
	})
}
