/*
Copyright 2026.

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

package main

import (
	"flag"
	"os"

	// Import all Kubernetes client auth plugins (e.g. Azure, GCP, OIDC, etc.)
	// to ensure that exec-entrypoint and run can make use of them.
	_ "k8s.io/client-go/plugin/pkg/client/auth"

	"k8s.io/apimachinery/pkg/runtime"
	utilruntime "k8s.io/apimachinery/pkg/util/runtime"
	clientgoscheme "k8s.io/client-go/kubernetes/scheme"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/cache"
	"sigs.k8s.io/controller-runtime/pkg/healthz"
	"sigs.k8s.io/controller-runtime/pkg/log/zap"
	metricsserver "sigs.k8s.io/controller-runtime/pkg/metrics/server"

	analyticsv1alpha1 "go.miloapis.net/rudderstack-provider/api/v1alpha1"
	"go.miloapis.net/rudderstack-provider/internal/controller"
	"go.miloapis.net/rudderstack-provider/internal/orgs"
	"go.miloapis.net/rudderstack-provider/internal/rules"
	// +kubebuilder:scaffold:imports
)

var (
	scheme   = runtime.NewScheme()
	setupLog = ctrl.Log.WithName("setup")
)

func init() {
	utilruntime.Must(clientgoscheme.AddToScheme(scheme))

	utilruntime.Must(analyticsv1alpha1.AddToScheme(scheme))
	// +kubebuilder:scaffold:scheme
}

func main() {
	var metricsAddr string
	var enableLeaderElection bool
	var leaderElectionNamespace string
	var probeAddr string
	var pipelineOpts pipelineOptions
	pipelineOpts.bindFlags(flag.CommandLine)
	flag.StringVar(&metricsAddr, "metrics-bind-address", "0",
		"The address the plain-HTTP metrics endpoint binds to, such as :8080. 0 disables it.")
	flag.StringVar(&probeAddr, "health-probe-bind-address", ":8081", "The address the probe endpoint binds to.")
	flag.StringVar(&leaderElectionNamespace, "leader-elect-namespace", "",
		"Namespace for the leader election Lease. Defaults to the pod's namespace, which does not exist when "+
			"the kubeconfig points at another API server such as Milo.")
	flag.BoolVar(&enableLeaderElection, "leader-elect", false,
		"Enable leader election for controller manager. "+
			"Enabling this will ensure there is only one active controller manager.")
	opts := zap.Options{}
	opts.BindFlags(flag.CommandLine)
	flag.Parse()

	ctrl.SetLogger(zap.New(zap.UseFlagOptions(&opts)))

	if err := pipelineOpts.validate(); err != nil {
		setupLog.Error(err, "Invalid pipeline flags")
		os.Exit(1)
	}

	// Metrics are served over plain HTTP. They hold only counters, and the
	// authenticated alternative would send scrapers' tokens to whichever API
	// server the kubeconfig names, which is Milo in production. The provider
	// registers no webhooks, so no webhook server runs.
	mgr, err := ctrl.NewManager(ctrl.GetConfigOrDie(), ctrl.Options{
		Scheme:                  scheme,
		Cache:                   cache.Options{ByObject: orgs.CacheByObject()},
		Metrics:                 metricsserver.Options{BindAddress: metricsAddr},
		HealthProbeBindAddress:  probeAddr,
		LeaderElection:          enableLeaderElection,
		LeaderElectionID:        "f98364c5.miloapis.com",
		LeaderElectionNamespace: leaderElectionNamespace,
	})
	if err != nil {
		setupLog.Error(err, "Failed to start manager")
		os.Exit(1)
	}

	celEnv, err := rules.NewEnv()
	if err != nil {
		setupLog.Error(err, "Failed to create CEL environment")
		os.Exit(1)
	}

	if err := (&controller.TrackingRuleReconciler{
		Client: mgr.GetClient(),
		Scheme: mgr.GetScheme(),
		Env:    celEnv,
	}).SetupWithManager(mgr); err != nil {
		setupLog.Error(err, "Failed to create controller", "controller", "trackingrule")
		os.Exit(1)
	}
	// +kubebuilder:scaffold:builder

	ctx := ctrl.SetupSignalHandler()

	store := rules.NewStore(celEnv)
	if err := store.SetupWithManager(ctx, mgr); err != nil {
		setupLog.Error(err, "Failed to set up TrackingRule store")
		os.Exit(1)
	}

	if pipelineOpts.enabled() {
		closeNATS, err := setupPipeline(ctx, mgr, &pipelineOpts, store)
		if err != nil {
			setupLog.Error(err, "Failed to set up audit event pipeline")
			os.Exit(1)
		}
		defer closeNATS()
	} else {
		setupLog.Info("No --nats-url set; running the TrackingRule controller only")
	}

	if err := mgr.AddHealthzCheck("healthz", healthz.Ping); err != nil {
		setupLog.Error(err, "Failed to set up health check")
		os.Exit(1)
	}
	if err := mgr.AddReadyzCheck("readyz", healthz.Ping); err != nil {
		setupLog.Error(err, "Failed to set up ready check")
		os.Exit(1)
	}

	setupLog.Info("Starting manager")
	if err := mgr.Start(ctx); err != nil {
		setupLog.Error(err, "Failed to run manager")
		os.Exit(1) //nolint:gocritic // the deferred NATS close does not matter on exit
	}
}
