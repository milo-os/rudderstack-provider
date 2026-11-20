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

package controller

import (
	"context"

	"github.com/google/cel-go/cel"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	logf "sigs.k8s.io/controller-runtime/pkg/log"

	analyticsv1alpha1 "go.miloapis.net/rudderstack-provider/api/v1alpha1"
	"go.miloapis.net/rudderstack-provider/internal/rules"
)

const (
	// ConditionReady reports whether a rule compiled and can be evaluated.
	ConditionReady = "Ready"

	ReasonCompiled          = "Compiled"
	ReasonInvalidExpression = "InvalidExpression"
)

// TrackingRuleReconciler validates TrackingRules and reports the result in
// their Ready condition. Only Ready rules are evaluated against audit events.
type TrackingRuleReconciler struct {
	client.Client
	Scheme *runtime.Scheme
	Env    *cel.Env
}

// +kubebuilder:rbac:groups=analytics.miloapis.com,resources=trackingrules,verbs=get;list;watch
// +kubebuilder:rbac:groups=analytics.miloapis.com,resources=trackingrules/status,verbs=get;update;patch

// Reconcile compiles the rule's CEL expressions and records whether they
// compiled.
func (r *TrackingRuleReconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	log := logf.FromContext(ctx)

	var rule analyticsv1alpha1.TrackingRule
	if err := r.Get(ctx, req.NamespacedName, &rule); err != nil {
		return ctrl.Result{}, client.IgnoreNotFound(err)
	}

	cond := metav1.Condition{
		Type:               ConditionReady,
		Status:             metav1.ConditionTrue,
		Reason:             ReasonCompiled,
		Message:            "All expressions compiled",
		ObservedGeneration: rule.Generation,
	}
	if _, err := rules.Compile(r.Env, &rule); err != nil {
		log.Info("TrackingRule has invalid expressions", "error", err.Error())
		cond.Status = metav1.ConditionFalse
		cond.Reason = ReasonInvalidExpression
		cond.Message = err.Error()
	}

	if !meta.SetStatusCondition(&rule.Status.Conditions, cond) && rule.Status.ObservedGeneration == rule.Generation {
		return ctrl.Result{}, nil
	}
	rule.Status.ObservedGeneration = rule.Generation
	// A conflict means the rule changed since it was read; the change
	// triggers another reconcile.
	if err := r.Status().Update(ctx, &rule); err != nil && !apierrors.IsConflict(err) {
		return ctrl.Result{}, err
	}
	return ctrl.Result{}, nil
}

// SetupWithManager sets up the controller with the Manager.
func (r *TrackingRuleReconciler) SetupWithManager(mgr ctrl.Manager) error {
	return ctrl.NewControllerManagedBy(mgr).
		For(&analyticsv1alpha1.TrackingRule{}).
		Named("trackingrule").
		Complete(r)
}
