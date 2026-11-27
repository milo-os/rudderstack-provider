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
	"errors"
	"io"
	"os"
	"path/filepath"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	utilyaml "k8s.io/apimachinery/pkg/util/yaml"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"

	analyticsv1alpha1 "go.miloapis.net/rudderstack-provider/api/v1alpha1"
	"go.miloapis.net/rudderstack-provider/internal/rules"
)

var _ = Describe("TrackingRule Controller", func() {
	ctx := context.Background()

	newRule := func(name, condition string) *analyticsv1alpha1.TrackingRule {
		return &analyticsv1alpha1.TrackingRule{
			Name: name,
			Spec: analyticsv1alpha1.TrackingRuleSpec{
				Event: "Domain Added",
				Match: analyticsv1alpha1.Match{
					Resource: &analyticsv1alpha1.ResourceSelector{Group: "networking.datumapis.com", Resource: "domains"},
					Verbs:    []string{"create"},
					Conditions: []analyticsv1alpha1.Condition{
						{Name: "human", Expression: condition},
					},
				},
			},
		}
	}

	reconcileReady := func(rule *analyticsv1alpha1.TrackingRule) *metav1.Condition {
		Expect(k8sClient.Create(ctx, rule)).To(Succeed())
		DeferCleanup(func() { Expect(k8sClient.Delete(ctx, rule)).To(Succeed()) })

		env, err := rules.NewEnv()
		Expect(err).NotTo(HaveOccurred())
		r := &TrackingRuleReconciler{Client: k8sClient, Scheme: k8sClient.Scheme(), Env: env}
		key := types.NamespacedName{Name: rule.Name}
		_, err = r.Reconcile(ctx, reconcile.Request{NamespacedName: key})
		Expect(err).NotTo(HaveOccurred())

		var got analyticsv1alpha1.TrackingRule
		Expect(k8sClient.Get(ctx, key, &got)).To(Succeed())
		Expect(got.Status.ObservedGeneration).To(Equal(got.Generation))
		return meta.FindStatusCondition(got.Status.Conditions, ConditionReady)
	}

	It("marks a rule with valid expressions Ready", func() {
		cond := reconcileReady(newRule("valid", `!user.username.startsWith("system:")`))
		Expect(cond).NotTo(BeNil())
		Expect(cond.Status).To(Equal(metav1.ConditionTrue))
		Expect(cond.Reason).To(Equal(ReasonCompiled))
	})

	It("marks a rule with an invalid expression not Ready", func() {
		cond := reconcileReady(newRule("invalid", `user.username ==`))
		Expect(cond).NotTo(BeNil())
		Expect(cond.Status).To(Equal(metav1.ConditionFalse))
		Expect(cond.Reason).To(Equal(ReasonInvalidExpression))
		Expect(cond.Message).To(ContainSubstring(`condition "human"`))
	})

	It("rejects an unknown limit", func() {
		rule := newRule("unknown-limit", "true")
		rule.Spec.Limit = "OncePerHour"
		Expect(k8sClient.Create(ctx, rule)).NotTo(Succeed())
	})

	It("accepts every sample rule and marks it Ready", func() {
		files, err := filepath.Glob(filepath.Join("..", "..", "config", "rules", "*.yaml"))
		Expect(err).NotTo(HaveOccurred())

		var count int
		for _, file := range files {
			if filepath.Base(file) == "kustomization.yaml" {
				continue
			}
			f, err := os.Open(file)
			Expect(err).NotTo(HaveOccurred())
			DeferCleanup(f.Close)

			dec := utilyaml.NewYAMLOrJSONDecoder(f, 4096)
			for {
				rule := &analyticsv1alpha1.TrackingRule{}
				if err := dec.Decode(rule); errors.Is(err, io.EOF) {
					break
				} else {
					Expect(err).NotTo(HaveOccurred(), file)
				}
				cond := reconcileReady(rule)
				Expect(cond).NotTo(BeNil(), rule.Name)
				Expect(cond.Status).To(Equal(metav1.ConditionTrue), "%s: %s", rule.Name, cond.Message)
				count++
			}
		}
		Expect(count).To(BeNumerically(">", 0))
	})
})
