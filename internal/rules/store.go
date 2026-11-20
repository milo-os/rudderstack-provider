package rules

import (
	"context"
	"slices"
	"strings"
	"sync"

	"github.com/google/cel-go/cel"
	toolscache "k8s.io/client-go/tools/cache"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/log"

	analyticsv1alpha1 "go.miloapis.net/rudderstack-provider/api/v1alpha1"
)

// Store holds the compiled form of every TrackingRule whose expressions
// compile. It is fed by the manager's informer, so it is populated on every
// replica, not only the leader that writes rule status.
type Store struct {
	env *cel.Env

	mu    sync.RWMutex
	rules map[string]*CompiledRule

	synced toolscache.InformerSynced
}

// NewStore returns an empty store that compiles rules in env.
func NewStore(env *cel.Env) *Store {
	return &Store{env: env, rules: map[string]*CompiledRule{}}
}

// SetupWithManager registers the store with the manager's TrackingRule
// informer.
func (s *Store) SetupWithManager(ctx context.Context, mgr ctrl.Manager) error {
	inf, err := mgr.GetCache().GetInformer(ctx, &analyticsv1alpha1.TrackingRule{})
	if err != nil {
		return err
	}
	reg, err := inf.AddEventHandler(toolscache.ResourceEventHandlerFuncs{
		AddFunc:    func(obj any) { s.upsert(ctx, obj) },
		UpdateFunc: func(_, obj any) { s.upsert(ctx, obj) },
		DeleteFunc: func(obj any) {
			if tomb, ok := obj.(toolscache.DeletedFinalStateUnknown); ok {
				obj = tomb.Obj
			}
			if rule, ok := obj.(*analyticsv1alpha1.TrackingRule); ok {
				s.Delete(rule.Name)
			}
		},
	})
	if err != nil {
		return err
	}
	s.synced = reg.HasSynced
	return nil
}

// HasSynced reports whether the store has seen every rule that existed when
// it was registered.
func (s *Store) HasSynced() bool {
	return s.synced != nil && s.synced()
}

func (s *Store) upsert(ctx context.Context, obj any) {
	rule, ok := obj.(*analyticsv1alpha1.TrackingRule)
	if !ok {
		return
	}
	if err := s.Upsert(rule); err != nil {
		// The controller reports this on the rule's Ready condition.
		log.FromContext(ctx).V(1).Info("Ignoring TrackingRule that does not compile", "rule", rule.Name, "error", err.Error())
	}
}

// Upsert compiles rule and stores it. A rule that does not compile is
// removed, so an edit that breaks a rule stops it from firing.
func (s *Store) Upsert(rule *analyticsv1alpha1.TrackingRule) error {
	cr, err := Compile(s.env, rule.DeepCopy())
	s.mu.Lock()
	defer s.mu.Unlock()
	if err != nil {
		delete(s.rules, rule.Name)
		return err
	}
	s.rules[rule.Name] = cr
	return nil
}

// Delete removes the named rule.
func (s *Store) Delete(name string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	delete(s.rules, name)
}

// List returns the stored rules ordered by name.
func (s *Store) List() []*CompiledRule {
	s.mu.RLock()
	defer s.mu.RUnlock()
	out := make([]*CompiledRule, 0, len(s.rules))
	for _, cr := range s.rules {
		out = append(out, cr)
	}
	slices.SortFunc(out, func(a, b *CompiledRule) int { return strings.Compare(a.Rule.Name, b.Rule.Name) })
	return out
}
