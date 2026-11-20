package rules

import (
	"testing"

	analyticsv1alpha1 "go.miloapis.net/rudderstack-provider/api/v1alpha1"
)

func TestStore(t *testing.T) {
	env, err := NewEnv()
	if err != nil {
		t.Fatal(err)
	}
	s := NewStore(env)

	rule := func(name, cond string) *analyticsv1alpha1.TrackingRule {
		r := &analyticsv1alpha1.TrackingRule{}
		r.Name = name
		r.Spec.Match.Verbs = []string{"create"}
		r.Spec.Match.Conditions = []analyticsv1alpha1.Condition{{Name: "c", Expression: cond}}
		return r
	}

	if err := s.Upsert(rule("b", "true")); err != nil {
		t.Fatal(err)
	}
	if err := s.Upsert(rule("a", "true")); err != nil {
		t.Fatal(err)
	}
	if got := s.List(); len(got) != 2 || got[0].Rule.Name != "a" || got[1].Rule.Name != "b" {
		t.Fatalf("List() = %v, want rules a and b in order", got)
	}

	// An edit that breaks a rule removes it.
	if err := s.Upsert(rule("a", "verb ==")); err == nil {
		t.Fatal("expected a compile error")
	}
	if got := s.List(); len(got) != 1 || got[0].Rule.Name != "b" {
		t.Fatalf("List() = %v, want only rule b", got)
	}

	s.Delete("b")
	if got := s.List(); len(got) != 0 {
		t.Fatalf("List() = %v, want no rules", got)
	}
	if s.HasSynced() {
		t.Fatal("HasSynced() = true before registering with an informer")
	}
}
