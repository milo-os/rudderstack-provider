package rules

import (
	"testing"

	analyticsv1alpha1 "go.miloapis.net/rudderstack-provider/api/v1alpha1"
	"go.miloapis.net/rudderstack-provider/internal/audit"
)

func mustCompile(t *testing.T, spec analyticsv1alpha1.TrackingRuleSpec) *CompiledRule {
	t.Helper()
	env, err := NewEnv()
	if err != nil {
		t.Fatal(err)
	}
	rule := &analyticsv1alpha1.TrackingRule{Spec: spec}
	rule.Name = "test"
	cr, err := Compile(env, rule)
	if err != nil {
		t.Fatal(err)
	}
	return cr
}

func domainEvent() *audit.Event {
	return &audit.Event{
		AuditID:   "a1",
		Verb:      "create",
		User:      audit.User{Username: "alice@example.com", UID: "u1"},
		UserAgent: "datumctl/v0.12.0",
		ObjectRef: &audit.ObjectRef{APIGroup: "networking.datumapis.com", Resource: "domains", Name: "example-com"},
		ResponseObject: map[string]any{
			"metadata": map[string]any{"name": "example-com"},
			"spec":     map[string]any{"domainName": "example.com"},
		},
	}
}

var domainResource = &analyticsv1alpha1.ResourceSelector{Group: "networking.datumapis.com", Resource: "domains"}

func TestEvaluateMatchesRequest(t *testing.T) {
	cr := mustCompile(t, analyticsv1alpha1.TrackingRuleSpec{
		Match: analyticsv1alpha1.Match{Resource: domainResource, Verbs: []string{"create"}},
	})

	tests := []struct {
		name   string
		mutate func(*audit.Event)
		want   bool
	}{
		{"match", func(*audit.Event) {}, true},
		{"other verb", func(e *audit.Event) { e.Verb = "delete" }, false},
		{"other resource", func(e *audit.Event) { e.ObjectRef.Resource = "httpproxies" }, false},
		{"other group", func(e *audit.Event) { e.ObjectRef.APIGroup = "" }, false},
		{"subresource", func(e *audit.Event) { e.ObjectRef.Subresource = "status" }, false},
		{"no object ref", func(e *audit.Event) { e.ObjectRef = nil }, false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			e := domainEvent()
			tt.mutate(e)
			res, err := cr.Evaluate(e, Activation(e, ""))
			if err != nil {
				t.Fatal(err)
			}
			if (res != nil) != tt.want {
				t.Fatalf("matched = %v, want %v", res != nil, tt.want)
			}
		})
	}
}

func TestEvaluateWithoutResourceMatchesAnyResource(t *testing.T) {
	cr := mustCompile(t, analyticsv1alpha1.TrackingRuleSpec{
		Match: analyticsv1alpha1.Match{
			Verbs:      []string{"list"},
			Conditions: []analyticsv1alpha1.Condition{{Name: "datumctl", Expression: `userAgent.startsWith("datumctl/")`}},
		},
	})
	e := domainEvent()
	e.Verb = "list"
	e.ObjectRef = nil
	res, err := cr.Evaluate(e, Activation(e, ""))
	if err != nil || res == nil {
		t.Fatalf("Evaluate() = %v, %v, want a match", res, err)
	}
}

func TestEvaluateConditions(t *testing.T) {
	tests := []struct {
		name       string
		conditions []analyticsv1alpha1.Condition
		want       bool
		wantErr    bool
	}{
		{"none", nil, true, false},
		{"one true", []analyticsv1alpha1.Condition{{Name: "a", Expression: `verb == "create"`}}, true, false},
		{"all false", []analyticsv1alpha1.Condition{{Name: "a", Expression: `verb == "delete"`}}, false, false},
		{"or", []analyticsv1alpha1.Condition{
			{Name: "a", Expression: `verb == "delete"`},
			{Name: "b", Expression: `object.spec.domainName == "example.com"`},
		}, true, false},
		{"error alone", []analyticsv1alpha1.Condition{{Name: "a", Expression: `object.spec.missing == "x"`}}, false, true},
		{"error with match", []analyticsv1alpha1.Condition{
			{Name: "a", Expression: `object.spec.missing == "x"`},
			{Name: "b", Expression: `tenant.type == "platform"`},
		}, true, false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cr := mustCompile(t, analyticsv1alpha1.TrackingRuleSpec{
				Match: analyticsv1alpha1.Match{Resource: domainResource, Verbs: []string{"create"}, Conditions: tt.conditions},
			})
			e := domainEvent()
			res, err := cr.Evaluate(e, Activation(e, ""))
			if (err != nil) != tt.wantErr {
				t.Fatalf("err = %v, wantErr %v", err, tt.wantErr)
			}
			if (res != nil) != tt.want {
				t.Fatalf("matched = %v, want %v", res != nil, tt.want)
			}
		})
	}
}

func TestEvaluatePropertiesAndUserID(t *testing.T) {
	cr := mustCompile(t, analyticsv1alpha1.TrackingRuleSpec{
		Match: analyticsv1alpha1.Match{Resource: domainResource, Verbs: []string{"create"}},
		Properties: []analyticsv1alpha1.Property{
			{Name: "domain", Expression: `object.spec.domainName`},
			{Name: "viaCLI", Expression: `userAgent.startsWith("datumctl/")`},
		},
	})
	e := domainEvent()
	res, err := cr.Evaluate(e, Activation(e, ""))
	if err != nil {
		t.Fatal(err)
	}
	if res.UserID != "u1" {
		t.Fatalf("UserID = %q, want the acting user's uid", res.UserID)
	}
	if res.Properties["domain"] != "example.com" || res.Properties["viaCLI"] != true {
		t.Fatalf("Properties = %v", res.Properties)
	}
}

func TestEvaluateUserID(t *testing.T) {
	tests := []struct {
		name    string
		userID  string
		user    audit.User
		want    string
		wantErr bool
	}{
		{"uid", "", audit.User{UID: "u1", Username: "alice"}, "u1", false},
		{"username fallback", "", audit.User{Username: "alice"}, "alice", false},
		{"no user", "", audit.User{}, "", true},
		{"expression", `object.metadata.name`, audit.User{UID: "system"}, "example-com", false},
		{"expression empty", `""`, audit.User{UID: "u1"}, "", true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cr := mustCompile(t, analyticsv1alpha1.TrackingRuleSpec{
				Match:  analyticsv1alpha1.Match{Resource: domainResource, Verbs: []string{"create"}},
				UserID: tt.userID,
			})
			e := domainEvent()
			e.User = tt.user
			res, err := cr.Evaluate(e, Activation(e, ""))
			if (err != nil) != tt.wantErr {
				t.Fatalf("err = %v, wantErr %v", err, tt.wantErr)
			}
			if err == nil && res.UserID != tt.want {
				t.Fatalf("UserID = %q, want %q", res.UserID, tt.want)
			}
		})
	}
}

func TestEvaluatePropertyMustBeScalar(t *testing.T) {
	cr := mustCompile(t, analyticsv1alpha1.TrackingRuleSpec{
		Match:      analyticsv1alpha1.Match{Resource: domainResource, Verbs: []string{"create"}},
		Properties: []analyticsv1alpha1.Property{{Name: "spec", Expression: `object.spec`}},
	})
	e := domainEvent()
	if _, err := cr.Evaluate(e, Activation(e, "")); err == nil {
		t.Fatal("expected an error for a map-valued property")
	}
}

func TestEvaluateGroupID(t *testing.T) {
	cr := mustCompile(t, analyticsv1alpha1.TrackingRuleSpec{
		Match:   analyticsv1alpha1.Match{Resource: domainResource, Verbs: []string{"create"}},
		GroupID: `object.metadata.name`,
	})
	e := domainEvent()
	res, err := cr.Evaluate(e, Activation(e, ""))
	if err != nil {
		t.Fatal(err)
	}
	if res.GroupID != "example-com" {
		t.Fatalf("GroupID = %q, want the expression's value", res.GroupID)
	}

	cr = mustCompile(t, analyticsv1alpha1.TrackingRuleSpec{
		Match: analyticsv1alpha1.Match{Resource: domainResource, Verbs: []string{"create"}},
	})
	if res, err = cr.Evaluate(e, Activation(e, "")); err != nil || res.GroupID != "" {
		t.Fatalf("GroupID = %q, %v, want empty without an expression", res.GroupID, err)
	}
}
