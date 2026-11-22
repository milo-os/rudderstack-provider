package rules

import (
	"strings"
	"testing"

	analyticsv1alpha1 "go.miloapis.net/rudderstack-provider/api/v1alpha1"
)

func TestCompile(t *testing.T) {
	env, err := NewEnv()
	if err != nil {
		t.Fatal(err)
	}

	tests := []struct {
		name        string
		conditions  []analyticsv1alpha1.Condition
		properties  []analyticsv1alpha1.Property
		userID      string
		anonymousID string
		groupID     string
		wantErr     string
	}{
		{
			name: "valid",
			conditions: []analyticsv1alpha1.Condition{
				{Name: "datumctl", Expression: `userAgent.startsWith("datumctl/")`},
				{Name: "human", Expression: `!user.username.startsWith("system:")`},
			},
			properties: []analyticsv1alpha1.Property{
				{Name: "domain", Expression: `objectRef.name`},
			},
			userID: `object.metadata.name`,
		},
		{
			name:    "groupId not a string",
			groupID: `1 + 1`,
			wantErr: "groupId: must evaluate to string",
		},
		{
			name:    "userId not a string",
			userID:  `verb == "create"`,
			wantErr: "userId: must evaluate to string",
		},
		{
			name:        "anonymousId",
			anonymousID: `objectRef.namespace`,
		},
		{
			name:        "userId and anonymousId",
			userID:      `object.metadata.name`,
			anonymousID: `objectRef.namespace`,
			wantErr:     "set userId or anonymousId, not both",
		},
		{
			name:        "anonymousId not a string",
			anonymousID: `1 + 1`,
			wantErr:     "anonymousId: must evaluate to string",
		},
		{
			name:       "syntax error",
			conditions: []analyticsv1alpha1.Condition{{Name: "broken", Expression: `verb ==`}},
			wantErr:    `condition "broken"`,
		},
		{
			name:       "condition not boolean",
			conditions: []analyticsv1alpha1.Condition{{Name: "string", Expression: `verb`}},
			wantErr:    "must evaluate to bool",
		},
		{
			name:       "unknown variable",
			properties: []analyticsv1alpha1.Property{{Name: "p", Expression: `request.name`}},
			wantErr:    `property "p"`,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			rule := &analyticsv1alpha1.TrackingRule{}
			rule.Spec.Match.Conditions = tt.conditions
			rule.Spec.Properties = tt.properties
			rule.Spec.UserID = tt.userID
			rule.Spec.AnonymousID = tt.anonymousID
			rule.Spec.GroupID = tt.groupID

			cr, err := Compile(env, rule)
			if tt.wantErr == "" {
				if err != nil {
					t.Fatalf("unexpected error: %v", err)
				}
				if len(cr.Conditions) != len(tt.conditions) || len(cr.Properties) != len(tt.properties) {
					t.Fatalf("compiled %d conditions and %d properties", len(cr.Conditions), len(cr.Properties))
				}
				if (cr.UserID != nil) != (tt.userID != "") {
					t.Fatalf("userId compiled = %v, want %v", cr.UserID != nil, tt.userID != "")
				}
				return
			}
			if err == nil || !strings.Contains(err.Error(), tt.wantErr) {
				t.Fatalf("error = %v, want it to contain %q", err, tt.wantErr)
			}
		})
	}
}
