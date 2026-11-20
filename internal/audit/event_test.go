package audit

import (
	"testing"
	"time"
)

func TestParse(t *testing.T) {
	e, err := Parse([]byte(`{
		"auditID": "a1",
		"stage": "ResponseComplete",
		"verb": "create",
		"user": {"username": "alice@example.com", "uid": "u1"},
		"userAgent": "datumctl/v0.12.0",
		"objectRef": {"apiGroup": "networking.datumapis.com", "resource": "domains", "name": "example-com"},
		"responseStatus": {"code": 201},
		"requestReceivedTimestamp": "2026-11-19T10:00:00.000000Z"
	}`))
	if err != nil {
		t.Fatal(err)
	}
	if e.AuditID != "a1" || e.User.UID != "u1" || e.ObjectRef.Resource != "domains" || e.ResponseStatus.Code != 201 {
		t.Fatalf("unexpected event: %+v", e)
	}
	if want := time.Date(2026, 11, 19, 10, 0, 0, 0, time.UTC); !e.Timestamp().Equal(want) {
		t.Fatalf("Timestamp() = %v, want %v", e.Timestamp(), want)
	}
}

func TestTrackable(t *testing.T) {
	tests := []struct {
		name  string
		event Event
		want  bool
	}{
		{"complete", Event{Stage: StageResponseComplete, ResponseStatus: &ResponseStatus{Code: 201}}, true},
		{"no stage", Event{ResponseStatus: &ResponseStatus{Code: 200}}, true},
		{"request received", Event{Stage: "RequestReceived"}, false},
		{"failed", Event{Stage: StageResponseComplete, ResponseStatus: &ResponseStatus{Code: 409}}, false},
		{"dry run", Event{Stage: StageResponseComplete, RequestURI: "/apis/x/v1/domains?dryRun=All"}, false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := tt.event.Trackable(); got != tt.want {
				t.Fatalf("Trackable() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestTenant(t *testing.T) {
	tests := []struct {
		name  string
		event Event
		want  Tenant
	}{
		{
			name:  "annotations",
			event: Event{Annotations: map[string]string{ScopeTypeAnnotation: "project", ScopeNameAnnotation: "p1"}},
			want:  Tenant{Type: "project", Name: "p1"},
		},
		{
			name:  "user extra",
			event: Event{User: User{Extra: map[string][]string{ParentTypeExtra: {"Organization"}, ParentNameExtra: {"o1"}}}},
			want:  Tenant{Type: "Organization", Name: "o1"},
		},
		{
			name: "platform",
			want: Tenant{Type: "platform"},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := tt.event.Tenant(); got != tt.want {
				t.Fatalf("Tenant() = %+v, want %+v", got, tt.want)
			}
		})
	}
}

func TestObject(t *testing.T) {
	resp := map[string]any{"kind": "Domain"}
	req := map[string]any{"kind": "DeleteOptions"}
	if got := (&Event{ResponseObject: resp, RequestObject: req}).Object(); got["kind"] != "Domain" {
		t.Fatalf("Object() = %v, want the response object", got)
	}
	if got := (&Event{RequestObject: req}).Object(); got["kind"] != "DeleteOptions" {
		t.Fatalf("Object() = %v, want the request object", got)
	}
	if got := (&Event{}).Object(); got == nil {
		t.Fatal("Object() = nil, want an empty map")
	}
}

func TestActorKind(t *testing.T) {
	const suffix = "iam.datumapis.com"
	tests := []struct {
		username string
		suffix   string
		want     string
	}{
		{"alice@example.com", suffix, ActorUser},
		{"system:serviceaccount:kube-system:generic-garbage-collector", suffix, ActorSystem},
		{"system:control@analytics.miloapis.com", suffix, ActorSystem},
		{"my-service-account-yz2j00@personal-project-efb26a2b.iam.datumapis.com", suffix, ActorServiceAccount},
		{"my-staging-service-acco-p8emii@project-zdhcz.iam.datumapis.com", suffix, ActorServiceAccount},
		{"ci-p8emii@project-zdhcz.iam.datumapis.com", "." + suffix, ActorServiceAccount},
		{"bob@iam.datumapis.com", suffix, ActorUser},
		{"alice@datumapis.com", suffix, ActorUser},
		{"ci-p8emii@project-zdhcz.iam.datumapis.com", "", ActorUser},
	}
	for _, tt := range tests {
		e := &Event{User: User{Username: tt.username}}
		if got := e.ActorKind(tt.suffix); got != tt.want {
			t.Errorf("ActorKind(%q) with suffix %q = %q, want %q", tt.username, tt.suffix, got, tt.want)
		}
	}
}
