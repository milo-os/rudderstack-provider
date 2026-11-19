package pipeline

import (
	"encoding/json"
	"errors"
	"maps"
	"slices"
	"testing"
	"time"

	"github.com/go-logr/logr"
	"github.com/nats-io/nats.go/jetstream"

	analyticsv1alpha1 "go.miloapis.net/rudderstack-provider/api/v1alpha1"
	"go.miloapis.net/rudderstack-provider/internal/orgs"
	"go.miloapis.net/rudderstack-provider/internal/rudderstack"
	"go.miloapis.net/rudderstack-provider/internal/rules"
)

type recordingSubmitter struct {
	calls []rudderstack.Message
	msgs  []jetstream.Msg
}

func (r *recordingSubmitter) Submit(msg jetstream.Msg, calls []rudderstack.Message) {
	r.msgs = append(r.msgs, msg)
	r.calls = append(r.calls, calls...)
}

func newTestProcessor(t *testing.T, specs map[string]analyticsv1alpha1.TrackingRuleSpec) (*Processor, *recordingSubmitter) {
	t.Helper()
	env, err := rules.NewEnv()
	if err != nil {
		t.Fatal(err)
	}
	store := rules.NewStore(env)
	for name, spec := range specs {
		r := &analyticsv1alpha1.TrackingRule{Spec: spec}
		r.Name = name
		if err := store.Upsert(r); err != nil {
			t.Fatal(err)
		}
	}
	sub := &recordingSubmitter{}
	return &Processor{Rules: store, Batcher: sub, Orgs: testOrgs(), Limiter: &fakeLimiter{}, Log: logr.Discard()}, sub
}

// testOrgs knows project p1, owned by acme.
func testOrgs() *orgs.Resolver {
	r := orgs.NewResolver()
	r.Set("p1", "acme")
	return r
}

func auditMsg(t *testing.T, overrides map[string]any) *fakeMsg {
	t.Helper()
	ev := map[string]any{
		"auditID":        "a1",
		"stage":          "ResponseComplete",
		"verb":           "create",
		"user":           map[string]any{"username": "alice@example.com", "uid": "u1"},
		"userAgent":      "datumctl/v0.12.0",
		"objectRef":      map[string]any{"apiGroup": "networking.datumapis.com", "resource": "domains", "name": "example-com"},
		"responseStatus": map[string]any{"code": 201},
		"responseObject": map[string]any{
			"metadata": map[string]any{"name": "example-com"},
			"spec":     map[string]any{"domainName": "example.com"},
		},
		"requestReceivedTimestamp": "2026-11-19T10:00:00Z",
		"annotations": map[string]any{
			"platform.miloapis.com/scope.type": "Organization",
			"platform.miloapis.com/scope.name": "acme",
		},
	}
	maps.Copy(ev, overrides)
	data, err := json.Marshal(ev)
	if err != nil {
		t.Fatal(err)
	}
	return &fakeMsg{data: data}
}

var domainAdded = analyticsv1alpha1.TrackingRuleSpec{
	Event: "Domain Added",
	Call:  analyticsv1alpha1.CallTrack,
	Match: analyticsv1alpha1.Match{
		Resource: &analyticsv1alpha1.ResourceSelector{Group: "networking.datumapis.com", Resource: "domains"},
		Verbs:    []string{"create"},
	},
	Properties: []analyticsv1alpha1.Property{{Name: "domain", Expression: "object.spec.domainName"}},
}

func TestProcessorTrack(t *testing.T) {
	p, sub := newTestProcessor(t, map[string]analyticsv1alpha1.TrackingRuleSpec{"domain-added": domainAdded})
	msg := auditMsg(t, nil)
	p.Handle(msg)

	if acked, _ := msg.state(); acked {
		t.Fatal("matched message was acked before delivery")
	}
	if len(sub.calls) != 1 {
		t.Fatalf("calls = %v, want 1", sub.calls)
	}
	c := sub.calls[0]
	if c.Type != rudderstack.TypeTrack || c.Event != "Domain Added" || c.UserID != "u1" || c.MessageID != "a1/domain-added" {
		t.Fatalf("call = %+v", c)
	}
	if !c.OriginalTimestamp.Equal(time.Date(2026, 11, 19, 10, 0, 0, 0, time.UTC)) {
		t.Fatalf("originalTimestamp = %v", c.OriginalTimestamp)
	}
	if c.Properties["domain"] != "example.com" || c.Traits != nil {
		t.Fatalf("properties = %v, traits = %v", c.Properties, c.Traits)
	}
	if c.Context["groupId"] != "acme" {
		t.Fatalf("context = %v, want groupId acme", c.Context)
	}
}

func TestProcessorIdentify(t *testing.T) {
	spec := domainAdded
	spec.Call = analyticsv1alpha1.CallIdentify
	p, sub := newTestProcessor(t, map[string]analyticsv1alpha1.TrackingRuleSpec{"identify": spec})
	p.Handle(auditMsg(t, nil))

	if len(sub.calls) != 1 {
		t.Fatalf("calls = %v, want 1", sub.calls)
	}
	c := sub.calls[0]
	if c.Type != rudderstack.TypeIdentify || c.Event != "" || c.Traits["domain"] != "example.com" || c.Properties != nil {
		t.Fatalf("call = %+v", c)
	}
}

func TestProcessorAcksWithoutCalls(t *testing.T) {
	groupWithoutOrg := domainAdded
	groupWithoutOrg.Call = analyticsv1alpha1.CallGroup

	tests := []struct {
		name  string
		specs map[string]analyticsv1alpha1.TrackingRuleSpec
		msg   func(*testing.T) *fakeMsg
	}{
		{"invalid", map[string]analyticsv1alpha1.TrackingRuleSpec{"r": domainAdded},
			func(*testing.T) *fakeMsg { return &fakeMsg{data: []byte("{not json")} }},
		{"failed request", map[string]analyticsv1alpha1.TrackingRuleSpec{"r": domainAdded},
			func(t *testing.T) *fakeMsg {
				return auditMsg(t, map[string]any{"responseStatus": map[string]any{"code": 409}})
			}},
		{"unmatched", map[string]analyticsv1alpha1.TrackingRuleSpec{"r": domainAdded},
			func(t *testing.T) *fakeMsg { return auditMsg(t, map[string]any{"verb": "delete"}) }},
		{"group call without an organization", map[string]analyticsv1alpha1.TrackingRuleSpec{"r": groupWithoutOrg},
			func(t *testing.T) *fakeMsg { return auditMsg(t, map[string]any{"annotations": nil}) }},
		{"evaluation error", map[string]analyticsv1alpha1.TrackingRuleSpec{"r": domainAdded},
			func(t *testing.T) *fakeMsg { return auditMsg(t, map[string]any{"responseObject": map[string]any{}}) }},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			p, sub := newTestProcessor(t, tt.specs)
			msg := tt.msg(t)
			p.Handle(msg)
			if acked, _ := msg.state(); !acked {
				t.Fatal("message was not acked")
			}
			if len(sub.calls) != 0 {
				t.Fatalf("calls = %v, want none", sub.calls)
			}
		})
	}
}

func TestProcessorOneMessageManyRules(t *testing.T) {
	second := domainAdded
	second.Event = "Domain Added Again"
	p, sub := newTestProcessor(t, map[string]analyticsv1alpha1.TrackingRuleSpec{"a": domainAdded, "b": second})
	p.Handle(auditMsg(t, nil))

	if len(sub.msgs) != 1 || len(sub.calls) != 2 {
		t.Fatalf("submitted %d messages with %d calls, want 1 with 2", len(sub.msgs), len(sub.calls))
	}
	if sub.calls[0].MessageID != "a1/a" || sub.calls[1].MessageID != "a1/b" {
		t.Fatalf("messageIds = %q, %q", sub.calls[0].MessageID, sub.calls[1].MessageID)
	}
}

func TestProcessorResolvesProjectOrganization(t *testing.T) {
	p, sub := newTestProcessor(t, map[string]analyticsv1alpha1.TrackingRuleSpec{"domain-added": domainAdded})
	p.Handle(auditMsg(t, map[string]any{"annotations": map[string]any{
		"platform.miloapis.com/scope.type": "project",
		"platform.miloapis.com/scope.name": "p1",
	}}))
	p.Handle(auditMsg(t, map[string]any{"auditID": "a2", "annotations": map[string]any{
		"platform.miloapis.com/scope.type": "project",
		"platform.miloapis.com/scope.name": "unknown",
	}}))

	if len(sub.calls) != 2 {
		t.Fatalf("calls = %+v, want 2", sub.calls)
	}
	if sub.calls[0].Context["groupId"] != "acme" {
		t.Fatalf("context = %v, want the project's organization", sub.calls[0].Context)
	}
	// An unknown project is still sent, without a groupId.
	if _, ok := sub.calls[1].Context["groupId"]; ok {
		t.Fatalf("context = %v, want no groupId for an unknown project", sub.calls[1].Context)
	}
}

func TestProcessorGroup(t *testing.T) {
	spec := analyticsv1alpha1.TrackingRuleSpec{
		Event:   "Organization",
		Call:    analyticsv1alpha1.CallGroup,
		Subject: analyticsv1alpha1.SubjectOrganization,
		GroupID: "object.metadata.name",
		Match: analyticsv1alpha1.Match{
			Resource: &analyticsv1alpha1.ResourceSelector{Group: "resourcemanager.miloapis.com", Resource: "organizations"},
			Verbs:    []string{"create"},
		},
		Properties: []analyticsv1alpha1.Property{{Name: "name", Expression: "object.metadata.name"}},
	}
	p, sub := newTestProcessor(t, map[string]analyticsv1alpha1.TrackingRuleSpec{"org": spec})
	p.Handle(auditMsg(t, map[string]any{
		"objectRef":      map[string]any{"apiGroup": "resourcemanager.miloapis.com", "resource": "organizations"},
		"responseObject": map[string]any{"metadata": map[string]any{"name": "globex"}},
		"annotations":    nil,
	}))

	if len(sub.calls) != 1 {
		t.Fatalf("calls = %+v, want 1", sub.calls)
	}
	c := sub.calls[0]
	if c.Type != rudderstack.TypeGroup || c.GroupID != "globex" || c.UserID != "u1" || c.Traits["name"] != "globex" || c.Event != "" {
		t.Fatalf("call = %+v", c)
	}
	if _, ok := c.Context["groupId"]; ok {
		t.Fatalf("group call carries context.groupId: %v", c.Context)
	}
}

var datumctlUsed = analyticsv1alpha1.TrackingRuleSpec{
	Event: "datumctl Used",
	Limit: analyticsv1alpha1.LimitOncePerDay,
	Match: analyticsv1alpha1.Match{
		Verbs:      []string{"create", "list"},
		Conditions: []analyticsv1alpha1.Condition{{Name: "datumctl", Expression: `userAgent.startsWith("datumctl/")`}},
	},
}

func TestProcessorOncePerDay(t *testing.T) {
	p, sub := newTestProcessor(t, map[string]analyticsv1alpha1.TrackingRuleSpec{"datumctl-used": datumctlUsed})

	first := auditMsg(t, nil)
	p.Handle(first)
	second := auditMsg(t, map[string]any{"auditID": "a2", "verb": "list"})
	p.Handle(second)
	otherUser := auditMsg(t, map[string]any{"auditID": "a3", "user": map[string]any{"uid": "u2"}})
	p.Handle(otherUser)
	nextDay := auditMsg(t, map[string]any{"auditID": "a4", "requestReceivedTimestamp": "2026-11-20T00:00:01Z"})
	p.Handle(nextDay)

	ids := make([]string, 0, len(sub.calls))
	for _, c := range sub.calls {
		ids = append(ids, c.MessageID)
	}
	want := []string{"datumctl-used/u1/2026-11-19", "datumctl-used/u2/2026-11-19", "datumctl-used/u1/2026-11-20"}
	if !slices.Equal(ids, want) {
		t.Fatalf("messageIds = %v, want %v", ids, want)
	}
	if acked, _ := second.state(); !acked {
		t.Fatal("suppressed message was not acked")
	}
}

func TestProcessorOncePerDayRedelivery(t *testing.T) {
	// The first delivery took today's key but its send failed. The
	// redelivery must still send, with the same messageId.
	p, sub := newTestProcessor(t, map[string]analyticsv1alpha1.TrackingRuleSpec{"datumctl-used": datumctlUsed})
	p.Handle(auditMsg(t, nil))
	redelivered := auditMsg(t, nil)
	redelivered.delivered = 2
	p.Handle(redelivered)

	if len(sub.calls) != 2 || sub.calls[0].MessageID != sub.calls[1].MessageID {
		t.Fatalf("calls = %+v, want the call resent with the same messageId", sub.calls)
	}
}

func TestProcessorOncePerDayFailsOpen(t *testing.T) {
	p, sub := newTestProcessor(t, map[string]analyticsv1alpha1.TrackingRuleSpec{"datumctl-used": datumctlUsed})
	p.Limiter = &fakeLimiter{err: errors.New("nats: timeout")}
	p.Handle(auditMsg(t, nil))
	p.Handle(auditMsg(t, map[string]any{"auditID": "a2"}))

	if len(sub.calls) != 2 || sub.calls[0].MessageID != sub.calls[1].MessageID {
		t.Fatalf("calls = %+v, want both sent with one messageId for RudderStack to deduplicate", sub.calls)
	}
}

func TestProcessorOncePerDayPerOrganization(t *testing.T) {
	spec := datumctlUsed
	spec.Subject = analyticsv1alpha1.SubjectOrganization
	p, sub := newTestProcessor(t, map[string]analyticsv1alpha1.TrackingRuleSpec{"org-active": spec})

	// Two users in the same organization count once.
	p.Handle(auditMsg(t, nil))
	p.Handle(auditMsg(t, map[string]any{"auditID": "a2", "user": map[string]any{"uid": "u2"}}))
	// A request with no organization cannot be counted per organization.
	p.Handle(auditMsg(t, map[string]any{"auditID": "a3", "annotations": nil}))

	if len(sub.calls) != 1 || sub.calls[0].MessageID != "org-active/acme/2026-11-19" {
		t.Fatalf("calls = %+v, want one call for acme", sub.calls)
	}
}
