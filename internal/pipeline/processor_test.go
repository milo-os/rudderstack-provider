package pipeline

import (
	"encoding/json"
	"maps"
	"testing"
	"time"

	"github.com/go-logr/logr"
	"github.com/nats-io/nats.go/jetstream"

	analyticsv1alpha1 "go.miloapis.net/rudderstack-provider/api/v1alpha1"
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
	return &Processor{Rules: store, Batcher: sub, Log: logr.Discard()}, sub
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
	limited := domainAdded
	limited.Limit = analyticsv1alpha1.LimitOncePerDay
	group := domainAdded
	group.Call = analyticsv1alpha1.CallGroup

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
		{"limit not implemented", map[string]analyticsv1alpha1.TrackingRuleSpec{"r": limited},
			func(t *testing.T) *fakeMsg { return auditMsg(t, nil) }},
		{"group not implemented", map[string]analyticsv1alpha1.TrackingRuleSpec{"r": group},
			func(t *testing.T) *fakeMsg { return auditMsg(t, nil) }},
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
