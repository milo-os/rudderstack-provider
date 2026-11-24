package pipeline

import (
	"strings"
	"testing"

	dto "github.com/prometheus/client_model/go"

	"go.miloapis.net/rudderstack-provider/internal/metrics"
)

func excludedEvents(t *testing.T) float64 {
	t.Helper()
	var m dto.Metric
	if err := metrics.Events.WithLabelValues("excluded").Write(&m); err != nil {
		t.Fatal(err)
	}
	return m.GetCounter().GetValue()
}

func TestNewExclusionsIgnoresEmptyEntries(t *testing.T) {
	// An unset env var reaches the flag as "", which splits to [""].
	x := NewExclusions(strings.Split("", ","), strings.Split(" p1 , ,p2", ","), nil)
	if x.Len() != 2 || !x.Projects["p1"] || !x.Projects["p2"] {
		t.Fatalf("exclusions = %+v, want projects p1 and p2 only", x)
	}
}

func TestExcludedAccountsAreNotSent(t *testing.T) {
	// The sample rules turn each of these requests into at least one call
	// when nothing is excluded, so an empty result means the exclusion held.
	cases := []struct {
		name     string
		exclude  Exclusions
		override map[string]any
	}{
		{"user by uid", NewExclusions([]string{"u1"}, nil, nil), nil},
		{"user by username", NewExclusions([]string{"alice@example.com"}, nil, nil), nil},
		{"project the request was made in", NewExclusions(nil, []string{"p1"}, nil),
			map[string]any{"annotations": projectScope("p1")}},
		{"organization the request was made in", NewExclusions(nil, nil, []string{"acme"}), nil},
		{"organization owning the project", NewExclusions(nil, nil, []string{"acme"}),
			map[string]any{"annotations": projectScope("p1")}},
		{"project being created", NewExclusions(nil, []string{"canary"}, nil),
			map[string]any{
				"objectRef":      map[string]any{"apiGroup": "resourcemanager.miloapis.com", "resource": "projects", "name": "canary"},
				"responseObject": map[string]any{"metadata": map[string]any{"name": "canary"}, "spec": map[string]any{"ownerRef": map[string]any{"kind": "Organization", "name": "acme"}}},
			}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			p, sub := sampleProcessor(t)
			lim := &fakeLimiter{}
			p.Limiter = lim
			msg := auditMsg(t, tc.override)

			// The same request without exclusions makes calls.
			control, controlSub := sampleProcessor(t)
			control.Handle(auditMsg(t, tc.override))
			if len(controlSub.calls) == 0 {
				t.Fatal("control request made no calls; the case does not test anything")
			}

			p.Exclude = tc.exclude
			before := excludedEvents(t)
			p.Handle(msg)
			if len(sub.calls) != 0 {
				t.Fatalf("events = %v, want none", events(sub))
			}
			if !msg.acked {
				t.Fatal("excluded message was not acked")
			}
			if got := excludedEvents(t) - before; got != 1 {
				t.Fatalf("events_total{result=excluded} rose by %v, want 1", got)
			}
			if len(lim.seen) != 0 {
				t.Fatalf("limiter keys taken = %v, want none for an excluded event", lim.seen)
			}
		})
	}
}

func TestExclusionsLeaveOtherAccountsAlone(t *testing.T) {
	p, sub := sampleProcessor(t)
	p.Exclude = NewExclusions([]string{"someone-else"}, []string{"p9"}, []string{"globex"})
	p.Handle(auditMsg(t, map[string]any{"annotations": projectScope("p1")}))

	c := callFor(t, sub, "Domain Added")
	if c.UserID != "u1" || c.Context["groupId"] != "acme" {
		t.Fatalf("call = %+v", c)
	}
}
