package pipeline

import (
	"errors"
	"io"
	"maps"
	"os"
	"path/filepath"
	"slices"
	"testing"

	"github.com/go-logr/logr"
	utilyaml "k8s.io/apimachinery/pkg/util/yaml"

	dto "github.com/prometheus/client_model/go"

	analyticsv1alpha1 "go.miloapis.net/rudderstack-provider/api/v1alpha1"
	"go.miloapis.net/rudderstack-provider/internal/metrics"
	"go.miloapis.net/rudderstack-provider/internal/rudderstack"
	"go.miloapis.net/rudderstack-provider/internal/rules"
)

// sampleProcessor loads the rules in config/rules.
func sampleProcessor(t *testing.T) (*Processor, *recordingSubmitter) {
	t.Helper()
	env, err := rules.NewEnv()
	if err != nil {
		t.Fatal(err)
	}
	store := rules.NewStore(env)

	files, err := filepath.Glob(filepath.Join("..", "..", "config", "rules", "*.yaml"))
	if err != nil {
		t.Fatal(err)
	}
	for _, file := range files {
		if filepath.Base(file) == "kustomization.yaml" {
			continue
		}
		f, err := os.Open(file)
		if err != nil {
			t.Fatal(err)
		}
		dec := utilyaml.NewYAMLOrJSONDecoder(f, 4096)
		for {
			rule := &analyticsv1alpha1.TrackingRule{}
			if err := dec.Decode(rule); errors.Is(err, io.EOF) {
				break
			} else if err != nil {
				t.Fatalf("%s: %v", file, err)
			}
			if err := store.Upsert(rule); err != nil {
				t.Fatalf("%s: %s: %v", file, rule.Name, err)
			}
		}
		_ = f.Close()
	}
	sub := &recordingSubmitter{}
	return &Processor{Rules: store, Batcher: sub, Orgs: testOrgs(), Limiter: &fakeLimiter{}, Log: logr.Discard()}, sub
}

func projectScope(name string) map[string]any {
	return map[string]any{
		"platform.miloapis.com/scope.type": "project",
		"platform.miloapis.com/scope.name": name,
	}
}

// callFor returns the one call for event. The test events also match the
// activity signal rules, which are checked separately.
func callFor(t *testing.T, sub *recordingSubmitter, event string) rudderstack.Message {
	t.Helper()
	var found []rudderstack.Message
	for _, c := range sub.calls {
		if c.Event == event {
			found = append(found, c)
		}
	}
	if len(found) != 1 {
		t.Fatalf("calls = %+v, want exactly one %q", sub.calls, event)
	}
	return found[0]
}

func events(sub *recordingSubmitter) []string {
	out := make([]string, 0, len(sub.calls))
	for _, c := range sub.calls {
		out = append(out, c.Event)
	}
	slices.Sort(out)
	return out
}

func TestSampleDomainAddedByHuman(t *testing.T) {
	p, sub := sampleProcessor(t)
	p.Handle(auditMsg(t, map[string]any{"annotations": projectScope("p1")}))

	c := callFor(t, sub, "Domain Added")
	if c.UserID != "u1" || c.Context["groupId"] != "acme" {
		t.Fatalf("call = %+v", c)
	}
	want := map[string]any{"name": "example-com", "namespace": "", "project": "p1", "changeSource": "human"}
	for k, v := range want {
		if c.Properties[k] != v {
			t.Fatalf("properties[%q] = %v, want %v (all: %v)", k, c.Properties[k], v, c.Properties)
		}
	}
}

func TestSampleDomainRemovedByGarbageCollector(t *testing.T) {
	// Deleting a project deletes its domains as the garbage collector. The
	// removal must still be sent so downstream counts go down.
	p, sub := sampleProcessor(t)
	p.Handle(auditMsg(t, map[string]any{
		"verb":           "delete",
		"user":           map[string]any{"username": "system:serviceaccount:kube-system:generic-garbage-collector", "uid": "gc"},
		"responseStatus": map[string]any{"code": 200},
		"responseObject": map[string]any{"kind": "Status", "status": "Success"},
		"annotations":    projectScope("p1"),
	}))

	c := callFor(t, sub, "Domain Removed")
	if c.Properties["changeSource"] != "system" || c.Properties["name"] != "example-com" || c.Properties["project"] != "p1" {
		t.Fatalf("call = %+v", c)
	}
	// The garbage collector is not a user, whatever its user agent.
	if got := events(sub); !slices.Equal(got, []string{"Domain Removed"}) {
		t.Fatalf("events = %v, want only Domain Removed", got)
	}
}

func TestSampleProjectResourcesAddedAndRemoved(t *testing.T) {
	// These resources live in projects, so their organization is the
	// project's owner, as for domains.
	cases := []struct {
		group, resource, verb, event string
	}{
		{"dns.networking.miloapis.com", "dnszones", "create", "DNS Zone Added"},
		{"dns.networking.miloapis.com", "dnszones", "delete", "DNS Zone Removed"},
		{"networking.datumapis.com", "connectors", "create", "Tunnel Added"},
		{"networking.datumapis.com", "connectors", "delete", "Tunnel Removed"},
		{"networking.datumapis.com", "networkloadbalancers", "create", "Network Load Balancer Added"},
		{"networking.datumapis.com", "networkloadbalancers", "delete", "Network Load Balancer Removed"},
		{"dns.networking.miloapis.com", "dnsrecordsets", "create", "DNS Record Set Added"},
		{"dns.networking.miloapis.com", "dnsrecordsets", "delete", "DNS Record Set Removed"},
		{"iam.miloapis.com", "serviceaccounts", "create", "Service Account Created"},
		{"iam.miloapis.com", "serviceaccounts", "delete", "Service Account Deleted"},
		{"identity.miloapis.com", "serviceaccountkeys", "create", "Service Account Key Created"},
		{"identity.miloapis.com", "serviceaccountkeys", "delete", "Service Account Key Deleted"},
	}
	for _, tc := range cases {
		t.Run(tc.event, func(t *testing.T) {
			p, sub := sampleProcessor(t)
			p.Handle(auditMsg(t, map[string]any{
				"verb":        tc.verb,
				"objectRef":   map[string]any{"apiGroup": tc.group, "resource": tc.resource, "namespace": "default", "name": "r1"},
				"annotations": projectScope("p2"),
			}))

			c := callFor(t, sub, tc.event)
			want := map[string]any{"name": "r1", "namespace": "default", "project": "p2", "changeSource": "human"}
			if c.UserID != "u1" || c.Context["groupId"] != "initech" || !maps.Equal(c.Properties, want) {
				t.Fatalf("call = %+v", c)
			}
		})
	}
}

func TestSampleProjectOutsideProjectScope(t *testing.T) {
	// Projects are created in an organization scope, so project is empty.
	p, sub := sampleProcessor(t)
	p.Handle(auditMsg(t, map[string]any{
		"objectRef": map[string]any{"apiGroup": "resourcemanager.miloapis.com", "resource": "projects", "name": "p1"},
	}))

	c := callFor(t, sub, "Project Created")
	if c.Properties["project"] != "" || c.Context["groupId"] != "acme" {
		t.Fatalf("call = %+v", c)
	}
}

func TestSampleProjectDeletedInPlatformScope(t *testing.T) {
	// The console deletes projects through the platform API, so the request
	// has no organization scope. The organization comes from the project.
	p, sub := sampleProcessor(t)
	p.Handle(auditMsg(t, map[string]any{
		"verb":           "delete",
		"userAgent":      "Mozilla/5.0",
		"objectRef":      map[string]any{"apiGroup": "resourcemanager.miloapis.com", "resource": "projects", "name": "p1"},
		"responseStatus": map[string]any{"code": 200},
		"responseObject": map[string]any{
			"metadata": map[string]any{"name": "p1", "deletionTimestamp": "2026-11-19T10:00:00Z"},
			"spec":     map[string]any{"ownerRef": map[string]any{"kind": "Organization", "name": "globex"}},
		},
		"annotations": map[string]any{"platform.miloapis.com/scope.type": "global"},
	}))

	c := callFor(t, sub, "Project Deleted")
	if c.Context["groupId"] != "globex" || c.Properties["name"] != "p1" {
		t.Fatalf("call = %+v", c)
	}
}

func TestSampleProjectDeletedWithoutObject(t *testing.T) {
	// A delete answered with a Status carries no project, so the call falls
	// back to the request's organization scope instead of failing.
	p, sub := sampleProcessor(t)
	p.Handle(auditMsg(t, map[string]any{
		"verb":           "delete",
		"objectRef":      map[string]any{"apiGroup": "resourcemanager.miloapis.com", "resource": "projects", "name": "p1"},
		"responseStatus": map[string]any{"code": 200},
		"responseObject": map[string]any{"kind": "Status", "status": "Success"},
	}))

	c := callFor(t, sub, "Project Deleted")
	if c.Context["groupId"] != "acme" {
		t.Fatalf("call = %+v", c)
	}
}

// stripeEvent is the audit event for stripe-provider recording a core Event
// on a PaymentMethod. stripe-provider is a system account acting outside any
// organization scope; the Event carries its organization as an annotation.
func stripeEvent(t *testing.T, auditID, reason string) *fakeMsg {
	t.Helper()
	return auditMsg(t, map[string]any{
		"auditID":   auditID,
		"user":      map[string]any{"username": "system:serviceaccount:stripe-provider", "uid": "stripe-sa"},
		"userAgent": "stripe-provider/v0.1.0",
		"objectRef": map[string]any{"apiGroup": "", "resource": "events", "namespace": "organization-globex", "name": "pm1.1234"},
		"responseObject": map[string]any{
			"metadata": map[string]any{
				"name": "pm1.1234",
				"annotations": map[string]any{
					"platform.miloapis.com/scope.type": "Organization",
					"platform.miloapis.com/scope.name": "globex",
				},
			},
			"reason":         reason,
			"involvedObject": map[string]any{"kind": "PaymentMethod", "namespace": "organization-globex", "name": "pm1"},
		},
		"annotations": map[string]any{"platform.miloapis.com/scope.type": "global"},
	})
}

func ruleErrors(t *testing.T, rule string) float64 {
	t.Helper()
	var m dto.Metric
	if err := metrics.RuleErrors.WithLabelValues(rule).Write(&m); err != nil {
		t.Fatal(err)
	}
	return m.GetCounter().GetValue()
}

func TestSamplePaymentMethodStarted(t *testing.T) {
	p, sub := sampleProcessor(t)
	p.Handle(auditMsg(t, map[string]any{
		"userAgent": "Mozilla/5.0",
		"objectRef": map[string]any{"apiGroup": "billing.miloapis.com", "resource": "paymentmethods", "namespace": "organization-acme", "name": "pm1"},
	}))

	c := callFor(t, sub, "Payment Method Started")
	if c.UserID != "u1" || c.AnonymousID != "" || c.Context["groupId"] != "acme" || c.Properties["changeSource"] != "human" {
		t.Fatalf("call = %+v", c)
	}
}

func TestSamplePaymentMethodAttached(t *testing.T) {
	// Stripe confirmed the card. No person made the request, so the call is
	// about the organization and never names stripe-provider's account.
	p, sub := sampleProcessor(t)
	p.Handle(stripeEvent(t, "a1", "PaymentMethodAttached"))

	c := callFor(t, sub, "Payment Method Attached")
	if c.UserID != "" || c.AnonymousID != "globex" || c.Context["groupId"] != "globex" || c.Properties["name"] != "pm1" {
		t.Fatalf("call = %+v", c)
	}
}

func TestSampleOtherEventsAreIgnored(t *testing.T) {
	// Every core Event create reaches the attached rule; others must neither
	// send nor count as rule errors.
	p, sub := sampleProcessor(t)
	before := ruleErrors(t, "payment-method-attached")
	p.Handle(stripeEvent(t, "a1", "PaymentMethodFailed"))
	p.Handle(auditMsg(t, map[string]any{
		"auditID":        "a2",
		"objectRef":      map[string]any{"apiGroup": "", "resource": "events", "namespace": "default", "name": "e1"},
		"responseObject": map[string]any{"metadata": map[string]any{"name": "e1"}},
	}))

	for _, c := range sub.calls {
		if c.Event == "Payment Method Attached" {
			t.Fatalf("unexpected call %+v", c)
		}
	}
	if after := ruleErrors(t, "payment-method-attached"); after != before {
		t.Fatalf("rule errors went from %v to %v", before, after)
	}
}

func TestSampleActivitySignalsAreLimited(t *testing.T) {
	// A human's datumctl read sends datumctl Used and Active once a day.
	p, sub := sampleProcessor(t)
	p.Handle(auditMsg(t, map[string]any{"verb": "list", "objectRef": nil}))
	p.Handle(auditMsg(t, map[string]any{"auditID": "a2", "verb": "list", "objectRef": nil}))

	if got := events(sub); !slices.Equal(got, []string{"Active", "datumctl Used"}) {
		t.Fatalf("events = %v, want Active and datumctl Used once each", got)
	}
}

func TestSampleConsoleRequestIsActiveOnly(t *testing.T) {
	p, sub := sampleProcessor(t)
	p.Handle(auditMsg(t, map[string]any{"verb": "list", "objectRef": nil, "userAgent": "Mozilla/5.0"}))

	if got := events(sub); !slices.Equal(got, []string{"Active"}) {
		t.Fatalf("events = %v, want only Active", got)
	}
}

func TestSampleOrganizationTraits(t *testing.T) {
	p, sub := sampleProcessor(t)
	p.Handle(auditMsg(t, map[string]any{
		"verb":      "create",
		"userAgent": "Mozilla/5.0",
		"objectRef": map[string]any{"apiGroup": "resourcemanager.miloapis.com", "resource": "organizations"},
		"responseObject": map[string]any{"metadata": map[string]any{
			"name":        "globex",
			"annotations": map[string]any{"kubernetes.io/display-name": "Globex Corp"},
		}},
		"annotations": nil,
	}))

	c := callFor(t, sub, "")
	if c.Type != rudderstack.TypeGroup || c.GroupID != "globex" || c.Traits["displayName"] != "Globex Corp" || c.Traits["name"] != "globex" {
		t.Fatalf("call = %+v", c)
	}
}

func TestSampleMachineAccountIsNotAUser(t *testing.T) {
	// A customer's CI pipeline running datumctl as a machine account.
	p, sub := sampleProcessor(t)
	p.ServiceAccountEmailSuffix = "iam.datumapis.com"
	ci := map[string]any{"username": "ci-p8emii@project-zdhcz.iam.datumapis.com", "uid": "sa-1"}

	p.Handle(auditMsg(t, map[string]any{"verb": "list", "objectRef": nil, "user": ci}))
	if got := events(sub); len(got) != 0 {
		t.Fatalf("events = %v, want no activity signals for a machine account", got)
	}

	p.Handle(auditMsg(t, map[string]any{"auditID": "a2", "user": ci, "annotations": projectScope("p1")}))
	if c := callFor(t, sub, "Domain Added"); c.Properties["changeSource"] != "automation" {
		t.Fatalf("changeSource = %v, want automation", c.Properties["changeSource"])
	}
}

func TestSampleDNSRecordSetsByPlatformAreSkipped(t *testing.T) {
	// Controllers write most record sets: zone defaults and records that
	// external-dns publishes for proxies. Neither side of those is sent, so
	// Added and Removed stay paired. Machine accounts are customers and still
	// count.
	for _, tc := range []struct {
		username, want string
	}{
		{"system:control@networking.datumapis.com", ""},
		{"system:control@dns.networking.miloapis.com", ""},
		{"system:control@platform.miloapis.com", ""},
		{"ci-p8emii@project-zdhcz.iam.datumapis.com", "automation"},
	} {
		for _, v := range []struct{ verb, event string }{
			{"create", "DNS Record Set Added"},
			{"delete", "DNS Record Set Removed"},
		} {
			t.Run(tc.username+"/"+v.verb, func(t *testing.T) {
				p, sub := sampleProcessor(t)
				p.ServiceAccountEmailSuffix = "iam.datumapis.com"
				p.Handle(auditMsg(t, map[string]any{
					"verb":        v.verb,
					"user":        map[string]any{"username": tc.username, "uid": "x1"},
					"objectRef":   map[string]any{"apiGroup": "dns.networking.miloapis.com", "resource": "dnsrecordsets", "namespace": "default", "name": "r1"},
					"annotations": projectScope("p2"),
				}))

				if tc.want == "" {
					if slices.Contains(events(sub), v.event) {
						t.Fatalf("events = %v, want no %q for a platform actor", events(sub), v.event)
					}
					return
				}
				if c := callFor(t, sub, v.event); c.Properties["changeSource"] != tc.want {
					t.Fatalf("changeSource = %v, want %v", c.Properties["changeSource"], tc.want)
				}
			})
		}
	}
}
