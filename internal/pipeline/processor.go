package pipeline

import (
	"strings"

	"github.com/go-logr/logr"
	"github.com/nats-io/nats.go/jetstream"

	analyticsv1alpha1 "go.miloapis.net/rudderstack-provider/api/v1alpha1"
	"go.miloapis.net/rudderstack-provider/internal/audit"
	"go.miloapis.net/rudderstack-provider/internal/metrics"
	"go.miloapis.net/rudderstack-provider/internal/rudderstack"
	"go.miloapis.net/rudderstack-provider/internal/rules"
	"go.miloapis.net/rudderstack-provider/internal/version"
)

// libraryName identifies the provider in each call's context.library.
const libraryName = "rudderstack-provider"

// RuleLister returns the rules to evaluate.
type RuleLister interface {
	List() []*rules.CompiledRule
}

// Submitter queues the calls an event produced.
type Submitter interface {
	Submit(msg jetstream.Msg, calls []rudderstack.Message)
}

// Processor turns audit events into RudderStack calls.
type Processor struct {
	Rules   RuleLister
	Batcher Submitter
	Log     logr.Logger
}

// Handle processes one JetStream message. Messages that produce no calls are
// acked immediately; the rest are acked by the batcher once delivered.
func (p *Processor) Handle(msg jetstream.Msg) {
	ev, err := audit.Parse(msg.Data())
	if err != nil {
		// Redelivering an event that cannot be parsed would never succeed.
		p.Log.Error(err, "Skipping audit event that cannot be parsed")
		metrics.Events.WithLabelValues("invalid").Inc()
		ack(msg)
		return
	}
	if !ev.Trackable() {
		metrics.Events.WithLabelValues("untracked").Inc()
		ack(msg)
		return
	}

	calls := p.calls(ev)
	if len(calls) == 0 {
		metrics.Events.WithLabelValues("unmatched").Inc()
		ack(msg)
		return
	}
	metrics.Events.WithLabelValues("matched").Inc()
	p.Batcher.Submit(msg, calls)
}

// calls evaluates every rule against ev and returns the calls to send.
func (p *Processor) calls(ev *audit.Event) []rudderstack.Message {
	var (
		calls []rudderstack.Message
		vars  map[string]any
	)
	for _, cr := range p.Rules.List() {
		spec := &cr.Rule.Spec
		if reason := unsupported(spec); reason != "" {
			metrics.RulesSkipped.WithLabelValues(cr.Rule.Name, reason).Inc()
			continue
		}
		if vars == nil {
			vars = rules.Activation(ev)
		}

		res, err := cr.Evaluate(ev, vars)
		if err != nil {
			// Usually an expression reading a field this event lacks.
			metrics.RuleErrors.WithLabelValues(cr.Rule.Name).Inc()
			p.Log.V(1).Info("TrackingRule failed to evaluate", "rule", cr.Rule.Name, "auditID", ev.AuditID, "error", err.Error())
			continue
		}
		if res == nil {
			continue
		}
		metrics.RuleMatches.WithLabelValues(cr.Rule.Name).Inc()
		calls = append(calls, newMessage(cr.Rule, ev, res))
	}
	return calls
}

// unsupported returns why a rule cannot be evaluated by this version, or ""
// when it can. Limits and group calls arrive with organization resolution.
func unsupported(spec *analyticsv1alpha1.TrackingRuleSpec) string {
	if spec.Limit != "" {
		return "limit_" + strings.ToLower(string(spec.Limit))
	}
	if spec.Call == analyticsv1alpha1.CallGroup {
		return "call_group"
	}
	return ""
}

// newMessage builds the call for a matched rule. The messageId combines the
// audit ID and rule name so a redelivered event produces the same ID and
// RudderStack drops the duplicate.
func newMessage(rule *analyticsv1alpha1.TrackingRule, ev *audit.Event, res *rules.Result) rudderstack.Message {
	m := rudderstack.Message{
		UserID:            res.UserID,
		MessageID:         ev.AuditID + "/" + rule.Name,
		OriginalTimestamp: ev.Timestamp(),
		Context: map[string]any{
			"library": map[string]any{"name": libraryName, "version": version.Version},
		},
	}
	if tenant := ev.Tenant(); strings.EqualFold(tenant.Type, "organization") && tenant.Name != "" {
		m.Context["groupId"] = tenant.Name
	}

	switch rule.Spec.Call {
	case analyticsv1alpha1.CallIdentify:
		m.Type = rudderstack.TypeIdentify
		m.Traits = res.Properties
	default:
		m.Type = rudderstack.TypeTrack
		m.Event = rule.Spec.Event
		m.Properties = res.Properties
	}
	return m
}

func ack(msg jetstream.Msg) {
	// A failed ack means the message is redelivered and processed again;
	// the deterministic messageId makes that safe.
	_ = msg.Ack()
}
