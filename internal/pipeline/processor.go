package pipeline

import (
	"context"
	"strings"
	"time"

	"github.com/go-logr/logr"
	"github.com/nats-io/nats.go/jetstream"

	analyticsv1alpha1 "go.miloapis.net/rudderstack-provider/api/v1alpha1"
	"go.miloapis.net/rudderstack-provider/internal/audit"
	"go.miloapis.net/rudderstack-provider/internal/limit"
	"go.miloapis.net/rudderstack-provider/internal/metrics"
	"go.miloapis.net/rudderstack-provider/internal/rudderstack"
	"go.miloapis.net/rudderstack-provider/internal/rules"
	"go.miloapis.net/rudderstack-provider/internal/version"
)

const (
	// libraryName identifies the provider in each call's context.library.
	libraryName = "rudderstack-provider"

	// limiterTimeout bounds one once-per-day check.
	limiterTimeout = 5 * time.Second
)

// OrgResolver returns the organization a request was made in.
type OrgResolver interface {
	Organization(t audit.Tenant) string
}

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
	Orgs    OrgResolver
	Limiter limit.Limiter
	Log     logr.Logger
	// ServiceAccountEmailSuffix identifies customer machine accounts, so
	// rules can tell them from people through actor.kind.
	ServiceAccountEmailSuffix string
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

	calls := p.calls(msg, ev)
	if len(calls) == 0 {
		metrics.Events.WithLabelValues("unmatched").Inc()
		ack(msg)
		return
	}
	metrics.Events.WithLabelValues("matched").Inc()
	p.Batcher.Submit(msg, calls)
}

// calls evaluates every rule against ev and returns the calls to send.
func (p *Processor) calls(msg jetstream.Msg, ev *audit.Event) []rudderstack.Message {
	var (
		calls []rudderstack.Message
		vars  map[string]any
		org   *string
	)
	// A redelivered message is resent in full: its once-per-day keys were
	// already taken by its first delivery, whose send failed.
	redelivered := isRedelivered(msg)

	for _, cr := range p.Rules.List() {
		rule := cr.Rule
		if vars == nil {
			vars = rules.Activation(ev, p.ServiceAccountEmailSuffix)
		}

		res, err := cr.Evaluate(ev, vars)
		if err != nil {
			// Usually an expression reading a field this event lacks.
			p.ruleError(rule, ev, err.Error())
			continue
		}
		if res == nil {
			continue
		}

		groupID := res.GroupID
		if groupID == "" {
			if org == nil {
				org = new(p.organization(ev))
			}
			groupID = *org
		}
		if groupID == "" && (rule.Spec.Call == analyticsv1alpha1.CallGroup || rule.Spec.Subject == analyticsv1alpha1.SubjectOrganization) {
			p.ruleError(rule, ev, "no organization for the request; set groupId")
			continue
		}

		m := newMessage(rule, ev, res, groupID)
		if rule.Spec.Limit == analyticsv1alpha1.LimitOncePerDay && !p.allowToday(rule, ev, &m, groupID, redelivered) {
			continue
		}
		metrics.RuleMatches.WithLabelValues(rule.Name).Inc()
		calls = append(calls, m)
	}
	return calls
}

// allowToday applies a once-per-day limit. It gives m a messageId shared by
// every call the rule makes for the subject that day, so a duplicate that gets
// past the limiter is dropped by RudderStack.
func (p *Processor) allowToday(rule *analyticsv1alpha1.TrackingRule, ev *audit.Event, m *rudderstack.Message, groupID string, redelivered bool) bool {
	subject := m.UserID
	if rule.Spec.Subject == analyticsv1alpha1.SubjectOrganization {
		subject = groupID
	}
	day := ev.Timestamp()
	if day.IsZero() {
		day = time.Now()
	}
	m.MessageID = rule.Name + "/" + subject + "/" + day.UTC().Format(time.DateOnly)

	if redelivered {
		return true
	}
	ctx, cancel := context.WithTimeout(context.Background(), limiterTimeout)
	defer cancel()
	allowed, err := p.Limiter.Allow(ctx, limit.DailyKey(rule.Name, subject, day))
	if err != nil {
		// Sending is the safe failure: the messageId deduplicates it.
		metrics.LimitErrors.WithLabelValues(rule.Name).Inc()
		p.Log.Error(err, "Once-per-day limit unavailable, sending anyway", "rule", rule.Name)
		return true
	}
	if !allowed {
		metrics.LimitSuppressed.WithLabelValues(rule.Name).Inc()
	}
	return allowed
}

// organization resolves the organization the request was made in.
func (p *Processor) organization(ev *audit.Event) string {
	tenant := ev.Tenant()
	org := p.Orgs.Organization(tenant)
	if org == "" && strings.EqualFold(tenant.Type, "project") {
		metrics.OrgsUnresolved.Inc()
	}
	return org
}

func (p *Processor) ruleError(rule *analyticsv1alpha1.TrackingRule, ev *audit.Event, msg string) {
	metrics.RuleErrors.WithLabelValues(rule.Name).Inc()
	p.Log.V(1).Info("TrackingRule failed to evaluate", "rule", rule.Name, "auditID", ev.AuditID, "error", msg)
}

// newMessage builds the call for a matched rule. The messageId combines the
// audit ID and rule name so a redelivered event produces the same ID and
// RudderStack drops the duplicate.
func newMessage(rule *analyticsv1alpha1.TrackingRule, ev *audit.Event, res *rules.Result, groupID string) rudderstack.Message {
	m := rudderstack.Message{
		UserID:            res.UserID,
		MessageID:         ev.AuditID + "/" + rule.Name,
		OriginalTimestamp: ev.Timestamp(),
		Context: map[string]any{
			"library": map[string]any{"name": libraryName, "version": version.Version},
		},
	}

	switch rule.Spec.Call {
	case analyticsv1alpha1.CallIdentify:
		m.Type = rudderstack.TypeIdentify
		m.Traits = res.Properties
	case analyticsv1alpha1.CallGroup:
		m.Type = rudderstack.TypeGroup
		m.GroupID = groupID
		m.Traits = res.Properties
		return m
	default:
		m.Type = rudderstack.TypeTrack
		m.Event = rule.Spec.Event
		m.Properties = res.Properties
	}
	if groupID != "" {
		m.Context["groupId"] = groupID
	}
	return m
}

func isRedelivered(msg jetstream.Msg) bool {
	md, err := msg.Metadata()
	return err == nil && md.NumDelivered > 1
}

func ack(msg jetstream.Msg) {
	// A failed ack means the message is redelivered and processed again;
	// the deterministic messageId makes that safe.
	_ = msg.Ack()
}
