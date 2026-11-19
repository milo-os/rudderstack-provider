// Package metrics defines the provider's Prometheus metrics. They are
// registered with controller-runtime's registry and served on the manager's
// metrics endpoint.
package metrics

import (
	"github.com/prometheus/client_golang/prometheus"
	"sigs.k8s.io/controller-runtime/pkg/metrics"
)

const (
	namespace = "rudderstack_provider"

	labelResult = "result"
	labelRule   = "rule"
)

var (
	// Events counts consumed audit events by outcome: invalid, untracked,
	// unmatched or matched.
	Events = prometheus.NewCounterVec(prometheus.CounterOpts{
		Namespace: namespace,
		Name:      "events_total",
		Help:      "Audit events consumed, by outcome.",
	}, []string{labelResult})

	// RuleMatches counts events each rule turned into a call.
	RuleMatches = prometheus.NewCounterVec(prometheus.CounterOpts{
		Namespace: namespace,
		Name:      "rule_matches_total",
		Help:      "Audit events that matched a rule and produced a call.",
	}, []string{labelRule})

	// RuleErrors counts events a rule failed to evaluate.
	RuleErrors = prometheus.NewCounterVec(prometheus.CounterOpts{
		Namespace: namespace,
		Name:      "rule_errors_total",
		Help:      "Audit events a rule failed to evaluate.",
	}, []string{labelRule})

	// LimitSuppressed counts calls a once-per-day limit held back.
	LimitSuppressed = prometheus.NewCounterVec(prometheus.CounterOpts{
		Namespace: namespace,
		Name:      "limit_suppressed_total",
		Help:      "Calls not sent because the rule already sent for the subject today.",
	}, []string{labelRule})

	// LimitErrors counts limiter failures. The call is sent anyway.
	LimitErrors = prometheus.NewCounterVec(prometheus.CounterOpts{
		Namespace: namespace,
		Name:      "limit_errors_total",
		Help:      "Limiter failures; the call is sent anyway and deduplicated by messageId.",
	}, []string{labelRule})

	// OrgsUnresolved counts project-scoped events whose project is not known
	// yet, sent without a groupId.
	OrgsUnresolved = prometheus.NewCounter(prometheus.CounterOpts{
		Namespace: namespace,
		Name:      "orgs_unresolved_total",
		Help:      "Project-scoped events sent without a groupId because the project's organization is not known yet.",
	})

	// Calls counts RudderStack calls by outcome: sent or dropped.
	Calls = prometheus.NewCounterVec(prometheus.CounterOpts{
		Namespace: namespace,
		Name:      "calls_total",
		Help:      "RudderStack calls, by outcome.",
	}, []string{labelResult})

	// Flushes counts batch sends by outcome: success, retry or rejected.
	Flushes = prometheus.NewCounterVec(prometheus.CounterOpts{
		Namespace: namespace,
		Name:      "flushes_total",
		Help:      "Batch sends to RudderStack, by outcome.",
	}, []string{labelResult})

	// FlushDuration is how long each batch send took.
	FlushDuration = prometheus.NewHistogram(prometheus.HistogramOpts{
		Namespace: namespace,
		Name:      "flush_duration_seconds",
		Help:      "Duration of batch sends to RudderStack.",
		Buckets:   prometheus.DefBuckets,
	})

	// MessagesNacked counts JetStream messages returned for redelivery.
	MessagesNacked = prometheus.NewCounter(prometheus.CounterOpts{
		Namespace: namespace,
		Name:      "messages_nacked_total",
		Help:      "JetStream messages returned for redelivery after a failed send.",
	})

	// PendingCalls is the number of calls waiting to be sent.
	PendingCalls = prometheus.NewGauge(prometheus.GaugeOpts{
		Namespace: namespace,
		Name:      "pending_calls",
		Help:      "Calls buffered and waiting to be sent.",
	})
)

func init() {
	metrics.Registry.MustRegister(Events, RuleMatches, RuleErrors, LimitSuppressed, LimitErrors, OrgsUnresolved, Calls, Flushes, FlushDuration, MessagesNacked, PendingCalls)
}
