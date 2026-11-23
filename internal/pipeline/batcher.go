// Package pipeline consumes audit events, turns them into RudderStack calls
// and delivers them.
package pipeline

import (
	"context"
	"encoding/json"
	"sync"
	"time"

	"github.com/go-logr/logr"
	"github.com/nats-io/nats.go/jetstream"

	"go.miloapis.net/rudderstack-provider/internal/metrics"
	"go.miloapis.net/rudderstack-provider/internal/rudderstack"
)

const (
	// nakBaseDelay is how long a message waits before its first redelivery
	// after a failed send. An immediate nak would hot-loop while RudderStack
	// is down.
	nakBaseDelay = 10 * time.Second

	// nakMaxDelay caps the backoff. Each failed delivery doubles the delay
	// up to this, so a long outage redelivers each message a few hundred
	// times rather than every 10 seconds, and sending resumes within this
	// long of RudderStack recovering. It must stay far below the
	// AUDIT_EVENTS retention, which is what bounds an outage without loss.
	nakMaxDelay = 5 * time.Minute

	// batchOverheadBytes covers the {"batch":[...]} envelope and the commas
	// between calls when sizing a request body.
	batchOverheadBytes = 16
)

// BatchConfig controls when the batcher sends.
type BatchConfig struct {
	// MaxCalls is the most calls in one request.
	MaxCalls int
	// MaxBytes is the largest request body. It is capped at RudderStack's
	// limit.
	MaxBytes int
	// FlushInterval is the longest a call waits before it is sent.
	FlushInterval time.Duration
	// SendTimeout bounds one request to RudderStack.
	SendTimeout time.Duration
}

// tracked is a JetStream message whose calls are in flight. It is acked when
// its last call is delivered, or nacked if any of its calls failed
// temporarily.
type tracked struct {
	msg       jetstream.Msg
	remaining int
	failed    bool
}

type item struct {
	call  rudderstack.Message
	size  int
	owner *tracked
}

// Batcher buffers calls and sends them in batches, acknowledging each
// JetStream message only after RudderStack accepted every call it produced.
//
// Memory is bounded by the JetStream consumer's maxAckPending: unacked
// messages are never more than that, and each holds a few small calls.
type Batcher struct {
	sender rudderstack.Sender
	cfg    BatchConfig
	log    logr.Logger

	mu      sync.Mutex
	pending []item
	bytes   int

	full chan struct{}
}

// NewBatcher returns a batcher that delivers through sender.
func NewBatcher(sender rudderstack.Sender, cfg BatchConfig, log logr.Logger) *Batcher {
	if cfg.MaxBytes <= 0 || cfg.MaxBytes > rudderstack.MaxBatchBytes {
		cfg.MaxBytes = rudderstack.MaxBatchBytes
	}
	return &Batcher{sender: sender, cfg: cfg, log: log, full: make(chan struct{}, 1)}
}

// Submit queues the calls produced by msg. Calls too large for RudderStack
// are dropped; if none remain the message is acked immediately.
func (b *Batcher) Submit(msg jetstream.Msg, calls []rudderstack.Message) {
	owner := &tracked{msg: msg}
	items := make([]item, 0, len(calls))
	for _, c := range calls {
		data, err := json.Marshal(c)
		if err != nil || len(data) > rudderstack.MaxMessageBytes {
			b.log.Error(err, "Dropping RudderStack call that cannot be sent", "event", c.Event, "messageId", c.MessageID, "bytes", len(data))
			metrics.Calls.WithLabelValues("dropped").Inc()
			continue
		}
		items = append(items, item{call: c, size: len(data) + 1, owner: owner})
	}
	if len(items) == 0 {
		ack(msg)
		return
	}
	owner.remaining = len(items)

	b.mu.Lock()
	b.pending = append(b.pending, items...)
	for _, it := range items {
		b.bytes += it.size
	}
	metrics.PendingCalls.Set(float64(len(b.pending)))
	ready := len(b.pending) >= b.cfg.MaxCalls || b.bytes+batchOverheadBytes >= b.cfg.MaxBytes
	b.mu.Unlock()

	if ready {
		select {
		case b.full <- struct{}{}:
		default:
		}
	}
}

// Start sends batches until ctx ends, then sends whatever is still pending.
// It implements manager.Runnable.
func (b *Batcher) Start(ctx context.Context) error {
	ticker := time.NewTicker(b.cfg.FlushInterval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			// Deliver what is buffered so it is acked before shutdown rather
			// than redelivered to another replica after ackWait.
			b.flush(context.Background(), false)
			return nil
		case <-b.full:
			b.flush(ctx, true)
		case <-ticker.C:
			b.flush(ctx, false)
		}
	}
}

// NeedLeaderElection reports that every replica sends its own batches.
func (b *Batcher) NeedLeaderElection() bool { return false }

// flush sends pending calls in batches. With onlyFull it stops once what is
// left would not fill a batch, leaving it for the next tick.
func (b *Batcher) flush(ctx context.Context, onlyFull bool) {
	for {
		batch := b.take(onlyFull)
		if len(batch) == 0 {
			return
		}
		b.send(ctx, batch)
	}
}

// take removes the next batch from the buffer, within the call and byte
// limits.
func (b *Batcher) take(onlyFull bool) []item {
	b.mu.Lock()
	defer b.mu.Unlock()
	if len(b.pending) == 0 {
		return nil
	}
	if onlyFull && len(b.pending) < b.cfg.MaxCalls && b.bytes+batchOverheadBytes < b.cfg.MaxBytes {
		return nil
	}

	n, size := 0, batchOverheadBytes
	for n < len(b.pending) && n < b.cfg.MaxCalls && (n == 0 || size+b.pending[n].size <= b.cfg.MaxBytes) {
		size += b.pending[n].size
		n++
	}
	batch := b.pending[:n:n]
	b.pending = b.pending[n:]
	b.bytes -= size - batchOverheadBytes
	metrics.PendingCalls.Set(float64(len(b.pending)))
	return batch
}

// send delivers batch. A permanent rejection is split in half until the calls
// RudderStack refuses are isolated and dropped, so one bad call does not hold
// back the rest.
func (b *Batcher) send(ctx context.Context, batch []item) {
	calls := make([]rudderstack.Message, len(batch))
	for i, it := range batch {
		calls[i] = it.call
	}

	sendCtx, cancel := context.WithTimeout(ctx, b.cfg.SendTimeout)
	start := time.Now()
	err := b.sender.Send(sendCtx, calls)
	cancel()
	metrics.FlushDuration.Observe(time.Since(start).Seconds())

	switch {
	case err == nil:
		metrics.Flushes.WithLabelValues("success").Inc()
		metrics.Calls.WithLabelValues("sent").Add(float64(len(batch)))
		b.settle(batch, false)

	case rudderstack.IsPermanent(err) && len(batch) > 1:
		metrics.Flushes.WithLabelValues("rejected").Inc()
		b.log.Info("RudderStack rejected a batch, splitting it to isolate the bad calls", "calls", len(batch), "error", err.Error())
		// Splitting sends more requests; keep the messages from being
		// redelivered meanwhile.
		for _, it := range batch {
			_ = it.owner.msg.InProgress()
		}
		mid := len(batch) / 2
		b.send(ctx, batch[:mid])
		b.send(ctx, batch[mid:])

	case rudderstack.IsPermanent(err):
		metrics.Flushes.WithLabelValues("rejected").Inc()
		metrics.Calls.WithLabelValues("dropped").Inc()
		b.log.Error(err, "RudderStack rejected a call, dropping it", "event", batch[0].call.Event, "messageId", batch[0].call.MessageID)
		b.settle(batch, false)

	default:
		metrics.Flushes.WithLabelValues("retry").Inc()
		b.log.Error(err, "Failed to send batch to RudderStack, its messages will be redelivered", "calls", len(batch))
		b.settle(batch, true)
	}
}

// settle records the outcome of batch's calls and acks or nacks each message
// whose last call is now settled.
func (b *Batcher) settle(batch []item, failed bool) {
	b.mu.Lock()
	var done []*tracked
	for _, it := range batch {
		o := it.owner
		o.failed = o.failed || failed
		o.remaining--
		if o.remaining == 0 {
			done = append(done, o)
		}
	}
	b.mu.Unlock()

	for _, o := range done {
		if o.failed {
			if err := o.msg.NakWithDelay(nakDelay(o.msg)); err != nil {
				b.log.Error(err, "Failed to nak message")
			}
			metrics.MessagesNacked.Inc()
			continue
		}
		ack(o.msg)
	}
}

// nakDelay is the redelivery delay for msg: nakBaseDelay on its first
// delivery, doubling with each delivery after that, up to nakMaxDelay.
func nakDelay(msg jetstream.Msg) time.Duration {
	attempt := uint64(1)
	if md, err := msg.Metadata(); err == nil && md.NumDelivered > 0 {
		attempt = md.NumDelivered
	}
	delay := nakBaseDelay
	for i := uint64(1); i < attempt && delay < nakMaxDelay; i++ {
		delay *= 2
	}
	return min(delay, nakMaxDelay)
}
