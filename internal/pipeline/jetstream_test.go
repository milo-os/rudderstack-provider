package pipeline

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"time"

	"github.com/go-logr/logr"
	"github.com/nats-io/nats-server/v2/server"
	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"

	analyticsv1alpha1 "go.miloapis.net/rudderstack-provider/api/v1alpha1"
	"go.miloapis.net/rudderstack-provider/internal/rudderstack"
)

// startJetStream runs an in-process NATS server with JetStream and returns a
// durable consumer on an AUDIT_EVENTS stream, as deployed.
func startJetStream(t *testing.T, ackWait time.Duration) (jetstream.JetStream, jetstream.Consumer) {
	t.Helper()
	srv, err := server.NewServer(&server.Options{Host: "127.0.0.1", Port: -1, JetStream: true, StoreDir: t.TempDir(), NoLog: true, NoSigs: true})
	if err != nil {
		t.Fatal(err)
	}
	go srv.Start()
	if !srv.ReadyForConnections(5 * time.Second) {
		t.Fatal("NATS server did not start")
	}
	t.Cleanup(srv.Shutdown)

	nc, err := nats.Connect(srv.ClientURL())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(nc.Close)
	js, err := jetstream.New(nc)
	if err != nil {
		t.Fatal(err)
	}

	ctx := context.Background()
	if _, err := js.CreateStream(ctx, jetstream.StreamConfig{Name: "AUDIT_EVENTS", Subjects: []string{"audit.k8s.>"}}); err != nil {
		t.Fatal(err)
	}
	cons, err := js.CreateOrUpdateConsumer(ctx, "AUDIT_EVENTS", jetstream.ConsumerConfig{
		Durable:   "rudderstack-provider",
		AckPolicy: jetstream.AckExplicitPolicy,
		AckWait:   ackWait,
	})
	if err != nil {
		t.Fatal(err)
	}
	return js, cons
}

// runPipeline starts a batcher and consumer for the domain-added rule and
// returns once the consumer is attached.
func runPipeline(t *testing.T, cons jetstream.Consumer, sender rudderstack.Sender) {
	t.Helper()
	p, _ := newTestProcessor(t, map[string]analyticsv1alpha1.TrackingRuleSpec{"domain-added": domainAdded})
	batcher := NewBatcher(sender, BatchConfig{MaxCalls: 10, FlushInterval: 20 * time.Millisecond, SendTimeout: time.Second}, logr.Discard())
	p.Batcher = batcher
	consumer := &Consumer{JetStream: cons, Processor: p, Synced: func() bool { return true }, Log: logr.Discard()}

	ctx, cancel := context.WithCancel(context.Background())
	errs := make(chan error, 2)
	go func() { errs <- batcher.Start(ctx) }()
	go func() { errs <- consumer.Start(ctx) }()
	t.Cleanup(func() {
		cancel()
		for range 2 {
			if err := <-errs; err != nil {
				t.Error(err)
			}
		}
	})

	waitFor(t, "consumer to attach", func() bool { return consumer.ReadyCheck(nil) == nil })
}

func waitFor(t *testing.T, what string, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for !cond() {
		if time.Now().After(deadline) {
			t.Fatalf("timed out waiting for %s", what)
		}
		time.Sleep(10 * time.Millisecond)
	}
}

func pendingAcks(t *testing.T, cons jetstream.Consumer) (pending int, unacked int) {
	t.Helper()
	info, err := cons.Info(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	return int(info.NumPending), info.NumAckPending
}

func publish(t *testing.T, js jetstream.JetStream, overrides map[string]any) {
	t.Helper()
	if _, err := js.Publish(context.Background(), "audit.k8s.test", auditMsg(t, overrides).data); err != nil {
		t.Fatal(err)
	}
}

func TestJetStreamDeliversAndAcks(t *testing.T) {
	js, cons := startJetStream(t, 30*time.Second)
	sender := &fakeSender{}
	runPipeline(t, cons, sender)

	publish(t, js, nil)                              // matches
	publish(t, js, map[string]any{"verb": "delete"}) // does not match
	publish(t, js, map[string]any{"auditID": "a2"})  // matches

	waitFor(t, "every message to be acked", func() bool {
		pending, unacked := pendingAcks(t, cons)
		return pending == 0 && unacked == 0
	})
	if got := sender.sent(); len(got) != 2 || got[0] != "a1/domain-added" || got[1] != "a2/domain-added" {
		t.Fatalf("sent %v, want both matching events", got)
	}
}

func TestJetStreamRedeliversAfterFailedSend(t *testing.T) {
	js, cons := startJetStream(t, 30*time.Second)

	// RudderStack is down for the first attempt only. The first nak delay is
	// 10s, so the redelivered attempt arrives after it.
	var attempts atomic.Int32
	sender := &fakeSender{fail: func([]rudderstack.Message) error {
		if attempts.Add(1) == 1 {
			return errors.New("connection refused")
		}
		return nil
	}}
	runPipeline(t, cons, sender)
	publish(t, js, nil)

	deadline := time.Now().Add(nakBaseDelay + 5*time.Second)
	for {
		pending, unacked := pendingAcks(t, cons)
		if pending == 0 && unacked == 0 && len(sender.sent()) == 1 {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("not redelivered: pending=%d unacked=%d attempts=%d sent=%v", pending, unacked, attempts.Load(), sender.sent())
		}
		time.Sleep(50 * time.Millisecond)
	}
	if attempts.Load() != 2 {
		t.Fatalf("attempts = %d, want one failure and one redelivery", attempts.Load())
	}
}
